//! Worker dispatch batches execution, never invocation identity or commits.
use crate::{access::RunId, system::SystemRun};
use futures::{
    FutureExt, StreamExt,
    future::BoxFuture,
    stream::{BoxStream, FuturesUnordered},
};

#[cfg(feature = "parallel")]
mod executor;
mod plan;
pub(crate) use plan::{Admission, RunPlan, prepare, prepare_calls};

pub(crate) type Row = (RunId, SystemRun);
pub(crate) type Run = BoxFuture<'static, Row>;
pub(crate) type Runs = BoxStream<'static, Row>;

/// The executor-agnostic path retains one flat future set. Only worker
/// dispatch needs a stream of independently completing batch streams.
pub(crate) enum Queue {
    Cooperative(FuturesUnordered<Run>),
    #[cfg(feature = "parallel")]
    Workers(futures::stream::SelectAll<Runs>),
}

impl Queue {
    pub(crate) fn new() -> Self {
        #[cfg(feature = "parallel")]
        if tokio::runtime::Handle::try_current().is_ok() {
            return Self::Workers(futures::stream::SelectAll::new());
        }

        Self::Cooperative(FuturesUnordered::new())
    }

    pub(crate) fn push_calls<C>(&mut self, calls: Vec<C>)
    where C: FnOnce() -> Row + Send + 'static,
    {
        match self {
            // Preserve the existing no-runtime fallback; direct calls target
            // worker batches in this first slice.
            Self::Cooperative(pending) => pending.extend(calls.into_iter().map(|call| async move { call() }.boxed())),
            #[cfg(feature = "parallel")]
            Self::Workers(pending) => pending.push(call_batch(calls)),
        }
    }

    pub(crate) fn push_batch<F>(&mut self, runs: Vec<F>)
    where
        F: std::future::Future<Output = Row> + Send + 'static,
    {
        match self {
            Self::Cooperative(pending) => pending.extend(runs.into_iter().map(FutureExt::boxed)),
            #[cfg(feature = "parallel")]
            Self::Workers(pending) => pending.push(batch(runs)),
        }
    }
}

impl futures::Stream for Queue {
    type Item = Row;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Row>> {
        match &mut *self {
            Self::Cooperative(pending) => pending.poll_next_unpin(cx),
            #[cfg(feature = "parallel")]
            Self::Workers(pending) => pending.poll_next_unpin(cx),
        }
    }
}

/// Keep several batches per worker; tiny plans retain row-level dispatch.
pub(crate) fn grain(ready: usize, maximum: usize) -> usize {
    #[cfg(feature = "parallel")]
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        return ready
            .div_ceil(runtime.metrics().num_workers().saturating_mul(4).max(1))
            .clamp(1, maximum);
    }
    maximum
}

pub(crate) fn batch<F>(runs: Vec<F>) -> Runs
where
    F: std::future::Future<Output = Row> + Send + 'static,
{
    #[cfg(feature = "parallel")]
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        return executor::spawn(&runtime, runs);
    }

    runs.into_iter().collect::<FuturesUnordered<_>>().boxed()
}

#[cfg(feature = "parallel")]
fn call_batch<C>(calls: Vec<C>) -> Runs
where C: FnOnce() -> Row + Send + 'static,
{
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        return executor::spawn_calls(&runtime, calls);
    }
    futures::stream::iter(calls).map(|call| call()).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{FutureExt, channel::oneshot};

    fn row(id: u64) -> Row {
        (
            RunId::test(id),
            SystemRun {
                completed: true,
                outputs: Vec::new(),
                memo_updates: Vec::new(),
                writes: Vec::new(),
            },
        )
    }

    #[test]
    fn cooperative_batches_preserve_each_invocation() {
        futures::executor::block_on(async {
            let mut queue = Queue::new();
            queue.push_batch(vec![async { row(1) }.boxed(), async { row(2) }.boxed()]);
            let values = queue.collect::<Vec<_>>().await;
            let mut ids: Vec<_> = values
                .into_iter()
                .map(|(owner, _)| u64::from(owner.slot))
                .collect();
            ids.sort_unstable();
            assert_eq!(ids, [1, 2]);
        });
    }

    async fn gated_row(id: u64, gate: Option<oneshot::Receiver<()>>) -> Row {
        if let Some(gate) = gate {
            gate.await.unwrap();
        }
        row(id)
    }

    #[test]
    #[cfg(feature = "parallel")]
    fn ready_row_streams_before_an_awaiting_sibling_finishes() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let (release, waiting) = oneshot::channel::<()>();
                // Both rows have the same concrete, !Unpin future type.
                let mut values = batch(vec![gated_row(1, Some(waiting)), gated_row(2, None)]);
                assert_eq!(values.next().await.unwrap().0.slot, 2);
                release.send(()).unwrap();
                assert_eq!(values.next().await.unwrap().0.slot, 1);
                assert!(values.next().await.is_none());
            });
    }

    #[test]
    #[cfg(feature = "parallel")]
    fn a_full_batch_completes_before_the_receiver_is_polled() {
        use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            const COUNT: usize = 130;
            let (finished, waiting) = oneshot::channel();
            let signal = Arc::new(Mutex::new(Some(finished)));
            let remaining = Arc::new(AtomicUsize::new(COUNT));
            let runs = (0..COUNT).map(|id| {
                let signal = Arc::clone(&signal);
                let remaining = Arc::clone(&remaining);
                async move {
                    if remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                        signal.lock().unwrap().take().unwrap().send(()).unwrap();
                    }
                    row(id as u64)
                }
            }).collect();
            drop(signal);
            let values = batch(runs);
            // On this single-thread runtime, the last synchronous try_send
            // follows the signal before the producer yields back to us.
            waiting.await.unwrap();
            assert_eq!(remaining.load(Ordering::SeqCst), 0);
            let mut values = values;
            let mut ids = Vec::new();
            while let Some(Some((owner, _))) = values.next().now_or_never() {
                ids.push(u64::from(owner.slot));
            }
            assert_eq!(ids.len(), COUNT, "ready completions must remain visible to the driver drain");
            ids.sort_unstable();
            assert_eq!(ids, (0..COUNT as u64).collect::<Vec<_>>());
        });
    }

    #[test]
    #[cfg(feature = "parallel")]
    fn synchronous_worker_panic_is_propagated() {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            let calls: Vec<fn() -> Row> = vec![|| panic!("sync kernel failed"), || row(2)];
            let mut values = call_batch(calls);
            assert!(std::panic::AssertUnwindSafe(values.next()).catch_unwind().await.is_err());
        });
    }

    #[test]
    #[cfg(feature = "parallel")]
    fn cancelled_sync_batch_stops_between_calls() {
        use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
        tokio::runtime::Builder::new_multi_thread().worker_threads(1).build().unwrap().block_on(async {
            let (started, entered) = oneshot::channel();
            let (release, resume) = std::sync::mpsc::channel();
            let (dropped, observed) = oneshot::channel();
            let second_ran = Arc::new(AtomicBool::new(false));
            let ran = Arc::clone(&second_ran);
            let notice = Dropped(Some(dropped));
            let calls: Vec<Box<dyn FnOnce() -> Row + Send>> = vec![
                Box::new(move || {
                    started.send(()).unwrap();
                    resume.recv_timeout(std::time::Duration::from_secs(3)).unwrap();
                    row(1)
                }),
                Box::new(move || {
                    let _notice = notice;
                    ran.store(true, Ordering::SeqCst);
                    row(2)
                }),
            ];
            let values = call_batch(calls);
            entered.await.unwrap();
            drop(values);
            release.send(()).unwrap();
            observed.await.unwrap();
            assert!(!second_ran.load(Ordering::SeqCst));
        });
    }

    struct Dropped(Option<oneshot::Sender<()>>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }

    #[test]
    #[cfg(feature = "parallel")]
    fn dropping_batch_cancels_remaining_rows() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let (started, entered) = oneshot::channel();
                let (dropped, observed) = oneshot::channel();
                let values = batch(vec![
                    async move {
                        let _guard = Dropped(Some(dropped));
                        started.send(()).unwrap();
                        futures::future::pending::<()>().await;
                        row(1)
                    }
                    .boxed(),
                    futures::future::pending::<Row>().boxed(),
                ]);
                entered.await.unwrap();
                drop(values);
                observed.await.unwrap();
            });
    }

    #[test]
    #[cfg(feature = "parallel")]
    fn worker_panic_is_not_reported_as_successful_exhaustion() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let mut values = batch(vec![
                    async {
                        panic!("kernel failed");
                        #[allow(unreachable_code)]
                        row(1)
                    }
                    .boxed(),
                    futures::future::pending::<Row>().boxed(),
                ]);
                let result = std::panic::AssertUnwindSafe(values.next())
                    .catch_unwind()
                    .await;
                assert!(result.is_err());
            });
    }
}

#[cfg(all(test, feature = "parallel"))]
mod integration_tests {
    use crate::{Bowl, Commands, Component, Entity, MutRef, Query, With};
    use futures::{
        FutureExt,
        channel::oneshot,
        future::{BoxFuture, Shared},
    };

    struct Request(Option<Shared<BoxFuture<'static, ()>>>);

    struct Answer;

    struct Trigger;

    struct Release(Option<oneshot::Sender<()>>);

    impl Component for Request {}
    impl Component for Answer {}
    impl Component for Trigger {}
    impl Component for Release {}

    async fn answer(requests: Query<(Entity, &Request)>, mut commands: Commands<(Answer,)>) {
        let (entity, request) = requests.item();
        if let Some(waiting) = &request.0 {
            waiting.clone().await;
        }

        commands.entity(entity).insert(Answer);
    }

    async fn release(
        _ready: Query<&Answer, With<Trigger>>,
        releases: Query<(Entity, MutRef<'_, Release>)>,
    ) {
        let (_, mut release) = releases.item();
        if let Some(sender) = release.0.take() {
            sender.send(()).unwrap();
        }
    }

    #[test]
    fn published_row_unblocks_awaiting_sibling_through_downstream_system() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let bowl = Bowl::builder().system(answer).system(release).build();
                bowl.set_dispatch_batch_size(16);
                assert_eq!(bowl.dispatch_batch_size(), 16);
                assert_eq!(super::grain(64, 16), 16);

                let (sender, receiver) = oneshot::channel();
                let waiting = async move { receiver.await.unwrap() }.boxed().shared();
                bowl.insert((Release(Some(sender)),)).await;
                // These first two rows share a worker batch. The first cannot
                // finish until the second publishes and a downstream system runs.
                bowl.insert((Request(Some(waiting)),)).await;
                bowl.insert((Request(None), Trigger)).await;
                for _ in 2..64 {
                    bowl.insert((Request(None),)).await;
                }

                assert_eq!(bowl.scoop::<Query<&Answer>>().await.len(), 64);
            });
    }
}
