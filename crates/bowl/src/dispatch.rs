//! Worker dispatch batches execution, never invocation identity or commits.
use crate::{system::SystemRun, world::SystemInvocation};
use futures::{
    StreamExt,
    future::BoxFuture,
    stream::{BoxStream, FuturesUnordered},
};

pub(crate) type Row = (SystemInvocation, SystemRun);
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

    pub(crate) fn push_batch(&mut self, runs: Vec<Run>) {
        match self {
            Self::Cooperative(pending) => pending.extend(runs),
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

pub(crate) fn batch(runs: Vec<Run>) -> Runs {
    #[cfg(feature = "parallel")]
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        if runs.len() == 1 {
            let run = runs.into_iter().next().expect("one run");
            return futures::stream::once(SpawnedRun {
                handle: runtime.spawn(run),
            })
            .boxed();
        }
        if !runs.is_empty() {
            // One sender and at most one result per row: bounded storage even if
            // every row finishes before the evaluator polls this stream again.
            let (sender, receiver) = futures::channel::mpsc::channel(runs.len());
            let handle = runtime.spawn(async move {
                let mut sender = sender;
                let mut pending: FuturesUnordered<_> = runs.into_iter().collect();
                while let Some(row) = pending.next().await {
                    if let Err(error) = sender.try_send(row) {
                        if error.is_disconnected() {
                            return;
                        }
                        panic!("completion capacity covers every admitted row");
                    }
                }
            });
            return Box::pin(SpawnedBatch { receiver, handle });
        }
    }
    // Preserve executor-agnostic cooperative operation without feature changes.
    runs.into_iter().collect::<FuturesUnordered<_>>().boxed()
}

#[cfg(feature = "parallel")]
struct SpawnedRun {
    handle: tokio::task::JoinHandle<Row>,
}

#[cfg(feature = "parallel")]
impl std::future::Future for SpawnedRun {
    type Output = Row;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Row> {
        std::pin::Pin::new(&mut self.handle)
            .poll(cx)
            .map(|result| result.expect("system task panicked or was aborted"))
    }
}

#[cfg(feature = "parallel")]
impl Drop for SpawnedRun {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(feature = "parallel")]
struct SpawnedBatch {
    receiver: futures::channel::mpsc::Receiver<Row>,
    handle: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "parallel")]
impl futures::Stream for SpawnedBatch {
    type Item = Row;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Row>> {
        use std::{future::Future, task::Poll};
        match self.receiver.poll_next_unpin(cx) {
            Poll::Ready(None) => std::pin::Pin::new(&mut self.handle).poll(cx).map(|result| {
                result.expect("system batch panicked or was aborted");
                None
            }),
            ready => ready,
        }
    }
}

#[cfg(feature = "parallel")]
impl Drop for SpawnedBatch {
    fn drop(&mut self) {
        // Read and write batches never mix. Dropping the reader stream cancels
        // pending reads; writer streams are drained by the preemption path.
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Entity, world::SystemId};
    use futures::{FutureExt, channel::oneshot};

    fn row(id: u64) -> Row {
        (
            SystemInvocation {
                system: SystemId(0),
                keys: vec![Entity::from_raw(id)],
            },
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
                .map(|(owner, _)| owner.keys[0].raw())
                .collect();
            ids.sort_unstable();
            assert_eq!(ids, [1, 2]);
        });
    }

    #[test]
    #[cfg(feature = "parallel")]
    fn ready_row_streams_before_an_awaiting_sibling_finishes() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let (release, waiting) = oneshot::channel::<()>();
                let mut values = batch(vec![
                    async move {
                        waiting.await.unwrap();
                        row(1)
                    }
                    .boxed(),
                    async { row(2) }.boxed(),
                ]);
                assert_eq!(values.next().await.unwrap().0.keys[0].raw(), 2);
                release.send(()).unwrap();
                assert_eq!(values.next().await.unwrap().0.keys[0].raw(), 1);
                assert!(values.next().await.is_none());
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
