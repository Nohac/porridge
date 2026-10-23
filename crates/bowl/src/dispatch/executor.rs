//! Generic worker executor shared with the isolated dispatch benchmark.
//! Keep engine planning, row identity and publication outside this module.
use futures::{
    StreamExt,
    stream::{BoxStream, FuturesUnordered},
};

/// Static submission seam: production uses direct spawning; the benchmark can
/// change only the task-erasure boundary while retaining this executor loop.
pub(crate) trait SpawnTask {
    fn spawn<F, T>(runtime: &tokio::runtime::Handle, future: F) -> tokio::task::JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static;
}

struct DirectTask;

impl SpawnTask for DirectTask {
    fn spawn<F, T>(runtime: &tokio::runtime::Handle, future: F) -> tokio::task::JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        runtime.spawn(future)
    }
}

pub(crate) fn spawn<F, T>(runtime: &tokio::runtime::Handle, runs: Vec<F>) -> BoxStream<'static, T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    spawn_with::<DirectTask, F, T>(runtime, runs)
}

pub(crate) fn spawn_with<S: SpawnTask, F, T>(
    runtime: &tokio::runtime::Handle,
    runs: Vec<F>,
) -> BoxStream<'static, T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    if runs.len() == 1 {
        let run = runs.into_iter().next().expect("one run");
        return futures::stream::once(SpawnedRun {
            handle: S::spawn(runtime, run),
        })
        .boxed();
    }
    if !runs.is_empty() {
        // One sender and at most one result per row: bounded storage even if
        // every row finishes before the evaluator polls this stream again.
        let (sender, receiver) = tokio::sync::mpsc::channel(runs.len());
        let handle = S::spawn(runtime, async move {
            // Admission fixed this batch's size; allocate its pinned task
            // storage together instead of one future-set node per row.
            let mut pending: futures_buffered::FuturesUnorderedBounded<_> =
                runs.into_iter().collect();
            while let Some(row) = pending.next().await {
                match sender.try_send(row) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        panic!("completion capacity covers every admitted row");
                    }
                }
            }
        });
        return Box::pin(SpawnedBatch { receiver, handle });
    }
    runs.into_iter().collect::<FuturesUnordered<_>>().boxed()
}

/// One runtime task for a batch of plain calls. Results stream individually;
/// there is no future set or per-row kernel future in this loop.
pub(crate) fn spawn_calls<C, T>(runtime: &tokio::runtime::Handle, calls: Vec<C>) -> BoxStream<'static, T>
where C: FnOnce() -> T + Send + 'static, T: Send + 'static,
{
    if calls.len() == 1 {
        let call = calls.into_iter().next().expect("one call");
        return futures::stream::once(SpawnedRun {
            handle: runtime.spawn(async move { call() }),
        }).boxed();
    }
    if calls.is_empty() {
        return futures::stream::empty().boxed();
    }
    let (sender, receiver) = tokio::sync::mpsc::channel(calls.len());
    let handle = runtime.spawn(async move {
        for call in calls {
            // Aborting a running sync function cannot preempt its body. Stop
            // cancelled read batches between calls; writer streams are drained.
            if sender.is_closed() { return; }
            match sender.try_send(call()) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    panic!("completion capacity covers every admitted row");
                }
            }
        }
    });
    Box::pin(SpawnedBatch { receiver, handle })
}

struct SpawnedRun<T> {
    handle: tokio::task::JoinHandle<T>,
}

impl<T> std::future::Future for SpawnedRun<T> {
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<T> {
        std::pin::Pin::new(&mut self.handle)
            .poll(cx)
            .map(|result| result.expect("system task panicked or was aborted"))
    }
}

impl<T> Drop for SpawnedRun<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct SpawnedBatch<T> {
    receiver: tokio::sync::mpsc::Receiver<T>,
    handle: tokio::task::JoinHandle<()>,
}

impl<T> futures::Stream for SpawnedBatch<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        use std::{future::Future, task::Poll};
        // The driver drains ready results before publishing/replanning. A
        // channel budget must not hide buffered completions at that boundary.
        // Only receiving bypasses the budget; worker futures and handle polls
        // keep their normal scheduling/cancellation behavior.
        let received = {
            let receive = std::future::poll_fn(|cx| self.receiver.poll_recv(cx));
            std::pin::pin!(tokio::task::unconstrained(receive)).poll(cx)
        };
        match received {
            Poll::Ready(None) => std::pin::Pin::new(&mut self.handle).poll(cx).map(|result| {
                result.expect("system batch panicked or was aborted");
                None
            }),
            ready => ready,
        }
    }
}

impl<T> Drop for SpawnedBatch<T> {
    fn drop(&mut self) {
        // Read and write batches never mix. Dropping the reader stream cancels
        // pending reads; writer streams are drained by the preemption path.
        self.handle.abort();
    }
}
