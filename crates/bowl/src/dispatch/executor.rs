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
        let (sender, receiver) = futures::channel::mpsc::channel(runs.len());
        let handle = S::spawn(runtime, async move {
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
    runs.into_iter().collect::<FuturesUnordered<_>>().boxed()
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
    receiver: futures::channel::mpsc::Receiver<T>,
    handle: tokio::task::JoinHandle<()>,
}

impl<T> futures::Stream for SpawnedBatch<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
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

impl<T> Drop for SpawnedBatch<T> {
    fn drop(&mut self) {
        // Read and write batches never mix. Dropping the reader stream cancels
        // pending reads; writer streams are drained by the preemption path.
        self.handle.abort();
    }
}
