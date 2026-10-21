//! Admission is shared by typed kernels and the erased hook fallback.
use std::{collections::HashSet, future::Future, sync::Arc};

use crate::{
    access::Reservations,
    query::AccessKind,
    system::{BoxedSystem, PlannedSystemRun, SystemRun, SystemStats},
    world::SystemInvocation,
};

use super::Queue;

pub(crate) struct Admission<'a> {
    pub(crate) system: &'a BoxedSystem,
    pub(crate) maximum: usize,
    pub(crate) running: &'a mut HashSet<SystemInvocation>,
    pub(crate) reservations: &'a mut Reservations,
    pub(crate) read_owners: &'a mut HashSet<SystemInvocation>,
    pub(crate) deferred_conflicts: &'a mut bool,
    pub(crate) readers: &'a mut Queue,
    pub(crate) writers: &'a mut Queue,
}

/// Type erasure occurs once per system plan, before its concrete row futures
/// are constructed. Dispatch consumes the plan without borrowing the registry.
pub(crate) trait RunPlan: Send {
    fn dispatch(self: Box<Self>, admission: &mut Admission<'_>);
}

struct TypedPlan<I>(I);

pub(crate) fn prepare<I, F>(runs: I) -> Box<dyn RunPlan>
where
    I: ExactSizeIterator<Item = PlannedSystemRun<F>> + Send + 'static,
    F: Future<Output = SystemRun> + Send + 'static,
{
    Box::new(TypedPlan(runs))
}

impl<I, F> RunPlan for TypedPlan<I>
where
    I: ExactSizeIterator<Item = PlannedSystemRun<F>> + Send + 'static,
    F: Future<Output = SystemRun> + Send + 'static,
{
    fn dispatch(self: Box<Self>, admission: &mut Admission<'_>) {
        let size = super::grain(self.0.len(), admission.maximum);
        let mut readers = Vec::new();
        let mut writers = Vec::new();

        for planned in self.0 {
            if !admission.running.insert(planned.owner.clone()) {
                continue;
            }

            if admission.reservations.conflicts(&planned.access) {
                *admission.deferred_conflicts = true;
                admission.system.force_replan(&planned.owner.keys);
                admission.running.remove(&planned.owner);
                continue;
            }

            let writer = planned
                .access
                .iter()
                .any(|access| access.kind == AccessKind::Write);
            let owner = planned.owner;
            admission.reservations.insert(owner.clone(), planned.access);
            if !writer {
                admission.read_owners.insert(owner.clone());
            }

            let stats = Arc::clone(&admission.system.stats);
            let run = async move { (owner, timed(planned.run, stats).await) };
            // These futures share one concrete type. No per-row future box is
            // needed on workers; FuturesUnordered pins its own typed nodes.
            let (batch, queue) = if writer {
                (&mut writers, &mut admission.writers)
            } else {
                (&mut readers, &mut admission.readers)
            };
            batch.push(run);
            if batch.len() == size {
                queue.push_batch(std::mem::take(batch));
            }
        }

        if !readers.is_empty() {
            admission.readers.push_batch(readers);
        }
        if !writers.is_empty() {
            admission.writers.push_batch(writers);
        }
    }
}

/// Stack pinning keeps a concrete, potentially !Unpin future without adding
/// a box. Measure poll time only, excluding time suspended between polls.
async fn timed<F: Future>(run: F, stats: Arc<SystemStats>) -> F::Output {
    let mut run = std::pin::pin!(run);
    std::future::poll_fn(|cx| {
        let start = std::time::Instant::now();
        let result = run.as_mut().poll(cx);
        stats.run_nanos.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        if result.is_ready() {
            stats
                .runs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    })
    .await
}
