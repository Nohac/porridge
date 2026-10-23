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
        dispatch_rows(self.0, admission,
            |owner, run, stats, queued| async move { (owner, timed(run, stats, queued).await) },
            |queue, batch| queue.push_batch(batch));
    }
}

struct CallPlan<I>(I);

pub(crate) fn prepare_calls<I, C>(calls: I) -> Box<dyn RunPlan>
where
    I: ExactSizeIterator<Item = PlannedSystemRun<C>> + Send + 'static,
    C: FnOnce() -> SystemRun + Send + 'static,
{
    Box::new(CallPlan(calls))
}

impl<I, C> RunPlan for CallPlan<I>
where
    I: ExactSizeIterator<Item = PlannedSystemRun<C>> + Send + 'static,
    C: FnOnce() -> SystemRun + Send + 'static,
{
    fn dispatch(self: Box<Self>, admission: &mut Admission<'_>) {
        dispatch_rows(self.0, admission,
            |owner, run, stats, queued| move || (owner, timed_call(run, stats, queued)),
            |queue, batch| queue.push_calls(batch));
    }
}

fn dispatch_rows<I, R, Q>(
    runs: I, admission: &mut Admission<'_>,
    mut wrap: impl FnMut(SystemInvocation, R, Arc<SystemStats>, Option<std::time::Instant>) -> Q,
    submit: impl Fn(&mut Queue, Vec<Q>),
) where I: ExactSizeIterator<Item = PlannedSystemRun<R>> {
    use crate::metrics::{self, Counter::*};
    let _timer = metrics::Timer::new(AdmissionNs);
    metrics::add(AdmissionCandidates, runs.len() as u64);
    let (mut duplicates, mut conflicts, mut admitted, mut batches) = (0, 0, 0, 0);
    let size = super::grain(runs.len(), admission.maximum);
    let mut readers = Vec::new();
    let mut writers = Vec::new();

    for planned in runs {
        if !admission.running.insert(planned.owner.clone()) {
            duplicates += 1;
            continue;
        }

        if admission.reservations.conflicts(&planned.access) {
            conflicts += 1;
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

        admitted += 1;
        let queued = metrics::enabled().then(std::time::Instant::now);
        let stats = Arc::clone(&admission.system.stats);
        let run = wrap(owner, planned.run, stats, queued);
        let (batch, queue) = if writer {
            (&mut writers, &mut admission.writers)
        } else {
            (&mut readers, &mut admission.readers)
        };
        batch.push(run);
        if batch.len() == size {
            batches += 1;
            submit(queue, std::mem::take(batch));
        }
    }

    if !readers.is_empty() {
        batches += 1;
        submit(admission.readers, readers);
    }
    if !writers.is_empty() {
        batches += 1;
        submit(admission.writers, writers);
    }
    metrics::add(AlreadyRunning, duplicates);
    metrics::add(ConflictDeferrals, conflicts);
    metrics::add(AdmittedRows, admitted);
    metrics::add(WorkerBatches, batches);
}

fn timed_call<C: FnOnce() -> SystemRun>(
    run: C, stats: Arc<SystemStats>, queued: Option<std::time::Instant>,
) -> SystemRun {
    let start = std::time::Instant::now();
    if let Some(queued) = queued {
        crate::metrics::add(crate::metrics::Counter::FirstPollNs, queued.elapsed().as_nanos() as u64);
        crate::metrics::add(crate::metrics::Counter::StartedRows, 1);
    }
    let result = run();
    stats.run_nanos.fetch_add(start.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
    stats.runs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if queued.is_some() {
        crate::metrics::add(crate::metrics::Counter::SyncCalls, 1);
    }
    result
}

/// Stack pinning keeps a concrete, potentially !Unpin future without adding
/// a box. Measure poll time only, excluding time suspended between polls.
async fn timed<F: Future>(
    run: F,
    stats: Arc<SystemStats>,
    mut queued: Option<std::time::Instant>,
) -> F::Output {
    let trace = queued.is_some();
    let mut polls = 0;
    let mut run = std::pin::pin!(run);
    std::future::poll_fn(|cx| {
        let start = std::time::Instant::now();
        if let Some(queued) = queued.take() {
            crate::metrics::add(
                crate::metrics::Counter::FirstPollNs,
                queued.elapsed().as_nanos() as u64,
            );
            crate::metrics::add(crate::metrics::Counter::StartedRows, 1);
        }
        if trace {
            polls += 1;
        }
        let result = run.as_mut().poll(cx);
        stats.run_nanos.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        if result.is_ready() {
            if trace {
                crate::metrics::add(crate::metrics::Counter::RowPolls, polls);
            }
            stats
                .runs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    })
    .await
}
