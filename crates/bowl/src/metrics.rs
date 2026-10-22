//! Opt-in process-wide diagnostics for isolated fixed-work profiling.
//! Keep hardware-counter runs disabled; diagnostics deliberately add work.
use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    time::Instant,
};

static ENABLED: AtomicBool = AtomicBool::new(false);

macro_rules! counters {
    ($($variant:ident => $name:literal),* $(,)?) => {
        #[derive(Clone, Copy)]
        pub(crate) enum Counter { $($variant,)* Count }
        const NAMES: &[&str] = &[$($name,)*];
        static VALUES: [AtomicU64; Counter::Count as usize] =
            [const { AtomicU64::new(0) }; Counter::Count as usize];
    };
}

counters! {
    PlanCalls => "plan_calls",
    CandidateRows => "candidate_rows",
    MemoHits => "memo_hits",
    DependencyRecords => "dependency_records",
    AccessRecords => "access_records",
    AdmissionNs => "admission_ns",
    AdmissionCandidates => "admission_candidates",
    AlreadyRunning => "already_running",
    ConflictDeferrals => "conflict_deferrals",
    AdmittedRows => "admitted_rows",
    WorkerBatches => "dispatch_batches",
    FirstPollNs => "enqueue_to_first_poll_ns",
    StartedRows => "started_rows",
    RowPolls => "completed_row_polls",
    PublicationNs => "publication_ns",
    PublicationCalls => "publication_calls",
    StalePublications => "stale_publications",
    AcceptedOutputs => "accepted_output_groups",
    BufferedCommands => "buffered_commands",
    RevisionChecks => "revision_checks",
    ReconciledWrites => "reconciled_writes",
    CleanupNs => "bound_cleanup_ns",
    CleanupRoots => "bound_cleanup_roots",
    HealingNs => "ambient_healing_ns",
    HealingMemoEntries => "ambient_healing_memo_entries",
    HealingVictims => "ambient_healing_victims",
}

pub(crate) fn enabled() -> bool {
    ENABLED.load(Relaxed)
}

/// Enable only for an isolated diagnostic run, with no other bowls in flight.
/// No reset is necessary: subtract snapshots around the workload.
#[doc(hidden)]
pub fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Relaxed);
}

/// Cumulative counters; read at quiescent boundaries for a coherent delta.
#[doc(hidden)]
pub fn snapshot() -> Vec<(&'static str, u64)> {
    NAMES
        .iter()
        .zip(&VALUES)
        .map(|(&name, value)| (name, value.load(Relaxed)))
        .collect()
}

pub(crate) fn add(counter: Counter, amount: u64) {
    if enabled() {
        VALUES[counter as usize].fetch_add(amount, Relaxed);
    }
}

pub(crate) struct Timer {
    counter: Counter,
    start: Option<Instant>,
}

impl Timer {
    pub(crate) fn new(counter: Counter) -> Self {
        Self {
            counter,
            start: enabled().then(Instant::now),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            VALUES[self.counter as usize].fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
        }
    }
}
