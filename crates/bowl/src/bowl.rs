use std::{
    any::{TypeId, type_name},
    collections::{HashMap, HashSet},
    fmt,
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex, atomic},
};

use futures::{
    FutureExt,
    channel::oneshot,
    future::join_all,
    lock::Mutex,
    stream::{FuturesUnordered, StreamExt},
};
use variadics_please::all_tuples;

use crate::{
    Component, Entity, IntoSystem, Query, QueryResult,
    commands::{BaseCommandOp, CommandOp, InsertBaseCommand, RemoveComponentBaseCommand},
    declare::{Schema, ShapeDesc},
    query::{
        Access, AccessKind, ArgBundle, CowQueryParam, EntityMutResult, ExternalFilter,
        ExternalQueryFilter, ExternalReadQueryParam, Mut, MutResult, Named, QueryArgs,
    },
    system::{BoxedSystem, MemoEntry, Phase, SystemRun},
    world::{Snapshot, SystemId, SystemInvocation, TryUpdate, World},
};

const DEFAULT_COMMIT_LIMIT: u64 = 10_000;

/// How many times one generation may be preempted before further
/// preemptive writes degrade to deferred (spec/epochs.md): they wait for
/// the generation's natural end instead of forcing a boundary, so forward
/// progress is guaranteed under write bursts.
const PREEMPTION_BUDGET: u32 = 4;

/// Guardrail for one external evaluation attempt.
///
/// A bowl settles when no normal-phase system invocation can make progress.
/// `CommitLimit` does not define settlement; it only bounds how many accepted
/// non-cleanup commits one caller may drive while trying to reach that fixed
/// point. Use [`CommitLimit::None`] for intentionally never-settling or
/// externally-cancelled systems.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLimit {
    /// Drive evaluation until it settles or the caller cancels the async
    /// operation.
    None,
    /// Panic after more than this many accepted non-cleanup commits happen in
    /// one external evaluation attempt.
    Max(u64),
}

impl Default for CommitLimit {
    fn default() -> Self {
        Self::Max(DEFAULT_COMMIT_LIMIT)
    }
}

/// Async-first database and system runner.
///
/// `Bowl` is cheap to clone and all public operations take `&self`. This is
/// deliberate: callers can clone a bowl handle into tasks and submit reads or
/// inputs concurrently.
///
/// A bowl has two separate coordination concepts:
///
/// ```text
/// runner lock
///   decides who is allowed to execute systems right now
///
/// generation state
///   decides what each caller is waiting for
/// ```
#[derive(Clone)]
pub struct Bowl {
    inner: Arc<Inner>,
}

struct Inner {
    /// Protects world contents, registered systems, memo tables, pending input,
    /// and generation bookkeeping.
    ///
    /// This lock must only be held for short bookkeeping sections. It must not
    /// be held while user systems run.
    state: Mutex<State>,
    /// Lock-free mirror of `State::completed_generation`, so the hot wait
    /// paths (every scoop caller spinning on "is my generation done yet")
    /// stop contending on the state lock.
    completed_generation: std::sync::atomic::AtomicU64,
    /// Read-mostly mirror of `State::settled_snapshot`, so stale-tolerant
    /// readers (`.last_settled()`) never touch the state lock at all.
    settled_read: std::sync::RwLock<Option<Arc<Snapshot>>>,
    /// Single-permit evaluator lock.
    ///
    /// Holding this guard means the caller is the only active runner. The guard
    /// may be held across system execution; `state` must not be.
    runner: Mutex<()>,
    /// Configurable non-convergence guardrail.
    ///
    /// This is kept outside async state so changing it does not require an
    /// executor turn. The value is tiny and copied when an operation starts.
    commit_limit: StdMutex<CommitLimit>,
    /// Bound entity handles cannot `await` in `Drop`, so dropped handles enqueue
    /// their entity here. The next bowl operation drains this queue after
    /// evaluation has had a chance to materialize request outputs.
    deferred_bound_cleanup: StdMutex<Vec<Entity>>,
    /// External mutators waiting for a preemption boundary
    /// (spec/epochs.md). An atomic outside the state lock so the runner's
    /// per-wave preempt probe costs one load instead of a lock round-trip.
    preempt_waiters: std::sync::atomic::AtomicUsize,
    /// Wakes the runner's in-flight await when a mutator registers, so the
    /// preemption boundary is reached promptly.
    preempt_signal: futures::task::AtomicWaker,
    /// Wakes the runner's open window when the last preempt waiter
    /// finishes applying.
    preempt_done: futures::task::AtomicWaker,
}

/// World counters identifying the state a snapshot was taken at.
type SnapshotKey = (u64, u64, u64);

fn snapshot_key(world: &World) -> SnapshotKey {
    (
        world.next_entity_raw(),
        world.revision_raw(),
        world.mutations_raw(),
    )
}

struct State {
    world: World,
    /// Reusable snapshot keyed by the world counters it was taken at.
    ///
    /// Repeated reads of an unchanged world share one structural clone
    /// instead of paying O(entries) per scoop.
    snapshot_cache: Option<(SnapshotKey, Arc<Snapshot>)>,
    systems: Vec<BoxedSystem>,
    memo: HashMap<SystemInvocation, MemoEntry>,
    completed_generation: u64,
    running_generation: Option<u64>,
    next_generation: u64,
    pending_generation: Option<u64>,
    pending_inputs: Vec<Box<dyn BaseCommandOp>>,
    /// External inputs that arrived while an epoch (an active settle) was
    /// driving, tagged with their arrival sequence. Promoted into
    /// `pending_inputs` at epoch boundaries by settles whose entry
    /// watermark covers them, so mid-epoch generations never drain input
    /// that arrived after the settle began (spec/epochs.md).
    deferred_inputs: Vec<(u64, Box<dyn BaseCommandOp>)>,
    /// Insert/spawn commands issued by `Phase::Settle` systems. The settle
    /// phase cannot drive its own settle forward: these queue as owned
    /// derived writes for the start of the next run.
    deferred_settle: Vec<(SystemInvocation, Box<dyn CommandOp>)>,
    /// The registered entity schema, if any: derived writes are
    /// shape-checked against it at commit in debug builds.
    schema: Option<Arc<Vec<ShapeDesc>>>,
    /// Monotonic arrival sequence for deferred inputs.
    input_seq: u64,
    /// Number of callers currently inside `settle()`. Non-zero means an
    /// epoch is active: external inserts defer and external muts preempt.
    settling: usize,
    /// The runner is paused at a preemption boundary; waiting mutators may
    /// apply their writes now.
    preempt_window: bool,
    /// A preemptive write was applied between generations; the next
    /// generation restarts through `Phase::Startup` so settle-scoped claims
    /// can be retracted before fresh derivations plan.
    preempt_restart: bool,
    /// Generation waiters, each tagged with the generation it needs.
    /// Completions wake only satisfied waiters — the rest stay registered,
    /// so a waiter locks the state once per wait, not once per generation
    /// (the broadcast version produced ~100 herd acquisitions per settle).
    waiters: Vec<(u64, oneshot::Sender<()>)>,
    /// External mutators waiting for a preemption boundary. Notified when
    /// a window opens or a generation completes, instead of spin-polling
    /// the state lock every yield.
    boundary_waiters: Vec<oneshot::Sender<()>>,
    settled_revision: u64,
    /// Snapshot retained at the last settle for `last_settled` scoops.
    /// Invalidated by destructive takes (a retained snapshot pins every
    /// component cell).
    settled_snapshot: Option<Arc<Snapshot>>,
    /// One-shot subscribers to fire (with the settled revision) when a
    /// settle that performed work completes.
    settle_watchers: Vec<oneshot::Sender<u64>>,
    normal_clean: bool,
    startup_ran: bool,
}

/// Result of inserting a new entity into the next evaluation generation.
pub struct InsertedEntity {
    bowl: Bowl,
    entity: Entity,
    generation: u64,
}

impl InsertedEntity {
    /// Returns the raw entity id created by the insert.
    ///
    /// This is an identity only. Future destructive reads should be modeled
    /// with a stronger bound-entity capability rather than plain `Entity`.
    pub fn entity(&self) -> Entity {
        self.entity
    }

    /// Turns this inserted entity into a scoped request capability.
    ///
    /// A [`BoundEntity`] can destructively take outputs from this exact entity.
    /// Dropping it without taking schedules the entity and its derived outputs
    /// for cleanup by the next bowl operation.
    pub fn bind(self) -> BoundEntity {
        BoundEntity {
            bowl: self.bowl,
            entity: Some(self.entity),
            generation: self.generation,
        }
    }
}

/// Scoped capability for consuming outputs from one inserted entity.
pub struct BoundEntity {
    bowl: Bowl,
    entity: Option<Entity>,
    generation: u64,
}

/// External handle to one existing entity, from [`Bowl::entity`].
pub struct BowlEntity {
    bowl: Bowl,
    entity: Entity,
}

impl BowlEntity {
    /// Queues a bundle onto this entity. Same epoch semantics as
    /// [`Bowl::insert`], including `.preempting()`.
    pub fn insert<B>(&self, bundle: B) -> InsertBuilder<B>
    where
        B: Bundle,
    {
        InsertBuilder {
            bowl: self.bowl.clone(),
            bundle,
            preempting: false,
            target: Some(self.entity),
        }
    }

    /// Queues removal of component `T` from this entity, mirroring
    /// `commands.entity(..).remove::<T>()` inside systems. Same epoch
    /// semantics as [`Bowl::insert`], including `.preempting()`.
    ///
    /// External writers need retraction as well as insertion: an LSP
    /// `didClose` must be able to retract an `OpenBuffer` fact it inserted.
    pub fn remove<T: Component>(&self) -> RemoveBuilder<T> {
        RemoveBuilder {
            bowl: self.bowl.clone(),
            entity: self.entity,
            preempting: false,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Builder for [`BowlEntity::remove`]; awaiting it queues the removal.
pub struct RemoveBuilder<T> {
    bowl: Bowl,
    entity: Entity,
    preempting: bool,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> RemoveBuilder<T> {
    /// Forces an epoch boundary instead of deferring to the next epoch (see
    /// [`InsertBuilder::preempting`]).
    pub fn preempting(mut self) -> Self {
        self.preempting = true;
        self
    }
}

impl<T: Component> IntoFuture for RemoveBuilder<T> {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.bowl
                .remove_component_inner::<T>(self.entity, self.preempting)
                .await;
        })
    }
}

/// Report from [`Bowl::explain`]: why a system did or did not run.
#[derive(Debug)]
pub struct ExplainReport {
    /// Whether any registered system matched the queried name.
    pub registered: bool,
    /// The phase the system runs during, if registered.
    pub phase: Option<Phase>,
    /// Invocations the system's queries currently match, after joins and
    /// filters. Zero explains "nothing to run" — e.g. a demand join starved
    /// the tuple product.
    pub matched_rows: usize,
    /// Matched invocations skipped because their memoized deps are
    /// unchanged. Equal to `matched_rows` explains "already up to date".
    pub memoized_rows: usize,
    /// Viewed component stores that changed since the system's last run.
    /// Nonzero while everything is memoized is the ambient-staleness
    /// footgun signature: the system's `View`s moved but nothing reran.
    /// That is deliberate `View` semantics — if the system must react,
    /// the data belongs in a tracked input (fingerprinted-index pattern).
    pub stale_views: usize,
}

/// Builder for [`Bowl::insert`]; awaiting it queues the bundle.
pub struct InsertBuilder<B> {
    bowl: Bowl,
    bundle: B,
    preempting: bool,
    target: Option<Entity>,
}

impl<B> InsertBuilder<B> {
    /// Forces an epoch boundary instead of deferring to the next epoch
    /// (spec/epochs.md): in-flight read-only work is dropped, writers are
    /// drained, and the restarted generation drains this input. Cooperative
    /// (deferred) is the default because a new entity invalidates no
    /// in-flight work.
    pub fn preempting(mut self) -> Self {
        self.preempting = true;
        self
    }
}

impl<B> IntoFuture for InsertBuilder<B>
where
    B: Bundle,
{
    type Output = InsertedEntity;
    type IntoFuture = Pin<Box<dyn Future<Output = InsertedEntity> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.bowl
                .insert_inner(self.bundle, self.preempting, self.target)
                .await
        })
    }
}

/// Builder for an external bowl scoop.
///
/// `ScoopBuilder` can be awaited directly to produce the requested result, or
/// it can first receive runtime filter arguments with [`ScoopBuilder::args`].
pub struct ScoopBuilder<S> {
    bowl: Bowl,
    args: QueryArgs,
    last_settled: bool,
    _marker: PhantomData<S>,
}

impl<S> ScoopBuilder<S> {
    /// Adds one or more shared runtime arguments used by `Where` filter
    /// expressions.
    pub fn args(mut self, values: impl ArgBundle) -> Self {
        values.insert_into(&mut self.args, None);
        self
    }

    /// Adds one or more runtime arguments scoped to a named query.
    ///
    /// `Named<Tag, Query<...>>` filters check args for `Tag` first, then fall
    /// back to shared args inserted with [`ScoopBuilder::args`].
    pub fn args_for<Tag>(mut self, values: impl ArgBundle) -> Self
    where
        Tag: 'static,
    {
        values.insert_into(&mut self.args, Some(TypeId::of::<Tag>()));
        self
    }

    /// Reads the last settled state without waiting for the bowl to settle
    /// (spec/epochs.md, stale reads). The pressure valve for
    /// latency-tolerant readers — a live status view keeps rendering while
    /// an epoch churns. Falls back to the current world when no settled
    /// view has been retained yet (fresh bowl, or invalidated by a
    /// destructive take).
    pub fn last_settled(mut self) -> Self {
        self.last_settled = true;
        self
    }
}

impl<S> ScoopBuilder<S>
where
    S: ExternalScoop,
{
    async fn materialize(self) -> S::Output {
        SCOOP_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _scoop_timer = ScopeTimer(&SCOOP_NANOS, std::time::Instant::now());
        if self.last_settled {
            // Lock-free: the settled snapshot is published to a
            // read-mostly slot at settle time, so stale-tolerant readers
            // bypass the state lock entirely.
            let retained = self
                .bowl
                .inner
                .settled_read
                .read()
                .expect("settled-read slot poisoned")
                .as_ref()
                .map(Arc::clone);
            let snapshot = match retained {
                Some(snapshot) => snapshot,
                None => self.bowl.snapshot().await,
            };
            return S::materialize(&self.bowl, &snapshot, &self.args, None);
        }

        self.bowl.settle().await;
        self.bowl.drain_deferred_bound_cleanup().await;
        let snapshot = self.bowl.snapshot().await;
        S::materialize(&self.bowl, &snapshot, &self.args, None)
    }
}

impl<Q, F> ScoopBuilder<Query<Q, F>>
where
    Q: ExternalReadQueryParam + Send,
    F: ExternalQueryFilter<Q> + Send,
{
    /// Reads only rows whose tracked components changed after `cursor` —
    /// the delta source for state-sync replication
    /// (spec/daemon-client.md, revision-cursor reads). Pair with
    /// [`Bowl::settled_revision`] as the cursor source and
    /// [`Bowl::next_settle`] to wake when new deltas exist.
    pub async fn changed_since(self, cursor: u64) -> QueryResult<Q, F> {
        self.bowl.settle().await;
        self.bowl.drain_deferred_bound_cleanup().await;
        let snapshot = self.bowl.snapshot().await;
        let mut result =
            QueryResult::<Q, F>::new(self.bowl.clone(), Arc::clone(&snapshot), &self.args, None);
        result.retain_rows(|state| {
            Q::deps(&snapshot, state)
                .iter()
                .any(|dep| dep.revision_raw() > cursor)
        });
        result
    }

    /// Destructively consumes the matched rows: materializes them, then
    /// removes the row entities under the same state lock — the
    /// deliver-then-delete contract for stream facts
    /// (spec/daemon-client.md, drain reads). The returned result stays
    /// readable from its snapshot after the removal.
    pub async fn drain(self) -> QueryResult<Q, F>
    where
        Q::State: crate::query::EntityQueryState,
    {
        self.bowl.settle().await;
        self.bowl.drain_deferred_bound_cleanup().await;

        let mut state = lock_state(&self.bowl.inner.state).await;
        let snapshot = snapshot_locked(&mut state);
        let result =
            QueryResult::<Q, F>::new(self.bowl.clone(), Arc::clone(&snapshot), &self.args, None);

        let was_settled = bowl_is_settled(&state);
        let entities: Vec<Entity> = result
            .rows()
            .iter()
            .map(|row| crate::query::EntityQueryState::entity(row))
            .collect();
        for entity in entities {
            cleanup_bound_entity(&mut state, entity);
        }
        if was_settled {
            state.settled_revision = state.world.revision_raw();
        }

        result
    }
}

impl<Q, F> ScoopBuilder<Query<Q, F>>
where
    Q: CowQueryParam,
    F: ExternalFilter<Q::State>,
{
    /// Mutates every row matched by this query.
    ///
    /// The closure runs synchronously while the live world is locked. Do not
    /// call back into the same bowl from inside the closure.
    pub async fn for_each<Func>(self, mut func: Func)
    where
        Func: for<'a> FnMut(Q::Item<'a>),
    {
        self.bowl.settle().await;
        self.bowl.drain_deferred_bound_cleanup().await;

        let mut state = lock_state(&self.bowl.inner.state).await;
        let rows = crate::query::external_filtered_cow_rows::<Q, F>(&state.world, &self.args, None);
        let mut changed = false;

        for row in rows {
            changed |= Q::for_each_mut(&mut state.world, &row, |item| func(item));
        }

        if changed {
            state.normal_clean = false;
            if state.pending_generation.is_none() {
                let next_generation = state.next_generation;
                state.pending_generation = Some(next_generation);
            }
        }
    }
}

impl<Tag, Q, F> ScoopBuilder<Named<Tag, Query<Q, F>>>
where
    Tag: 'static,
    Q: CowQueryParam,
    F: ExternalFilter<Q::State>,
{
    /// Mutates every row matched by this named query.
    ///
    /// Scoped args from `args_for::<Tag>(...)` override shared args inserted
    /// with `args(...)`.
    pub async fn for_each<Func>(self, mut func: Func)
    where
        Func: for<'a> FnMut(Q::Item<'a>),
    {
        self.bowl.settle().await;
        self.bowl.drain_deferred_bound_cleanup().await;

        let mut state = lock_state(&self.bowl.inner.state).await;
        let rows = crate::query::external_filtered_cow_rows::<Q, F>(
            &state.world,
            &self.args,
            Some(TypeId::of::<Tag>()),
        );
        let mut changed = false;

        for row in rows {
            changed |= Q::for_each_mut(&mut state.world, &row, |item| func(item));
        }

        if changed {
            state.normal_clean = false;
            if state.pending_generation.is_none() {
                let next_generation = state.next_generation;
                state.pending_generation = Some(next_generation);
            }
        }
    }
}

impl<S> IntoFuture for ScoopBuilder<S>
where
    S: ExternalScoop + Send + 'static,
{
    type Output = S::Output;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.materialize())
    }
}

/// Type-level description of one or more result sets to scoop from a settled
/// bowl.
///
/// `Query<T, F>` scoops one result set. Tuples of `ExternalScoop` specs scoop
/// multiple independent result sets from the same snapshot.
pub trait ExternalScoop: Send {
    /// Result produced by awaiting `Bowl::scoop::<Self>()`.
    type Output;

    #[doc(hidden)]
    fn materialize(
        bowl: &Bowl,
        snapshot: &Arc<Snapshot>,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Self::Output;
}

impl<Q, F> ExternalScoop for Query<Q, F>
where
    Q: ExternalReadQueryParam + Send,
    F: ExternalQueryFilter<Q> + Send,
{
    type Output = QueryResult<Q, F>;

    fn materialize(
        bowl: &Bowl,
        snapshot: &Arc<Snapshot>,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Self::Output {
        QueryResult::new(bowl.clone(), Arc::clone(snapshot), args, scope)
    }
}

impl<T, F> ExternalScoop for Query<(Mut<T>,), F>
where
    T: Component,
    F: ExternalFilter<Entity> + Send,
{
    type Output = MutResult<T, F>;

    fn materialize(
        bowl: &Bowl,
        snapshot: &Arc<Snapshot>,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Self::Output {
        MutResult::new(
            bowl.clone(),
            crate::query::external_mut_rows::<T, F>(snapshot, args, scope),
        )
    }
}

impl<T, F> ExternalScoop for Query<(Entity, Mut<T>), F>
where
    T: Component,
    F: ExternalFilter<Entity> + Send,
{
    type Output = EntityMutResult<T, F>;

    fn materialize(
        bowl: &Bowl,
        snapshot: &Arc<Snapshot>,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Self::Output {
        EntityMutResult::new(
            bowl.clone(),
            crate::query::external_mut_rows::<T, F>(snapshot, args, scope),
        )
    }
}

impl<Tag, S> ExternalScoop for Named<Tag, S>
where
    Tag: Send + 'static,
    S: ExternalScoop,
{
    type Output = S::Output;

    fn materialize(
        bowl: &Bowl,
        snapshot: &Arc<Snapshot>,
        args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> Self::Output {
        S::materialize(bowl, snapshot, args, Some(TypeId::of::<Tag>()))
    }
}

impl BoundEntity {
    /// Returns the raw bound entity id.
    pub fn entity(&self) -> Entity {
        self.entity
            .expect("bound entity was already closed or consumed")
    }

    /// Waits for evaluation, takes the requested components, then closes the
    /// bound entity.
    ///
    /// Required components fail the take when absent. `Option<T>` entries are
    /// allowed to be absent. The bound entity and all remaining outputs scoped
    /// to it are cleaned up regardless of success.
    pub async fn take<T>(mut self) -> Result<T::Output, TakeError>
    where
        T: TakeBundle,
    {
        let entity = self
            .entity
            .take()
            .expect("bound entity was already closed or consumed");

        let mut commit_budget = CommitBudget::new(self.bowl.commit_limit());
        self.bowl
            .ensure_evaluated(self.generation, &mut commit_budget)
            .await;
        self.bowl.settle().await;

        let result = loop {
            let mut state = lock_state(&self.bowl.inner.state).await;
            // Taking unwraps component cells, which must not be kept alive by
            // the shared snapshot cache or the retained settled snapshot.
            state.snapshot_cache = None;
            state.settled_snapshot = None;
            *self
                .bowl
                .inner
                .settled_read
                .write()
                .expect("settled-read slot poisoned") = None;

            // In-flight snapshots and live query results can still share the
            // cells this take needs; removing a shared cell would lose the
            // value. Holders are transient (evaluation waves, concurrent
            // scoops), so yield until they drop. Snapshot creation requires
            // the state lock held here, so an unblocked check cannot be
            // invalidated before the take below.
            if T::blocked(&state.world, entity) {
                drop(state);
                yield_once().await;
                continue;
            }

            let was_settled = bowl_is_settled(&state);
            let result = T::take(&mut state.world, entity);
            cleanup_bound_entity(&mut state, entity);
            // Only extend an actually-settled state over this removal; see
            // `drain_deferred_bound_cleanup` for why an unconditional sync
            // here starves other callers' pending settles.
            if was_settled {
                state.settled_revision = state.world.revision_raw();
            }
            break result;
        };

        self.bowl.drain_deferred_bound_cleanup().await;
        result
    }
}

impl Drop for BoundEntity {
    fn drop(&mut self) {
        let Some(entity) = self.entity.take() else {
            return;
        };

        self.bowl
            .inner
            .deferred_bound_cleanup
            .lock()
            .expect("deferred bound cleanup lock poisoned")
            .push(entity);
    }
}

/// Error returned by [`BoundEntity::take`] when a required component is absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakeError {
    entity: Entity,
    component: &'static str,
}

impl TakeError {
    /// Entity that was missing a required component.
    pub fn entity(&self) -> Entity {
        self.entity
    }

    /// Rust type name of the missing component.
    pub fn component(&self) -> &'static str {
        self.component
    }
}

impl fmt::Display for TakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "entity {} is missing required component {}",
            self.entity.raw(),
            self.component
        )
    }
}

impl std::error::Error for TakeError {}

/// Components that can be taken from a bound entity.
///
/// Taking returns `Arc<T>` handles to preserve true destructive removal from
/// the live bowl without requiring `T: Clone`.
pub trait TakeBundle {
    /// Value returned by a successful take.
    type Output;

    #[doc(hidden)]
    fn take(world: &mut World, entity: Entity) -> Result<Self::Output, TakeError>;

    /// Whether any component this bundle would take is still shared with a
    /// live snapshot or query result, making the take momentarily
    /// impossible without losing the value.
    #[doc(hidden)]
    fn blocked(world: &World, entity: Entity) -> bool;
}

impl<T> TakeBundle for T
where
    T: Component,
{
    type Output = Arc<T>;

    fn take(world: &mut World, entity: Entity) -> Result<Self::Output, TakeError> {
        world.remove_component::<T>(entity).ok_or(TakeError {
            entity,
            component: type_name::<T>(),
        })
    }

    fn blocked(world: &World, entity: Entity) -> bool {
        world.component_pinned::<T>(entity)
    }
}

impl<T> TakeBundle for Option<T>
where
    T: Component,
{
    type Output = Option<Arc<T>>;

    fn take(world: &mut World, entity: Entity) -> Result<Self::Output, TakeError> {
        Ok(world.remove_component::<T>(entity))
    }

    fn blocked(world: &World, entity: Entity) -> bool {
        world.component_pinned::<T>(entity)
    }
}

/// One queued build step, applied in call order at [`BowlBuilder::build`].
enum BuildStep {
    System(Box<dyn FnOnce(&Bowl)>),
    Plugin(Box<dyn Plugin>),
}

/// A reusable unit of bowl content: entity shapes plus the systems that
/// produce and consume them, added with [`BowlBuilder::plugin`].
///
/// Shapes and systems travel together, so installing a plugin cannot
/// desync its schema fragment from its systems — the bowl's schema is the
/// union of every fragment collected at build time, before any store
/// exists.
pub trait Plugin {
    /// Entity shapes this plugin contributes to the bowl schema (its
    /// systems' outputs and, for base inputs, what they query). Plugins
    /// that only derive over other fragments' shapes contribute none.
    fn shapes(&self) -> Vec<ShapeDesc> {
        Vec::new()
    }

    /// Registers the plugin's systems. Runs at build time, after the
    /// schema universe is sealed.
    fn build(&self, bowl: &mut Registrar<'_>);
}

/// System registration handle passed to [`Plugin::build`]. The only way
/// to register systems is through the builder lifecycle — a built bowl's
/// system set is sealed.
pub struct Registrar<'a> {
    bowl: &'a Bowl,
}

impl Registrar<'_> {
    /// Registers a system. Systems run in registration order semantics
    /// (buffered outputs commit in registration order).
    pub fn system<S, M>(&mut self, system: S)
    where
        S: IntoSystem<M>,
    {
        self.bowl.register_system(system);
    }
}

/// Builds a [`Bowl`]: schema fragments and plugins first, then a sealed
/// bowl. This is the only construction path — the schema universe must be
/// closed before any store exists (presence bit layout), and the sealed
/// system set is what lets registration-time analyses and the planner
/// treat the graph as total.
pub struct BowlBuilder {
    shapes: Vec<ShapeDesc>,
    steps: Vec<BuildStep>,
}

impl BowlBuilder {
    /// Adds an entity-schema fragment (`#[derive(Schema)]`). The app's own
    /// schema is just another fragment alongside plugin contributions.
    pub fn schema<S: Schema>(mut self) -> Self {
        self.shapes.extend(S::shapes());
        self
    }

    /// Adds a plugin: its shape fragment joins the schema universe and its
    /// systems register at this position in build order.
    pub fn plugin<P: Plugin + 'static>(mut self, plugin: P) -> Self {
        self.steps.push(BuildStep::Plugin(Box::new(plugin)));
        self
    }

    /// Registers a system at this position in build order.
    pub fn system<S, M>(mut self, system: S) -> Self
    where
        S: IntoSystem<M> + 'static,
    {
        self.steps
            .push(BuildStep::System(Box::new(move |bowl: &Bowl| {
                bowl.register_system(system);
            })));
        self
    }

    /// Seals and constructs the bowl: unions every schema fragment, lays
    /// out the presence-bit universe, then runs registrations in order.
    /// A builder with no schema fragments yields a schema-less bowl
    /// (conformance and presence indexing skipped).
    pub fn build(mut self) -> Bowl {
        for step in &self.steps {
            if let BuildStep::Plugin(plugin) = step {
                self.shapes.extend(plugin.shapes());
            }
        }

        let bowl = Bowl::new();
        if !self.shapes.is_empty() {
            let mut state = bowl
                .inner
                .state
                .try_lock()
                .expect("freshly constructed bowl state is uncontended");
            // The schema closes the component universe, so presence bits
            // can be laid out once, before any store exists.
            let mut universe = Vec::new();
            for shape in &self.shapes {
                for (type_id, _) in shape.required.iter().chain(&shape.optional) {
                    if !universe.contains(type_id) {
                        universe.push(*type_id);
                    }
                }
            }
            state.world.init_presence(universe);
            state.schema = Some(Arc::new(self.shapes));
        }

        for step in self.steps {
            match step {
                BuildStep::System(register) => register(&bowl),
                BuildStep::Plugin(plugin) => plugin.build(&mut Registrar { bowl: &bowl }),
            }
        }

        bowl
    }
}

/// Debug counters for profiling settle behavior (debug builds only).
pub static SETTLE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static GENERATION_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Engine-internal time buckets (nanoseconds, process-global): where a
/// settle's wall time goes when it is not inside a system.
pub static SNAPSHOT_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static COMMIT_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SETTLE_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WAVE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// State-lock telemetry: cumulative time spent *waiting* to acquire,
/// cumulative time the lock was *held*, and acquisition count.
pub static STATE_LOCK_WAIT_NANOS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static STATE_LOCK_HELD_NANOS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static STATE_LOCK_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// External read telemetry: scoop calls and their end-to-end time (queue
/// wait + snapshot + materialization).
pub static SCOOP_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SCOOP_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Timed guard over the state lock: hold time is recorded on drop.
struct StateGuard<'a> {
    guard: futures::lock::MutexGuard<'a, State>,
    since: Option<std::time::Instant>,
}

impl<'a> std::ops::Deref for StateGuard<'a> {
    type Target = State;

    fn deref(&self) -> &State {
        &self.guard
    }
}

impl std::ops::DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.guard
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        if let Some(since) = self.since {
            STATE_LOCK_HELD_NANOS.fetch_add(
                since.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
}

/// Acquires the state lock, with wait/held telemetry in debug builds
/// (measurement-free in release: the two `Instant` reads and three
/// atomics per acquisition are visible on µs-scale settles).
async fn lock_state(state: &Mutex<State>) -> StateGuard<'_> {
    if !cfg!(debug_assertions) {
        return StateGuard {
            guard: state.lock().await,
            since: None,
        };
    }
    let wait_start = std::time::Instant::now();
    let guard = state.lock().await;
    STATE_LOCK_WAIT_NANOS.fetch_add(
        wait_start.elapsed().as_nanos() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    STATE_LOCK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    StateGuard {
        guard,
        since: Some(std::time::Instant::now()),
    }
}

/// One system's profile row: registration name plus cumulative counters.
pub struct ProfileEntry {
    pub name: &'static str,
    pub runs: u64,
    pub plan_nanos: u64,
    pub run_nanos: u64,
}

/// Adds elapsed time to a global bucket on drop.
struct ScopeTimer<'a>(&'a std::sync::atomic::AtomicU64, std::time::Instant);

impl Drop for ScopeTimer<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(
            self.1.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

/// A spawned system run that aborts if the runner drops it (preemption
/// discards read-only work; a detached task must not keep holding cell
/// guards).
#[cfg(feature = "parallel")]
struct SpawnedRun<T> {
    handle: tokio::task::JoinHandle<T>,
}

#[cfg(feature = "parallel")]
impl<T> std::future::Future for SpawnedRun<T> {
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle)
            .poll(cx)
            .map(|result| result.expect("system task panicked or was aborted"))
    }
}

#[cfg(feature = "parallel")]
impl<T> Drop for SpawnedRun<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Dispatches a planned run: onto ambient tokio workers under the
/// `parallel` feature (falling back to cooperative polling without a
/// runtime), always cooperative otherwise — the engine itself stays
/// executor-agnostic.
fn dispatch_run(
    run: impl std::future::Future<Output = (SystemInvocation, SystemRun)> + Send + 'static,
) -> futures::future::BoxFuture<'static, (SystemInvocation, SystemRun)> {
    #[cfg(feature = "parallel")]
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return Box::pin(SpawnedRun {
                handle: handle.spawn(run),
            });
        }
    }
    Box::pin(run)
}

/// Poll-time measuring wrapper: charges only the time spent inside the
/// inner future's `poll` to the owning system, so awaiting siblings in the
/// same wave is not misattributed.
struct TimedRun<F> {
    inner: F,
    stats: Arc<crate::system::SystemStats>,
}

impl<F: std::future::Future + Unpin> std::future::Future for TimedRun<F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let start = std::time::Instant::now();
        let result = std::pin::Pin::new(&mut self.inner).poll(cx);
        self.stats.run_nanos.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        if result.is_ready() {
            self.stats
                .runs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }
}

impl Bowl {
    /// Starts building a bowl — the only construction path. See
    /// [`BowlBuilder`].
    pub fn builder() -> BowlBuilder {
        BowlBuilder {
            shapes: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// Creates the empty inner bowl.
    ///
    /// The initial completed generation is `0`; the first inserted input is
    /// assigned to generation `1`.
    fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    world: World::new(),
                    snapshot_cache: None,
                    systems: Vec::new(),
                    memo: HashMap::new(),
                    completed_generation: 0,
                    running_generation: None,
                    next_generation: 1,
                    pending_generation: None,
                    pending_inputs: Vec::new(),
                    deferred_inputs: Vec::new(),
                    deferred_settle: Vec::new(),
                    schema: None,
                    input_seq: 0,
                    settling: 0,
                    preempt_window: false,
                    preempt_restart: false,
                    waiters: Vec::new(),
                    boundary_waiters: Vec::new(),
                    settled_revision: 0,
                    settled_snapshot: None,
                    settle_watchers: Vec::new(),
                    normal_clean: true,
                    startup_ran: false,
                }),
                completed_generation: std::sync::atomic::AtomicU64::new(0),
                settled_read: std::sync::RwLock::new(None),
                runner: Mutex::new(()),
                commit_limit: StdMutex::new(CommitLimit::default()),
                deferred_bound_cleanup: StdMutex::new(Vec::new()),
                preempt_waiters: std::sync::atomic::AtomicUsize::new(0),
                preempt_signal: futures::task::AtomicWaker::new(),
                preempt_done: futures::task::AtomicWaker::new(),
            }),
        }
    }

    /// Updates the commit guardrail used by future evaluation attempts.
    ///
    /// The default is `CommitLimit::Max(10_000)`. Set [`CommitLimit::None`] for
    /// intentionally open-ended systems and rely on normal async cancellation or
    /// executor-specific timeout wrappers at the call site.
    pub fn set_commit_limit(&self, limit: CommitLimit) {
        *self
            .inner
            .commit_limit
            .lock()
            .expect("commit limit lock poisoned") = limit;
    }

    /// Returns the current commit guardrail.
    pub fn commit_limit(&self) -> CommitLimit {
        *self
            .inner
            .commit_limit
            .lock()
            .expect("commit limit lock poisoned")
    }

    pub(crate) async fn with_component_original<T, F, R>(
        &self,
        entity: Entity,
        original_revision: Option<crate::world::Revision>,
        deferred: bool,
        f: F,
    ) -> Option<R>
    where
        T: Component,
        F: FnOnce(&mut T) -> R,
    {
        let mut f = f;
        let mut waiter = PreemptWaiter::new(self, deferred);
        loop {
            let boundary = {
                let mut state = lock_state(&self.inner.state).await;
                if state.world.revision::<T>(entity) != original_revision {
                    waiter.finish();
                    return None;
                }

                if waiter.boundary_reached(&mut state) {
                    match apply_component_mutation::<T, F, R>(&mut state, entity, f) {
                        TryUpdate::Applied { result, .. } => {
                            waiter.finish();
                            return Some(result);
                        }
                        TryUpdate::Missing => {
                            waiter.finish();
                            return None;
                        }
                        // A held cell releases without touching the state
                        // lock; keep the yield path for busy retries.
                        TryUpdate::Busy(back) => {
                            f = back;
                            None
                        }
                    }
                } else {
                    let (sender, receiver) = oneshot::channel();
                    state.boundary_waiters.push(sender);
                    Some(receiver)
                }
            };
            match boundary {
                Some(receiver) => {
                    let _ = receiver.await;
                }
                None => yield_once().await,
            }
        }
    }

    pub(crate) async fn with_component_mut<T, F, R>(
        &self,
        entity: Entity,
        deferred: bool,
        f: F,
    ) -> Option<R>
    where
        T: Component,
        F: FnOnce(&mut T) -> R,
    {
        let mut f = f;
        let mut waiter = PreemptWaiter::new(self, deferred);
        loop {
            let boundary = {
                let mut state = lock_state(&self.inner.state).await;
                if waiter.boundary_reached(&mut state) {
                    match apply_component_mutation::<T, F, R>(&mut state, entity, f) {
                        TryUpdate::Applied { result, .. } => {
                            waiter.finish();
                            return Some(result);
                        }
                        TryUpdate::Missing => {
                            waiter.finish();
                            return None;
                        }
                        // A held cell releases without touching the state
                        // lock, so busy-retry keeps the yield path.
                        TryUpdate::Busy(back) => {
                            f = back;
                            None
                        }
                    }
                } else {
                    let (sender, receiver) = oneshot::channel();
                    state.boundary_waiters.push(sender);
                    Some(receiver)
                }
            };
            match boundary {
                Some(receiver) => {
                    let _ = receiver.await;
                }
                None => yield_once().await,
            }
        }
    }

    /// Returns whether the live world currently holds derived outputs owned by
    /// `owner`.
    ///
    /// Snapshots do not carry the ownership index, so settled hooks check the
    /// live bowl instead.
    pub(crate) async fn has_derived_owned(&self, owner: &SystemInvocation) -> bool {
        lock_state(&self.inner.state).await.world.has_derived_owned(owner)
    }

    /// Registers a system. Build-time only ([`BowlBuilder::system`] /
    /// [`Registrar::system`]): the builder owns the bowl exclusively, so
    /// the state lock is uncontended.
    ///
    /// Systems are stored in registration order. During evaluation, systems
    /// plan from the same structural snapshot and are polled concurrently from
    /// the active runner. Their buffered outputs are still committed in
    /// registration order.
    fn register_system<S, M>(&self, system: S)
    where
        S: IntoSystem<M>,
    {
        let mut state = self
            .inner
            .state
            .try_lock()
            .expect("systems register at build time, before the bowl is shared");
        let id = SystemId(state.systems.len());
        let system = system.into_system(id);
        if cfg!(debug_assertions) {
            warn_same_phase_conflicts(&state, &system);
        }
        state.systems.push(system);
        if state.pending_generation.is_none() {
            let next_generation = state.next_generation;
            state.pending_generation = Some(next_generation);
        }
    }

    /// Queues a new entity bundle for the next evaluation generation.
    ///
    /// Insertion does not immediately mutate the completed world. Instead the
    /// bundle is converted into base commands and batched with any other
    /// pending inputs:
    ///
    /// ```text
    /// generation 10 running
    ///   insert A -> pending generation 11
    ///   insert B -> pending generation 11
    ///
    /// generation 11 starts with A and B together
    /// ```
    ///
    /// The returned [`InsertedEntity`] remembers the generation that will
    /// include this bundle.
    pub fn insert<B>(&self, bundle: B) -> InsertBuilder<B>
    where
        B: Bundle,
    {
        InsertBuilder {
            bowl: self.clone(),
            bundle,
            preempting: false,
            target: None,
        }
    }

    /// External access to an *existing* entity, mirroring
    /// `commands.entity(..)` inside systems.
    ///
    /// The main use is targeted inserts (spec/daemon-client.md): clients
    /// applying replicated deltas and long-running task adapters reporting
    /// completion facts write onto entities they did not create.
    pub fn entity(&self, entity: Entity) -> BowlEntity {
        BowlEntity {
            bowl: self.clone(),
            entity,
        }
    }

    /// Explains why `system` (matched by function-name suffix) did or did
    /// not run: whether it is registered at all, which phase it runs in, how
    /// many invocations its queries currently match after joins and filters,
    /// and how many of those are memo-current (skipped as up to date).
    ///
    /// Distinguishes the silent no-run causes that are otherwise guesswork:
    /// no matching rows (e.g. a demand join starved the tuple product), all
    /// rows memoized, the wrong phase, the wrong system name entirely — or
    /// ambient staleness, where the system's `View`s moved but its tracked
    /// deps did not ([`ExplainReport::stale_views`]).
    /// Cumulative per-system profile (plan time, poll time, completed
    /// runs) in registration order.
    pub async fn profile_all(&self) -> Vec<ProfileEntry> {
        let state = lock_state(&self.inner.state).await;
        state
            .systems
            .iter()
            .map(|system| ProfileEntry {
                name: system.name,
                runs: system.stats.runs.load(std::sync::atomic::Ordering::Relaxed),
                plan_nanos: system
                    .stats
                    .plan_nanos
                    .load(std::sync::atomic::Ordering::Relaxed),
                run_nanos: system
                    .stats
                    .run_nanos
                    .load(std::sync::atomic::Ordering::Relaxed),
            })
            .collect()
    }

    /// [`Bowl::explain`] for every registered system, in registration
    /// order, with each system's name — the end-of-run diagnostic dump.
    pub async fn explain_all(&self) -> Vec<(&'static str, ExplainReport)> {
        let names: Vec<&'static str> = {
            let state = lock_state(&self.inner.state).await;
            state.systems.iter().map(|system| system.name).collect()
        };
        let mut reports = Vec::with_capacity(names.len());
        for name in names {
            reports.push((name, self.explain(name).await));
        }
        reports
    }

    pub async fn explain(&self, system: &str) -> ExplainReport {
        let (target, memo, snapshot) = {
            let mut state = lock_state(&self.inner.state).await;
            let target = state
                .systems
                .iter()
                .enumerate()
                .find(|(_, candidate)| candidate.name.ends_with(system))
                .map(|(index, candidate)| (index, candidate.clone()));
            let memo = state.memo.clone();
            let snapshot = snapshot_locked(&mut state);
            (target, memo, snapshot)
        };

        let Some((index, target)) = target else {
            return ExplainReport {
                registered: false,
                phase: None,
                matched_rows: 0,
                memoized_rows: 0,
                stale_views: 0,
            };
        };

        let (matched_rows, memoized_rows) = target.runnable.row_counts(&snapshot, &memo);

        // A viewed store that moved past the revision an invocation last
        // planned from is ambient staleness: the view changed, nothing
        // reran, and nothing ever will unless a tracked dep moves too.
        let system_id = SystemId(index);
        let mut viewed: Vec<TypeId> = target
            .view_sets
            .iter()
            .flatten()
            .copied()
            .collect();
        viewed.sort_unstable();
        viewed.dedup();
        let stale_views = viewed
            .iter()
            .filter(|type_id| {
                let watermark = snapshot.store_watermark(**type_id);
                memo.iter().any(|(owner, entry)| {
                    owner.system == system_id && watermark > entry.planned_revision
                })
            })
            .count();

        ExplainReport {
            registered: true,
            phase: Some(target.phase),
            matched_rows,
            memoized_rows,
            stale_views,
        }
    }

    /// Queues an external component removal with the same epoch semantics
    /// as [`Bowl::insert`]: idle bowls queue it for the next pending
    /// generation, active epochs defer it to the next one, and
    /// `.preempting()` forces a boundary.
    async fn remove_component_inner<T: Component>(&self, entity: Entity, preempting: bool) {
        let mut commands: Vec<Box<dyn BaseCommandOp>> =
            vec![Box::new(RemoveComponentBaseCommand::<T> {
                entity,
                _marker: PhantomData,
            })];

        {
            let mut state = lock_state(&self.inner.state).await;
            let next_generation = state.next_generation;

            if state.settling == 0 {
                state.pending_inputs.append(&mut commands);
                state.pending_generation.get_or_insert(next_generation);
                return;
            }

            if !preempting {
                // An epoch is actively driving: the retraction belongs to
                // the next epoch, like any other deferred input.
                state.input_seq += 1;
                let tag = state.input_seq;
                state
                    .deferred_inputs
                    .extend(commands.into_iter().map(|command| (tag, command)));
                return;
            }
        }

        // Preempting removal mid-epoch: force a boundary so the restarted
        // generation drains this retraction instead of the next epoch.
        let mut waiter = PreemptWaiter::new(self, false);
        loop {
            let boundary = {
                let mut state = lock_state(&self.inner.state).await;
                if waiter.boundary_reached(&mut state) {
                    waiter.finish();
                    state.pending_inputs.append(&mut commands);
                    let next_generation = state.next_generation;
                    state.pending_generation.get_or_insert(next_generation);
                    return;
                }
                let (sender, receiver) = oneshot::channel();
                state.boundary_waiters.push(sender);
                receiver
            };
            let _ = boundary.await;
        }
    }

    async fn insert_inner<B>(
        &self,
        bundle: B,
        preempting: bool,
        target: Option<Entity>,
    ) -> InsertedEntity
    where
        B: Bundle,
    {
        let (entity, mut commands) = {
            let mut state = lock_state(&self.inner.state).await;
            let entity = target.unwrap_or_else(|| {
                B::singleton_key()
                    .map(|key| state.world.singleton_entity_or_spawn(key))
                    .unwrap_or_else(|| state.world.spawn_empty())
            });
            let mut commands = Vec::new();
            bundle.queue(entity, &mut commands);
            let next_generation = state.next_generation;

            if state.settling == 0 {
                state.pending_inputs.extend(commands);
                let generation = *state.pending_generation.get_or_insert(next_generation);
                return InsertedEntity {
                    bowl: self.clone(),
                    entity,
                    generation,
                };
            }

            if !preempting {
                // An epoch is actively driving: this input belongs to the
                // next epoch (spec/epochs.md). No pending generation exists
                // for it yet, so record the completed generation —
                // `ensure_evaluated` must be trivially satisfied (a later
                // target would hot-spin on a generation that never
                // materializes); the caller's own settle drives the promoted
                // work via its watermark.
                state.input_seq += 1;
                let tag = state.input_seq;
                state
                    .deferred_inputs
                    .extend(commands.into_iter().map(|command| (tag, command)));
                let generation = state.completed_generation;
                return InsertedEntity {
                    bowl: self.clone(),
                    entity,
                    generation,
                };
            }

            (entity, commands)
        };

        // Preempting insert mid-epoch: force a boundary (cancel → queue →
        // continue) so the restarted generation drains this input instead
        // of the next epoch.
        let mut waiter = PreemptWaiter::new(self, false);
        loop {
            let boundary = {
                let mut state = lock_state(&self.inner.state).await;
                if waiter.boundary_reached(&mut state) {
                    waiter.finish();
                    state.pending_inputs.append(&mut commands);
                    let next_generation = state.next_generation;
                    let generation = *state.pending_generation.get_or_insert(next_generation);
                    return InsertedEntity {
                        bowl: self.clone(),
                        entity,
                        generation,
                    };
                }
                let (sender, receiver) = oneshot::channel();
                state.boundary_waiters.push(sender);
                receiver
            };
            let _ = boundary.await;
        }
    }

    /// Evaluates as needed and returns one or more query results from the latest relevant
    /// generation.
    ///
    /// Fast paths:
    ///
    /// ```text
    /// idle and clean:
    ///   read the completed world immediately
    ///
    /// runner active:
    ///   wait for the running generation
    ///
    /// pending input exists before this read:
    ///   help run or wait for the pending generation
    /// ```
    ///
    /// A read-only query never starts duplicate work if another caller is
    /// already evaluating the target generation.
    pub fn scoop<S>(&self) -> ScoopBuilder<S> {
        ScoopBuilder {
            bowl: self.clone(),
            args: QueryArgs::default(),
            last_settled: false,
            _marker: PhantomData,
        }
    }

    /// Drains bound entities whose handles were dropped without `take`.
    async fn drain_deferred_bound_cleanup(&self) {
        let cleanup = {
            let mut cleanup = self
                .inner
                .deferred_bound_cleanup
                .lock()
                .expect("deferred bound cleanup lock poisoned");
            std::mem::take(&mut *cleanup)
        };

        if cleanup.is_empty() {
            return;
        }

        let mut state = lock_state(&self.inner.state).await;
        let was_settled = bowl_is_settled(&state);
        for entity in cleanup {
            cleanup_bound_entity(&mut state, entity);
        }
        // Removing bound entities does not unsettle a settled bowl, but it
        // must not *declare* an unsettled bowl settled: another caller's
        // pending work may still need a settle (including the settled-hook
        // pass that re-materializes gate markers), and syncing here would
        // let that caller's `settle()` exit through the revision fast path.
        if was_settled {
            state.settled_revision = state.world.revision_raw();
        }
    }

    /// Runs generations until the bowl has no pending work and the last
    /// generation produced no tracked changes.
    ///
    /// A settle is an epoch (spec/epochs.md): external inputs arriving while
    /// any settle is active are deferred to the next epoch, so mid-epoch
    /// generations run against a frozen input set.
    async fn settle(&self) {
        if cfg!(debug_assertions) {
            SETTLE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let settle_start = std::time::Instant::now();
        let _settle_timer = ScopeTimer(&SETTLE_NANOS, settle_start);
        // Settled fast path: with nothing pending, running, changed, or
        // deferred there is no epoch to drive (and none to freeze), so the
        // guard bookkeeping is skipped entirely — settled reads keep their
        // single-lock cost.
        {
            let mut state = lock_state(&self.inner.state).await;
            if state.pending_generation.is_none()
                && state.running_generation.is_none()
                && state.world.revision_raw() == state.settled_revision
                && state.deferred_inputs.is_empty()
            {
                refresh_settled_snapshot(&self.inner, &mut state);
                return;
            }
        }

        let epoch = EpochGuard::enter(self).await;
        let mut commit_budget = CommitBudget::new(self.commit_limit());

        loop {
            let target = {
                let mut state = lock_state(&self.inner.state).await;
                if state.pending_generation.is_none()
                    && state.running_generation.is_none()
                    && state.world.revision_raw() == state.settled_revision
                {
                    // Epoch boundary: inputs that arrived before this settle
                    // entered are its responsibility; drive them as the next
                    // epoch. Anything newer stays deferred.
                    if promote_deferred_inputs(&mut state, epoch.watermark) {
                        continue;
                    }
                    refresh_settled_snapshot(&self.inner, &mut state);
                    return;
                }

                state
                    .pending_generation
                    .or(state.running_generation)
                    .unwrap_or(state.completed_generation)
            };

            self.ensure_evaluated(target, &mut commit_budget).await;

            let clean_and_settled = {
                let mut state = lock_state(&self.inner.state).await;
                let clean = state.pending_generation.is_none()
                    && state.running_generation.is_none();
                if clean && state.world.revision_raw() == state.settled_revision {
                    if promote_deferred_inputs(&mut state, epoch.watermark) {
                        continue;
                    }
                    refresh_settled_snapshot(&self.inner, &mut state);
                    return;
                }
                clean && state.normal_clean
            };

            if clean_and_settled {
                if self.run_settled_hooks(&mut commit_budget).await {
                    self.enqueue_next_generation().await;
                    continue;
                }

                self.run_settle_phase().await;
                let mut state = lock_state(&self.inner.state).await;
                if promote_deferred_inputs(&mut state, epoch.watermark) {
                    continue;
                }
                refresh_settled_snapshot(&self.inner, &mut state);
                // This settle performed work: fire the settle watchers.
                let settled_revision = state.settled_revision;
                let watchers = std::mem::take(&mut state.settle_watchers);
                drop(state);
                for watcher in watchers {
                    let _ = watcher.send(settled_revision);
                }
                return;
            }

            self.enqueue_next_generation().await;
        }
    }

    async fn run_settled_hooks(&self, commit_budget: &mut CommitBudget) -> bool {
        let (systems, mut memo) = {
            let mut state = lock_state(&self.inner.state).await;
            (state.systems.clone(), std::mem::take(&mut state.memo))
        };

        let snapshot = self.snapshot().await;
        let bowl = self.clone();
        let runs = join_all(
            systems
                .iter()
                .filter(|system| system.phase != Phase::Settle)
                .map(|system| system.run_settled(bowl.clone(), &snapshot, &memo)),
        )
        .await;

        let progress = if runs.is_empty() {
            CommitProgress::default()
        } else {
            commit_system_runs(&mut memo, &self.inner.state, runs).await
        };
        commit_budget.record(progress.commits);

        let mut state = lock_state(&self.inner.state).await;
        state.memo = memo;
        if !progress.needs_followup {
            state.settled_revision = state.world.revision_raw();
        }

        progress.needs_followup
    }

    /// Runs the `Phase::Settle` systems once, at convergence. Their removal
    /// commands apply within the settle (reaping stale facts before settled
    /// reads return); their insert/spawn commands defer to the next run.
    async fn run_settle_phase(&self) {
        let (systems, mut memo) = {
            let mut state = lock_state(&self.inner.state).await;
            (state.systems.clone(), std::mem::take(&mut state.memo))
        };

        let snapshot = self.snapshot().await;
        let mut runs = systems
            .iter()
            .filter(|system| system.phase == Phase::Settle)
            .flat_map(|system| {
                system.stream_runs(self.clone(), Arc::clone(&snapshot), &memo, None)
            })
            .map(|planned| {
                let owner = planned.owner;
                async move {
                    let run = planned.run.await;
                    (owner, run)
                }
            })
            .collect::<FuturesUnordered<_>>();

        while let Some((_owner, run)) = runs.next().await {
            commit_system_run(&mut memo, &self.inner.state, run, Some(Phase::Settle)).await;
        }

        let mut state = lock_state(&self.inner.state).await;
        state.memo = memo;
        state.settled_revision = state.world.revision_raw();
    }

    async fn enqueue_next_generation(&self) {
        let mut state = lock_state(&self.inner.state).await;
        if state.pending_generation.is_none() {
            let next_generation = state.next_generation;
            state.pending_generation = Some(next_generation);
        }
    }

    /// Drives the bowl until `target` has completed.
    ///
    /// This is the single-flight loop:
    ///
    /// ```text
    /// target complete -> return
    /// runner acquired -> run one pending generation
    /// runner busy     -> wait for progress
    /// ```
    ///
    /// The loop is intentionally written around `runner.try_lock()`. Acquiring
    /// that guard is the authority for becoming the evaluator; generation fields
    /// only describe what is running or pending.
    async fn ensure_evaluated(&self, target: u64, commit_budget: &mut CommitBudget) {
        loop {
            if self.completed_generation().await >= target {
                return;
            }

            if let Some(runner) = self.inner.runner.try_lock() {
                self.run_evaluation(runner, commit_budget).await;
            } else {
                self.wait_for_generation(target).await;
            }
        }
    }

    async fn completed_generation(&self) -> u64 {
        self.inner
            .completed_generation
            .load(atomic::Ordering::Acquire)
    }

    /// Revision counter of the last settled state — the cursor source for
    /// [`changed_since`](ScoopBuilder::changed_since) delta reads.
    pub async fn settled_revision(&self) -> u64 {
        lock_state(&self.inner.state).await.settled_revision
    }

    /// Resolves after the next settle that performed work, with the settled
    /// revision — the push signal for publishers
    /// (spec/daemon-client.md, settle notifications). No-op settles (reads
    /// of an already-settled bowl) do not fire it.
    pub async fn next_settle(&self) -> u64 {
        let receiver = {
            let mut state = lock_state(&self.inner.state).await;
            let (sender, receiver) = oneshot::channel();
            state.settle_watchers.push(sender);
            receiver
        };
        receiver.await.unwrap_or(0)
    }

    /// Returns the current world snapshot, sharing the cached one when the
    /// world has not changed since it was taken.
    ///
    /// Component values are stored in shared guarded cells, so a fresh
    /// snapshot is a structural clone of the store maps, not of user data.
    async fn snapshot(&self) -> Arc<Snapshot> {
        let mut state = lock_state(&self.inner.state).await;
        snapshot_locked(&mut state)
    }

    /// Suspends until any generation completes, then lets the caller re-check
    /// its target.
    ///
    /// Waiters are deliberately broad: waking does not mean the specific target
    /// completed, only that progress happened. The caller loops and verifies the
    /// generation again, which also handles newly queued work.
    async fn wait_for_generation(&self, target: u64) {
        let receiver = {
            let mut state = lock_state(&self.inner.state).await;
            if state.completed_generation >= target {
                return;
            }

            let (sender, receiver) = oneshot::channel();
            state.waiters.push((target, sender));
            receiver
        };

        let _ = receiver.await;
    }

    /// Runs one pending generation while holding the runner guard.
    ///
    /// The method is split into three phases:
    ///
    /// ```text
    /// start_evaluation:
    ///   drain base inputs, mark running generation, clone snapshot/systems/memo
    ///
    /// run systems:
    ///   poll systems and invalid rows concurrently without holding state lock
    ///
    /// commit:
    ///   replace derived outputs owned by each invocation, advance generation,
    ///   wake waiters
    /// ```
    ///
    /// If no pending generation exists, there is nothing to run. This can happen
    /// when a caller wins the runner race after another caller already completed
    /// the work it was waiting for.
    async fn run_evaluation(
        &self,
        _runner: futures::lock::MutexGuard<'_, ()>,
        commit_budget: &mut CommitBudget,
    ) {
        let Some((generation, systems, memo, startup)) = self.start_evaluation().await else {
            return;
        };

        // The driver is an ordinary caller's future and may be dropped at any
        // await point (timeouts, LSP request cancellation). The guard owns the
        // memo table and, on an abandoned run, restores state so any waiter
        // can be promoted to a new driver: committed invocations are already
        // durable in the world, only in-flight work is lost and replanned.
        let mut guard = EvaluationGuard {
            bowl: self.clone(),
            generation,
            memo: Some(memo),
        };

        let mut normal_phase_changed = false;

        let mut phases: &[Phase] = Phase::ordered(startup);
        let mut index = 0;
        let mut preemptions: u32 = 0;
        while index < phases.len() {
            // Past the budget, preemptive writes degrade to deferred: the
            // runner stops honoring boundaries mid-phase and waiters apply
            // at the generation's natural end.
            let allow_preempt = preemptions < PREEMPTION_BUDGET;
            match self
                .run_phase_streaming(
                    &systems,
                    phases[index],
                    guard.memo_mut(),
                    commit_budget,
                    allow_preempt,
                )
                .await
            {
                PhaseRun::Completed(changed) => {
                    normal_phase_changed |= changed;
                    index += 1;
                }
                PhaseRun::Preempted(changed) => {
                    normal_phase_changed |= changed;
                    preemptions += 1;
                    {
                        let mut state = lock_state(&self.inner.state).await;
                        state.preempt_restart = false;
                    }
                    // Aborted in-flight runs advanced planned marks for
                    // work that never committed; force full replans so the
                    // restarted phases see those rows again.
                    for system in systems.iter() {
                        system.reset_full();
                    }
                    // A preempted generation restarts through the Startup
                    // slot so settle-scoped claims can be retracted before
                    // fresh derivations plan (spec/epochs.md).
                    phases = Phase::ordered(true);
                    index = 0;
                }
            }
        }

        let memo = guard.complete();
        let waiters = {
            let mut state = lock_state(&self.inner.state).await;
            state.memo = memo;
            state.normal_clean = !normal_phase_changed;
            state.completed_generation = generation;
            self.inner
                .completed_generation
                .store(generation, atomic::Ordering::Release);
            if cfg!(debug_assertions) {
                GENERATION_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            state.running_generation = None;
            // A completed generation is a natural mutation boundary.
            for boundary in state.boundary_waiters.drain(..) {
                let _ = boundary.send(());
            }
            let mut satisfied = Vec::new();
            state.waiters.retain_mut(|(target, sender)| {
                if *target <= generation {
                    // Sender is consumed by `send`; move it out via a
                    // placeholder channel.
                    let (placeholder, _) = oneshot::channel();
                    satisfied.push(std::mem::replace(sender, placeholder));
                    false
                } else {
                    true
                }
            });
            satisfied
        };

        for waiter in waiters {
            let _ = waiter.send(());
        }
    }

    /// Runs one normal phase to quiescence.
    ///
    /// Each planning wave reads from one structural snapshot. As individual
    /// systems finish, their outputs are committed immediately. If any commit
    /// changes the live world, the same phase is planned again from the updated
    /// world before the runner advances to the next phase.
    async fn run_phase_streaming(
        &self,
        systems: &[BoxedSystem],
        phase: Phase,
        memo: &mut HashMap<SystemInvocation, MemoEntry>,
        commit_budget: &mut CommitBudget,
        allow_preempt: bool,
    ) -> PhaseRun {
        let mut phase_changed = false;
        let mut running = HashSet::new();
        let mut running_access: HashMap<SystemInvocation, Vec<Access>> = HashMap::new();
        // Tiered preemption drops read-only work wholesale while draining
        // writers, so in-flight runs are split by access class.
        let mut read_runs = FuturesUnordered::new();
        let mut write_runs = FuturesUnordered::new();
        let mut read_owners: HashSet<SystemInvocation> = HashSet::new();
        let mut needs_plan = true;
        let mut deferred_conflicts = false;

        loop {
            if allow_preempt
                && self
                    .inner
                    .preempt_waiters
                    .load(atomic::Ordering::SeqCst)
                    > 0
            {
                // Tiered preemption (spec/epochs.md): drop read-only
                // invocations (their buffered commands vanish unapplied),
                // drain write-holders to completion (a partial `MutRef`
                // write is not revocable), pause so the waiting mutators
                // apply at this boundary, then hand the generation back for
                // a restart through the Startup slot.
                drop(read_runs);
                for owner in read_owners.drain() {
                    running.remove(&owner);
                    running_access.remove(&owner);
                }
                while let Some((owner, run)) = write_runs.next().await {
                    running.remove(&owner);
                    running_access.remove(&owner);
                    let progress =
                        commit_system_run(memo, &self.inner.state, run, Some(phase)).await;
                    commit_budget.record(progress.commits);
                    phase_changed |= progress.needs_followup;
                }

                self.open_preempt_window().await;
                return PhaseRun::Preempted(phase_changed);
            }

            if needs_plan {
                deferred_conflicts = false;
                WAVE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let snapshot_start = std::time::Instant::now();
                let snapshot = self.snapshot().await;
                let (plan_log, plan_epoch) = {
                    let state = lock_state(&self.inner.state).await;
                    state.world.plan_log()
                };
                SNAPSHOT_NANOS.fetch_add(
                    snapshot_start.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                // Planner memoization: a system whose interested stores
                // haven't moved past its last planned mark cannot have new
                // rows or stale deps. When *no* system needs planning the
                // whole wave setup is skipped — cloning the memo per wave
                // is the dominant cost of quiet waves.
                if !systems
                    .iter()
                    .any(|system| system.phase == phase && system.peek_needs_planning(&snapshot))
                {
                    match self
                        .await_next_run(&mut read_runs, &mut write_runs, allow_preempt)
                        .await
                    {
                        RunEvent::Preempt => {
                            needs_plan = false;
                            continue;
                        }
                        RunEvent::Drained => return PhaseRun::Completed(phase_changed),
                        RunEvent::Finished(first) => {
                            let mut batch = vec![first];
                            while let Some(Some(next)) = read_runs.next().now_or_never() {
                                batch.push(next);
                            }
                            while let Some(Some(next)) = write_runs.next().now_or_never() {
                                batch.push(next);
                            }
                            for (owner, run) in batch {
                                running.remove(&owner);
                                running_access.remove(&owner);
                                read_owners.remove(&owner);
                                let progress = commit_system_run(
                                    memo,
                                    &self.inner.state,
                                    run,
                                    Some(phase),
                                )
                                .await;
                                commit_budget.record(progress.commits);
                                phase_changed |= progress.needs_followup;
                                if progress.stale {
                                    systems[owner.system.0].force_replan(&owner.keys);
                                }
                            }
                            continue;
                        }
                    }
                }
                for system in systems
                    .iter()
                    .filter(|system| system.phase == phase && system.needs_planning(&snapshot))
                {
                    let plan_start = std::time::Instant::now();
                    // Delta planning: a system whose cursor is current for
                    // this epoch plans only the entities written since its
                    // last plan; anything else (fresh registration, epoch
                    // roll, resets) plans fully once and joins the deltas.
                    let forced = system.take_forced_dirty();
                    let hint: Option<Vec<Entity>> = if system.delta_eligible
                        && system
                            .plan_epoch
                            .load(std::sync::atomic::Ordering::Relaxed)
                            == plan_epoch
                    {
                        let pos = system.log_pos.load(std::sync::atomic::Ordering::Relaxed)
                            as usize;
                        let interest = system
                            .interest
                            .as_ref()
                            .expect("delta-eligible systems have bounded interest");
                        let mut entities: Vec<Entity> = plan_log[pos.min(plan_log.len())..]
                            .iter()
                            .filter(|(type_id, _)| interest.contains(type_id))
                            .map(|(_, entity)| *entity)
                            .collect();
                        entities.extend(forced);
                        entities.sort_unstable();
                        entities.dedup();
                        Some(entities)
                    } else {
                        // Full plan covers any forced rows implicitly.
                        None
                    };
                    let planned_runs = system.stream_runs(
                        self.clone(),
                        Arc::clone(&snapshot),
                        memo,
                        hint.as_deref(),
                    );
                    if system.delta_eligible {
                        system
                            .log_pos
                            .store(plan_log.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        system
                            .plan_epoch
                            .store(plan_epoch, std::sync::atomic::Ordering::Relaxed);
                    }
                    system.stats.plan_nanos.fetch_add(
                        plan_start.elapsed().as_nanos() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    for planned in planned_runs {
                        if !running.insert(planned.owner.clone()) {
                            continue;
                        }

                        if conflicts_with_running(&planned.access, &running_access) {
                            deferred_conflicts = true;
                            // The deferred row must be replanned once the
                            // conflict frees, even if no store moves —
                            // row-granular, so the rest of the system's
                            // plan stays memoized.
                            systems[planned.owner.system.0].force_replan(&planned.owner.keys);
                            running.remove(&planned.owner);
                            continue;
                        }

                        let writer = planned
                            .access
                            .iter()
                            .any(|access| access.kind == AccessKind::Write);
                        let owner = planned.owner;
                        running_access.insert(owner.clone(), planned.access);
                        if !writer {
                            read_owners.insert(owner.clone());
                        }
                        let timed = TimedRun {
                            inner: planned.run,
                            stats: Arc::clone(&system.stats),
                        };
                        let run = async move {
                            let run = timed.await;
                            (owner, run)
                        };
                        // Parallel execution when an executor is ambient
                        // (feature `parallel`): planned runs are owned +
                        // `Send`, `Access` scheduling already keeps
                        // conflicting rows apart, and commits stay
                        // serialized on the driver.
                        let run = dispatch_run(run);
                        if writer {
                            write_runs.push(run);
                        } else {
                            read_runs.push(run);
                        }
                    }
                }
            }

            let first = match self
                .await_next_run(&mut read_runs, &mut write_runs, allow_preempt)
                .await
            {
                RunEvent::Preempt => {
                    needs_plan = false;
                    continue;
                }
                RunEvent::Drained => return PhaseRun::Completed(phase_changed),
                RunEvent::Finished(first) => first,
            };

            // Commit everything that has already finished before deciding
            // whether the phase needs another planning wave.
            let mut batch = vec![first];
            while let Some(Some(next)) = read_runs.next().now_or_never() {
                batch.push(next);
            }
            while let Some(Some(next)) = write_runs.next().now_or_never() {
                batch.push(next);
            }

            let mut followup = false;
            let mut stale = false;
            for (owner, run) in batch {
                running.remove(&owner);
                running_access.remove(&owner);
                read_owners.remove(&owner);
                let progress = commit_system_run(memo, &self.inner.state, run, Some(phase)).await;
                commit_budget.record(progress.commits);
                followup |= progress.needs_followup;
                if progress.stale {
                    // A discarded stale run leaves its row memo-invalid;
                    // force it (row-granular) back through planning.
                    systems[owner.system.0].force_replan(&owner.keys);
                }
                stale |= progress.stale;
            }

            if followup {
                phase_changed = true;
            }

            // Replan only when a commit changed the world, a stale run left
            // its row memo-invalid, or a conflict-deferred row is waiting for
            // the access rows this batch released.
            needs_plan = followup || stale || deferred_conflicts;
        }
    }

    /// Awaits the next finished invocation, waking early when a mutator
    /// registers for preemption so the runner reaches a boundary promptly.
    ///
    /// Each stream is polled at most once per task poll: re-polling a
    /// stream whose sibling is empty would defeat `FuturesUnordered`'s
    /// fairness yield and let self-waking futures (`yield_once`) run to
    /// completion within a single driver poll — silently removing
    /// cancellation boundaries.
    async fn await_next_run<F>(
        &self,
        read_runs: &mut FuturesUnordered<F>,
        write_runs: &mut FuturesUnordered<F>,
        allow_preempt: bool,
    ) -> RunEvent
    where
        F: Future<Output = (SystemInvocation, SystemRun)>,
    {
        futures::future::poll_fn(|context| {
            // AtomicWaker protocol: check, register, re-check — a mutator
            // registering between the checks wakes the registered waker.
            if allow_preempt {
                let waiters = &self.inner.preempt_waiters;
                if waiters.load(atomic::Ordering::SeqCst) > 0 {
                    return std::task::Poll::Ready(RunEvent::Preempt);
                }
                self.inner.preempt_signal.register(context.waker());
                if waiters.load(atomic::Ordering::SeqCst) > 0 {
                    return std::task::Poll::Ready(RunEvent::Preempt);
                }
            }

            let read = read_runs.poll_next_unpin(context);
            if let std::task::Poll::Ready(Some(item)) = read {
                return std::task::Poll::Ready(RunEvent::Finished(item));
            }

            let write = write_runs.poll_next_unpin(context);
            if let std::task::Poll::Ready(Some(item)) = write {
                return std::task::Poll::Ready(RunEvent::Finished(item));
            }

            match (read, write) {
                (std::task::Poll::Ready(None), std::task::Poll::Ready(None)) => {
                    std::task::Poll::Ready(RunEvent::Drained)
                }
                _ => std::task::Poll::Pending,
            }
        })
        .await
    }

    /// Pauses the runner at a preemption boundary until every waiting
    /// mutator has applied its write.
    async fn open_preempt_window(&self) {
        {
            let mut state = lock_state(&self.inner.state).await;
            state.preempt_window = true;
            for boundary in state.boundary_waiters.drain(..) {
                let _ = boundary.send(());
            }
        }

        // Wait for every registered preempt waiter to apply, woken by
        // `PreemptWaiter::finish`/`Drop` instead of spin-polling.
        futures::future::poll_fn(|context| {
            if self.inner.preempt_waiters.load(atomic::Ordering::SeqCst) == 0 {
                return std::task::Poll::Ready(());
            }
            self.inner.preempt_done.register(context.waker());
            if self.inner.preempt_waiters.load(atomic::Ordering::SeqCst) == 0 {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;

        let mut state = lock_state(&self.inner.state).await;
        state.preempt_window = false;
    }

    /// Starts a pending generation and returns the immutable inputs needed to
    /// run systems without holding the state lock.
    ///
    /// New inserts that arrive after this point see `next_generation` already
    /// advanced, so they are queued for the following generation rather than
    /// being injected into the snapshot currently running.
    async fn start_evaluation(
        &self,
    ) -> Option<(
        u64,
        Vec<BoxedSystem>,
        HashMap<SystemInvocation, MemoEntry>,
        bool,
    )> {
        let mut state = lock_state(&self.inner.state).await;
        let generation = state.pending_generation.take()?;
        state.world.maybe_roll_plan_epoch();
        let inputs = std::mem::take(&mut state.pending_inputs);

        // Settle-phase inserts deferred from the previous settle land first:
        // they are this run's opening state (gate markers, seeded facts),
        // applied as ordinary owned derived writes.
        let deferred_settle = std::mem::take(&mut state.deferred_settle);
        for (owner, command) in deferred_settle {
            command.apply(&mut state.world, &owner);
        }
        for input in inputs {
            input.apply(&mut state.world);
        }
        state.world.flush_derived_from();
        // Deferred settle inserts are recorded as derived writes; drain
        // them here so the next commit's debug checks (same-phase flag,
        // declared-output honesty) do not misattribute them to that
        // commit's writer.
        let _ = state.world.take_written_derived();

        state.running_generation = Some(generation);
        state.next_generation = generation + 1;
        state.normal_clean = false;
        // A preemptive write applied between generations restarts phase
        // ordering through the Startup slot (spec/epochs.md); dropped-driver
        // hygiene also resets a stale open window here.
        let startup = !state.startup_ran || std::mem::take(&mut state.preempt_restart);
        state.startup_ran = true;
        state.preempt_window = false;

        let systems = state.systems.clone();
        let memo = std::mem::take(&mut state.memo);

        Some((generation, systems, memo, startup))
    }
}

/// Outcome of driving one phase to quiescence.
enum PhaseRun {
    /// The phase converged; the payload reports whether it changed the
    /// world.
    Completed(bool),
    /// A preemption boundary was taken mid-phase: read-only in-flight work
    /// was dropped, writers drained, and waiting mutators applied. The
    /// generation restarts through the Startup slot.
    Preempted(bool),
}

/// Outcome of awaiting the next in-flight invocation.
enum RunEvent {
    Finished((SystemInvocation, SystemRun)),
    /// Both run streams are exhausted.
    Drained,
    /// A mutator registered for preemption; handle the boundary.
    Preempt,
}

/// External-mutation side of the preemption protocol (spec/epochs.md).
///
/// An external `Mut` is preemptive by default: while an epoch is driving,
/// the mutator registers as a preempt waiter (waking the runner's in-flight
/// await) and applies its write only at a boundary — the runner's opened
/// preemption window, or the gap between generations. On an idle bowl it
/// applies immediately.
struct PreemptWaiter {
    bowl: Bowl,
    registered: bool,
    /// A deferred writer never requests preemption: it waits for a natural
    /// boundary (between generations, the epoch's end, or a window opened
    /// by someone else's preemption).
    deferred: bool,
}

impl PreemptWaiter {
    fn new(bowl: &Bowl, deferred: bool) -> Self {
        Self {
            bowl: bowl.clone(),
            registered: false,
            deferred,
        }
    }

    /// Whether the caller is at a valid boundary to apply an external
    /// mutation; registers it as a preempt waiter otherwise (unless
    /// deferred).
    fn boundary_reached(&mut self, state: &mut State) -> bool {
        if state.settling == 0 && state.running_generation.is_none() {
            // Idle bowl: plain live mutation.
            return true;
        }

        if state.running_generation.is_none() || state.preempt_window {
            // Between generations, or the runner paused at an opened
            // preemption boundary: the next generation restarts through
            // `Phase::Startup` so settle-scoped claims are retracted before
            // fresh derivations plan.
            state.preempt_restart = true;
            return true;
        }

        if self.deferred {
            // Wait for the boundary without forcing one.
            return false;
        }

        // Mid-generation: request preemption and wait for the boundary.
        if !self.registered {
            self.registered = true;
            self.bowl
                .inner
                .preempt_waiters
                .fetch_add(1, atomic::Ordering::SeqCst);
            self.bowl.inner.preempt_signal.wake();
        }
        false
    }

    fn finish(&mut self) {
        if self.registered {
            self.registered = false;
            self.bowl
                .inner
                .preempt_waiters
                .fetch_sub(1, atomic::Ordering::SeqCst);
            self.bowl.inner.preempt_done.wake();
        }
    }
}

impl Drop for PreemptWaiter {
    fn drop(&mut self) {
        // A registered mutator dropped mid-wait must deregister or the
        // runner's boundary wait would starve.
        self.finish();
    }
}

/// Marks a settle as an active epoch for its whole duration.
///
/// The entry watermark records which deferred inputs this settle is
/// responsible for: inputs that arrived before it entered are promoted at
/// its epoch boundaries; anything newer stays deferred to preserve the
/// freeze. When the last settler leaves (including a cancelled one),
/// everything left promotes so deferral never becomes loss.
struct EpochGuard {
    bowl: Bowl,
    watermark: u64,
}

impl EpochGuard {
    async fn enter(bowl: &Bowl) -> EpochGuard {
        let mut state = lock_state(&bowl.inner.state).await;
        state.settling += 1;
        let watermark = state.input_seq;
        EpochGuard {
            bowl: bowl.clone(),
            watermark,
        }
    }
}

impl Drop for EpochGuard {
    fn drop(&mut self) {
        // The state lock is never held across an await anywhere in the
        // crate, so a bounded spin acquires it even from a sync drop (same
        // justification as `EvaluationGuard`).
        let mut state = loop {
            if let Some(state) = self.bowl.inner.state.try_lock() {
                break state;
            }
            std::thread::yield_now();
        };
        state.settling -= 1;
        if state.settling == 0 {
            promote_deferred_inputs(&mut state, u64::MAX);
        }
        // The epoch's end is a mutation boundary too: a mutator that
        // registered during the Settle phase (no generation completion
        // follows it) must be woken here or it would starve.
        let boundary_waiters: Vec<_> = state.boundary_waiters.drain(..).collect();
        drop(state);
        for boundary in boundary_waiters {
            let _ = boundary.send(());
        }
    }
}

/// Returns the current world snapshot under an already-held state lock,
/// sharing the cached one when the world has not changed.
fn snapshot_locked(state: &mut State) -> Arc<Snapshot> {
    let key = snapshot_key(&state.world);
    if let Some((cached_key, snapshot)) = &state.snapshot_cache {
        if *cached_key == key {
            return Arc::clone(snapshot);
        }
    }
    let snapshot = Arc::new(state.world.clone());
    state.snapshot_cache = Some((key, Arc::clone(&snapshot)));
    snapshot
}

/// Retains a snapshot of the settled world for `last_settled` scoops,
/// sharing the cached snapshot when the world has not moved.
fn refresh_settled_snapshot(inner: &Inner, state: &mut State) {
    let snapshot = snapshot_locked(state);
    state.settled_snapshot = Some(Arc::clone(&snapshot));
    *inner
        .settled_read
        .write()
        .expect("settled-read slot poisoned") = Some(snapshot);
}

/// Promotes deferred inputs whose arrival tag is covered by `watermark`.
/// Returns whether anything was promoted.
fn promote_deferred_inputs(state: &mut State, watermark: u64) -> bool {
    if state.deferred_inputs.is_empty() {
        return false;
    }

    let mut kept = Vec::new();
    let mut promoted = false;
    for (tag, input) in state.deferred_inputs.drain(..).collect::<Vec<_>>() {
        if tag <= watermark {
            state.pending_inputs.push(input);
            promoted = true;
        } else {
            kept.push((tag, input));
        }
    }
    state.deferred_inputs = kept;

    if promoted {
        let next_generation = state.next_generation;
        state.pending_generation.get_or_insert(next_generation);
    }
    promoted
}

/// Restores evaluation bookkeeping when a driver future is dropped mid-run.
///
/// Holds the memo table for the duration of one evaluation. On normal
/// completion the runner takes it back with [`EvaluationGuard::complete`]; on
/// an abandoned run, `Drop` returns the memo to state, re-queues the
/// generation, and wakes waiters so another caller can drive it.
struct EvaluationGuard {
    bowl: Bowl,
    generation: u64,
    memo: Option<HashMap<SystemInvocation, MemoEntry>>,
}

impl EvaluationGuard {
    fn memo_mut(&mut self) -> &mut HashMap<SystemInvocation, MemoEntry> {
        self.memo
            .as_mut()
            .expect("evaluation guard already completed")
    }

    fn complete(mut self) -> HashMap<SystemInvocation, MemoEntry> {
        self.memo
            .take()
            .expect("evaluation guard already completed")
    }
}

impl Drop for EvaluationGuard {
    fn drop(&mut self) {
        let Some(memo) = self.memo.take() else {
            return;
        };

        // The state guard is never held across an await point anywhere in the
        // crate, so at drop time the lock is either free or held by a task
        // mid-poll on another thread for a short synchronous section.
        let mut state = loop {
            if let Some(state) = self.bowl.inner.state.try_lock() {
                break state;
            }
            std::thread::yield_now();
        };

        state.memo = memo;
        state.running_generation = None;
        if state.pending_generation.is_none() {
            state.pending_generation = Some(self.generation);
        }
        // The abandoned run advanced planned marks and delta cursors for
        // work it never committed; force every system through a full plan
        // so the promoted driver sees the uncommitted rows again.
        for system in &state.systems {
            system.reset_full();
        }
        // Abandoned run: wake everyone regardless of target, so a waiter
        // can be promoted to a fresh driver.
        let waiters = std::mem::take(&mut state.waiters);
        drop(state);

        for (_, waiter) in waiters {
            let _ = waiter.send(());
        }
    }
}

fn apply_component_mutation<T, F, R>(state: &mut State, entity: Entity, f: F) -> TryUpdate<R, F>
where
    T: Component,
    F: FnOnce(&mut T) -> R,
{
    let outcome = state.world.try_update_component_live::<T, F, R>(entity, f);

    if let TryUpdate::Applied { changed: true, .. } = &outcome {
        state.normal_clean = false;
        if state.pending_generation.is_none() {
            let next_generation = state.next_generation;
            state.pending_generation = Some(next_generation);
        }
    }

    outcome
}

/// Suspends once so other tasks (typically a guard holder we are waiting on)
/// can make progress before a retry.
async fn yield_once() {
    let mut yielded = false;
    futures::future::poll_fn(move |context| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

fn conflicts_with_running(
    access: &[Access],
    running_access: &HashMap<SystemInvocation, Vec<Access>>,
) -> bool {
    running_access.values().any(|running| {
        access
            .iter()
            .any(|candidate| running.iter().any(|active| candidate.conflicts(*active)))
    })
}

/// Registration-time same-phase analysis (debug builds, schema required
/// for precision): when a newly registered system could produce an entity
/// that a same-phase system's `View` matches — or vice versa — warn at
/// `add_system`, before any commit ever races. The check goes through the
/// schema's shapes, so shared vocabulary components on unrelated shapes do
/// not trip it. A warning rather than a refusal because marker-gated
/// same-phase consumers (whose gate defers them a generation) are
/// legitimate and undetectable statically; the commit-time flag remains
/// the precise enforcement with its dynamic zero-row exemption.
fn warn_same_phase_conflicts(state: &State, new: &BoxedSystem) {
    let Some(schema) = state.schema.as_ref() else {
        return;
    };
    for existing in &state.systems {
        if existing.phase != new.phase {
            continue;
        }
        warn_producer_viewer_pair(schema, new, existing);
        warn_producer_viewer_pair(schema, existing, new);
    }
}

fn warn_producer_viewer_pair(
    schema: &[ShapeDesc],
    producer: &BoxedSystem,
    viewer: &BoxedSystem,
) {
    let Some(declared) = producer.declared_outputs.as_ref() else {
        return;
    };
    for required in viewer.view_sets.iter() {
        if required.is_empty() {
            continue;
        }
        // The producer must write a component of the view's required set
        // (the potentially completing write), and some declared shape must
        // be able to carry the whole required set.
        let touches = required.iter().any(|type_id| declared.contains(type_id));
        if !touches {
            continue;
        }
        let matchable = schema.iter().any(|shape| {
            required.iter().all(|type_id| shape.contains(*type_id))
                && declared.iter().any(|type_id| shape.contains(*type_id))
        });
        if matchable {
            tracing::warn!(
                producer = producer.name,
                viewer = viewer.name,
                phase = ?producer.phase,
                "same-phase ambient consumption: `{}` declares outputs that can \
                 complete an entity `{}` Views in the same phase — move one across \
                 a phase boundary or make the read tracked (marker-gated consumers \
                 can ignore this)",
                producer.name,
                viewer.name,
            );
        }
    }
}

/// Debug-build shape conformance for one entity's write bundle: the bundle
/// must fit inside some declared shape, and after the write that shape's
/// required components must all be present on the entity. Panics with the
/// nearest shape and what is missing or extra.
fn check_shape_conformance(
    world: &World,
    schema: &[ShapeDesc],
    entity: Entity,
    bundle: &[(TypeId, &'static str)],
    writer: &'static str,
) {
    // Shapes whose component set covers the whole bundle are candidates.
    // Shapes may overlap, so an incomplete candidate is only a failure if
    // no other candidate completes; the panic names the nearest one.
    let mut best: Option<(&ShapeDesc, usize)> = None;
    let mut nearest_incomplete: Option<(&ShapeDesc, Vec<&'static str>)> = None;
    for shape in schema {
        let covered = bundle
            .iter()
            .filter(|(type_id, _)| shape.contains(*type_id))
            .count();
        if covered == bundle.len() {
            // Candidate: check required completeness post-apply.
            let missing = shape
                .required
                .iter()
                .filter(|(type_id, _)| !world.has_dyn(*type_id, entity))
                .map(|(_, name)| *name)
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return;
            }
            if nearest_incomplete
                .as_ref()
                .is_none_or(|(_, best_missing)| missing.len() < best_missing.len())
            {
                nearest_incomplete = Some((shape, missing));
            }
            continue;
        }
        if best.is_none_or(|(_, best_covered)| covered > best_covered) {
            best = Some((shape, covered));
        }
    }

    if let Some((shape, missing)) = nearest_incomplete {
        panic!(
            "system `{writer}` wrote entity {} as shape `{}` but left required \
             component(s) missing: {}",
            entity.raw(),
            shape.name,
            missing.join(", ")
        );
    }

    let written = bundle
        .iter()
        .map(|(_, name)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    match best {
        Some((shape, _)) => {
            let extra = bundle
                .iter()
                .filter(|(type_id, _)| !shape.contains(*type_id))
                .map(|(_, name)| *name)
                .collect::<Vec<_>>();
            panic!(
                "system `{writer}` wrote [{written}] on entity {}, which matches no \
                 declared shape; nearest is `{}`, which does not include: {}",
                entity.raw(),
                shape.name,
                extra.join(", ")
            );
        }
        None => panic!(
            "system `{writer}` wrote [{written}] on entity {} but the registered \
             schema declares no shapes",
            entity.raw()
        ),
    }
}

#[derive(Default)]
struct CommitProgress {
    needs_followup: bool,
    /// The run was discarded because its captured deps went stale before the
    /// commit. The owning row is still memo-invalid and must be replanned.
    stale: bool,
    commits: u64,
}

struct CommitBudget {
    limit: CommitLimit,
    commits: u64,
}

impl CommitBudget {
    fn new(limit: CommitLimit) -> Self {
        Self { limit, commits: 0 }
    }

    fn record(&mut self, commits: u64) {
        if commits == 0 {
            return;
        }

        self.commits = self
            .commits
            .checked_add(commits)
            .expect("commit budget counter overflowed");

        let CommitLimit::Max(limit) = self.limit else {
            return;
        };

        assert!(
            self.commits <= limit,
            "bowl commit limit exceeded: accepted {} non-cleanup commits while trying to settle; current limit is {limit}",
            self.commits
        );
    }
}

async fn commit_system_runs(
    memo: &mut HashMap<SystemInvocation, MemoEntry>,
    state: &Mutex<State>,
    runs: Vec<SystemRun>,
) -> CommitProgress {
    let _commit_timer = ScopeTimer(&COMMIT_NANOS, std::time::Instant::now());
    let mut progress = CommitProgress::default();
    for run in runs {
        let next = commit_system_run(memo, state, run, None).await;
        progress.needs_followup |= next.needs_followup;
        progress.commits += next.commits;
    }

    progress
}

async fn commit_system_run(
    memo: &mut HashMap<SystemInvocation, MemoEntry>,
    state: &Mutex<State>,
    run: SystemRun,
    phase: Option<Phase>,
) -> CommitProgress {
    let defer_inserts = phase == Some(Phase::Settle);
    let outputs = run.outputs;
    let memo_updates = run.memo_updates;
    let writes = run.writes;

    let mut state = lock_state(state).await;
    let before_revision = state.world.revision_raw();
    let before_mutations = state.world.mutations_raw();

    if !memo_updates
        .iter()
        .all(|(_owner, entry)| entry.is_current(&state.world))
    {
        // The commands are discarded, but in-place `MutRef` writes already
        // happened and are not revocable: reconcile their revisions so
        // downstream consumers still observe the change.
        state.world.reconcile_written(&writes);
        let needs_followup = state.world.revision_raw() != before_revision;
        return CommitProgress {
            needs_followup,
            stale: true,
            commits: u64::from(needs_followup),
        };
    }

    // Replace outputs by diffing: commands apply over the invocation's old
    // outputs so unchanged fingerprints keep their revisions, then whatever
    // the rerun did not re-emit is removed.
    //
    // With `defer_inserts` (Phase::Settle commits), inserts and spawns are
    // held back as next-run inputs instead of applying now. The stale sweep
    // still runs against what applied immediately, so a settle system that
    // re-defers the same output each settle sees it removed at settle and
    // reinstated when the next run starts — emergent ephemerality.
    for output in outputs {
        let writer = output.owner.system;
        let previous = state.world.take_derived_outputs(&output.owner);
        let mut deferred: Vec<(SystemInvocation, Box<dyn CommandOp>)> = Vec::new();
        for command in output.commands {
            if defer_inserts && command.defers_at_settle() {
                deferred.push((output.owner.clone(), command));
                continue;
            }
            command.apply(&mut state.world, &output.owner);
        }
        // Anchor capture is deferred to buffer end; flush before the stale
        // sweep so re-emitted DerivedFrom entries count as re-emitted.
        state.world.flush_derived_from();
        state.world.finish_derived_spawns(&output.owner);
        state.world.remove_derived_stale(&output.owner, previous);
        state.deferred_settle.append(&mut deferred);

        // Debug flag: producing an entity that a same-phase system reads
        // ambiently is a silent race — the viewer may already have run and
        // will never replan for this commit. The check is entity-granular:
        // a write races a view only if the written entity ends up carrying
        // *every* component the view requires (shared vocabulary
        // components on unrelated entities do not trip it, and a write
        // that completes a previously partial row does). Tracked consumers
        // are exempt (their deps change; the replanner reruns them), and
        // so are viewers with no matched rows in this generation
        // (marker-gated consumers whose gate defers them to a later
        // generation cannot have raced anything).
        if cfg!(debug_assertions) {
            let written = state.world.take_written_derived();

            // (The declared-output honesty backstop that used to live here
            // is gone: with no public wildcard and strict typed `Commands`,
            // undeclared emission is unrepresentable outside the engine's
            // own test doubles — the type system carries the contract.)

            // Schema conformance: each entity's write bundle must fit one
            // declared shape, and after the write that shape's required
            // components must all be present — spawns arrive complete,
            // incremental writes may finish a shape another commit
            // started.
            if let Some(schema) = state.schema.clone() {
                let mut per_entity: HashMap<Entity, Vec<(TypeId, &'static str)>> = HashMap::new();
                for (type_id, entity, type_name) in &written {
                    per_entity
                        .entry(*entity)
                        .or_default()
                        .push((*type_id, type_name));
                }
                for (entity, bundle) in per_entity {
                    check_shape_conformance(
                        &state.world,
                        &schema,
                        entity,
                        &bundle,
                        state.systems[writer.0].name,
                    );
                }
            }

            if let Some(phase) = phase {
                for (type_id, entity, type_name) in written {
                    for (index, system) in state.systems.iter().enumerate() {
                        if index == writer.0 || system.phase != phase {
                            continue;
                        }
                        let races = system.view_sets.iter().any(|required| {
                            required.contains(&type_id)
                                && required
                                    .iter()
                                    .all(|component| state.world.has_dyn(*component, entity))
                        });
                        if races && system.runnable.row_counts(&state.world, memo).0 > 0 {
                            panic!(
                                "component `{type_name}` completed an entity that a \
                                 same-phase ({phase:?}) `View` matches: the producing \
                                 commit is produced and ambiently consumed in the same \
                                 phase and races the viewer. Move the producer or the \
                                 consumer across a phase boundary, or make the read a \
                                 tracked `Query` input."
                            );
                        }
                    }
                }
            }
        }
    }

    state.world.reconcile_written(&writes);
    for (owner, mut entry) in memo_updates {
        entry.refresh_written(&state.world, &writes);
        memo.insert(owner, entry);
    }

    // Memo entries keyed by removed entities can never match a planned row
    // again; drop them so long-running bowls do not accumulate dead entries.
    let removed = state.world.take_removed_entities();
    if !removed.is_empty() {
        let keys = removed.into_iter().collect::<HashSet<_>>();
        remove_memo_touched_by(memo, &keys);
    }

    let needs_followup = state.world.revision_raw() != before_revision
        || state.world.mutations_raw() != before_mutations;
    CommitProgress {
        needs_followup,
        stale: false,
        commits: u64::from(needs_followup),
    }
}

fn remove_memo_touched_by(memo: &mut HashMap<SystemInvocation, MemoEntry>, keys: &HashSet<Entity>) {
    memo.retain(|owner, _| !owner.keys.iter().any(|key| keys.contains(key)));
}

/// Whether the bowl is fully settled: no pending or running generation and
/// no tracked change since the last settle completed.
fn bowl_is_settled(state: &State) -> bool {
    state.pending_generation.is_none()
        && state.running_generation.is_none()
        && state.world.revision_raw() == state.settled_revision
}

fn cleanup_bound_entity(state: &mut State, entity: Entity) {
    let mut frontier = HashSet::from([entity]);
    let mut removed_entities = HashSet::new();

    while !frontier.is_empty() {
        remove_memo_touched_by(&mut state.memo, &frontier);

        let removed = state.world.remove_derived_touched_by(&frontier);
        let mut next_frontier = HashSet::new();

        for entity in removed {
            if removed_entities.insert(entity) {
                next_frontier.insert(entity);
            }
        }

        frontier = next_frontier;
    }

    let keys = HashSet::from([entity]);
    let removed_owners = state.world.remove_entity(entity);
    remove_memo_touched_by(&mut state.memo, &keys);

    for owner in removed_owners {
        state.world.remove_derived_owned(&owner);
        state.memo.remove(&owner);
    }
}

macro_rules! impl_take_bundle_tuple {
    ($($T:ident),*) => {
        impl<$($T: TakeBundle),*> TakeBundle for ($($T,)*)
        {
            type Output = ($($T::Output,)*);

            #[allow(non_snake_case)]
            fn take(world: &mut World, entity: Entity) -> Result<Self::Output, TakeError> {
                Ok(($($T::take(world, entity)?,)*))
            }

            fn blocked(world: &World, entity: Entity) -> bool {
                false $(|| $T::blocked(world, entity))*
            }
        }
    };
}

all_tuples!(impl_take_bundle_tuple, 2, 8, T);

macro_rules! impl_external_scoop_tuple {
    ($($S:ident),*) => {
        impl<$($S: ExternalScoop),*> ExternalScoop for ($($S,)*)
        {
            type Output = ($($S::Output,)*);

            fn materialize(
                bowl: &Bowl,
                snapshot: &Arc<Snapshot>,
                args: &QueryArgs,
                scope: Option<TypeId>,
            ) -> Self::Output {
                ($($S::materialize(bowl, snapshot, args, scope),)*)
            }
        }
    };
}

all_tuples!(impl_external_scoop_tuple, 2, 8, S);

/// A group of components inserted onto one newly-created entity.
///
/// This trait is implemented for tuples of components. It is public because it
/// appears in [`Bowl::insert`]'s bounds, but users normally interact with it by
/// passing tuples:
///
/// ```text
/// bowl.insert((FilePath(path), FileText(text))).await
/// ```
pub trait Bundle: Send + 'static {
    #[doc(hidden)]
    fn singleton_key() -> Option<TypeId>;

    #[doc(hidden)]
    fn queue(self, entity: Entity, commands: &mut Vec<Box<dyn BaseCommandOp>>);

    #[doc(hidden)]
    fn insert_derived(self, world: &mut World, entity: Entity, owner: SystemInvocation);
}

fn collect_singleton_key<T>(key: &mut Option<TypeId>)
where
    T: Component,
{
    let Some(next_key) = T::singleton_key() else {
        return;
    };

    if key.replace(next_key).is_some() {
        panic!("bundles can contain at most one singleton marker");
    }
}

macro_rules! impl_bundle {
    ($($T:ident),*) => {
        impl<$($T: Component),*> Bundle for ($($T,)*)
        {
            fn singleton_key() -> Option<TypeId> {
                let mut key = None;
                $(collect_singleton_key::<$T>(&mut key);)*
                key
            }

            #[allow(non_snake_case)]
            fn queue(self, entity: Entity, commands: &mut Vec<Box<dyn BaseCommandOp>>) {
                let ($($T,)*) = self;
                $(commands.push(Box::new(InsertBaseCommand { entity, value: $T }));)*
            }

            #[allow(non_snake_case)]
            fn insert_derived(self, world: &mut World, entity: Entity, owner: SystemInvocation) {
                let ($($T,)*) = self;
                $(world.insert_derived(entity, $T, owner.clone());)*
            }
        }
    };
}

all_tuples!(impl_bundle, 1, 8, T);

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use futures::executor::block_on;

    use crate::declare::Anything;

    use crate::{
        And, Bowl, Commands, CommitLimit, Component, ComponentHookContext, Cow, DerivedFrom,
        Entity, Eq, Gte, In, Mut, MutRef, Named, Phase, Query, RelationshipEdge,
        RelationshipRetraction, RelationshipTarget, Singleton, SystemExt, View, Where, With,
        Without, cleanup_stale_derived, hash_component, relationship_retractions_for,
    };

    struct A(u32);
    struct B(u32);
    struct C(u32);
    struct D(u32);
    struct Count(usize);
    struct Sum(u32);
    struct Request;
    struct Answer(u32);
    struct NonCloneAnswer(u32);
    struct Note;
    #[derive(Clone, PartialEq)]
    struct MutableA(u32);
    struct Hooked;
    struct UntrackedMarker;
    #[derive(Clone, PartialEq)]
    struct Label(&'static str);
    #[derive(Clone, PartialEq, PartialOrd)]
    struct Rank(u32);
    struct FingerprintedRank(u32);

    impl Component for A {}
    impl Component for B {}
    impl Component for C {}
    impl Component for D {}
    impl Component for Count {}
    impl Component for Sum {}
    impl Component for Request {}
    impl Component for Answer {}
    impl Component for NonCloneAnswer {}
    impl Component for Note {}
    impl Component for MutableA {}
    impl Component for Label {}
    impl Component for Rank {}
    impl Component for FingerprintedRank {
        fn fingerprint(&self) -> Option<u64> {
            Some(self.0 as u64)
        }
    }
    impl Component for UntrackedMarker {
        fn tracked() -> bool {
            false
        }
    }
    impl Component for Hooked {
        fn on_insert(context: ComponentHookContext) {
            assert!(context.entity().raw() < u64::MAX);
            HOOK_INSERTS.fetch_add(1, Ordering::SeqCst);
        }

        fn on_remove(context: ComponentHookContext) {
            assert!(context.entity().raw() < u64::MAX);
            HOOK_REMOVES.fetch_add(1, Ordering::SeqCst);
        }

        fn on_entity_remove(context: ComponentHookContext) {
            assert!(context.entity().raw() < u64::MAX);
            HOOK_ENTITY_REMOVES.fetch_add(1, Ordering::SeqCst);
        }
    }

    static REQUEST_RUNS: AtomicUsize = AtomicUsize::new(0);
    static CLEAN_RUNS: AtomicUsize = AtomicUsize::new(0);
    static HOOK_INSERTS: AtomicUsize = AtomicUsize::new(0);
    static HOOK_REMOVES: AtomicUsize = AtomicUsize::new(0);
    static HOOK_ENTITY_REMOVES: AtomicUsize = AtomicUsize::new(0);
    static HOOK_TEST_LOCK: StdMutex<()> = StdMutex::new(());
    static REQUEST_TEST_LOCK: StdMutex<()> = StdMutex::new(());
    static ACCESS_TEST_LOCK: StdMutex<()> = StdMutex::new(());
    static ACTIVE_READERS: AtomicUsize = AtomicUsize::new(0);
    static ACTIVE_WRITERS: AtomicUsize = AtomicUsize::new(0);
    static MAX_ACTIVE_READERS: AtomicUsize = AtomicUsize::new(0);
    static MAX_ACTIVE_WRITERS: AtomicUsize = AtomicUsize::new(0);
    static PHASE_LOG: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());
    static SYSTEM_HOOK_LOG: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());

    async fn yield_once() {
        let mut yielded = false;
        futures::future::poll_fn(move |context| {
            if yielded {
                std::task::Poll::Ready(())
            } else {
                yielded = true;
                context.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await;
    }

    fn reset_access_counters() {
        ACTIVE_READERS.store(0, Ordering::SeqCst);
        ACTIVE_WRITERS.store(0, Ordering::SeqCst);
        MAX_ACTIVE_READERS.store(0, Ordering::SeqCst);
        MAX_ACTIVE_WRITERS.store(0, Ordering::SeqCst);
    }

    fn record_max(atomic: &AtomicUsize, value: usize) {
        let mut current = atomic.load(Ordering::SeqCst);
        while value > current {
            match atomic.compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    async fn make_b(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (entity, a) = query.item();
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn make_b_with_hook_log(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (entity, a) = query.item();
        SYSTEM_HOOK_LOG
            .lock()
            .expect("system hook log lock poisoned")
            .push("row");
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn make_b_uncounted(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (entity, a) = query.item();
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn make_c(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (entity, a) = query.item();
        CLEAN_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(C(a.0 + 1));
    }

    async fn make_c_from_b(query: Query<(Entity, &B)>, mut commands: Commands<Anything>) {
        let (entity, b) = query.item();
        commands.entity(entity).insert(C(b.0 + 1));
    }

    async fn count_bs(
        query: Query<(Entity, &A)>,
        bs: View<'_, (Entity, &B)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(bs.len()));
    }

    async fn count_cs(
        query: Query<(Entity, &A)>,
        cs: View<'_, (Entity, &C)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(cs.len()));
    }

    async fn spawn_b(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (_entity, a) = query.item();
        commands.insert((B(a.0 + 1),));
    }

    async fn spawn_a_from_a(query: Query<Entity, With<A>>, mut commands: Commands<Anything>) {
        let _entity = query.item();
        commands.insert((A(0),));
    }

    async fn count_tagged_a(query: Query<(Entity, &A), With<Request>>, mut commands: Commands<Anything>) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(1));
    }

    async fn sum_a_b(
        a_query: Query<(Entity, &A)>,
        b_query: Query<(Entity, &B)>,
        mut commands: Commands<Anything>,
    ) {
        let (a_entity, a) = a_query.item();
        let (b_entity, b) = b_query.item();
        commands.entity(a_entity).insert(Sum(a.0 + b.0));
        commands.entity(b_entity).insert(Sum(a.0 + b.0));
    }

    async fn count_a_when_c_exists(
        a_query: Query<(Entity, &A)>,
        c_query: Query<(Entity, &C)>,
        bs: View<'_, (Entity, &B)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _a) = a_query.item();
        let (_ready, _c) = c_query.item();
        commands.entity(entity).insert(Count(bs.len()));
    }

    async fn write_singleton_count(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (_entity, a) = query.item();
        commands.insert((Singleton::<Count>::new(), Count(a.0 as usize)));
    }

    async fn copy_rank_to_count(query: Query<(Entity, &Rank)>, mut commands: Commands<Anything>) {
        let (entity, rank) = query.item();
        commands.entity(entity).insert(Count(rank.0 as usize));
    }

    async fn copy_rank_to_count_counted(query: Query<(Entity, &Rank)>, mut commands: Commands<Anything>) {
        let (entity, rank) = query.item();
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(Count(rank.0 as usize));
    }

    async fn copy_fingerprinted_rank_to_count(
        query: Query<(Entity, &FingerprintedRank)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, rank) = query.item();
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(Count(rank.0 as usize));
    }

    async fn read_rank_for_access_test(query: Query<(Entity, &Rank)>) {
        let (_entity, _rank) = query.item();
        let readers = ACTIVE_READERS.fetch_add(1, Ordering::SeqCst) + 1;
        record_max(&MAX_ACTIVE_READERS, readers);
        assert_eq!(ACTIVE_WRITERS.load(Ordering::SeqCst), 0);
        yield_once().await;
        assert_eq!(ACTIVE_WRITERS.load(Ordering::SeqCst), 0);
        ACTIVE_READERS.fetch_sub(1, Ordering::SeqCst);
    }

    async fn read_rank_for_access_test_again(query: Query<(Entity, &Rank)>) {
        read_rank_for_access_test(query).await;
    }

    async fn write_rank_for_access_test(query: Query<(Entity, MutRef<'_, Rank>)>) {
        let (_entity, rank) = query.item();
        let writers = ACTIVE_WRITERS.fetch_add(1, Ordering::SeqCst) + 1;
        record_max(&MAX_ACTIVE_WRITERS, writers);
        assert_eq!(ACTIVE_READERS.load(Ordering::SeqCst), 0);
        assert!(rank.entity().raw() < u64::MAX);
        yield_once().await;
        assert_eq!(ACTIVE_READERS.load(Ordering::SeqCst), 0);
        ACTIVE_WRITERS.fetch_sub(1, Ordering::SeqCst);
    }

    async fn startup_phase(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (entity, _a) = query.item();
        PHASE_LOG
            .lock()
            .expect("phase log lock poisoned")
            .push("startup");
        commands.entity(entity).insert(B(1));
    }

    async fn evaluate_phase(query: Query<(Entity, &A)>, bs: View<'_, (Entity, &B)>) {
        let (_entity, _a) = query.item();
        if bs.len() == 1 {
            PHASE_LOG
                .lock()
                .expect("phase log lock poisoned")
                .push("evaluate-after-startup");
        }
    }

    async fn cleanup_phase(query: Query<(Entity, &A)>, bs: View<'_, (Entity, &B)>) {
        let (_entity, _a) = query.item();
        if bs.len() == 1 {
            PHASE_LOG
                .lock()
                .expect("phase log lock poisoned")
                .push("cleanup");
        }
    }

    async fn remove_hooked_entity(query: Query<(Entity, &Hooked)>, mut commands: Commands<Anything>) {
        let (entity, _hooked) = query.item();
        commands.remove(entity);
    }

    async fn mark_b_processed(query: Query<(Entity, &B)>, mut commands: Commands<Anything>) {
        let (entity, _b) = query.item();
        commands.entity(entity).insert(D(1));
    }

    async fn count_after_note(
        _: Query<Entity, With<Note>>,
        query: Query<(Entity, &A)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(1));
    }

    async fn count_bs_after_note(
        _: Query<Entity, With<Note>>,
        query: Query<(Entity, &D)>,
        processed: View<'_, (Entity, &D)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _d) = query.item();
        commands.entity(entity).insert(Count(processed.len()));
    }

    async fn answer_after_untracked_marker(
        _: Query<Entity, With<UntrackedMarker>>,
        query: Query<(Entity, &Request)>,
        processed: View<'_, (Entity, &D)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _request) = query.item();
        commands
            .entity(entity)
            .insert(Answer(processed.len() as u32));
    }

    async fn cleanup_untracked_marker(
        query: Query<Entity, With<UntrackedMarker>>,
        mut commands: Commands<Anything>,
    ) {
        commands.remove(query.item());
    }

    async fn mixed_param_system(
        a_query: Query<(Entity, &A)>,
        bs: View<'_, (Entity, &B)>,
        mut commands: Commands<Anything>,
        c_query: Query<(Entity, &C)>,
        d_query: Query<(Entity, &D)>,
        counts: View<'_, (Entity, &Count)>,
    ) {
        let (entity, a) = a_query.item();
        let (_c_entity, c) = c_query.item();
        let (_d_entity, d) = d_query.item();
        commands.entity(entity).insert(Sum(a.0
            + c.0
            + d.0
            + bs.len() as u32
            + counts.len() as u32));
    }

    async fn answer_request(query: Query<(Entity, &Request)>, mut commands: Commands<Anything>) {
        let (entity, _request) = query.item();
        commands.entity(entity).insert(Answer(42));
    }

    async fn answer_request_with_non_clone(
        query: Query<(Entity, &Request)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _request) = query.item();
        commands.entity(entity).insert(NonCloneAnswer(42));
    }

    async fn make_derived_from_answer_from_view(
        query: Query<(Entity, &Request)>,
        values: View<'_, (Entity, &MutableA)>,
        mut commands: Commands<Anything>,
    ) {
        let (_request, _request_marker) = query.item();
        let (entity, a) = values.iter().next().unwrap();
        commands.insert((DerivedFrom::new(entity), Answer(a.0)));
    }

    async fn make_multi_derived_from_answer_from_view(
        query: Query<(Entity, &Request)>,
        values: View<'_, (Entity, &MutableA)>,
        labels: View<'_, (Entity, &Label)>,
        mut commands: Commands<Anything>,
    ) {
        let (_request, _request_marker) = query.item();
        let (value_entity, value) = values.iter().next().unwrap();
        let (label_entity, _label) = labels.iter().next().unwrap();
        commands.insert((
            DerivedFrom::many([value_entity, label_entity]),
            Answer(value.0),
        ));
    }

    struct Doomed;
    impl Component for Doomed {}

    async fn make_b_after_yield(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        yield_once().await;
        let (entity, a) = query.item();
        commands.entity(entity).insert(B(a.0 + 1));
    }

    #[test]
    fn cancelled_evaluation_driver_does_not_wedge_the_bowl() {
        use std::future::IntoFuture;
        use std::task::Context;

        let bowl = Bowl::builder()
            .system(make_b_after_yield)
            .build();
        block_on(async {
            bowl.insert((A(1),)).await;
        });

        // Become the evaluation driver and suspend mid-run: one poll takes the
        // runner lock, consumes the pending generation, takes the memo table,
        // and parks inside the phase loop at the system's yield point. Then
        // drop the driver, simulating a cancelled caller (LSP cancel, timeout).
        {
            let waker = futures::task::noop_waker();
            let mut context = Context::from_waker(&waker);
            let mut driver = bowl.scoop::<Query<(Entity, &B)>>().into_future();
            assert!(
                driver.as_mut().poll(&mut context).is_pending(),
                "driver should suspend mid-evaluation"
            );
        }

        // A subsequent caller must still be able to drive the bowl to a
        // settled result. Run it on another thread so a wedged bowl shows up
        // as a timeout instead of hanging the test suite.
        let (sender, receiver) = std::sync::mpsc::channel();
        let reader = bowl.clone();
        std::thread::spawn(move || {
            let rows = block_on(async { reader.scoop::<Query<(Entity, &B)>>().await.len() });
            let _ = sender.send(rows);
        });

        let rows = receiver
            .recv_timeout(std::time::Duration::from_secs(3))
            .expect("bowl wedged after the evaluation driver was cancelled");
        assert_eq!(rows, 1);
    }

    async fn increment_once(
        query: Query<(Entity, MutRef<'_, MutableA>), Without<Note>>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, mut a) = query.item();
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
        a.0 += 1;
        commands.entity(entity).insert(Note);
    }

    #[test]
    fn system_mut_applies_non_idempotent_mutation_exactly_once() {
        block_on(async {
            let _guard = REQUEST_TEST_LOCK
                .lock()
                .expect("request test lock poisoned");
            REQUEST_RUNS.store(0, Ordering::SeqCst);

            let bowl = Bowl::builder()
                .system(increment_once)
                .build();
            bowl.insert((MutableA(0),)).await;

            let values = bowl.scoop::<Query<(Entity, &MutableA)>>().await;
            assert_eq!(values.collect()[0].1.0, 1);
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);
        });
    }

    async fn set_rank_unconditionally(query: Query<(Entity, MutRef<'_, Rank>)>) {
        let (_entity, mut rank) = query.item();
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
        rank.0 = 42;
    }

    #[test]
    fn system_mut_write_does_not_invalidate_its_own_memo() {
        block_on(async {
            let _guard = REQUEST_TEST_LOCK
                .lock()
                .expect("request test lock poisoned");
            REQUEST_RUNS.store(0, Ordering::SeqCst);

            let bowl = Bowl::builder()
                .system(set_rank_unconditionally)
                .build();
            bowl.insert((Rank(1),)).await;

            // A system that always writes its Mut row must still settle after
            // one run: the commit absorbs the row's own write into the memo.
            let ranks = bowl.scoop::<Query<(Entity, &Rank)>>().await;
            assert_eq!(ranks.collect()[0].1.0, 42);
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);
        });
    }

    async fn remove_doomed(query: Query<Entity, With<Doomed>>, mut commands: Commands<Anything>) {
        commands.remove(query.item());
    }

    #[test]
    fn removing_an_entity_purges_its_memo_entries() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(make_b_uncounted)
                .system(remove_doomed)
                .build();
            let entity = bowl.insert((A(1), Doomed)).await.entity();

            assert_eq!(bowl.scoop::<Query<(Entity, &A)>>().await.len(), 0);

            let state = super::lock_state(&bowl.inner.state).await;
            assert!(
                state
                    .memo
                    .keys()
                    .all(|owner| !owner.keys.contains(&entity)),
                "memo still holds entries keyed by the removed entity"
            );
        });
    }

    async fn spawn_b_note_from_a(query: Query<(Entity, &MutableA)>, mut commands: Commands<Anything>) {
        let (entity, a) = query.item();
        commands.insert((DerivedFrom::new(entity), B(a.0 % 2)));
    }

    #[test]
    fn rerun_replaces_spawned_outputs_and_reuses_entity_ids() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(spawn_b_note_from_a)
                .build();
            let source = bowl.insert((MutableA(0),)).await.entity();

            let first = bowl.scoop::<Query<(Entity, &B)>>().await;
            assert_eq!(first.len(), 1);
            let first_entity = first.collect()[0].0;

            for round in 1..4u32 {
                bowl.scoop::<Query<(Entity, Cow<MutableA>)>>()
                    .for_each(|(_, a)| a.0 += 2)
                    .await;

                let notes = bowl.scoop::<Query<(Entity, &B)>>().await;
                // Old spawned outputs must be replaced, not accumulated, and
                // idempotent reruns keep the same derived entity id.
                assert_eq!(notes.len(), 1, "round {round}");
                assert_eq!(notes.collect()[0].0, first_entity, "round {round}");
            }

            assert_ne!(first_entity, source);
        });
    }

    #[test]
    fn query_runs_pending_generation() {
        block_on(async {
            let _guard = REQUEST_TEST_LOCK
                .lock()
                .expect("request test lock poisoned");
            REQUEST_RUNS.store(0, Ordering::SeqCst);
            let bowl = Bowl::builder()
                .system(make_b)
                .build();

            let inserted = bowl.insert((A(41),)).await;
            let result = bowl.scoop::<Query<(Entity, &B)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, inserted.entity());
            assert_eq!(rows[0].1.0, 42);
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn clean_query_does_not_rerun_systems() {
        block_on(async {
            CLEAN_RUNS.store(0, Ordering::SeqCst);
            let bowl = Bowl::builder()
                .system(make_c)
                .build();

            bowl.insert((A(1),)).await;
            let result = bowl.scoop::<Query<(Entity, &C)>>().await;
            let rows = result.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 2);
            assert_eq!(CLEAN_RUNS.load(Ordering::SeqCst), 1);

            assert_eq!(bowl.scoop::<Query<(Entity, &C)>>().await.len(), 1);
            assert_eq!(CLEAN_RUNS.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn derived_from_cleanup_removes_outputs_when_owner_revision_changes() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(make_derived_from_answer_from_view)
                .system(cleanup_stale_derived.run_during(Phase::Settle))
                .build();

            let inserted = bowl.insert((MutableA(1),)).await;
            bowl.insert((Request,)).await;
            let result = bowl.scoop::<Query<(Entity, &Answer)>>().await;
            let rows = result.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 1);

            bowl.scoop::<Query<(Entity, Cow<MutableA>)>>()
                .for_each(|(entity, value)| {
                    if entity == inserted.entity() {
                        value.0 = 2;
                    }
                })
                .await;

            let result = bowl.scoop::<Query<(Entity, &Answer)>>().await;
            let rows = result.collect();
            assert_eq!(rows.len(), 0);
        });
    }

    #[test]
    fn derived_from_many_cleanup_removes_outputs_when_any_owner_revision_changes() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(make_multi_derived_from_answer_from_view)
                .system(cleanup_stale_derived.run_during(Phase::Settle))
                .build();

            bowl.insert((MutableA(1),)).await;
            let label = bowl.insert((Label("before"),)).await;
            bowl.insert((Request,)).await;

            let result = bowl.scoop::<Query<(Entity, &Answer)>>().await;
            let rows = result.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 1);

            bowl.scoop::<Query<(Entity, Cow<Label>)>>()
                .for_each(|(entity, label_value)| {
                    if entity == label.entity() {
                        label_value.0 = "after";
                    }
                })
                .await;

            let result = bowl.scoop::<Query<(Entity, &Answer)>>().await;
            let rows = result.collect();
            assert_eq!(rows.len(), 0);
        });
    }

    #[test]
    fn async_system_can_read_ambient_view() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(count_bs)
                .build();

            bowl.insert((A(1),)).await;
            bowl.insert((B(10),)).await;
            bowl.insert((B(20),)).await;

            let result = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 2);
        });
    }

    #[test]
    fn commands_can_insert_derived_entities() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(spawn_b)
                .build();

            bowl.insert((A(41),)).await;
            let result = bowl.scoop::<Query<(Entity, &B)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 42);
        });
    }

    #[test]
    fn with_filter_does_not_appear_in_query_item() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(count_tagged_a)
                .build();

            bowl.insert((A(1),)).await;
            bowl.insert((Request, A(2))).await;

            let result = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 1);
        });
    }

    #[test]
    fn two_query_system_runs_cross_product_rows() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(sum_a_b)
                .build();

            bowl.insert((A(2),)).await;
            bowl.insert((B(3),)).await;

            let result = bowl.scoop::<Query<(Entity, &Sum)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|(_, sum)| sum.0 == 5));
        });
    }

    #[test]
    fn two_query_view_system_can_gate_on_readiness() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(count_a_when_c_exists)
                .build();

            bowl.insert((A(1),)).await;
            bowl.insert((B(10),)).await;
            assert_eq!(bowl.scoop::<Query<(Entity, &Count)>>().await.len(), 0);

            bowl.insert((C(0),)).await;
            let result = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 1);
        });
    }

    #[test]
    fn evaluate_phase_replans_before_complete_phase_runs() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(make_b_uncounted)
                .system(make_c_from_b)
                .system(count_cs.run_during(Phase::Complete))
                .build();

            bowl.insert((A(1),)).await;

            let result = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 1);
        });
    }

    #[test]
    fn singleton_insert_reuses_entity() {
        block_on(async {
            let bowl = Bowl::builder().build();

            let first = bowl
                .insert((Singleton::<A>::new(), A(1), Request))
                .await
                .entity();
            let second = bowl
                .insert((Singleton::<A>::new(), A(2), Request))
                .await
                .entity();

            let result = bowl.scoop::<Query<(Entity, &A)>>().await;
            let rows = result.collect();

            assert_eq!(first, second);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, first);
            assert_eq!(rows[0].1.0, 2);
        });
    }

    #[test]
    fn derived_singleton_insert_reuses_entity() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(write_singleton_count)
                .build();

            bowl.insert((A(1),)).await;
            bowl.insert((A(2),)).await;

            let result = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 2);
        });
    }

    #[test]
    #[should_panic(expected = "bundles can contain at most one singleton marker")]
    fn bundle_rejects_multiple_singleton_markers() {
        block_on(async {
            let bowl = Bowl::builder().build();
            bowl.insert((Singleton::<A>::new(), Singleton::<B>::new(), A(1), B(2)))
                .await;
        });
    }

    #[test]
    fn commit_limit_is_configurable() {
        let bowl = Bowl::builder().build();

        assert_eq!(bowl.commit_limit(), CommitLimit::Max(10_000));

        bowl.set_commit_limit(CommitLimit::None);
        assert_eq!(bowl.commit_limit(), CommitLimit::None);

        bowl.set_commit_limit(CommitLimit::Max(2));
        assert_eq!(bowl.commit_limit(), CommitLimit::Max(2));
    }

    #[test]
    #[should_panic(expected = "bowl commit limit exceeded")]
    fn commit_limit_panics_on_non_converging_commits() {
        block_on(async {
            let bowl = Bowl::builder().system(spawn_a_from_a).build();
            bowl.set_commit_limit(CommitLimit::Max(2));

            bowl.insert((A(1),)).await;
            bowl.scoop::<Query<Entity, With<A>>>().await;
        });
    }

    #[test]
    fn systems_can_run_during_specific_phases() {
        block_on(async {
            PHASE_LOG.lock().expect("phase log lock poisoned").clear();

            let bowl = Bowl::builder()
                .system(startup_phase.run_during(Phase::Startup))
                .system(evaluate_phase)
                .system(cleanup_phase.run_during(Phase::Settle))
                .build();

            bowl.insert((A(1),)).await;
            bowl.scoop::<Query<(Entity, &B)>>().await;

            let log = PHASE_LOG.lock().expect("phase log lock poisoned").clone();
            assert_eq!(log, ["startup", "evaluate-after-startup", "cleanup"]);

            bowl.insert((A(2),)).await;
            bowl.scoop::<Query<(Entity, &B)>>().await;

            let log = PHASE_LOG.lock().expect("phase log lock poisoned").clone();
            assert_eq!(
                log,
                [
                    "startup",
                    "evaluate-after-startup",
                    "cleanup",
                    "evaluate-after-startup",
                    "cleanup"
                ]
            );
        });
    }

    #[test]
    fn component_lifecycle_hooks_fire_for_insert_take_and_entity_remove() {
        block_on(async {
            let _guard = HOOK_TEST_LOCK.lock().expect("hook test lock poisoned");
            HOOK_INSERTS.store(0, Ordering::SeqCst);
            HOOK_REMOVES.store(0, Ordering::SeqCst);
            HOOK_ENTITY_REMOVES.store(0, Ordering::SeqCst);

            let bowl = Bowl::builder().build();
            let hooked = bowl.insert((Hooked,)).await.bind();
            hooked.take::<Hooked>().await.unwrap();

            assert_eq!(HOOK_INSERTS.load(Ordering::SeqCst), 1);
            assert_eq!(HOOK_REMOVES.load(Ordering::SeqCst), 1);
            assert_eq!(HOOK_ENTITY_REMOVES.load(Ordering::SeqCst), 0);

            {
                let _hooked = bowl.insert((Hooked,)).await.bind();
            }

            bowl.scoop::<Query<Entity>>().await;

            assert_eq!(HOOK_INSERTS.load(Ordering::SeqCst), 2);
            assert_eq!(HOOK_REMOVES.load(Ordering::SeqCst), 2);
            assert_eq!(HOOK_ENTITY_REMOVES.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn remove_command_removes_entity_and_fires_lifecycle_hooks() {
        block_on(async {
            let _guard = HOOK_TEST_LOCK.lock().expect("hook test lock poisoned");
            HOOK_INSERTS.store(0, Ordering::SeqCst);
            HOOK_REMOVES.store(0, Ordering::SeqCst);
            HOOK_ENTITY_REMOVES.store(0, Ordering::SeqCst);

            let bowl = Bowl::builder()
                .system(remove_hooked_entity)
                .build();
            bowl.insert((Hooked,)).await;

            assert_eq!(bowl.scoop::<Query<(Entity, &Hooked)>>().await.len(), 0);
            assert_eq!(HOOK_INSERTS.load(Ordering::SeqCst), 1);
            assert_eq!(HOOK_REMOVES.load(Ordering::SeqCst), 1);
            assert_eq!(HOOK_ENTITY_REMOVES.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn untracked_components_do_not_invalidate_clean_systems() {
        block_on(async {
            let _guard = REQUEST_TEST_LOCK
                .lock()
                .expect("request test lock poisoned");
            REQUEST_RUNS.store(0, Ordering::SeqCst);

            let bowl = Bowl::builder()
                .system(make_b)
                .build();
            bowl.insert((A(1),)).await;

            assert_eq!(bowl.scoop::<Query<(Entity, &B)>>().await.len(), 1);
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);

            bowl.insert((UntrackedMarker,)).await;

            assert_eq!(bowl.scoop::<Query<(Entity, &B)>>().await.len(), 1);
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn external_queries_support_bound_where_filters() {
        block_on(async {
            let bowl = Bowl::builder().build();
            bowl.insert((A(1), Label("main"), Rank(1))).await;
            bowl.insert((A(2), Label("main"), Rank(3))).await;
            bowl.insert((A(3), Label("lib"), Rank(4))).await;

            let result = bowl
                .scoop::<Query<(Entity, &A), Where<And<Eq<Label>, Gte<Rank>>>>>()
                .args((Label("main"), Rank(2)))
                .await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 2);
        });
    }

    #[test]
    #[should_panic(expected = "duplicate query argument")]
    fn query_args_reject_duplicate_arg_types_in_same_scope() {
        let _builder = Bowl::builder()
            .build()
            .scoop::<Query<(Entity, &A), Where<Eq<Label>>>>()
            .args((Label("main"), Label("lib")));
    }

    #[test]
    fn scoop_can_return_multiple_independent_query_results() {
        block_on(async {
            let bowl = Bowl::builder().build();
            bowl.insert((A(1), Label("main"))).await;
            bowl.insert((A(2), Label("lib"))).await;

            let (all, main) = bowl
                .scoop::<(Query<(Entity, &A)>, Query<(Entity, &A), Where<Eq<Label>>>)>()
                .args(Label("main"))
                .await;

            assert_eq!(all.len(), 2);
            assert_eq!(main.len(), 1);
            assert_eq!(main.collect()[0].1.0, 1);
        });
    }

    #[test]
    fn named_scoops_bind_args_to_individual_queries() {
        block_on(async {
            struct Main;
            struct Lib;

            let bowl = Bowl::builder().build();
            bowl.insert((A(1), Label("main"))).await;
            bowl.insert((A(2), Label("lib"))).await;

            let (main, lib) = bowl
                .scoop::<(
                    Named<Main, Query<(Entity, &A), Where<Eq<Label>>>>,
                    Named<Lib, Query<(Entity, &A), Where<Eq<Label>>>>,
                )>()
                .args_for::<Main>(Label("main"))
                .args_for::<Lib>(Label("lib"))
                .await;

            assert_eq!(main.collect()[0].1.0, 1);
            assert_eq!(lib.collect()[0].1.0, 2);
        });
    }

    #[test]
    fn external_queries_can_mutate_bound_rows() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(copy_rank_to_count)
                .build();
            bowl.insert((Label("main"), Rank(1))).await;
            bowl.insert((Label("lib"), Rank(2))).await;

            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(
                counts
                    .collect()
                    .iter()
                    .map(|(_, count)| count.0)
                    .sum::<usize>(),
                3
            );

            bowl.scoop::<Query<(Entity, Cow<Rank>), Where<Eq<Label>>>>()
                .args(Label("main"))
                .for_each(|(_entity, rank)| {
                    rank.0 = 10;
                })
                .await;

            let ranks = bowl
                .scoop::<Query<(Entity, &Rank), Where<Eq<Label>>>>()
                .args(Label("main"))
                .await;
            let rows = ranks.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 10);

            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(
                counts
                    .collect()
                    .iter()
                    .map(|(_, count)| count.0)
                    .sum::<usize>(),
                12
            );
        });
    }

    #[test]
    fn external_mut_handles_update_inside_scoped_closure() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(copy_rank_to_count)
                .build();
            bowl.insert((Label("main"), Rank(1))).await;
            bowl.insert((Label("lib"), Rank(2))).await;

            let ranks = bowl
                .scoop::<Query<(Entity, Mut<Rank>), Where<Eq<Label>>>>()
                .args(Label("main"))
                .await;
            assert_eq!(ranks.len(), 1);

            let rows = ranks.collect();
            let updated = rows[0].1.with_latest(|rank| {
                rank.0 = 7;
                rank.0
            });
            assert_eq!(updated.await, Some(7));

            let ranks = bowl
                .scoop::<Query<(Entity, &Rank), Where<Eq<Label>>>>()
                .args(Label("main"))
                .await;
            let rows = ranks.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 7);

            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(
                counts
                    .collect()
                    .iter()
                    .map(|(_, count)| count.0)
                    .sum::<usize>(),
                9
            );
        });
    }

    #[test]
    fn external_mut_handles_without_entity_update_live_components() {
        block_on(async {
            let bowl = Bowl::builder().build();
            bowl.insert((MutableA(1),)).await;

            let values = bowl.scoop::<Query<(Mut<MutableA>,)>>().await;
            assert_eq!(values.len(), 1);

            let mut values = values.collect();
            let value = values.pop().unwrap();
            assert_eq!(value.with_latest(|value| value.0 += 4).await, Some(()));

            let values = bowl.scoop::<Query<(Entity, &MutableA)>>().await;
            let rows = values.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 5);
        });
    }

    #[test]
    fn external_mut_skips_invalidation_when_fingerprint_is_unchanged() {
        block_on(async {
            let _guard = REQUEST_TEST_LOCK
                .lock()
                .expect("request test lock poisoned");
            REQUEST_RUNS.store(0, Ordering::SeqCst);

            let bowl = Bowl::builder()
                .system(copy_fingerprinted_rank_to_count)
                .build();
            bowl.insert((FingerprintedRank(1),)).await;

            {
                let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
                assert_eq!(counts.collect()[0].1.0, 1);
            }
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);

            let handle = bowl
                .scoop::<Query<(Mut<FingerprintedRank>,)>>()
                .await
                .collect()
                .pop()
                .unwrap();
            assert_eq!(handle.with_latest(|rank| rank.0 = 1).await, Some(()));

            {
                let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
                assert_eq!(counts.collect()[0].1.0, 1);
            }
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);

            assert_eq!(handle.with_latest(|rank| rank.0 = 2).await, Some(()));

            {
                let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
                assert_eq!(counts.collect()[0].1.0, 2);
            }
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn external_mut_conservatively_invalidates_without_fingerprint() {
        block_on(async {
            let _guard = REQUEST_TEST_LOCK
                .lock()
                .expect("request test lock poisoned");
            REQUEST_RUNS.store(0, Ordering::SeqCst);

            let bowl = Bowl::builder()
                .system(copy_rank_to_count_counted)
                .build();
            bowl.insert((Rank(1),)).await;

            {
                let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
                assert_eq!(counts.collect()[0].1.0, 1);
            }
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);

            let handle = bowl
                .scoop::<Query<(Mut<Rank>,)>>()
                .await
                .collect()
                .pop()
                .unwrap();
            assert_eq!(handle.with_latest(|rank| rank.0 = 1).await, Some(()));

            {
                let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
                assert_eq!(counts.collect()[0].1.0, 1);
            }
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn external_mut_original_rejects_stale_handles() {
        block_on(async {
            let bowl = Bowl::builder().build();
            bowl.insert((Rank(1),)).await;

            let handle = bowl
                .scoop::<Query<(Mut<Rank>,)>>()
                .await
                .collect()
                .pop()
                .unwrap();

            assert_eq!(handle.with_latest(|rank| rank.0 = 2).await, Some(()));
            assert_eq!(handle.with_original(|rank| rank.0 = 3).await, None);

            let latest = bowl.scoop::<Query<(Entity, &Rank)>>().await;
            let rows = latest.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 2);
        });
    }

    #[test]
    fn external_mut_does_not_require_clone() {
        block_on(async {
            let bowl = Bowl::builder().build();
            bowl.insert((NonCloneAnswer(1),)).await;

            let handle = bowl
                .scoop::<Query<(Mut<NonCloneAnswer>,)>>()
                .await
                .collect()
                .pop()
                .unwrap();

            assert_eq!(handle.with_latest(|answer| answer.0 = 9).await, Some(()));

            let answers = bowl.scoop::<Query<(Entity, &NonCloneAnswer)>>().await;
            let rows = answers.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 9);
        });
    }

    #[test]
    fn external_mut_succeeds_while_structural_snapshot_is_alive() {
        block_on(async {
            let bowl = Bowl::builder().build();
            bowl.insert((Rank(1),)).await;

            let snapshot = bowl.scoop::<Query<(Entity, &Rank)>>().await;
            let handle = bowl
                .scoop::<Query<(Mut<Rank>,)>>()
                .await
                .collect()
                .pop()
                .unwrap();

            assert_eq!(handle.with_latest(|rank| rank.0 = 2).await, Some(()));
            assert_eq!(snapshot.collect()[0].1.0, 2);

            let latest = bowl.scoop::<Query<(Entity, &Rank)>>().await;
            assert_eq!(latest.collect()[0].1.0, 2);
        });
    }

    #[test]
    fn scheduler_allows_multiple_readers_of_same_row() {
        block_on(async {
            let _guard = ACCESS_TEST_LOCK.lock().expect("access test lock poisoned");
            reset_access_counters();

            let bowl = Bowl::builder()
                .system(read_rank_for_access_test)
                .system(read_rank_for_access_test_again)
                .build();
            bowl.insert((Rank(1),)).await;

            bowl.scoop::<Query<Entity>>().await;

            assert_eq!(ACTIVE_READERS.load(Ordering::SeqCst), 0);
            assert_eq!(ACTIVE_WRITERS.load(Ordering::SeqCst), 0);
            assert_eq!(MAX_ACTIVE_READERS.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn scheduler_serializes_read_write_access_to_same_row() {
        block_on(async {
            let _guard = ACCESS_TEST_LOCK.lock().expect("access test lock poisoned");
            reset_access_counters();

            let bowl = Bowl::builder()
                .system(read_rank_for_access_test)
                .system(write_rank_for_access_test)
                .build();
            bowl.insert((Rank(1),)).await;

            bowl.scoop::<Query<Entity>>().await;

            assert_eq!(ACTIVE_READERS.load(Ordering::SeqCst), 0);
            assert_eq!(ACTIVE_WRITERS.load(Ordering::SeqCst), 0);
            assert_eq!(MAX_ACTIVE_READERS.load(Ordering::SeqCst), 1);
            assert_eq!(MAX_ACTIVE_WRITERS.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn scheduler_allows_writes_to_different_rows() {
        block_on(async {
            let _guard = ACCESS_TEST_LOCK.lock().expect("access test lock poisoned");
            reset_access_counters();

            let bowl = Bowl::builder()
                .system(write_rank_for_access_test)
                .build();
            bowl.insert((Rank(1),)).await;
            bowl.insert((Rank(2),)).await;

            bowl.scoop::<Query<Entity>>().await;

            assert_eq!(ACTIVE_READERS.load(Ordering::SeqCst), 0);
            assert_eq!(ACTIVE_WRITERS.load(Ordering::SeqCst), 0);
            assert_eq!(MAX_ACTIVE_WRITERS.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn cleanup_runs_after_normal_phases_settle() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(make_b_uncounted.on_complete(|mut commands: Commands<Anything>| { commands.insert((Singleton::<Note>::new(), Note, UntrackedMarker)); }))
                .system(count_after_note.run_during(Phase::Complete))
                .system(cleanup_untracked_marker.run_during(Phase::Settle))
                .build();

            bowl.insert((A(1),)).await;

            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;

            assert_eq!(counts.len(), 1);
            assert_eq!(bowl.scoop::<Query<(Entity, &Note)>>().await.len(), 0);
        });
    }

    #[test]
    fn on_complete_waits_for_same_phase_upstream_work_to_settle() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(make_b_uncounted)
                .system(mark_b_processed.on_settled(|mut commands: Commands<Anything>| { commands.insert((Singleton::<Note>::new(), Note, UntrackedMarker)); }))
                .system(count_bs_after_note.run_during(Phase::Complete))
                .system(cleanup_untracked_marker.run_during(Phase::Settle))
                .build();

            bowl.insert((B(0),)).await;
            assert_eq!(bowl.scoop::<Query<(Entity, &Count)>>().await.len(), 1);

            bowl.insert((A(1),)).await;

            let result = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let counts = result.collect();

            assert_eq!(counts.len(), 2);
            assert!(counts.iter().all(|(_, count)| count.0 == 2));
        });
    }

    #[test]
    fn on_complete_does_not_publish_gate_while_upstream_work_is_pending() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(make_b_uncounted)
                .system(mark_b_processed.on_settled(|mut commands: Commands<Anything>| { commands.insert((Singleton::<UntrackedMarker>::new(), UntrackedMarker)); }))
                .system(answer_after_untracked_marker.run_during(Phase::Complete))
                .system(cleanup_untracked_marker.run_during(Phase::Settle))
                .build();

            bowl.insert((B(0),)).await;
            bowl.scoop::<Query<(Entity, &D)>>().await;

            let answer = bowl.insert((A(1), Request)).await.bind();

            assert_eq!(answer.take::<Answer>().await.unwrap().0, 2);
        });
    }

    #[test]
    fn on_settled_runs_before_cleanup_and_can_continue_evaluation() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(make_b_uncounted)
                .system(mark_b_processed.on_settled(|mut commands: Commands<Anything>| { commands.insert((Singleton::<UntrackedMarker>::new(), UntrackedMarker)); }))
                .system(answer_after_untracked_marker.run_during(Phase::Complete))
                .system(cleanup_untracked_marker.run_during(Phase::Settle))
                .build();

            bowl.insert((B(0),)).await;
            bowl.scoop::<Query<(Entity, &D)>>().await;

            let answer = bowl.insert((A(1), Request)).await.bind();

            assert_eq!(answer.take::<Answer>().await.unwrap().0, 2);
            assert_eq!(
                bowl.scoop::<Query<(Entity, &UntrackedMarker)>>()
                    .await
                    .len(),
                0
            );
        });
    }

    #[test]
    fn on_start_and_on_complete_wrap_planned_system_work() {
        block_on(async {
            SYSTEM_HOOK_LOG
                .lock()
                .expect("system hook log lock poisoned")
                .clear();

            let bowl = Bowl::builder()
                .system(make_b_with_hook_log .on_start(|_commands: Commands<Anything>| { SYSTEM_HOOK_LOG .lock() .expect("system hook log lock poisoned") .push("start"); }) .on_complete(|_commands: Commands<Anything>| { SYSTEM_HOOK_LOG .lock() .expect("system hook log lock poisoned") .push("complete"); }),)
                .build();

            bowl.insert((A(1),)).await;
            bowl.scoop::<Query<(Entity, &B)>>().await;

            assert_eq!(
                *SYSTEM_HOOK_LOG
                    .lock()
                    .expect("system hook log lock poisoned"),
                vec!["start", "row", "complete"]
            );

            bowl.scoop::<Query<(Entity, &B)>>().await;

            assert_eq!(
                *SYSTEM_HOOK_LOG
                    .lock()
                    .expect("system hook log lock poisoned"),
                vec!["start", "row", "complete"]
            );
        });
    }

    #[test]
    fn system_params_support_arbitrary_mixed_order() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(mixed_param_system)
                .build();

            bowl.insert((A(1),)).await;
            bowl.insert((B(10),)).await;
            bowl.insert((B(20),)).await;
            bowl.insert((C(2),)).await;
            bowl.insert((D(3),)).await;

            let result = bowl.scoop::<Query<(Entity, &Sum)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 8);
        });
    }

    async fn answer_request_with_note(query: Query<(Entity, &Request)>, mut commands: Commands<Anything>) {
        let (entity, _request) = query.item();
        commands.entity(entity).insert(Answer(42));
        commands.entity(entity).insert(Note);
    }

    #[test]
    fn bound_entity_take_consumes_output_and_cleans_scope() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(answer_request)
                .build();

            let request = bowl.insert((Request,)).await.bind();
            let answer = request.take::<Answer>().await.unwrap();

            assert_eq!(answer.0, 42);
            assert_eq!(bowl.scoop::<Query<(Entity, &Answer)>>().await.len(), 0);
        });
    }

    #[test]
    fn bound_entity_take_does_not_require_clone() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(answer_request_with_non_clone)
                .build();

            let request = bowl.insert((Request,)).await.bind();
            let answer = request.take::<NonCloneAnswer>().await.unwrap();

            assert_eq!(answer.0, 42);
            assert_eq!(
                bowl.scoop::<Query<(Entity, &NonCloneAnswer)>>().await.len(),
                0
            );
        });
    }

    #[test]
    fn bound_entity_take_supports_required_and_optional_outputs() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(answer_request)
                .build();

            let request = bowl.insert((Request,)).await.bind();
            let (answer, note) = request.take::<(Answer, Option<Note>)>().await.unwrap();

            assert_eq!(answer.0, 42);
            assert!(note.is_none());
            assert_eq!(bowl.scoop::<Query<(Entity, &Answer)>>().await.len(), 0);
        });
    }

    #[test]
    fn bound_entity_take_removes_leftover_outputs() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(answer_request_with_note)
                .build();

            let request = bowl.insert((Request,)).await.bind();
            let answer = request.take::<Answer>().await.unwrap();

            assert_eq!(answer.0, 42);
            assert_eq!(bowl.scoop::<Query<(Entity, &Note)>>().await.len(), 0);
        });
    }

    static DEF_HOLD: AtomicBool = AtomicBool::new(false);
    static DEF_STARTED: AtomicUsize = AtomicUsize::new(0);
    static DEF_COMPLETED: AtomicUsize = AtomicUsize::new(0);
    static DEF_ANSWERS: StdMutex<Vec<(String, bool)>> = StdMutex::new(Vec::new());

    async fn def_startup_retract(
        query: Query<Entity, With<EpochEphemeral>>,
        mut commands: Commands<Anything>,
    ) {
        commands.remove(query.item());
    }

    async fn def_consume(
        _: Query<Entity, With<EpochReady>>,
        query: Query<(Entity, &EpochAsk)>,
        defs: View<'_, (Entity, &EpochDef)>,
    ) {
        let (_entity, ask) = query.item();
        DEF_STARTED.fetch_add(1, Ordering::SeqCst);
        while DEF_HOLD.load(Ordering::SeqCst) {
            yield_once().await;
        }
        let found = defs.iter().any(|(_, def)| def.0 == ask.0);
        DEF_ANSWERS.lock().unwrap().push((ask.0.clone(), found));
        DEF_COMPLETED.fetch_add(1, Ordering::SeqCst);
    }

    /// `.deferred()` opts a mutation out of preemption: in-flight work is
    /// never dropped; the write waits for a natural boundary and the
    /// following derivations reflect it.
    #[test]
    fn deferred_mut_waits_for_the_natural_boundary() {
        use std::future::IntoFuture;
        use std::task::Context;

        DEF_HOLD.store(false, Ordering::SeqCst);
        DEF_STARTED.store(0, Ordering::SeqCst);
        DEF_COMPLETED.store(0, Ordering::SeqCst);
        DEF_ANSWERS.lock().unwrap().clear();

        let bowl = Bowl::builder()
            .system(epoch_derive.on_settled(|mut commands: Commands<Anything>| {
                commands.insert((Singleton::<EpochReady>::new(), EpochReady, EpochEphemeral));
            }))
            .system(def_consume)
            // Boundary writes (deferred included) restart through Startup;
            // the marker pattern registers its retraction there so gated
            // consumers rerun against the post-write derivations.
            .system(def_startup_retract.run_during(Phase::Startup))
            .system(cleanup_epoch_ephemeral.run_during(Phase::Settle))
            .build();
        let handle = block_on(async {
            bowl.insert((EpochSrc("old".to_string()),)).await;
            bowl.insert((EpochAsk("new".to_string()),)).await;

            bowl.scoop::<Query<(Entity, Mut<EpochSrc>)>>()
                .await
                .collect()
                .pop()
                .unwrap()
                .1
        });
        assert_eq!(DEF_COMPLETED.load(Ordering::SeqCst), 1);

        DEF_HOLD.store(true, Ordering::SeqCst);
        block_on(bowl.insert((D(1),)).into_future());

        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut driver = Box::pin(bowl.scoop::<Query<(Entity, &EpochDef)>>().into_future());
        for _ in 0..1000 {
            if DEF_STARTED.load(Ordering::SeqCst) >= 2 {
                break;
            }
            assert!(driver.as_mut().poll(&mut context).is_pending());
        }
        assert_eq!(DEF_STARTED.load(Ordering::SeqCst), 2);

        let mutator = std::thread::spawn(move || {
            block_on(
                handle
                    .deferred()
                    .with_latest(|src| src.0 = "new".to_string()),
            )
        });

        DEF_HOLD.store(false, Ordering::SeqCst);
        block_on(driver);
        mutator.join().unwrap();
        block_on(bowl.scoop::<Query<(Entity, &EpochDef)>>().into_future());

        assert_eq!(
            DEF_STARTED.load(Ordering::SeqCst),
            DEF_COMPLETED.load(Ordering::SeqCst),
            "a deferred write must never drop in-flight work"
        );
        let last = DEF_ANSWERS.lock().unwrap().last().cloned();
        assert_eq!(
            last,
            Some(("new".to_string(), true)),
            "the write must land at a boundary and re-derive"
        );
    }

    static PI_HOLD: AtomicBool = AtomicBool::new(false);
    static PI_STARTED: AtomicUsize = AtomicUsize::new(0);
    static PI_COMPLETED: AtomicUsize = AtomicUsize::new(0);

    async fn pi_reader(query: Query<(Entity, &D)>, mut commands: Commands<Anything>) {
        let (entity, d) = query.item();
        PI_STARTED.fetch_add(1, Ordering::SeqCst);
        while PI_HOLD.load(Ordering::SeqCst) {
            yield_once().await;
        }
        PI_COMPLETED.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(Count(d.0 as usize));
    }

    /// `.preempting()` on an insert forces the epoch boundary: in-flight
    /// read-only work is dropped and the input joins the current epoch
    /// instead of waiting for the next one.
    #[test]
    fn preempting_insert_forces_the_boundary() {
        use std::future::IntoFuture;
        use std::task::Context;

        PI_HOLD.store(false, Ordering::SeqCst);
        PI_STARTED.store(0, Ordering::SeqCst);
        PI_COMPLETED.store(0, Ordering::SeqCst);

        let bowl = Bowl::builder()
            .system(pi_reader)
            .system(make_b_after_yield)
            .build();
        block_on(async {
            bowl.insert((D(5),)).await;
            bowl.scoop::<Query<(Entity, &Count)>>().await;
        });
        assert_eq!(PI_COMPLETED.load(Ordering::SeqCst), 1);

        PI_HOLD.store(true, Ordering::SeqCst);
        block_on(bowl.insert((D(6),)).into_future());

        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut driver = Box::pin(bowl.scoop::<Query<(Entity, &B)>>().into_future());
        for _ in 0..1000 {
            if PI_STARTED.load(Ordering::SeqCst) >= 2 {
                break;
            }
            assert!(driver.as_mut().poll(&mut context).is_pending());
        }
        assert_eq!(PI_STARTED.load(Ordering::SeqCst), 2);

        let inserter = std::thread::spawn({
            let bowl = bowl.clone();
            move || block_on(bowl.insert((A(9),)).preempting().into_future())
        });

        // The dropped reader replans in the restarted phases and suspends
        // again; then release it and finish the epoch. Sleep between polls
        // so the inserter thread gets scheduled and registers its
        // preemption before the loop gives up.
        for _ in 0..10_000 {
            if PI_STARTED.load(Ordering::SeqCst) >= 3 {
                break;
            }
            if driver.as_mut().poll(&mut context).is_ready() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
        PI_HOLD.store(false, Ordering::SeqCst);
        let rows = block_on(driver);
        inserter.join().unwrap();

        assert_eq!(
            PI_STARTED.load(Ordering::SeqCst),
            3,
            "the in-flight reader must be dropped and replanned"
        );
        assert_eq!(
            PI_COMPLETED.load(Ordering::SeqCst),
            2,
            "the dropped attempt must never complete"
        );
        assert!(
            rows.collect().iter().any(|(_, b)| b.0 == 10),
            "the preempting insert must be processed within this epoch"
        );
    }

    static LS_HOLD: AtomicBool = AtomicBool::new(false);
    static LS_STARTED: AtomicUsize = AtomicUsize::new(0);

    async fn ls_derive(query: Query<(Entity, &D)>, mut commands: Commands<Anything>) {
        let (entity, d) = query.item();
        LS_STARTED.fetch_add(1, Ordering::SeqCst);
        while LS_HOLD.load(Ordering::SeqCst) {
            yield_once().await;
        }
        commands.insert((DerivedFrom::new(entity), Sum(d.0)));
    }

    /// `.last_settled()` reads the retained settled view without waiting
    /// for the in-flight epoch — the stale-read pressure valve.
    #[test]
    fn last_settled_scoop_reads_without_waiting() {
        use std::future::IntoFuture;
        use std::task::Context;

        LS_HOLD.store(false, Ordering::SeqCst);
        LS_STARTED.store(0, Ordering::SeqCst);

        let bowl = Bowl::builder()
            .system(ls_derive)
            .build();
        block_on(async {
            bowl.insert((D(1),)).await;
        });
        assert_eq!(
            block_on(collect_sums(&bowl)),
            [1].into_iter().collect::<std::collections::BTreeSet<_>>()
        );

        LS_HOLD.store(true, Ordering::SeqCst);
        block_on(bowl.insert((D(2),)).into_future());

        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut driver = Box::pin(bowl.scoop::<Query<(Entity, &Sum)>>().into_future());
        for _ in 0..1000 {
            if LS_STARTED.load(Ordering::SeqCst) >= 2 {
                break;
            }
            assert!(driver.as_mut().poll(&mut context).is_pending());
        }
        assert_eq!(LS_STARTED.load(Ordering::SeqCst), 2);

        // A regular scoop would wait for the suspended epoch forever; the
        // stale read returns the previous settled view immediately.
        let stale = block_on(
            bowl.scoop::<Query<(Entity, &Sum)>>()
                .last_settled()
                .into_future(),
        );
        let values: std::collections::BTreeSet<u32> =
            stale.collect().into_iter().map(|(_, sum)| sum.0).collect();
        assert_eq!(values, [1].into_iter().collect());

        LS_HOLD.store(false, Ordering::SeqCst);
        block_on(driver);
        assert_eq!(
            block_on(collect_sums(&bowl)),
            [1, 2].into_iter().collect::<std::collections::BTreeSet<_>>()
        );
    }

    /// spec/daemon-client.md, revision-cursor reads: the delta source for
    /// state-sync replication.
    #[test]
    fn changed_since_reads_only_rows_past_the_cursor() {
        block_on(async {
            let bowl = Bowl::builder().build();
            let first = bowl.insert((MutableA(1),)).await;
            bowl.insert((MutableA(2),)).await;
            bowl.scoop::<Query<(Entity, &MutableA)>>().await;

            let cursor = bowl.settled_revision().await;

            let handle = bowl
                .scoop::<Query<(Entity, Mut<MutableA>)>>()
                .await
                .collect()
                .into_iter()
                .find(|(entity, _)| *entity == first.entity())
                .unwrap()
                .1;
            handle.with_latest(|a| a.0 = 9).await;

            let delta = bowl
                .scoop::<Query<(Entity, &MutableA)>>()
                .changed_since(cursor)
                .await;
            let rows = delta.collect();
            assert_eq!(rows.len(), 1, "only the mutated row is past the cursor");
            assert_eq!(rows[0].1.0, 9);

            // A fresh cursor sees nothing.
            let cursor = bowl.settled_revision().await;
            let delta = bowl
                .scoop::<Query<(Entity, &MutableA)>>()
                .changed_since(cursor)
                .await;
            assert_eq!(delta.len(), 0);
        });
    }

    /// spec/daemon-client.md, external targeted inserts: components land on
    /// an entity the caller did not create in this call.
    #[test]
    fn entity_insert_targets_an_existing_entity() {
        block_on(async {
            let bowl = Bowl::builder().build();
            let inserted = bowl.insert((A(7),)).await;
            bowl.entity(inserted.entity()).insert((Note,)).await;

            let rows = bowl
                .scoop::<Query<(Entity, &A), With<Note>>>()
                .await
                .collect()
                .len();
            assert_eq!(rows, 1);
        });
    }

    /// spec/daemon-client.md, drain reads: deliver-then-delete stream
    /// semantics — the result stays readable, the daemon keeps no backlog.
    #[test]
    fn drain_consumes_matched_rows() {
        block_on(async {
            let bowl = Bowl::builder().build();
            bowl.insert((C(1), Note)).await;
            bowl.insert((C(2), Note)).await;
            bowl.insert((C(3),)).await;

            let drained = bowl
                .scoop::<Query<(Entity, &C), With<Note>>>()
                .drain()
                .await;
            let values: std::collections::BTreeSet<u32> =
                drained.collect().into_iter().map(|(_, c)| c.0).collect();
            assert_eq!(values, [1, 2].into_iter().collect());

            // The drained rows are gone; the unmatched row survives.
            assert_eq!(
                bowl.scoop::<Query<(Entity, &C), With<Note>>>().await.len(),
                0
            );
            assert_eq!(bowl.scoop::<Query<(Entity, &C)>>().await.len(), 1);
        });
    }

    /// spec/daemon-client.md, settle notifications: a publisher wakes when
    /// a settle that performed work completes.
    #[test]
    fn next_settle_fires_after_working_settles() {
        use std::task::Context;

        let bowl = Bowl::builder()
            .system(make_b_after_yield)
            .build();
        block_on(async {
        });

        // Register the watcher deterministically (first poll registers),
        // then drive a working settle and await the notification.
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut notify = Box::pin(bowl.next_settle());
        assert!(notify.as_mut().poll(&mut context).is_pending());

        block_on(async {
            bowl.insert((A(1),)).await;
            bowl.scoop::<Query<(Entity, &B)>>().await;
        });

        let revision = block_on(notify);
        assert!(revision > 0);
    }

    /// A live query result shares the answer's component cell. Taking used to
    /// fail spuriously (and destroy the value) when any snapshot still pinned
    /// the cell; it must instead wait for the holder to drop.
    #[test]
    fn take_waits_for_pinning_query_results_to_release() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(answer_request)
                .build();

            let request = bowl.insert((Request,)).await.bind();

            // Settle and pin the Answer cell with a held query result.
            let pinned = bowl.scoop::<Query<(Entity, &Answer)>>().await;
            assert_eq!(pinned.len(), 1);

            let handle = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(20));
                drop(pinned);
            });

            let answer = request.take::<Answer>().await.unwrap();
            assert_eq!(answer.0, 42);
            handle.join().unwrap();
        });
    }

    #[test]
    fn dropped_bound_entity_is_cleaned_up_on_next_operation() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(answer_request)
                .build();

            {
                let _request = bowl.insert((Request,)).await.bind();
            }

            assert_eq!(bowl.scoop::<Query<(Entity, &Answer)>>().await.len(), 0);
            assert_eq!(bowl.scoop::<Query<(Entity, &Request)>>().await.len(), 0);
        });
    }

    static JOIN_PAIR_RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn join_pairs(
        namespaces: Query<(Entity, &A, &FingerprintedRank)>,
        members: Query<(Entity, &B), Where<Eq<FingerprintedRank>>>,
        mut commands: Commands<Anything>,
    ) {
        JOIN_PAIR_RUNS.fetch_add(1, Ordering::SeqCst);
        let (namespace, a, _rank) = namespaces.item();
        let (member, b) = members.item();
        commands.insert((
            DerivedFrom::many([namespace, member]),
            Sum(a.0 * 100 + b.0),
        ));
    }

    async fn collect_sums(bowl: &Bowl) -> std::collections::BTreeSet<u32> {
        bowl.scoop::<Query<(Entity, &Sum)>>()
            .await
            .collect()
            .into_iter()
            .map(|(_, sum)| sum.0)
            .collect()
    }

    #[test]
    fn bound_eq_join_runs_only_matching_pairs() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(join_pairs)
                .build();

            bowl.insert((A(1), FingerprintedRank(1))).await;
            bowl.insert((A(2), FingerprintedRank(2))).await;
            bowl.insert((B(10), FingerprintedRank(1))).await;
            bowl.insert((B(20), FingerprintedRank(1))).await;
            bowl.insert((B(30), FingerprintedRank(2), Note)).await;

            JOIN_PAIR_RUNS.store(0, Ordering::SeqCst);
            let sums = collect_sums(&bowl).await;
            assert_eq!(
                sums,
                [110, 120, 230].into_iter().collect(),
                "only key-matching pairs should run"
            );
            assert_eq!(JOIN_PAIR_RUNS.load(Ordering::SeqCst), 3);

            // Settling again replans nothing: every pair is memoized.
            collect_sums(&bowl).await;
            assert_eq!(JOIN_PAIR_RUNS.load(Ordering::SeqCst), 3);

            // A new member is a new pair row for its namespace only.
            bowl.insert((B(40), FingerprintedRank(1))).await;
            let sums = collect_sums(&bowl).await;
            assert_eq!(sums, [110, 120, 230, 140].into_iter().collect());
            assert_eq!(JOIN_PAIR_RUNS.load(Ordering::SeqCst), 4);

            // Touching one member's data reruns only that member's pair.
            for (_, member) in bowl
                .scoop::<Query<(Entity, Mut<B>), With<Note>>>()
                .await
                .collect()
            {
                member.with_latest(|b| b.0 = 31).await;
            }
            let sums = collect_sums(&bowl).await;
            assert_eq!(sums, [110, 120, 231, 140].into_iter().collect());
            assert_eq!(JOIN_PAIR_RUNS.load(Ordering::SeqCst), 5);
        });
    }

    async fn join_pairs_uncounted(
        namespaces: Query<(Entity, &A, &FingerprintedRank)>,
        members: Query<(Entity, &B), Where<Eq<FingerprintedRank>>>,
        mut commands: Commands<Anything>,
    ) {
        let (namespace, a, _rank) = namespaces.item();
        let (member, b) = members.item();
        commands.insert((
            DerivedFrom::many([namespace, member]),
            Sum(a.0 * 100 + b.0),
        ));
    }

    #[test]
    fn bound_eq_join_key_change_moves_the_pair() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(join_pairs_uncounted)
                .system(cleanup_stale_derived.run_during(Phase::Settle))
                .build();

            bowl.insert((A(1), FingerprintedRank(1))).await;
            bowl.insert((A(2), FingerprintedRank(2))).await;
            bowl.insert((B(10), FingerprintedRank(1))).await;

            assert_eq!(collect_sums(&bowl).await, [110].into_iter().collect());

            // Moving the member to the other key retires the old pair (its
            // derived output leaves through DerivedFrom cleanup) and forms a
            // new pair with the other namespace.
            for (_, rank) in bowl
                .scoop::<Query<(Entity, Mut<FingerprintedRank>), With<B>>>()
                .await
                .collect()
            {
                rank.with_latest(|rank| rank.0 = 2).await;
            }

            assert_eq!(collect_sums(&bowl).await, [210].into_iter().collect());
        });
    }

    async fn self_keyed_join(
        namespaces: Query<(Entity, &A, &FingerprintedRank)>,
        members: Query<(Entity, &B, &FingerprintedRank), Where<Eq<FingerprintedRank>>>,
        mut commands: Commands<Anything>,
    ) {
        let (namespace, a, _) = namespaces.item();
        let (member, b, rank) = members.item();
        commands.insert((
            DerivedFrom::many([namespace, member]),
            Sum(a.0 * 1000 + b.0 * 10 + rank.0),
        ));
    }

    #[test]
    fn bound_eq_join_allows_bound_query_reading_its_own_key() {
        block_on(async {
            // The bound query reads the key itself; provider resolution must
            // skip the bound param and bind to the namespace query.
            let bowl = Bowl::builder().system(self_keyed_join).build();

            bowl.insert((A(1), FingerprintedRank(1))).await;
            bowl.insert((B(2), FingerprintedRank(1))).await;
            bowl.insert((B(3), FingerprintedRank(9))).await;

            assert_eq!(collect_sums(&bowl).await, [1021].into_iter().collect());
        });
    }

    struct KeyB(u32);
    impl Component for KeyB {
        fn fingerprint(&self) -> Option<u64> {
            Some(crate::hash_component(&self.0))
        }
    }

    async fn compound_key_join(
        namespaces: Query<(Entity, &A, &FingerprintedRank, &KeyB)>,
        members: Query<(Entity, &B), Where<And<Eq<FingerprintedRank>, Eq<KeyB>>>>,
        mut commands: Commands<Anything>,
    ) {
        let (namespace, a, _, _) = namespaces.item();
        let (member, b) = members.item();
        commands.insert((
            DerivedFrom::many([namespace, member]),
            Sum(a.0 * 100 + b.0),
        ));
    }

    /// `Where<And<Eq<A>, Eq<B>>>` is a compound-key join: a pair forms only
    /// when every key matches its provider (overload-resolution shape).
    #[test]
    fn compound_bound_join_requires_every_key_to_match() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(compound_key_join)
                .build();

            bowl.insert((A(1), FingerprintedRank(1), KeyB(1))).await;
            bowl.insert((A(2), FingerprintedRank(1), KeyB(2))).await;
            bowl.insert((B(10), FingerprintedRank(1), KeyB(1))).await;
            bowl.insert((B(20), FingerprintedRank(1), KeyB(2))).await;
            bowl.insert((B(30), FingerprintedRank(2), KeyB(1))).await;

            assert_eq!(
                collect_sums(&bowl).await,
                [110, 220].into_iter().collect(),
                "rows matching only one of two keys must not pair"
            );
        });
    }

    async fn and_filtered_derive(
        query: Query<(Entity, &A), And<With<Note>, Without<C>>>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, a) = query.item();
        commands.insert((DerivedFrom::new(entity), Sum(a.0)));
    }

    /// Plain filter conjunction: `And<With<..>, Without<..>>` on one query.
    #[test]
    fn and_composes_plain_filters() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(and_filtered_derive)
                .build();

            bowl.insert((A(1), Note)).await;
            bowl.insert((A(2), Note, C(0))).await;
            bowl.insert((A(3),)).await;

            assert_eq!(collect_sums(&bowl).await, [1].into_iter().collect());
        });
    }

    async fn missing_provider_join(
        members: Query<(Entity, &B), Where<Eq<FingerprintedRank>>>,
        mut _commands: Commands<Anything>,
    ) {
        let _ = members.item();
    }

    #[test]
    #[should_panic(expected = "needs exactly one sibling query param")]
    fn bound_eq_without_provider_panics_at_registration() {
        block_on(async {
            let _bowl = Bowl::builder()
                .system(missing_provider_join)
                .build();
        });
    }

    async fn ambiguous_provider_join(
        _a_rows: Query<(Entity, &A, &FingerprintedRank)>,
        _c_rows: Query<(Entity, &C, &FingerprintedRank)>,
        members: Query<(Entity, &B), Where<Eq<FingerprintedRank>>>,
    ) {
        let _ = members.item();
    }

    #[test]
    #[should_panic(expected = "needs exactly one sibling query param")]
    fn bound_eq_with_ambiguous_providers_panics_at_registration() {
        block_on(async {
            let _bowl = Bowl::builder()
                .system(ambiguous_provider_join)
                .build();
        });
    }

    async fn view_bound_join(
        _namespaces: Query<(Entity, &A, &FingerprintedRank)>,
        _members: View<'_, (Entity, &B), Where<Eq<FingerprintedRank>>>,
    ) {
    }

    #[test]
    #[should_panic(expected = "does not support bound")]
    fn bound_eq_on_view_panics_at_registration() {
        block_on(async {
            let _bowl = Bowl::builder()
                .system(view_bound_join)
                .build();
        });
    }

    async fn derive_ranked_from_a(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (source, _a) = query.item();
        commands.insert((DerivedFrom::new(source), FingerprintedRank(5)));
    }

    async fn derive_sum_from_ranked(
        query: Query<(Entity, &FingerprintedRank)>,
        mut commands: Commands<Anything>,
    ) {
        let (ranked, rank) = query.item();
        commands.insert((DerivedFrom::new(ranked), Sum(rank.0 + 1)));
    }

    /// An upstream rerun that re-derives identical content must not retire
    /// second-order derived facts: the re-stamped untracked `DerivedFrom` on
    /// the intermediate entity may not lift its entity revision, or the
    /// downstream fact goes stale without its producer ever replanning.
    #[test]
    fn derived_fact_survives_upstream_rerun_with_unchanged_content() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(derive_ranked_from_a)
                .system(derive_sum_from_ranked)
                .system(cleanup_stale_derived.run_during(Phase::Settle))
                .build();

            bowl.insert((A(1),)).await;
            assert_eq!(collect_sums(&bowl).await, [6].into_iter().collect());

            // Bump the source: the first derivation reruns but re-emits an
            // identical fingerprinted fact, so the second derivation has no
            // reason to rerun — and its output must survive cleanup.
            for (_, a) in bowl.scoop::<Query<(Entity, Mut<A>)>>().await.collect() {
                a.with_latest(|a| a.0 = 2).await;
            }

            assert_eq!(collect_sums(&bowl).await, [6].into_iter().collect());
        });
    }

    async fn unhashed_key_join(
        _labels: Query<(Entity, &Label)>,
        members: Query<(Entity, &B), Where<Eq<Label>>>,
    ) {
        let _ = members.item();
    }

    #[test]
    #[should_panic(expected = "component(hash)")]
    fn bound_eq_join_requires_hashed_key() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(unhashed_key_join)
                .build();

            bowl.insert((Label("namespace"),)).await;
            bowl.insert((B(1), Label("namespace"))).await;

            bowl.scoop::<Query<(Entity, &B)>>().await;
        });
    }

    // ------------------------------------------------------------------
    // Epoch semantics (spec/epochs.md) — regression tests written ahead of
    // the implementation. They pin the designed behavior and FAIL against
    // the current engine by design. Missing-API topics (`.deferred()` on
    // muts, `.preempting()` on inserts, the preemption budget, stale-read
    // scoops) get their tests when their API skeletons exist; everything
    // below compiles against today's surface.
    // ------------------------------------------------------------------

    struct EpochSrc(String);
    struct EpochDef(String);
    struct EpochAsk(String);
    struct EpochReady;
    struct EpochEphemeral;

    impl Component for EpochSrc {
        fn fingerprint(&self) -> Option<u64> {
            Some(crate::hash_component(&self.0))
        }
    }
    impl Component for EpochDef {
        fn fingerprint(&self) -> Option<u64> {
            Some(crate::hash_component(&self.0))
        }
    }
    impl Component for EpochAsk {
        fn fingerprint(&self) -> Option<u64> {
            Some(crate::hash_component(&self.0))
        }
    }
    impl Component for EpochReady {
        fn tracked() -> bool {
            false
        }
    }
    impl Component for EpochEphemeral {
        fn tracked() -> bool {
            false
        }
    }

    async fn epoch_derive(query: Query<(Entity, &EpochSrc)>, mut commands: Commands<Anything>) {
        let (entity, src) = query.item();
        commands.insert((DerivedFrom::new(entity), EpochDef(src.0.clone())));
    }

    async fn cleanup_epoch_ephemeral(
        query: Query<Entity, With<EpochEphemeral>>,
        mut commands: Commands<Anything>,
    ) {
        commands.remove(query.item());
    }

    static FREEZE_RUNS: AtomicUsize = AtomicUsize::new(0);
    static FREEZE_HOLD: AtomicBool = AtomicBool::new(true);

    async fn freeze_derive(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (entity, a) = query.item();
        FREEZE_RUNS.fetch_add(1, Ordering::SeqCst);
        while FREEZE_HOLD.load(Ordering::SeqCst) {
            yield_once().await;
        }
        commands.entity(entity).insert(B(a.0));
    }

    /// spec/epochs.md, "Epochs": external inputs arriving while a settle is
    /// actively driving belong to the NEXT epoch; mid-epoch generations
    /// must not drain them. Fails today because `start_evaluation` drains
    /// all pending inputs into every generation, including mid-settle
    /// reopened ones.
    #[test]
    fn external_insert_mid_epoch_defers_to_the_next_epoch() {
        use std::future::IntoFuture;
        use std::task::Context;

        FREEZE_RUNS.store(0, Ordering::SeqCst);
        FREEZE_HOLD.store(true, Ordering::SeqCst);

        let bowl = Bowl::builder()
            .system(freeze_derive)
            .build();
        block_on(async {
            bowl.insert((A(1),)).await;
        });

        // Become the epoch driver and suspend inside the first generation.
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut driver = Box::pin(bowl.scoop::<Query<(Entity, &B)>>().into_future());
        for _ in 0..100 {
            if FREEZE_RUNS.load(Ordering::SeqCst) >= 1 {
                break;
            }
            assert!(driver.as_mut().poll(&mut context).is_pending());
        }
        assert_eq!(FREEZE_RUNS.load(Ordering::SeqCst), 1);

        // External input arriving mid-epoch.
        block_on(bowl.insert((A(2),)).into_future());

        FREEZE_HOLD.store(false, Ordering::SeqCst);
        let rows = block_on(driver).len();
        assert_eq!(
            FREEZE_RUNS.load(Ordering::SeqCst),
            1,
            "a mid-epoch insert must not be drained into the running epoch"
        );
        assert_eq!(
            rows, 1,
            "the in-flight epoch must complete on its frozen input set"
        );

        // The deferred input is not lost: the next settle processes it.
        let rows = block_on(bowl.scoop::<Query<(Entity, &B)>>().into_future()).len();
        assert_eq!(rows, 2, "the deferred input must run in the next epoch");
        assert_eq!(FREEZE_RUNS.load(Ordering::SeqCst), 2);
    }

    static LIE_HOLD: AtomicBool = AtomicBool::new(true);
    static LIE_CONSUMER_RUNNING: AtomicBool = AtomicBool::new(false);
    static LIE_ANSWERS: StdMutex<Vec<(String, bool)>> = StdMutex::new(Vec::new());

    async fn lie_consume(
        _: Query<Entity, With<EpochReady>>,
        query: Query<(Entity, &EpochAsk)>,
        defs: View<'_, (Entity, &EpochDef)>,
    ) {
        let (_entity, ask) = query.item();
        LIE_CONSUMER_RUNNING.store(true, Ordering::SeqCst);
        while LIE_HOLD.load(Ordering::SeqCst) {
            yield_once().await;
        }
        let found = defs.iter().any(|(_, def)| def.0 == ask.0);
        LIE_ANSWERS.lock().unwrap().push((ask.0.clone(), found));
    }

    /// spec/epochs.md, "Layer 2: the lie": with inputs frozen per epoch, a
    /// marker-gated consumer can never observe mid-derivation state — every
    /// answer it computes is consistent with settled derivations. Fails
    /// today: an input drained into the marker generation plans its
    /// derivation and the gated consumer in the same wave, so the consumer
    /// records an answer from a snapshot where the source exists but its
    /// derived fact does not.
    #[test]
    fn gated_consumers_never_observe_mid_derivation_state() {
        use std::future::IntoFuture;
        use std::task::Context;

        LIE_HOLD.store(true, Ordering::SeqCst);
        LIE_CONSUMER_RUNNING.store(false, Ordering::SeqCst);
        LIE_ANSWERS.lock().unwrap().clear();

        let bowl = Bowl::builder()
            .system(epoch_derive.on_settled(|mut commands: Commands<Anything>| { commands.insert((Singleton::<EpochReady>::new(), EpochReady, EpochEphemeral)); }))
            .system(lie_consume)
            .system(cleanup_epoch_ephemeral.run_during(Phase::Settle))
            .build();
        block_on(async {

            bowl.insert((EpochSrc("beta".to_string()),)).await;
            bowl.insert((EpochAsk("beta".to_string()),)).await;
        });

        // Drive until the gated consumer is suspended inside the marker
        // generation (the marker exists, cleanup has not yet run).
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut driver = Box::pin(bowl.scoop::<Query<(Entity, &EpochDef)>>().into_future());
        for _ in 0..1000 {
            if LIE_CONSUMER_RUNNING.load(Ordering::SeqCst) {
                break;
            }
            assert!(driver.as_mut().poll(&mut context).is_pending());
        }
        assert!(LIE_CONSUMER_RUNNING.load(Ordering::SeqCst));

        // A source and a question about it arrive together, mid-epoch.
        block_on(bowl.insert((EpochSrc("gamma".to_string()),)).into_future());
        block_on(bowl.insert((EpochAsk("gamma".to_string()),)).into_future());

        LIE_HOLD.store(false, Ordering::SeqCst);
        block_on(driver);

        // Let follow-up epochs settle everything that was deferred.
        block_on(bowl.scoop::<Query<(Entity, &EpochDef)>>().into_future());

        let answers = LIE_ANSWERS.lock().unwrap().clone();
        assert!(
            answers.iter().any(|(ask, _)| ask == "gamma"),
            "the deferred question must eventually be answered: {answers:?}"
        );
        assert!(
            answers.iter().all(|(_, found)| *found),
            "every computed answer must reflect settled derivations, \
             never a mid-derivation snapshot: {answers:?}"
        );
    }

    static PREEMPT_HOLD: AtomicBool = AtomicBool::new(false);
    static PREEMPT_STARTED: AtomicUsize = AtomicUsize::new(0);
    static PREEMPT_COMPLETED: AtomicUsize = AtomicUsize::new(0);
    static PREEMPT_STARTUP_CLEANUPS: AtomicUsize = AtomicUsize::new(0);
    static PREEMPT_ANSWERS: StdMutex<Vec<(String, bool)>> = StdMutex::new(Vec::new());

    async fn preempt_consume(
        _: Query<Entity, With<EpochReady>>,
        query: Query<(Entity, &EpochAsk)>,
        defs: View<'_, (Entity, &EpochDef)>,
    ) {
        let (_entity, ask) = query.item();
        PREEMPT_STARTED.fetch_add(1, Ordering::SeqCst);
        while PREEMPT_HOLD.load(Ordering::SeqCst) {
            yield_once().await;
        }
        let found = defs.iter().any(|(_, def)| def.0 == ask.0);
        PREEMPT_ANSWERS.lock().unwrap().push((ask.0.clone(), found));
        PREEMPT_COMPLETED.fetch_add(1, Ordering::SeqCst);
    }

    async fn preempt_startup_retract(
        query: Query<Entity, With<EpochEphemeral>>,
        mut commands: Commands<Anything>,
    ) {
        PREEMPT_STARTUP_CLEANUPS.fetch_add(1, Ordering::SeqCst);
        commands.remove(query.item());
    }

    /// spec/epochs.md, "Preemption": an external `Mut` is preemptive by
    /// default — cancel, write, continue. Pins three designed behaviors:
    /// tiered preemption drops the in-flight read-only consumer (one extra
    /// start, no extra completion), the `Phase::Startup` slot retracts the
    /// ephemeral marker on restart, and the restarted epoch computes its
    /// answer from the post-write world. Fails today: the mut applies
    /// mid-flight, nothing restarts, Startup never reruns, and the
    /// suspended consumer completes against the pre-write snapshot.
    #[test]
    fn preemptive_mut_restarts_the_epoch_and_retracts_markers() {
        use std::future::IntoFuture;
        use std::task::Context;

        PREEMPT_HOLD.store(false, Ordering::SeqCst);
        PREEMPT_STARTED.store(0, Ordering::SeqCst);
        PREEMPT_COMPLETED.store(0, Ordering::SeqCst);
        PREEMPT_STARTUP_CLEANUPS.store(0, Ordering::SeqCst);
        PREEMPT_ANSWERS.lock().unwrap().clear();

        let bowl = Bowl::builder()
            .system(epoch_derive.on_settled(|mut commands: Commands<Anything>| {
                commands.insert((Singleton::<EpochReady>::new(), EpochReady, EpochEphemeral));
            }))
            .system(preempt_consume)
            .system(preempt_startup_retract.run_during(Phase::Startup))
            .system(cleanup_epoch_ephemeral.run_during(Phase::Settle))
            .build();
        let handle = block_on(async {
            bowl.insert((EpochSrc("old".to_string()),)).await;
            bowl.insert((EpochAsk("new".to_string()),)).await;

            // First epoch settles normally; the answer is "not found".
            bowl.scoop::<Query<(Entity, Mut<EpochSrc>)>>()
                .await
                .collect()
                .pop()
                .unwrap()
                .1
        });
        assert_eq!(PREEMPT_COMPLETED.load(Ordering::SeqCst), 1);

        // Open a second epoch and suspend its gated consumer inside the
        // marker generation.
        PREEMPT_HOLD.store(true, Ordering::SeqCst);
        block_on(bowl.insert((C(7),)).into_future());

        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut driver = Box::pin(bowl.scoop::<Query<(Entity, &EpochDef)>>().into_future());
        for _ in 0..1000 {
            if PREEMPT_STARTED.load(Ordering::SeqCst) >= 2 {
                break;
            }
            assert!(driver.as_mut().poll(&mut context).is_pending());
        }
        assert_eq!(PREEMPT_STARTED.load(Ordering::SeqCst), 2);

        // Preemptive mut: cancel -> write -> continue. Applied at the epoch
        // boundary from another thread while we keep driving.
        let mutator = std::thread::spawn(move || {
            block_on(handle.with_latest(|src| src.0 = "new".to_string()))
        });

        // The restarted epoch replans the consumer with a fresh marker; the
        // dropped attempt never completes. Sleep between polls so the
        // mutator thread gets scheduled and registers its preemption before
        // the loop gives up (bounded so a regression fails instead of
        // hanging).
        for _ in 0..10_000 {
            if PREEMPT_STARTED.load(Ordering::SeqCst) >= 3 {
                break;
            }
            if driver.as_mut().poll(&mut context).is_ready() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
        PREEMPT_HOLD.store(false, Ordering::SeqCst);
        block_on(driver);
        mutator.join().unwrap();
        block_on(bowl.scoop::<Query<(Entity, &EpochDef)>>().into_future());

        assert!(
            PREEMPT_STARTUP_CLEANUPS.load(Ordering::SeqCst) >= 1,
            "preempt restart must run the Startup slot and retract the marker"
        );
        assert_eq!(
            PREEMPT_STARTED.load(Ordering::SeqCst),
            3,
            "the in-flight read-only consumer must be dropped and replanned"
        );
        assert_eq!(
            PREEMPT_COMPLETED.load(Ordering::SeqCst),
            2,
            "the dropped attempt must never complete"
        );
        let last = PREEMPT_ANSWERS.lock().unwrap().last().cloned();
        assert_eq!(
            last,
            Some(("new".to_string(), true)),
            "the restarted epoch must answer from the post-write world"
        );
    }

    // --- dsql-port regression tests (TODO §1, §2, §10, §12, §14) ---
    //
    // Each test pins one failure point reported from the ~10k-line dsql
    // port. They are expected to FAIL until the corresponding fix lands.

    async fn diagnose_then_stamp(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (file, _source) = query.item();
        // The natural writing order: emit the diagnostic first...
        commands.insert((Note, DerivedFrom::new(file)));
        // ...then stamp the parse result onto the same source entity.
        commands.entity(file).insert(B(1));
    }

    /// Friction 1 (TODO §2): `DerivedFrom` anchors capture revisions in
    /// command-application order. The diagnostic above applies before `B`
    /// lands on the file entity, so its captured anchor revision is already
    /// stale when the same buffer finishes — and cleanup silently reaps it.
    /// Anchors must be resolved at buffer end.
    #[test]
    fn derived_facts_emitted_before_same_buffer_anchor_writes_survive_cleanup() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(diagnose_then_stamp)
                .system(cleanup_stale_derived.run_during(Phase::Settle))
                .build();

            bowl.insert((A(1),)).await;

            let notes = bowl.scoop::<Query<(Entity, &Note)>>().await;
            assert_eq!(
                notes.collect().len(),
                1,
                "the diagnostic was born stale and silently reaped"
            );
        });
    }

    /// Friction 2 (TODO §1): the external write API is insert-only. An LSP
    /// `didClose` must be able to retract a fact it inserted.
    #[test]
    fn external_remove_retracts_a_component() {
        block_on(async {
            let bowl = Bowl::builder().build();
            let inserted = bowl.insert((A(1), B(2))).await;

            bowl.entity(inserted.entity()).remove::<B>().await;

            let bs = bowl.scoop::<Query<(Entity, &B)>>().await;
            assert_eq!(bs.collect().len(), 0, "the retracted component must be gone");
            let survivors = bowl.scoop::<Query<(Entity, &A)>>().await;
            assert_eq!(survivors.collect().len(), 1, "siblings must survive the removal");
        });
    }

    struct ParentLink(Entity);
    impl Component for ParentLink {}

    async fn lower_linked_pair(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (_source, _a) = query.item();
        let parent = commands.insert((B(7),));
        commands.insert((C(1), ParentLink(parent)));
    }

    /// Friction 3 (TODO §1): `Commands::insert` returned nothing, so lowering
    /// could not link parent/child facts by entity id within one buffer. It
    /// must hand back the reserved id before the buffer applies — reusing the
    /// previous run's slot ids so reruns stay id-stable.
    #[test]
    fn spawned_entities_link_by_id_within_one_buffer() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(lower_linked_pair)
                .build();

            bowl.insert((A(1),)).await;

            let links = bowl.scoop::<Query<(Entity, &ParentLink)>>().await;
            let rows = links.collect();
            assert_eq!(rows.len(), 1);
            let linked_parent = rows[0].1.0;

            let parents = bowl.scoop::<Query<(Entity, &B)>>().await;
            let parent_rows = parents.collect();
            assert_eq!(parent_rows.len(), 1);
            assert_eq!(
                parent_rows[0].0, linked_parent,
                "the link must resolve to the spawned parent entity"
            );
        });
    }

    /// Friction 4 (TODO §10): plain `View`s never invalidate — deliberately —
    /// but nothing surfaces that a system's ambient reads went stale; the
    /// system just quietly stops reacting. Detection folds into `explain`:
    /// everything memoized while `stale_views` is nonzero is the footgun
    /// signature (the remedy is making the data a tracked input, e.g. the
    /// fingerprinted-index pattern).
    #[test]
    fn explain_surfaces_stale_ambient_views() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(count_bs)
                .build();

            bowl.insert((A(1),)).await;
            bowl.insert((B(1),)).await;
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(counts.collect()[0].1.0, 1);
            let report = bowl.explain("count_bs").await;
            assert_eq!(report.stale_views, 0, "the system just ran against current views");

            bowl.insert((B(2),)).await;
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            // Documented View semantics: the count deliberately does not react.
            assert_eq!(counts.collect()[0].1.0, 1);

            let report = bowl.explain("count_bs").await;
            assert_eq!(report.memoized_rows, 1);
            assert_eq!(
                report.stale_views, 1,
                "one viewed store changed since the system's last run"
            );
        });
    }

    async fn produce_in_cleanup(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (_entity, _a) = query.item();
        commands.insert((C(9),));
    }

    async fn finalize_in_cleanup(
        query: Query<(Entity, &B)>,
        candidates: View<'_, (Entity, &C)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _b) = query.item();
        commands.entity(entity).insert(Count(candidates.len()));
    }

    /// Friction 5 (TODO §12): intra-phase ordering is undefined, so a system
    /// that ambiently consumes (`View`) what a same-phase sibling produces
    /// races it — tracked consumers replan on commit, ambient ones do not.
    /// The engine should flag the combination instead of racing silently.
    /// (`Phase::Settle` is immune by construction: its inserts defer to the
    /// next run, so the forward phases are where the race lives.)
    #[test]
    #[should_panic(expected = "consumed in the same phase")]
    fn same_phase_ambient_consumption_is_flagged() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(produce_in_cleanup.run_during(Phase::Complete))
                .system(finalize_in_cleanup.run_during(Phase::Complete))
                .build();

            bowl.insert((A(1),)).await;
            bowl.insert((B(1),)).await;
            bowl.scoop::<Query<(Entity, &Count)>>().await;
        });
    }

    async fn count_optional_b(
        query: Query<(Entity, &A, Option<&B>)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _a, b) = query.item();
        commands
            .entity(entity)
            .insert(Count(b.map(|b| b.0 as usize).unwrap_or(100)));
    }

    /// `Option<&T>` in a row tuple: the row matches whether or not `T` is
    /// present, the item reports it, and *both* transitions invalidate —
    /// absence is a tracked observation, not a skipped read. Systems stop
    /// carrying side-`View`s just to look up a maybe-present component.
    #[test]
    fn optional_parts_match_and_track_presence_and_absence() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(count_optional_b)
                .build();

            let with_b = bowl.insert((A(1), B(7))).await;
            let without_b = bowl.insert((A(2),)).await;

            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = counts.collect();
            assert_eq!(rows.len(), 2, "absent B must not exclude the row");
            let count_of = |entity: Entity, rows: &[(Entity, &Count)]| {
                rows.iter().find(|(e, _)| *e == entity).map(|(_, c)| c.0)
            };
            assert_eq!(count_of(with_b.entity(), &rows), Some(7));
            assert_eq!(count_of(without_b.entity(), &rows), Some(100));

            // Absence -> presence must rerun the row.
            bowl.entity(without_b.entity()).insert((B(9),)).await;
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = counts.collect();
            assert_eq!(
                count_of(without_b.entity(), &rows),
                Some(9),
                "a component appearing must invalidate the observing row"
            );

            // Presence -> absence must rerun the row.
            bowl.entity(with_b.entity()).remove::<B>().await;
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = counts.collect();
            assert_eq!(
                count_of(with_b.entity(), &rows),
                Some(100),
                "a component disappearing must invalidate the observing row"
            );
        });
    }

    type NoteParts = (Note, Count);

    async fn declared_writer(
        query: Query<(Entity, &A)>,
        mut commands: Commands<(NoteParts, B)>,
    ) {
        let (entity, _a) = query.item();
        let note: Entity<NoteParts> = commands.insert((Note, Count(7)));
        // Required parts can't be rewritten through the facet — the
        // untyped handle keeps plain membership semantics.
        commands.entity(note.untyped()).insert(Count(7));
        commands.entity(entity).insert(B(2));
    }

    /// Typed `Commands<S>`: declared writes (directly or through a group
    /// alias) compile and behave exactly like the wildcard; the declared
    /// set reaches the registry. Spawns are strict — the bundle matches the
    /// `NoteParts` shape and the returned handle is the typed facet, which
    /// the entity builder accepts directly. Emitting an undeclared
    /// component or spawning a partial shape is a compile error, which a
    /// test cannot demonstrate — the runtime honesty backstop is pinned
    /// separately.
    #[test]
    fn declared_outputs_permit_declared_writes() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(declared_writer)
                .build();
            bowl.insert((A(1),)).await;

            let notes = bowl.scoop::<Query<Entity, With<Note>>>().await;
            assert_eq!(notes.collect().len(), 1);
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(counts.collect()[0].1.0, 7);
        });
    }

    type OptionalShape = (Note, Option<Count>);

    async fn spawn_with_and_without_optional(
        query: Query<(Entity, &A)>,
        mut commands: Commands<(OptionalShape,)>,
    ) {
        let (entity, _a) = query.item();
        // Both bundles match the same shape: optional parts are exempt
        // from the completeness half of strict matching.
        let bare: Entity<OptionalShape> = commands.insert((Note,));
        let full: Entity<OptionalShape> = commands.insert((Note, Count(3)));
        // Facet handles compare across facets by identity and flow into
        // untyped positions explicitly.
        assert_ne!(bare, full);
        assert_eq!(bare, bare.untyped());
        commands.entity(full).insert(Count(3));
        let _ = (entity, bare.untyped(), full.raw());
    }

    /// Strict spawns with `Option<T>` shape parts: a bundle matches with
    /// or without the optional, both spawns land, and the typed facet
    /// handle behaves as a plain id (comparison, `untyped`, builders).
    #[test]
    fn strict_spawns_match_shapes_with_and_without_optionals() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(spawn_with_and_without_optional)
                .build();
            bowl.insert((A(1),)).await;

            let notes = bowl.scoop::<Query<Entity, With<Note>>>().await;
            assert_eq!(notes.collect().len(), 2);
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(counts.collect().len(), 1);
        });
    }

    /// Schema with overlapping shapes: `wide` covers everything `narrow`
    /// does plus `B`.
    struct OverlappingSchema;

    impl crate::declare::Schema for OverlappingSchema {
        fn shapes() -> Vec<crate::declare::ShapeDesc> {
            use std::any::{TypeId, type_name};
            vec![
                crate::declare::ShapeDesc {
                    name: "wide",
                    required: vec![
                        (TypeId::of::<Note>(), type_name::<Note>()),
                        (TypeId::of::<Count>(), type_name::<Count>()),
                        (TypeId::of::<B>(), type_name::<B>()),
                    ],
                    optional: Vec::new(),
                },
                crate::declare::ShapeDesc {
                    name: "narrow",
                    required: vec![
                        (TypeId::of::<Note>(), type_name::<Note>()),
                        (TypeId::of::<Count>(), type_name::<Count>()),
                    ],
                    optional: Vec::new(),
                },
            ]
        }
    }

    async fn spawn_narrow(
        query: Query<(Entity, &A)>,
        mut commands: Commands<((Note, Count),)>,
    ) {
        let (_entity, _a) = query.item();
        commands.insert((Note, Count(1)));
    }

    /// Shapes may overlap: a bundle that is covered-but-incomplete for one
    /// shape (`wide`, iterated first) must still pass when a later shape
    /// (`narrow`) completes — an incomplete candidate is only a failure if
    /// no candidate passes.
    #[test]
    fn conformance_accepts_any_passing_candidate_among_overlapping_shapes() {
        block_on(async {
            let bowl = Bowl::builder()
                .schema::<OverlappingSchema>()
                .system(spawn_narrow)
                .build();
            bowl.insert((A(1),)).await;

            let notes = bowl.scoop::<Query<Entity, With<Note>>>().await;
            assert_eq!(notes.collect().len(), 1);
        });
    }

    /// On a schema bowl multi-part row matching goes through the presence
    /// bitmaps, so every transition must keep the bits exact: base insert
    /// sets them, targeted removal clears them, re-insert restores them.
    /// (Equivalence with store probing is what the assertion checks — the
    /// probing path is the same query on a schema-less bowl.)
    #[test]
    fn presence_masks_track_insert_and_removal_transitions() {
        block_on(async {
            let bowl = Bowl::builder()
                .schema::<OverlappingSchema>()
                .build();
            let inserted = bowl.insert((Note, Count(1))).await;

            let rows = bowl.scoop::<Query<(Entity, &Note, &Count)>>().await;
            assert_eq!(rows.collect().len(), 1);

            bowl.entity(inserted.entity()).remove::<Count>().await;
            let rows = bowl.scoop::<Query<(Entity, &Note, &Count)>>().await;
            assert_eq!(
                rows.collect().len(),
                0,
                "a removed component must clear its presence bit"
            );
            let notes = bowl.scoop::<Query<(Entity, &Note)>>().await;
            assert_eq!(notes.collect().len(), 1, "the other bit must survive");

            bowl.entity(inserted.entity()).insert((Count(2),)).await;
            let rows = bowl.scoop::<Query<(Entity, &Note, &Count)>>().await;
            assert_eq!(rows.collect().len(), 1, "re-insert must restore the bit");
        });
    }

    static DELTA_STAGE1_RUNS: AtomicUsize = AtomicUsize::new(0);
    static DELTA_STAGE2_RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn delta_stage1(query: Query<(Entity, &A)>, mut commands: Commands<(B,)>) {
        let (entity, a) = query.item();
        DELTA_STAGE1_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn delta_stage2(query: Query<(Entity, &B)>, mut commands: Commands<(Count,)>) {
        let (entity, b) = query.item();
        DELTA_STAGE2_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(Count(b.0 as usize + 1));
    }

    /// Delta planning must be run-for-run identical to full planning: a
    /// derivation chain over N rows runs each stage exactly N times on the
    /// first settle and exactly once per touched row after — no missed
    /// reruns (under-planning) and no spurious ones (over-planning).
    #[test]
    fn delta_planning_matches_full_planning_run_counts() {
        DELTA_STAGE1_RUNS.store(0, Ordering::SeqCst);
        DELTA_STAGE2_RUNS.store(0, Ordering::SeqCst);
        block_on(async {
            let bowl = Bowl::builder()
                .system(delta_stage1)
                .system(delta_stage2)
                .build();

            let mut entities = Vec::new();
            for index in 0..10 {
                entities.push(bowl.insert((A(index),)).await);
            }
            bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(DELTA_STAGE1_RUNS.load(Ordering::SeqCst), 10);
            assert_eq!(DELTA_STAGE2_RUNS.load(Ordering::SeqCst), 10);

            // Touch one row: exactly one rerun per stage, correct values.
            bowl.entity(entities[3].entity()).insert((A(100),)).await;
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(DELTA_STAGE1_RUNS.load(Ordering::SeqCst), 11);
            assert_eq!(DELTA_STAGE2_RUNS.load(Ordering::SeqCst), 11);
            let rows = counts.collect();
            assert_eq!(rows.len(), 10);
            assert!(
                rows.iter()
                    .any(|(entity, count)| *entity == entities[3].entity() && count.0 == 102),
                "the touched row must re-derive through the chain"
            );

            // Untouched settles plan nothing and rerun nothing.
            bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(DELTA_STAGE1_RUNS.load(Ordering::SeqCst), 11);
            assert_eq!(DELTA_STAGE2_RUNS.load(Ordering::SeqCst), 11);

            // A fresh row after the steady state joins through deltas.
            bowl.insert((A(50),)).await;
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(DELTA_STAGE1_RUNS.load(Ordering::SeqCst), 12);
            assert_eq!(DELTA_STAGE2_RUNS.load(Ordering::SeqCst), 12);
            assert_eq!(counts.collect().len(), 11);
        });
    }

    static GATED_RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn gated_stage(
        demand: Query<Entity, With<Note>>,
        rows: Query<(Entity, &A)>,
        mut commands: Commands<(B,)>,
    ) {
        let _gate = demand.item();
        let (entity, a) = rows.item();
        GATED_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(B(a.0));
    }

    /// Multi-driver delta narrowing: a tiny gate query (≤1 row by store
    /// bound) must not disqualify the row query from delta planning — one
    /// touched row replans one product row. A *dirty* gate invalidates
    /// every product row, so it falls back to the full plan.
    #[test]
    fn multi_driver_systems_stay_delta_planned() {
        GATED_RUNS.store(0, Ordering::SeqCst);
        block_on(async {
            let bowl = Bowl::builder().system(gated_stage).build();

            bowl.insert((Note,)).await;
            let mut entities = Vec::new();
            for index in 0..10 {
                entities.push(bowl.insert((A(index),)).await);
            }
            bowl.scoop::<Query<(Entity, &B)>>().await;
            assert_eq!(GATED_RUNS.load(Ordering::SeqCst), 10);

            // One touched row: exactly one product rerun.
            bowl.entity(entities[2].entity()).insert((A(100),)).await;
            bowl.scoop::<Query<(Entity, &B)>>().await;
            assert_eq!(
                GATED_RUNS.load(Ordering::SeqCst),
                11,
                "a tiny gate must not force full replans"
            );

            // Idle settle: nothing.
            bowl.scoop::<Query<(Entity, &B)>>().await;
            assert_eq!(GATED_RUNS.load(Ordering::SeqCst), 11);
        });
    }

    type NoteShape = (Note, Count, Option<B>);

    static FACET_ANCHOR_RUNS: AtomicUsize = AtomicUsize::new(0);
    static FACET_TRACKED_RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn observe_facet(query: Query<Entity<NoteShape>>) {
        let _facet = query.item();
        FACET_ANCHOR_RUNS.fetch_add(1, Ordering::SeqCst);
    }

    async fn observe_tracked_facet(query: Query<(Entity<NoteShape>, crate::Tracked<NoteShape>)>) {
        let (_facet, _tracked) = query.item();
        FACET_TRACKED_RUNS.fetch_add(1, Ordering::SeqCst);
    }

    /// Facet queries: `Entity<H>` anchors rows to entities carrying `H`'s
    /// required set (optional parts vary freely) and contributes no memo
    /// deps — an unread part changing must not rerun the anchor-only row.
    /// `Tracked<H>` is the opt-in complement: it deps the row on every
    /// part, so the same change reruns it, and an optional part appearing
    /// invalidates the absence observation.
    #[test]
    fn facet_rows_match_required_sets_and_track_on_request() {
        FACET_ANCHOR_RUNS.store(0, Ordering::SeqCst);
        FACET_TRACKED_RUNS.store(0, Ordering::SeqCst);
        block_on(async {
            let bowl = Bowl::builder()
                .system(observe_facet)
                .system(observe_tracked_facet)
                .build();

            // Full shape, shape minus the optional, and a non-conforming
            // entity (missing required Count).
            let full = bowl.insert((Note, Count(1), B(1))).await;
            bowl.insert((Note, Count(2))).await;
            bowl.insert((Note,)).await;

            let rows = bowl.scoop::<Query<Entity<NoteShape>>>().await;
            assert_eq!(
                rows.collect().len(),
                2,
                "facet rows are entities with the whole required set"
            );
            assert_eq!(FACET_ANCHOR_RUNS.load(Ordering::SeqCst), 2);
            assert_eq!(FACET_TRACKED_RUNS.load(Ordering::SeqCst), 2);

            // Changing a part the anchor-only row never read must not
            // rerun it; the tracked row must rerun.
            bowl.entity(full.entity()).insert((Count(9),)).await;
            bowl.scoop::<Query<Entity<NoteShape>>>().await;
            assert_eq!(
                FACET_ANCHOR_RUNS.load(Ordering::SeqCst),
                2,
                "the facet anchor contributes no revision deps"
            );
            assert_eq!(
                FACET_TRACKED_RUNS.load(Ordering::SeqCst),
                3,
                "Tracked<H> deps the row on every part"
            );

            // An optional part appearing invalidates the tracked row's
            // absence observation (second entity gains B).
            let notes = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let plain = notes
                .collect()
                .into_iter()
                .find(|(_, count)| count.0 == 2)
                .expect("the optional-less row exists")
                .0;
            bowl.entity(plain).insert((B(5),)).await;
            bowl.scoop::<Query<Entity<NoteShape>>>().await;
            assert_eq!(
                FACET_TRACKED_RUNS.load(Ordering::SeqCst),
                4,
                "an optional part appearing must invalidate Tracked<H>"
            );
            assert_eq!(FACET_ANCHOR_RUNS.load(Ordering::SeqCst), 2);
        });
    }

    async fn restock_oranges(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (_entity, _a) = query.item();
        // An "orange with a price tag": Rank is shared vocabulary that many
        // kinds of entities carry.
        commands.insert((C(1), Rank(12)));
    }

    async fn count_priced_ds(
        query: Query<(Entity, &B)>,
        priced: View<'_, (Entity, &D, &Rank)>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _b) = query.item();
        commands.entity(entity).insert(Count(priced.len()));
    }

    /// The same-phase flag is entity-granular: producing an entity that a
    /// same-phase view can never match (an "orange" sharing only the
    /// vocabulary component `Rank` with a `(D, Rank)` view of "priced
    /// apples") is not a race and must not panic. Type-level overlap alone
    /// used to trip this — the dsql port's blocker.
    #[test]
    fn same_phase_flag_ignores_rows_the_viewer_cannot_match() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(restock_oranges.run_during(Phase::Complete))
                .system(count_priced_ds.run_during(Phase::Complete))
                .build();

            bowl.insert((A(1),)).await;
            bowl.insert((B(1),)).await;

            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            assert_eq!(
                counts.collect()[0].1.0,
                0,
                "oranges never appear in the (D, Rank) view"
            );
        });
    }

    async fn sticker_d(query: Query<(Entity, &Rank), Without<D>>, mut commands: Commands<Anything>) {
        let (entity, _rank) = query.item();
        // The write itself is only {D}, but it *completes* the (D, Rank)
        // row on an entity that already carried Rank.
        commands.entity(entity).insert(D(1));
    }

    /// The converse guarantee: a write that completes a previously partial
    /// row must still be flagged, even though the written set alone is not
    /// a superset of what the view requires — the check inspects the
    /// entity after the write, not the write itself.
    #[test]
    #[should_panic(expected = "consumed in the same phase")]
    fn same_phase_flag_catches_writes_completing_a_viewed_row() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(sticker_d.run_during(Phase::Complete))
                .system(count_priced_ds.run_during(Phase::Complete))
                .build();

            bowl.insert((Rank(3),)).await;
            bowl.insert((B(1),)).await;
            bowl.scoop::<Query<(Entity, &Count)>>().await;
        });
    }

    async fn resolve_or_default(
        query: Query<(Entity, &FingerprintedRank), With<Request>>,
        partner: Option<Query<(Entity, &D), Where<Eq<FingerprintedRank>>>>,
        mut commands: Commands<Anything>,
    ) {
        let (request, _key) = query.item();
        match partner {
            Some(partner) => {
                let (_entity, d) = partner.item();
                commands.entity(request).insert(Count(d.0 as usize));
            }
            None => {
                commands.entity(request).insert(Count(999));
            }
        }
    }

    /// Outer join: a bound `Where<Eq<K>>` join wrapped in `Option` runs one
    /// invocation per matched pair as usual, plus exactly one `None`
    /// invocation for a provider row with zero matches — instead of
    /// silently dropping it. The unfiltered "else branch" system this
    /// replaces (the hover service's `stamp`) can then fold into the join.
    #[test]
    fn outer_joins_run_unmatched_rows_with_none() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(resolve_or_default)
                .build();

            let matched = bowl.insert((Request, FingerprintedRank(1))).await;
            let unmatched = bowl.insert((Request, FingerprintedRank(2))).await;
            let partner = bowl.insert((D(42), FingerprintedRank(1))).await;

            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = counts.collect();
            let count_of = |entity: Entity, rows: &[(Entity, &Count)]| {
                rows.iter().find(|(e, _)| *e == entity).map(|(_, c)| c.0)
            };
            assert_eq!(
                count_of(matched.entity(), &rows),
                Some(42),
                "the matched pair must join as an inner join does"
            );
            assert_eq!(
                count_of(unmatched.entity(), &rows),
                Some(999),
                "an unmatched provider row must run once with None"
            );

            // Unmatched -> matched: a partner appearing replans the pair.
            bowl.insert((D(7), FingerprintedRank(2))).await;
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = counts.collect();
            assert_eq!(count_of(unmatched.entity(), &rows), Some(7));

            // Matched -> unmatched: the partner losing its component must
            // rerun the provider row as None again.
            bowl.entity(partner.entity()).remove::<D>().await;
            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;
            let rows = counts.collect();
            assert_eq!(
                count_of(matched.entity(), &rows),
                Some(999),
                "losing the partner must flap the row back to None"
            );
        });
    }

    // --- relationship tests (spec/joins.md, "Authoring shape") ---
    // Hand-written impls of what #[relationship]/#[relationship_target]
    // will derive; the engine maintenance is what is under test.

    #[derive(Clone, PartialEq)]
    struct MemberOf(Entity);
    impl Component for MemberOf {
        fn fingerprint(&self) -> Option<u64> {
            Some(hash_component(&self.0))
        }

        fn relationship_edge(&self) -> Option<RelationshipEdge> {
            Some(RelationshipEdge::new::<Members>(self.0))
        }
    }

    #[derive(Clone, PartialEq)]
    struct Members(Vec<Entity>);
    impl Component for Members {
        fn fingerprint(&self) -> Option<u64> {
            Some(hash_component(&self.0))
        }

        fn relationship_retractions(&self) -> Vec<RelationshipRetraction> {
            relationship_retractions_for(self)
        }

        fn relationship_members(&self) -> Option<Vec<Entity>> {
            Some(self.0.clone())
        }
    }
    impl RelationshipTarget for Members {
        type Edge = MemberOf;

        fn from_members(members: Vec<Entity>) -> Self {
            Members(members)
        }

        fn members(&self) -> &[Entity] {
            &self.0
        }
    }

    /// Inserting an edge maintains the ordered inverse on the target;
    /// retargeting moves membership between inverses; removing the edge
    /// retracts it, and an emptied inverse is removed outright.
    #[test]
    fn relationships_maintain_an_ordered_fingerprinted_inverse() {
        block_on(async {
            let bowl = Bowl::builder().build();
            let parent = bowl.insert((A(0),)).await;
            let parent2 = bowl.insert((A(1),)).await;
            let child_b = bowl.insert((B(1), MemberOf(parent.entity()))).await;
            let child_c = bowl.insert((C(1), MemberOf(parent.entity()))).await;

            let members_of = |rows: &[(Entity, &Members)], target: Entity| {
                rows.iter()
                    .find(|(entity, _)| *entity == target)
                    .map(|(_, members)| members.0.clone())
            };

            let result = bowl.scoop::<Query<(Entity, &Members)>>().await;
            let rows = result.collect();
            assert_eq!(rows.len(), 1, "the inverse must appear on the target");
            assert_eq!(
                members_of(&rows, parent.entity()),
                Some(vec![child_b.entity(), child_c.entity()]),
                "members are ordered by entity id"
            );

            // Retarget: child_c moves to parent2; both inverses update.
            bowl.entity(child_c.entity())
                .insert((MemberOf(parent2.entity()),))
                .await;
            let result = bowl.scoop::<Query<(Entity, &Members)>>().await;
            let rows = result.collect();
            assert_eq!(members_of(&rows, parent.entity()), Some(vec![child_b.entity()]));
            assert_eq!(members_of(&rows, parent2.entity()), Some(vec![child_c.entity()]));

            // Removing the edge retracts membership; empty inverses vanish.
            bowl.entity(child_b.entity()).remove::<MemberOf>().await;
            let result = bowl.scoop::<Query<(Entity, &Members)>>().await;
            let rows = result.collect();
            assert_eq!(
                members_of(&rows, parent.entity()),
                None,
                "an emptied inverse is removed, keeping With<Members> meaningful"
            );
            assert_eq!(members_of(&rows, parent2.entity()), Some(vec![child_c.entity()]));
        });
    }

    static MEMBERS_RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn observe_members(query: Query<(Entity, &Members)>) {
        let (_entity, _members) = query.item();
        MEMBERS_RUNS.fetch_add(1, Ordering::SeqCst);
    }

    /// Membership is a revision-level fact: unchanged edges keep the
    /// inverse's revision (fingerprint cutoff), so consumers tracked on it
    /// rerun exactly when membership changes.
    #[test]
    fn membership_changes_invalidate_tracked_consumers() {
        block_on(async {
            MEMBERS_RUNS.store(0, Ordering::SeqCst);
            let bowl = Bowl::builder()
                .system(observe_members)
                .build();

            let parent = bowl.insert((A(0),)).await;
            let child = bowl.insert((B(1), MemberOf(parent.entity()))).await;
            bowl.scoop::<Query<(Entity, &Members)>>().await;
            assert_eq!(MEMBERS_RUNS.load(Ordering::SeqCst), 1);

            // Re-inserting the identical edge is a fingerprint hit: the
            // inverse is untouched and nothing reruns.
            bowl.entity(child.entity())
                .insert((MemberOf(parent.entity()),))
                .await;
            bowl.scoop::<Query<(Entity, &Members)>>().await;
            assert_eq!(
                MEMBERS_RUNS.load(Ordering::SeqCst),
                1,
                "an unchanged edge must not invalidate the inverse"
            );

            // A new member is a real membership change.
            bowl.insert((C(1), MemberOf(parent.entity()))).await;
            bowl.scoop::<Query<(Entity, &Members)>>().await;
            assert_eq!(MEMBERS_RUNS.load(Ordering::SeqCst), 2);
        });
    }

    async fn remove_noted(query: Query<Entity, With<Note>>, mut commands: Commands<Anything>) {
        commands.remove(query.item());
    }

    /// Removing the target entity retracts every source's edge component —
    /// edge consistency, not lifetime policy (no despawn cascade).
    #[test]
    fn removing_the_target_retracts_source_edges() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(remove_noted)
                .build();

            let parent = bowl.insert((A(0),)).await;
            bowl.insert((B(1), MemberOf(parent.entity()))).await;
            let edges = bowl.scoop::<Query<(Entity, &MemberOf)>>().await;
            assert_eq!(edges.collect().len(), 1);

            bowl.entity(parent.entity()).insert((Note,)).await;
            let edges = bowl.scoop::<Query<(Entity, &MemberOf)>>().await;
            assert_eq!(
                edges.collect().len(),
                0,
                "sources must not keep dangling edges to a removed target"
            );
            // The source entity itself survives — no cascade.
            let sources = bowl.scoop::<Query<(Entity, &B)>>().await;
            assert_eq!(sources.collect().len(), 1);
        });
    }

    async fn tag_members(
        parents: Query<(Entity, &A, &Members)>,
        member: Query<(Entity, &B), Where<In<Members>>>,
        mut commands: Commands<Anything>,
    ) {
        let (_parent, _a, _members) = parents.item();
        let (member_entity, _b) = member.item();
        commands.entity(member_entity).insert(Note);
    }

    /// `Where<In<Members>>` is an identity join: one invocation per
    /// (set-holder, member) pair, matching rows whose *entity* is in the
    /// sibling-provided inverse. Membership changes re-pair by
    /// construction, since the provider's row depends on the inverse's
    /// revision.
    #[test]
    fn in_joins_pair_members_with_their_set() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(tag_members)
                .build();

            let parent = bowl.insert((A(0),)).await;
            bowl.insert((B(1), MemberOf(parent.entity()))).await;
            bowl.insert((B(2), MemberOf(parent.entity()))).await;
            let stray = bowl.insert((B(3),)).await;

            let noted = bowl.scoop::<Query<Entity, With<Note>>>().await;
            let rows = noted.collect();
            assert_eq!(rows.len(), 2, "only members pair with the set");
            assert!(!rows.contains(&stray.entity()));

            // Joining the set re-pairs: the provider row's dep on the
            // inverse moved, and the new (parent, stray) pair plans fresh.
            bowl.entity(stray.entity())
                .insert((MemberOf(parent.entity()),))
                .await;
            let noted = bowl.scoop::<Query<Entity, With<Note>>>().await;
            assert_eq!(noted.collect().len(), 3);
        });
    }

    struct Stamp(u32);
    impl Component for Stamp {}

    async fn stamp_member_value(
        parents: Query<(Entity, &A, &Members)>,
        member: Query<(Entity, &B), Where<In<Members>>>,
        mut commands: Commands<Anything>,
    ) {
        let (_parent, _a, _members) = parents.item();
        let (member_entity, b) = member.item();
        commands.entity(member_entity).insert(Stamp(b.0));
    }

    /// A member's pure *value* change never moves the inverse's
    /// fingerprint (membership is entity-id based) nor any provider
    /// store, so a delta-hinted provider row set would miss the pair —
    /// unless the hint translates member writes to their providers
    /// through the edge. Two providers keep the driver large enough to
    /// actually plan from hints instead of full-enumerating.
    #[test]
    fn member_value_changes_replan_hinted_pairs() {
        block_on(async {
            let bowl = Bowl::builder().system(stamp_member_value).build();

            let parent_one = bowl.insert((A(1),)).await;
            let parent_two = bowl.insert((A(2),)).await;
            let member = bowl.insert((B(10), MemberOf(parent_one.entity()))).await;
            bowl.insert((B(20), MemberOf(parent_two.entity()))).await;

            let stamps = bowl.scoop::<Query<(Entity, &Stamp)>>().await;
            let mut values: Vec<u32> =
                stamps.collect().into_iter().map(|(_, s)| s.0).collect();
            values.sort_unstable();
            assert_eq!(values, vec![10, 20]);

            // External mutation of the member's value: only the B store
            // moves; the provider row is untouched.
            let sources = bowl.scoop::<Query<(Entity, Mut<B>)>>().await;
            for (entity, value) in sources.collect() {
                if entity == member.entity() {
                    value.with_latest(|b| b.0 = 11).await;
                }
            }

            let stamps = bowl.scoop::<Query<(Entity, &Stamp)>>().await;
            let mut values: Vec<u32> =
                stamps.collect().into_iter().map(|(_, s)| s.0).collect();
            values.sort_unstable();
            assert_eq!(
                values,
                vec![11, 20],
                "the changed member's pair must replan through the edge translation"
            );
        });
    }

    async fn spawn_member_when_ranked(
        query: Query<(Entity, &ParentLink, &FingerprintedRank)>,
        mut commands: Commands<Anything>,
    ) {
        let (_entity, link, rank) = query.item();
        if rank.0 == 1 {
            commands.insert((B(7), MemberOf(link.0)));
        }
    }

    /// The derived-output sweep is a removal path too: when a rerun stops
    /// emitting an edge, the diff sweep must retract its membership.
    #[test]
    fn swept_derived_edges_retract_membership() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(spawn_member_when_ranked)
                .build();

            let parent = bowl.insert((A(0),)).await;
            let driver = bowl
                .insert((ParentLink(parent.entity()), FingerprintedRank(1)))
                .await;

            let result = bowl.scoop::<Query<(Entity, &Members)>>().await;
            assert_eq!(result.collect().len(), 1, "the derived edge must register");

            // Rerun without re-emitting: the sweep removes the spawned
            // child's components, including the edge.
            bowl.entity(driver.entity())
                .insert((FingerprintedRank(2),))
                .await;
            let result = bowl.scoop::<Query<(Entity, &Members)>>().await;
            assert_eq!(
                result.collect().len(),
                0,
                "a swept edge must retract its membership"
            );
        });
    }

    async fn settle_stamp(query: Query<(Entity, &A)>, mut commands: Commands<Anything>) {
        let (_entity, _a) = query.item();
        commands.insert((Note,));
    }

    /// `Phase::Settle` cannot drive its own settle forward: inserts issued
    /// there queue as inputs for the next run, so a settled read never sees
    /// them early, and the next run opens with them present.
    #[test]
    fn settle_phase_inserts_defer_to_the_next_run() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(settle_stamp.run_during(Phase::Settle))
                .build();

            bowl.insert((A(1),)).await;
            let notes = bowl.scoop::<Query<(Entity, &Note)>>().await;
            assert_eq!(
                notes.collect().len(),
                0,
                "a settle-phase insert must not land within its own settle"
            );

            // Any new input starts the next run, which opens with the
            // deferred insert applied.
            bowl.insert((B(1),)).await;
            let notes = bowl.scoop::<Query<(Entity, &Note)>>().await;
            assert_eq!(
                notes.collect().len(),
                1,
                "the deferred insert must open the next run"
            );
        });
    }

    async fn demand_gated_check(
        query: Query<(Entity, &A)>,
        _demand: Query<Entity, With<Note>>,
        mut commands: Commands<Anything>,
    ) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(1));
    }

    /// Friction 7 (TODO §14): when a system silently does not run, the cause
    /// (starved join product, memo hit, wrong phase, wrong name) is
    /// guesswork. `explain` must report it.
    #[test]
    fn explain_reports_why_a_system_did_not_run() {
        block_on(async {
            let bowl = Bowl::builder()
                .system(demand_gated_check)
                .build();

            bowl.insert((A(1),)).await;
            bowl.scoop::<Query<(Entity, &Count)>>().await;

            let report = bowl.explain("demand_gated_check").await;
            assert!(report.registered);
            assert_eq!(report.phase, Some(Phase::Evaluate));
            assert_eq!(
                report.matched_rows, 0,
                "the demand join starves the tuple product"
            );

            bowl.insert((Note,)).await;
            bowl.scoop::<Query<(Entity, &Count)>>().await;

            let report = bowl.explain("demand_gated_check").await;
            assert_eq!(report.matched_rows, 1);
            assert_eq!(
                report.memoized_rows, 1,
                "the row ran and is now memo-current"
            );
        });
    }
}
