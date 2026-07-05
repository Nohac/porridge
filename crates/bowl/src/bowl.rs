use std::{
    any::{TypeId, type_name},
    collections::{HashMap, HashSet},
    fmt,
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
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
    commands::{BaseCommandOp, InsertBaseCommand},
    query::{
        Access, ArgBundle, CowQueryParam, EntityMutResult, ExternalFilter, ExternalQueryFilter,
        ExternalReadQueryParam, Mut, MutResult, Named, QueryArgs,
    },
    system::{BoxedSystem, MemoEntry, Phase, SystemRun},
    world::{Snapshot, SystemId, SystemInvocation, World},
};

const DEFAULT_COMMIT_LIMIT: u64 = 10_000;

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
    waiters: Vec<oneshot::Sender<()>>,
    settled_revision: u64,
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

/// Builder for an external bowl scoop.
///
/// `ScoopBuilder` can be awaited directly to produce the requested result, or
/// it can first receive runtime filter arguments with [`ScoopBuilder::args`].
pub struct ScoopBuilder<S> {
    bowl: Bowl,
    args: QueryArgs,
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
}

impl<S> ScoopBuilder<S>
where
    S: ExternalScoop,
{
    async fn materialize(self) -> S::Output {
        self.bowl.settle().await;
        self.bowl.drain_deferred_bound_cleanup().await;
        let snapshot = self.bowl.snapshot().await;
        S::materialize(&self.bowl, &snapshot, &self.args, None)
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

        let mut state = self.bowl.inner.state.lock().await;
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

        let mut state = self.bowl.inner.state.lock().await;
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

        let result = {
            let mut state = self.bowl.inner.state.lock().await;
            // Taking unwraps component cells, which must not be kept alive by
            // the shared snapshot cache.
            state.snapshot_cache = None;
            let result = T::take(&mut state.world, entity);
            cleanup_bound_entity(&mut state, entity);
            state.settled_revision = state.world.revision_raw();
            result
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
}

impl<T> TakeBundle for Option<T>
where
    T: Component,
{
    type Output = Option<Arc<T>>;

    fn take(world: &mut World, entity: Entity) -> Result<Self::Output, TakeError> {
        Ok(world.remove_component::<T>(entity))
    }
}

impl Default for Bowl {
    fn default() -> Self {
        Self::new()
    }
}

impl Bowl {
    /// Creates an empty async bowl.
    ///
    /// The initial completed generation is `0`; the first inserted input is
    /// assigned to generation `1`.
    pub fn new() -> Self {
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
                    waiters: Vec::new(),
                    settled_revision: 0,
                    normal_clean: true,
                    startup_ran: false,
                }),
                runner: Mutex::new(()),
                commit_limit: StdMutex::new(CommitLimit::default()),
                deferred_bound_cleanup: StdMutex::new(Vec::new()),
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
        f: F,
    ) -> Option<R>
    where
        T: Component,
        F: FnOnce(&mut T) -> R,
    {
        let mut state = self.inner.state.lock().await;
        if state.world.revision::<T>(entity) != original_revision {
            return None;
        }

        apply_component_mutation::<T, F, R>(&mut state, entity, f)
    }

    pub(crate) async fn with_component_mut<T, F, R>(&self, entity: Entity, f: F) -> Option<R>
    where
        T: Component,
        F: FnOnce(&mut T) -> R,
    {
        let mut state = self.inner.state.lock().await;
        apply_component_mutation::<T, F, R>(&mut state, entity, f)
    }

    /// Returns whether the live world currently holds derived outputs owned by
    /// `owner`.
    ///
    /// Snapshots do not carry the ownership index, so settled hooks check the
    /// live bowl instead.
    pub(crate) async fn has_derived_owned(&self, owner: &SystemInvocation) -> bool {
        self.inner.state.lock().await.world.has_derived_owned(owner)
    }

    /// Registers a system.
    ///
    /// Systems are stored in registration order. During evaluation, systems
    /// plan from the same structural snapshot and are polled concurrently from
    /// the active runner. Their buffered outputs are still committed in
    /// registration order.
    ///
    /// This method is async only because registration mutates shared internal
    /// state through an executor-agnostic mutex.
    pub async fn add_system<S, M>(&self, system: S)
    where
        S: IntoSystem<M>,
    {
        let mut state = self.inner.state.lock().await;
        let id = SystemId(state.systems.len());
        state.systems.push(system.into_system(id));
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
    pub async fn insert<B>(&self, bundle: B) -> InsertedEntity
    where
        B: Bundle,
    {
        let mut state = self.inner.state.lock().await;
        let entity = B::singleton_key()
            .map(|key| state.world.singleton_entity_or_spawn(key))
            .unwrap_or_else(|| state.world.spawn_empty());
        let mut commands = Vec::new();
        bundle.queue(entity, &mut commands);
        state.pending_inputs.extend(commands);
        let next_generation = state.next_generation;
        let generation = *state.pending_generation.get_or_insert(next_generation);

        InsertedEntity {
            bowl: self.clone(),
            entity,
            generation,
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

        let mut state = self.inner.state.lock().await;
        for entity in cleanup {
            cleanup_bound_entity(&mut state, entity);
        }
        state.settled_revision = state.world.revision_raw();
    }

    /// Runs generations until the bowl has no pending work and the last
    /// generation produced no tracked changes.
    async fn settle(&self) {
        let mut commit_budget = CommitBudget::new(self.commit_limit());

        loop {
            let target = {
                let state = self.inner.state.lock().await;
                if state.pending_generation.is_none()
                    && state.running_generation.is_none()
                    && state.world.revision_raw() == state.settled_revision
                {
                    return;
                }

                state
                    .pending_generation
                    .or(state.running_generation)
                    .unwrap_or(state.completed_generation)
            };

            self.ensure_evaluated(target, &mut commit_budget).await;

            let (revision, settled_revision, clean, normal_clean) = {
                let state = self.inner.state.lock().await;
                (
                    state.world.revision_raw(),
                    state.settled_revision,
                    state.pending_generation.is_none() && state.running_generation.is_none(),
                    state.normal_clean,
                )
            };

            if clean && revision == settled_revision {
                return;
            }

            if clean && normal_clean {
                if self.run_settled_hooks(&mut commit_budget).await {
                    self.enqueue_next_generation().await;
                    continue;
                }

                self.run_cleanup_phase().await;
                return;
            }

            self.enqueue_next_generation().await;
        }
    }

    async fn run_settled_hooks(&self, commit_budget: &mut CommitBudget) -> bool {
        let (systems, mut memo) = {
            let mut state = self.inner.state.lock().await;
            (state.systems.clone(), std::mem::take(&mut state.memo))
        };

        let snapshot = self.snapshot().await;
        let bowl = self.clone();
        let runs = join_all(
            systems
                .iter()
                .filter(|system| system.phase != Phase::Cleanup)
                .map(|system| system.run_settled(bowl.clone(), &snapshot, &memo)),
        )
        .await;

        let progress = if runs.is_empty() {
            CommitProgress::default()
        } else {
            commit_system_runs(&mut memo, &self.inner.state, runs).await
        };
        commit_budget.record(progress.commits);

        let mut state = self.inner.state.lock().await;
        state.memo = memo;
        if !progress.needs_followup {
            state.settled_revision = state.world.revision_raw();
        }

        progress.needs_followup
    }

    async fn run_cleanup_phase(&self) {
        let (systems, mut memo) = {
            let mut state = self.inner.state.lock().await;
            (state.systems.clone(), std::mem::take(&mut state.memo))
        };

        let snapshot = self.snapshot().await;
        let memo_snapshot = Arc::new(memo.clone());
        let mut runs = systems
            .iter()
            .filter(|system| system.phase == Phase::Cleanup)
            .flat_map(|system| {
                system.stream_runs(self.clone(), Arc::clone(&snapshot), &memo_snapshot)
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
            commit_system_run(&mut memo, &self.inner.state, run).await;
        }

        let mut state = self.inner.state.lock().await;
        state.memo = memo;
        state.settled_revision = state.world.revision_raw();
    }

    async fn enqueue_next_generation(&self) {
        let mut state = self.inner.state.lock().await;
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
        self.inner.state.lock().await.completed_generation
    }

    /// Returns the current world snapshot, sharing the cached one when the
    /// world has not changed since it was taken.
    ///
    /// Component values are stored in shared guarded cells, so a fresh
    /// snapshot is a structural clone of the store maps, not of user data.
    async fn snapshot(&self) -> Arc<Snapshot> {
        let mut state = self.inner.state.lock().await;
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

    /// Suspends until any generation completes, then lets the caller re-check
    /// its target.
    ///
    /// Waiters are deliberately broad: waking does not mean the specific target
    /// completed, only that progress happened. The caller loops and verifies the
    /// generation again, which also handles newly queued work.
    async fn wait_for_generation(&self, target: u64) {
        let receiver = {
            let mut state = self.inner.state.lock().await;
            if state.completed_generation >= target {
                return;
            }

            let (sender, receiver) = oneshot::channel();
            state.waiters.push(sender);
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
        let Some((generation, systems, mut memo, startup)) = self.start_evaluation().await else {
            return;
        };

        let mut normal_phase_changed = false;

        for phase in Phase::ordered(startup) {
            normal_phase_changed |= self
                .run_phase_streaming(&systems, *phase, &mut memo, commit_budget)
                .await;
        }

        let waiters = {
            let mut state = self.inner.state.lock().await;
            state.memo = memo;
            state.normal_clean = !normal_phase_changed;
            state.completed_generation = generation;
            state.running_generation = None;
            std::mem::take(&mut state.waiters)
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
    ) -> bool {
        let mut phase_changed = false;
        let mut running = HashSet::new();
        let mut running_access: HashMap<SystemInvocation, Vec<Access>> = HashMap::new();
        let mut runs = FuturesUnordered::new();
        let mut needs_plan = true;
        let mut deferred_conflicts = false;

        loop {
            if needs_plan {
                deferred_conflicts = false;
                let snapshot = self.snapshot().await;
                let memo_snapshot = Arc::new(memo.clone());
                for planned in systems
                    .iter()
                    .filter(|system| system.phase == phase)
                    .flat_map(|system| {
                        system.stream_runs(self.clone(), Arc::clone(&snapshot), &memo_snapshot)
                    })
                {
                    if !running.insert(planned.owner.clone()) {
                        continue;
                    }

                    if conflicts_with_running(&planned.access, &running_access) {
                        running.remove(&planned.owner);
                        deferred_conflicts = true;
                        continue;
                    }

                    let owner = planned.owner;
                    running_access.insert(owner.clone(), planned.access);
                    runs.push(async move {
                        let run = planned.run.await;
                        (owner, run)
                    });
                }
            }

            let Some(first) = runs.next().await else {
                return phase_changed;
            };

            // Commit everything that has already finished before deciding
            // whether the phase needs another planning wave.
            let mut batch = vec![first];
            while let Some(Some(next)) = runs.next().now_or_never() {
                batch.push(next);
            }

            let mut followup = false;
            let mut stale = false;
            for (owner, run) in batch {
                running.remove(&owner);
                running_access.remove(&owner);
                let progress = commit_system_run(memo, &self.inner.state, run).await;
                commit_budget.record(progress.commits);
                followup |= progress.needs_followup;
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
        let mut state = self.inner.state.lock().await;
        let generation = state.pending_generation.take()?;
        let inputs = std::mem::take(&mut state.pending_inputs);

        for input in inputs {
            input.apply(&mut state.world);
        }

        state.running_generation = Some(generation);
        state.next_generation = generation + 1;
        state.normal_clean = false;
        let startup = !state.startup_ran;
        state.startup_ran = true;

        let systems = state.systems.clone();
        let memo = std::mem::take(&mut state.memo);

        Some((generation, systems, memo, startup))
    }
}

fn apply_component_mutation<T, F, R>(state: &mut State, entity: Entity, f: F) -> Option<R>
where
    T: Component,
    F: FnOnce(&mut T) -> R,
{
    let (changed, result) = state.world.update_component_live::<T, F, R>(entity, f)?;

    if changed {
        state.normal_clean = false;
        if state.pending_generation.is_none() {
            let next_generation = state.next_generation;
            state.pending_generation = Some(next_generation);
        }
    }

    Some(result)
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
    let mut progress = CommitProgress::default();
    for run in runs {
        let next = commit_system_run(memo, state, run).await;
        progress.needs_followup |= next.needs_followup;
        progress.commits += next.commits;
    }

    progress
}

async fn commit_system_run(
    memo: &mut HashMap<SystemInvocation, MemoEntry>,
    state: &Mutex<State>,
    run: SystemRun,
) -> CommitProgress {
    let outputs = run.outputs;
    let memo_updates = run.memo_updates;

    let mut state = state.lock().await;
    if !memo_updates
        .iter()
        .all(|(_owner, entry)| entry.is_current(&state.world))
    {
        return CommitProgress {
            stale: true,
            ..CommitProgress::default()
        };
    }

    let before_revision = state.world.revision_raw();
    let before_mutations = state.world.mutations_raw();

    // Replace outputs by diffing: commands apply over the invocation's old
    // outputs so unchanged fingerprints keep their revisions, then whatever
    // the rerun did not re-emit is removed.
    for output in outputs {
        let previous = state.world.take_derived_outputs(&output.owner);
        for command in output.commands {
            command.apply(&mut state.world, &output.owner);
        }
        state.world.finish_derived_spawns(&output.owner);
        state.world.remove_derived_stale(&output.owner, previous);
    }
    for (owner, entry) in memo_updates {
        memo.insert(owner, entry);
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
        atomic::{AtomicUsize, Ordering},
    };

    use futures::executor::block_on;

    use crate::{
        And, Bowl, Commands, CommitLimit, Component, ComponentHookContext, Cow, DerivedFrom,
        Entity, Eq, Gte, Mut, Named, Phase, Query, Singleton, SystemExt, View, Where, With,
        cleanup_stale_derived,
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

    async fn make_b(query: Query<(Entity, &A)>, mut commands: Commands) {
        let (entity, a) = query.item();
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn make_b_with_hook_log(query: Query<(Entity, &A)>, mut commands: Commands) {
        let (entity, a) = query.item();
        SYSTEM_HOOK_LOG
            .lock()
            .expect("system hook log lock poisoned")
            .push("row");
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn make_b_uncounted(query: Query<(Entity, &A)>, mut commands: Commands) {
        let (entity, a) = query.item();
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn make_c(query: Query<(Entity, &A)>, mut commands: Commands) {
        let (entity, a) = query.item();
        CLEAN_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(C(a.0 + 1));
    }

    async fn make_c_from_b(query: Query<(Entity, &B)>, mut commands: Commands) {
        let (entity, b) = query.item();
        commands.entity(entity).insert(C(b.0 + 1));
    }

    async fn count_bs(
        query: Query<(Entity, &A)>,
        bs: View<'_, (Entity, &B)>,
        mut commands: Commands,
    ) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(bs.len()));
    }

    async fn count_cs(
        query: Query<(Entity, &A)>,
        cs: View<'_, (Entity, &C)>,
        mut commands: Commands,
    ) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(cs.len()));
    }

    async fn spawn_b(query: Query<(Entity, &A)>, mut commands: Commands) {
        let (_entity, a) = query.item();
        commands.insert((B(a.0 + 1),));
    }

    async fn spawn_a_from_a(query: Query<Entity, With<A>>, mut commands: Commands) {
        let _entity = query.item();
        commands.insert((A(0),));
    }

    async fn count_tagged_a(query: Query<(Entity, &A), With<Request>>, mut commands: Commands) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(1));
    }

    async fn sum_a_b(
        a_query: Query<(Entity, &A)>,
        b_query: Query<(Entity, &B)>,
        mut commands: Commands,
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
        mut commands: Commands,
    ) {
        let (entity, _a) = a_query.item();
        let (_ready, _c) = c_query.item();
        commands.entity(entity).insert(Count(bs.len()));
    }

    async fn write_singleton_count(query: Query<(Entity, &A)>, mut commands: Commands) {
        let (_entity, a) = query.item();
        commands.insert((Singleton::<Count>::new(), Count(a.0 as usize)));
    }

    async fn copy_rank_to_count(query: Query<(Entity, &Rank)>, mut commands: Commands) {
        let (entity, rank) = query.item();
        commands.entity(entity).insert(Count(rank.0 as usize));
    }

    async fn copy_rank_to_count_counted(query: Query<(Entity, &Rank)>, mut commands: Commands) {
        let (entity, rank) = query.item();
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(Count(rank.0 as usize));
    }

    async fn copy_fingerprinted_rank_to_count(
        query: Query<(Entity, &FingerprintedRank)>,
        mut commands: Commands,
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

    async fn write_rank_for_access_test(query: Query<(Entity, Mut<Rank>)>) {
        let (_entity, rank) = query.item();
        let writers = ACTIVE_WRITERS.fetch_add(1, Ordering::SeqCst) + 1;
        record_max(&MAX_ACTIVE_WRITERS, writers);
        assert_eq!(ACTIVE_READERS.load(Ordering::SeqCst), 0);
        assert!(rank.entity().raw() < u64::MAX);
        yield_once().await;
        assert_eq!(ACTIVE_READERS.load(Ordering::SeqCst), 0);
        ACTIVE_WRITERS.fetch_sub(1, Ordering::SeqCst);
    }

    async fn startup_phase(query: Query<(Entity, &A)>, mut commands: Commands) {
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

    async fn remove_hooked_entity(query: Query<(Entity, &Hooked)>, mut commands: Commands) {
        let (entity, _hooked) = query.item();
        commands.remove(entity);
    }

    async fn mark_b_processed(query: Query<(Entity, &B)>, mut commands: Commands) {
        let (entity, _b) = query.item();
        commands.entity(entity).insert(D(1));
    }

    async fn count_after_note(
        _: Query<Entity, With<Note>>,
        query: Query<(Entity, &A)>,
        mut commands: Commands,
    ) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(1));
    }

    async fn count_bs_after_note(
        _: Query<Entity, With<Note>>,
        query: Query<(Entity, &D)>,
        processed: View<'_, (Entity, &D)>,
        mut commands: Commands,
    ) {
        let (entity, _d) = query.item();
        commands.entity(entity).insert(Count(processed.len()));
    }

    async fn answer_after_untracked_marker(
        _: Query<Entity, With<UntrackedMarker>>,
        query: Query<(Entity, &Request)>,
        processed: View<'_, (Entity, &D)>,
        mut commands: Commands,
    ) {
        let (entity, _request) = query.item();
        commands
            .entity(entity)
            .insert(Answer(processed.len() as u32));
    }

    async fn cleanup_untracked_marker(
        query: Query<Entity, With<UntrackedMarker>>,
        mut commands: Commands,
    ) {
        commands.remove(query.item());
    }

    async fn mixed_param_system(
        a_query: Query<(Entity, &A)>,
        bs: View<'_, (Entity, &B)>,
        mut commands: Commands,
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

    async fn answer_request(query: Query<(Entity, &Request)>, mut commands: Commands) {
        let (entity, _request) = query.item();
        commands.entity(entity).insert(Answer(42));
    }

    async fn answer_request_with_non_clone(
        query: Query<(Entity, &Request)>,
        mut commands: Commands,
    ) {
        let (entity, _request) = query.item();
        commands.entity(entity).insert(NonCloneAnswer(42));
    }

    async fn make_derived_from_answer_from_view(
        query: Query<(Entity, &Request)>,
        values: View<'_, (Entity, &MutableA)>,
        mut commands: Commands,
    ) {
        let (_request, _request_marker) = query.item();
        let (entity, a) = values.iter().next().unwrap();
        commands.insert((DerivedFrom::new(entity), Answer(a.0)));
    }

    async fn make_multi_derived_from_answer_from_view(
        query: Query<(Entity, &Request)>,
        values: View<'_, (Entity, &MutableA)>,
        labels: View<'_, (Entity, &Label)>,
        mut commands: Commands,
    ) {
        let (_request, _request_marker) = query.item();
        let (value_entity, value) = values.iter().next().unwrap();
        let (label_entity, _label) = labels.iter().next().unwrap();
        commands.insert((
            DerivedFrom::many([value_entity, label_entity]),
            Answer(value.0),
        ));
    }

    async fn spawn_b_note_from_a(query: Query<(Entity, &MutableA)>, mut commands: Commands) {
        let (entity, a) = query.item();
        commands.insert((DerivedFrom::new(entity), B(a.0 % 2)));
    }

    #[test]
    fn rerun_replaces_spawned_outputs_and_reuses_entity_ids() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(spawn_b_note_from_a).await;
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
            let bowl = Bowl::new();
            bowl.add_system(make_b).await;

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
            let bowl = Bowl::new();
            bowl.add_system(make_c).await;

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
            let bowl = Bowl::new();
            bowl.add_system(make_derived_from_answer_from_view).await;
            bowl.add_system(cleanup_stale_derived.run_during(Phase::Cleanup))
                .await;

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
            let bowl = Bowl::new();
            bowl.add_system(make_multi_derived_from_answer_from_view)
                .await;
            bowl.add_system(cleanup_stale_derived.run_during(Phase::Cleanup))
                .await;

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
            let bowl = Bowl::new();
            bowl.add_system(count_bs).await;

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
    fn system_added_after_input_runs_on_existing_rows() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.insert((A(41),)).await;
            bowl.add_system(make_b_uncounted).await;

            let result = bowl.scoop::<Query<(Entity, &B)>>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 42);
        });
    }

    #[test]
    fn commands_can_insert_derived_entities() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(spawn_b).await;

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
            let bowl = Bowl::new();
            bowl.add_system(count_tagged_a).await;

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
            let bowl = Bowl::new();
            bowl.add_system(sum_a_b).await;

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
            let bowl = Bowl::new();
            bowl.add_system(count_a_when_c_exists).await;

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
            let bowl = Bowl::new();
            bowl.add_system(make_b_uncounted).await;
            bowl.add_system(make_c_from_b).await;
            bowl.add_system(count_cs.run_during(Phase::Complete)).await;

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
            let bowl = Bowl::new();

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
            let bowl = Bowl::new();
            bowl.add_system(write_singleton_count).await;

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
            let bowl = Bowl::new();
            bowl.insert((Singleton::<A>::new(), Singleton::<B>::new(), A(1), B(2)))
                .await;
        });
    }

    #[test]
    fn commit_limit_is_configurable() {
        let bowl = Bowl::new();

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
            let bowl = Bowl::new();
            bowl.set_commit_limit(CommitLimit::Max(2));
            bowl.add_system(spawn_a_from_a).await;

            bowl.insert((A(1),)).await;
            bowl.scoop::<Query<Entity, With<A>>>().await;
        });
    }

    #[test]
    fn systems_can_run_during_specific_phases() {
        block_on(async {
            PHASE_LOG.lock().expect("phase log lock poisoned").clear();

            let bowl = Bowl::new();
            bowl.add_system(startup_phase.run_during(Phase::Startup))
                .await;
            bowl.add_system(evaluate_phase).await;
            bowl.add_system(cleanup_phase.run_during(Phase::Cleanup))
                .await;

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

            let bowl = Bowl::new();
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

            let bowl = Bowl::new();
            bowl.add_system(remove_hooked_entity).await;
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

            let bowl = Bowl::new();
            bowl.add_system(make_b).await;
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
            let bowl = Bowl::new();
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
        let _builder = Bowl::new()
            .scoop::<Query<(Entity, &A), Where<Eq<Label>>>>()
            .args((Label("main"), Label("lib")));
    }

    #[test]
    fn scoop_can_return_multiple_independent_query_results() {
        block_on(async {
            let bowl = Bowl::new();
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

            let bowl = Bowl::new();
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
            let bowl = Bowl::new();
            bowl.add_system(copy_rank_to_count).await;
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
            let bowl = Bowl::new();
            bowl.add_system(copy_rank_to_count).await;
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
            let bowl = Bowl::new();
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

            let bowl = Bowl::new();
            bowl.add_system(copy_fingerprinted_rank_to_count).await;
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

            let bowl = Bowl::new();
            bowl.add_system(copy_rank_to_count_counted).await;
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
            let bowl = Bowl::new();
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
            let bowl = Bowl::new();
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
            let bowl = Bowl::new();
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

            let bowl = Bowl::new();
            bowl.add_system(read_rank_for_access_test).await;
            bowl.add_system(read_rank_for_access_test_again).await;
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

            let bowl = Bowl::new();
            bowl.add_system(read_rank_for_access_test).await;
            bowl.add_system(write_rank_for_access_test).await;
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

            let bowl = Bowl::new();
            bowl.add_system(write_rank_for_access_test).await;
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
            let bowl = Bowl::new();
            bowl.add_system(make_b_uncounted.on_complete(|mut commands: Commands| {
                commands.insert((Singleton::<Note>::new(), Note, UntrackedMarker));
            }))
            .await;
            bowl.add_system(count_after_note.run_during(Phase::Complete))
                .await;
            bowl.add_system(cleanup_untracked_marker.run_during(Phase::Cleanup))
                .await;

            bowl.insert((A(1),)).await;

            let counts = bowl.scoop::<Query<(Entity, &Count)>>().await;

            assert_eq!(counts.len(), 1);
            assert_eq!(bowl.scoop::<Query<(Entity, &Note)>>().await.len(), 0);
        });
    }

    #[test]
    fn on_complete_waits_for_same_phase_upstream_work_to_settle() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(make_b_uncounted).await;
            bowl.add_system(mark_b_processed.on_settled(|mut commands: Commands| {
                commands.insert((Singleton::<Note>::new(), Note, UntrackedMarker));
            }))
            .await;
            bowl.add_system(count_bs_after_note.run_during(Phase::Complete))
                .await;
            bowl.add_system(cleanup_untracked_marker.run_during(Phase::Cleanup))
                .await;

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
            let bowl = Bowl::new();
            bowl.add_system(make_b_uncounted).await;
            bowl.add_system(mark_b_processed.on_settled(|mut commands: Commands| {
                commands.insert((Singleton::<UntrackedMarker>::new(), UntrackedMarker));
            }))
            .await;
            bowl.add_system(answer_after_untracked_marker.run_during(Phase::Complete))
                .await;
            bowl.add_system(cleanup_untracked_marker.run_during(Phase::Cleanup))
                .await;

            bowl.insert((B(0),)).await;
            bowl.scoop::<Query<(Entity, &D)>>().await;

            let answer = bowl.insert((A(1), Request)).await.bind();

            assert_eq!(answer.take::<Answer>().await.unwrap().0, 2);
        });
    }

    #[test]
    fn on_settled_runs_before_cleanup_and_can_continue_evaluation() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(make_b_uncounted).await;
            bowl.add_system(mark_b_processed.on_settled(|mut commands: Commands| {
                commands.insert((Singleton::<UntrackedMarker>::new(), UntrackedMarker));
            }))
            .await;
            bowl.add_system(answer_after_untracked_marker.run_during(Phase::Complete))
                .await;
            bowl.add_system(cleanup_untracked_marker.run_during(Phase::Cleanup))
                .await;

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

            let bowl = Bowl::new();
            bowl.add_system(
                make_b_with_hook_log
                    .on_start(|_commands: Commands| {
                        SYSTEM_HOOK_LOG
                            .lock()
                            .expect("system hook log lock poisoned")
                            .push("start");
                    })
                    .on_complete(|_commands: Commands| {
                        SYSTEM_HOOK_LOG
                            .lock()
                            .expect("system hook log lock poisoned")
                            .push("complete");
                    }),
            )
            .await;

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
            let bowl = Bowl::new();
            bowl.add_system(mixed_param_system).await;

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

    async fn answer_request_with_note(query: Query<(Entity, &Request)>, mut commands: Commands) {
        let (entity, _request) = query.item();
        commands.entity(entity).insert(Answer(42));
        commands.entity(entity).insert(Note);
    }

    #[test]
    fn bound_entity_take_consumes_output_and_cleans_scope() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(answer_request).await;

            let request = bowl.insert((Request,)).await.bind();
            let answer = request.take::<Answer>().await.unwrap();

            assert_eq!(answer.0, 42);
            assert_eq!(bowl.scoop::<Query<(Entity, &Answer)>>().await.len(), 0);
        });
    }

    #[test]
    fn bound_entity_take_does_not_require_clone() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(answer_request_with_non_clone).await;

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
            let bowl = Bowl::new();
            bowl.add_system(answer_request).await;

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
            let bowl = Bowl::new();
            bowl.add_system(answer_request_with_note).await;

            let request = bowl.insert((Request,)).await.bind();
            let answer = request.take::<Answer>().await.unwrap();

            assert_eq!(answer.0, 42);
            assert_eq!(bowl.scoop::<Query<(Entity, &Note)>>().await.len(), 0);
        });
    }

    #[test]
    fn dropped_bound_entity_is_cleaned_up_on_next_operation() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(answer_request).await;

            {
                let _request = bowl.insert((Request,)).await.bind();
            }

            assert_eq!(bowl.scoop::<Query<(Entity, &Answer)>>().await.len(), 0);
            assert_eq!(bowl.scoop::<Query<(Entity, &Request)>>().await.len(), 0);
        });
    }
}
