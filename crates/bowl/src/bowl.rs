use std::{
    any::{TypeId, type_name},
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Mutex as StdMutex},
};

use futures::{channel::oneshot, future::join_all, lock::Mutex};
use variadics_please::all_tuples;

use crate::{
    Component, Entity, IntoSystem, QueryResult,
    commands::{BaseCommandOp, InsertBaseCommand},
    query::QueryParam,
    system::{BoxedSystem, MemoEntry, Phase, SystemRun},
    world::{Snapshot, SystemId, SystemInvocation, World},
};

const DEFAULT_SETTLE_LIMIT: usize = 64;

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
    /// Bound entity handles cannot `await` in `Drop`, so dropped handles enqueue
    /// their entity here. The next bowl operation drains this queue after
    /// evaluation has had a chance to materialize request outputs.
    deferred_bound_cleanup: StdMutex<Vec<Entity>>,
}

struct State {
    world: World,
    systems: Vec<BoxedSystem>,
    memo: HashMap<SystemInvocation, MemoEntry>,
    completed_generation: u64,
    running_generation: Option<u64>,
    next_generation: u64,
    pending_generation: Option<u64>,
    pending_inputs: Vec<Box<dyn BaseCommandOp>>,
    waiters: Vec<oneshot::Sender<()>>,
    settled_revision: u64,
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

        self.bowl.ensure_evaluated(self.generation).await;
        self.bowl.settle().await;

        let result = {
            let mut state = self.bowl.inner.state.lock().await;
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
/// Taking returns `Arc<T>` handles because snapshots may still share component
/// payloads from previous generations. This preserves true destructive removal
/// from the live bowl without requiring `T: Clone`.
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
                    systems: Vec::new(),
                    memo: HashMap::new(),
                    completed_generation: 0,
                    running_generation: None,
                    next_generation: 1,
                    pending_generation: None,
                    pending_inputs: Vec::new(),
                    waiters: Vec::new(),
                    settled_revision: 0,
                    startup_ran: false,
                }),
                runner: Mutex::new(()),
                deferred_bound_cleanup: StdMutex::new(Vec::new()),
            }),
        }
    }

    /// Registers a system.
    ///
    /// Systems are stored in registration order. During evaluation, systems
    /// read from the same immutable snapshot and are polled concurrently from
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

    /// Evaluates as needed and returns a query result from the latest relevant
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
    pub async fn query<Q>(&self) -> QueryResult<Q>
    where
        Q: QueryParam,
    {
        self.settle().await;
        self.drain_deferred_bound_cleanup().await;
        let snapshot = self.snapshot().await;
        QueryResult::new(snapshot)
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
        for _ in 0..DEFAULT_SETTLE_LIMIT {
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

            self.ensure_evaluated(target).await;

            let (revision, settled_revision, clean) = {
                let state = self.inner.state.lock().await;
                (
                    state.world.revision_raw(),
                    state.settled_revision,
                    state.pending_generation.is_none() && state.running_generation.is_none(),
                )
            };

            if clean && revision == settled_revision {
                self.run_cleanup_phase().await;
                return;
            }

            self.enqueue_next_generation().await;
        }

        panic!("bowl did not settle within {DEFAULT_SETTLE_LIMIT} generations");
    }

    async fn run_cleanup_phase(&self) {
        let (systems, mut memo) = {
            let mut state = self.inner.state.lock().await;
            (state.systems.clone(), std::mem::take(&mut state.memo))
        };

        let snapshot = self.snapshot().await;
        let runs = join_all(
            systems
                .iter()
                .filter(|system| system.phase == Phase::Cleanup)
                .map(|system| system.run(&snapshot, &memo)),
        )
        .await;

        if !runs.is_empty() {
            commit_system_runs(&mut memo, &self.inner.state, runs, true).await;
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
    async fn ensure_evaluated(&self, target: u64) {
        loop {
            if self.completed_generation().await >= target {
                return;
            }

            if let Some(runner) = self.inner.runner.try_lock() {
                self.run_evaluation(runner).await;
            } else {
                self.wait_for_generation(target).await;
            }
        }
    }

    async fn completed_generation(&self) -> u64 {
        self.inner.state.lock().await.completed_generation
    }

    /// Clones the current world snapshot.
    ///
    /// Component values are stored behind `Arc`, so this is intended to be a
    /// cheap structural clone suitable for immutable system reads.
    async fn snapshot(&self) -> Snapshot {
        self.inner.state.lock().await.world.clone()
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
    async fn run_evaluation(&self, _runner: futures::lock::MutexGuard<'_, ()>) {
        let Some((generation, systems, mut memo, startup)) = self.start_evaluation().await else {
            return;
        };

        let mut normal_phase_changed = false;

        for phase in Phase::ordered(startup) {
            let snapshot = self.snapshot().await;
            let runs = join_all(
                systems
                    .iter()
                    .filter(|system| system.phase == *phase)
                    .map(|system| system.run(&snapshot, &memo)),
            )
            .await;

            if runs.is_empty() {
                continue;
            }

            let commit_completion_outputs = runs
                .iter()
                .flat_map(|run| run.outputs.iter())
                .all(|output| output.completion_only);
            normal_phase_changed |= commit_system_runs(
                &mut memo,
                &self.inner.state,
                runs,
                commit_completion_outputs,
            )
            .await;
        }

        let waiters = {
            let mut state = self.inner.state.lock().await;
            state.memo = memo;
            if !normal_phase_changed {
                state.settled_revision = state.world.revision_raw();
            }
            state.completed_generation = generation;
            state.running_generation = None;
            std::mem::take(&mut state.waiters)
        };

        for waiter in waiters {
            let _ = waiter.send(());
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
        let startup = !state.startup_ran;
        state.startup_ran = true;

        let systems = state.systems.clone();
        let memo = std::mem::take(&mut state.memo);

        Some((generation, systems, memo, startup))
    }
}

async fn commit_system_runs(
    memo: &mut HashMap<SystemInvocation, MemoEntry>,
    state: &Mutex<State>,
    runs: Vec<SystemRun>,
    commit_completion_outputs: bool,
) -> bool {
    let mut outputs = Vec::new();

    for run in runs {
        outputs.extend(
            run.outputs
                .into_iter()
                .filter(|output| commit_completion_outputs || !output.completion_only),
        );
        for (owner, entry) in run.memo_updates {
            memo.insert(owner, entry);
        }
    }

    let mut state = state.lock().await;
    let before_revision = state.world.revision_raw();
    for output in outputs {
        state.world.remove_derived_owned(&output.owner);
        for command in output.commands {
            command.apply(&mut state.world, &output.owner);
        }
    }
    state.world.revision_raw() != before_revision
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
        Bowl, Commands, Component, ComponentHookContext, Entity, Phase, Query, Singleton,
        SystemExt, View, With,
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
    struct Hooked;
    struct UntrackedMarker;

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
    static PHASE_LOG: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());

    async fn make_b(query: Query<(Entity, &A)>, mut commands: Commands) {
        let (entity, a) = query.item();
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
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

    async fn count_bs(
        query: Query<(Entity, &A)>,
        bs: View<'_, (Entity, &B)>,
        mut commands: Commands,
    ) {
        let (entity, _a) = query.item();
        commands.entity(entity).insert(Count(bs.len()));
    }

    async fn spawn_b(query: Query<(Entity, &A)>, mut commands: Commands) {
        let (_entity, a) = query.item();
        commands.insert((B(a.0 + 1),));
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
        commands.entity(entity).insert(Answer(processed.len() as u32));
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

    #[test]
    fn query_runs_pending_generation() {
        block_on(async {
            REQUEST_RUNS.store(0, Ordering::SeqCst);
            let bowl = Bowl::new();
            bowl.add_system(make_b).await;

            let inserted = bowl.insert((A(41),)).await;
            let result = bowl.query::<(Entity, &B)>().await;
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
            let result = bowl.query::<(Entity, &C)>().await;
            let rows = result.collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.0, 2);
            assert_eq!(CLEAN_RUNS.load(Ordering::SeqCst), 1);

            assert_eq!(bowl.query::<(Entity, &C)>().await.len(), 1);
            assert_eq!(CLEAN_RUNS.load(Ordering::SeqCst), 1);
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

            let result = bowl.query::<(Entity, &Count)>().await;
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

            let result = bowl.query::<(Entity, &B)>().await;
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
            let result = bowl.query::<(Entity, &B)>().await;
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

            let result = bowl.query::<(Entity, &Count)>().await;
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

            let result = bowl.query::<(Entity, &Sum)>().await;
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
            assert_eq!(bowl.query::<(Entity, &Count)>().await.len(), 0);

            bowl.insert((C(0),)).await;
            let result = bowl.query::<(Entity, &Count)>().await;
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

            let result = bowl.query::<(Entity, &A)>().await;
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

            let result = bowl.query::<(Entity, &Count)>().await;
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
            bowl.query::<(Entity, &B)>().await;

            let log = PHASE_LOG.lock().expect("phase log lock poisoned").clone();
            assert_eq!(log, ["startup", "evaluate-after-startup", "cleanup"]);

            bowl.insert((A(2),)).await;
            bowl.query::<(Entity, &B)>().await;

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

            bowl.query::<Entity>().await;

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

            assert_eq!(bowl.query::<(Entity, &Hooked)>().await.len(), 0);
            assert_eq!(HOOK_INSERTS.load(Ordering::SeqCst), 1);
            assert_eq!(HOOK_REMOVES.load(Ordering::SeqCst), 1);
            assert_eq!(HOOK_ENTITY_REMOVES.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn untracked_components_do_not_invalidate_clean_systems() {
        block_on(async {
            REQUEST_RUNS.store(0, Ordering::SeqCst);

            let bowl = Bowl::new();
            bowl.add_system(make_b).await;
            bowl.insert((A(1),)).await;

            assert_eq!(bowl.query::<(Entity, &B)>().await.len(), 1);
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);

            bowl.insert((UntrackedMarker,)).await;

            assert_eq!(bowl.query::<(Entity, &B)>().await.len(), 1);
            assert_eq!(REQUEST_RUNS.load(Ordering::SeqCst), 1);
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

            let counts = bowl.query::<(Entity, &Count)>().await;

            assert_eq!(counts.len(), 1);
            assert_eq!(bowl.query::<(Entity, &Note)>().await.len(), 0);
        });
    }

    #[test]
    fn on_complete_waits_for_same_phase_upstream_work_to_settle() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(make_b_uncounted).await;
            bowl.add_system(mark_b_processed.on_complete(|mut commands: Commands| {
                commands.insert((Singleton::<Note>::new(), Note, UntrackedMarker));
            }))
            .await;
            bowl.add_system(count_bs_after_note.run_during(Phase::Complete))
                .await;
            bowl.add_system(cleanup_untracked_marker.run_during(Phase::Cleanup))
                .await;

            bowl.insert((B(0),)).await;
            assert_eq!(bowl.query::<(Entity, &Count)>().await.len(), 1);

            bowl.insert((A(1),)).await;

            let result = bowl.query::<(Entity, &Count)>().await;
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
            bowl.add_system(mark_b_processed.on_complete(|mut commands: Commands| {
                commands.insert((Singleton::<UntrackedMarker>::new(), UntrackedMarker));
            }))
            .await;
            bowl.add_system(answer_after_untracked_marker.run_during(Phase::Complete))
                .await;
            bowl.add_system(cleanup_untracked_marker.run_during(Phase::Cleanup))
                .await;

            bowl.insert((B(0),)).await;
            bowl.query::<(Entity, &D)>().await;

            let answer = bowl.insert((A(1), Request)).await.bind();

            assert_eq!(answer.take::<Answer>().await.unwrap().0, 2);
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

            let result = bowl.query::<(Entity, &Sum)>().await;
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
            assert_eq!(bowl.query::<(Entity, &Answer)>().await.len(), 0);
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
            assert_eq!(bowl.query::<(Entity, &NonCloneAnswer)>().await.len(), 0);
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
            assert_eq!(bowl.query::<(Entity, &Answer)>().await.len(), 0);
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
            assert_eq!(bowl.query::<(Entity, &Note)>().await.len(), 0);
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

            assert_eq!(bowl.query::<(Entity, &Answer)>().await.len(), 0);
            assert_eq!(bowl.query::<(Entity, &Request)>().await.len(), 0);
        });
    }
}
