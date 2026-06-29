use std::{
    any::type_name,
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Mutex as StdMutex},
};

use futures::{channel::oneshot, lock::Mutex};
use variadics_please::all_tuples;

use crate::{
    Component, Entity, IntoSystem, QueryResult,
    commands::{BaseCommandOp, InsertBaseCommand},
    query::QueryParam,
    system::{BoxedSystem, MemoEntry},
    world::{Snapshot, SystemId, SystemInvocation, World},
};

const DEFAULT_SETTLE_LIMIT: usize = 64;

/// Async-first database and system runner.
///
/// `Bowl` is cheap to clone and all public operations take `&self`. This is
/// deliberate: callers should be able to share one bowl through `Arc<Bowl>` and
/// submit reads or inputs concurrently.
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
///
/// This avoids storing a fragile `is_running` flag while still letting readers
/// subscribe to the generation currently being produced.
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
pub trait TakeBundle {
    /// Value returned by a successful take.
    type Output;

    #[doc(hidden)]
    fn take(world: &mut World, entity: Entity) -> Result<Self::Output, TakeError>;
}

impl<T> TakeBundle for T
where
    T: Component + Clone,
{
    type Output = T;

    fn take(world: &mut World, entity: Entity) -> Result<Self::Output, TakeError> {
        world.remove_component::<T>(entity).ok_or(TakeError {
            entity,
            component: type_name::<T>(),
        })
    }
}

impl<T> TakeBundle for Option<T>
where
    T: Component + Clone,
{
    type Output = Option<T>;

    fn take(world: &mut World, entity: Entity) -> Result<Self::Output, TakeError> {
        Ok(world.remove_component::<T>(entity))
    }
}

impl Clone for Bowl {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
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
                }),
                runner: Mutex::new(()),
                deferred_bound_cleanup: StdMutex::new(Vec::new()),
            }),
        }
    }

    /// Registers a system.
    ///
    /// Systems are stored in registration order. In the current minimal async
    /// slice, systems are evaluated serially from an immutable snapshot. Later
    /// versions can run invocation futures concurrently without changing this
    /// public shape.
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
        let entity = state.world.spawn_empty();
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
        let mut last_revision = None;

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
                return;
            }

            if clean && last_revision == Some(revision) {
                let mut state = self.inner.state.lock().await;
                state.settled_revision = revision;
                return;
            }

            last_revision = Some(revision);
            self.enqueue_next_generation().await;
        }

        panic!("bowl did not settle within {DEFAULT_SETTLE_LIMIT} generations");
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
    ///   no state lock is held while user code executes
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
        let Some((generation, snapshot, systems, mut memo)) = self.start_evaluation().await else {
            return;
        };

        let mut outputs = Vec::new();
        for system in systems {
            outputs.extend(system.0.run(&snapshot, &mut memo).await);
        }

        let waiters = {
            let mut state = self.inner.state.lock().await;
            for output in outputs {
                state.world.remove_derived_owned(&output.owner);
                for command in output.commands {
                    command.apply(&mut state.world, &output.owner);
                }
            }

            state.memo = memo;
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
        Snapshot,
        Vec<BoxedSystem>,
        HashMap<SystemInvocation, MemoEntry>,
    )> {
        let mut state = self.inner.state.lock().await;
        let generation = state.pending_generation.take()?;
        let inputs = std::mem::take(&mut state.pending_inputs);

        for input in inputs {
            input.apply(&mut state.world);
        }

        state.running_generation = Some(generation);
        state.next_generation = generation + 1;

        let snapshot = state.world.clone();
        let systems = state.systems.clone();
        let memo = std::mem::take(&mut state.memo);

        Some((generation, snapshot, systems, memo))
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
    fn queue(self, entity: Entity, commands: &mut Vec<Box<dyn BaseCommandOp>>);

    #[doc(hidden)]
    fn insert_derived(self, world: &mut World, entity: Entity, owner: SystemInvocation);
}

macro_rules! impl_bundle {
    ($($T:ident),*) => {
        impl<$($T: Component),*> Bundle for ($($T,)*)
        {
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::executor::block_on;

    use crate::{Bowl, Commands, Component, Entity, Query, View};

    struct A(u32);
    struct B(u32);
    struct C(u32);
    struct Count(usize);
    struct Request;
    #[derive(Clone)]
    struct Answer(u32);
    #[derive(Clone)]
    struct Note;

    impl Component for A {}
    impl Component for B {}
    impl Component for C {}
    impl Component for Count {}
    impl Component for Request {}
    impl Component for Answer {}
    impl Component for Note {}

    static REQUEST_RUNS: AtomicUsize = AtomicUsize::new(0);
    static CLEAN_RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn make_b(Query((entity, a)): Query<(Entity, &A)>, mut commands: Commands) {
        REQUEST_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn make_b_uncounted(Query((entity, a)): Query<(Entity, &A)>, mut commands: Commands) {
        commands.entity(entity).insert(B(a.0 + 1));
    }

    async fn make_c(Query((entity, a)): Query<(Entity, &A)>, mut commands: Commands) {
        CLEAN_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(C(a.0 + 1));
    }

    async fn count_bs(
        Query((entity, _a)): Query<(Entity, &A)>,
        bs: View<'_, (Entity, &B)>,
        mut commands: Commands,
    ) {
        commands.entity(entity).insert(Count(bs.len()));
    }

    async fn spawn_b(Query((_entity, a)): Query<(Entity, &A)>, mut commands: Commands) {
        commands.insert((B(a.0 + 1),));
    }

    async fn answer_request(
        Query((entity, _request)): Query<(Entity, &Request)>,
        mut commands: Commands,
    ) {
        commands.entity(entity).insert(Answer(42));
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

    async fn answer_request_with_note(
        Query((entity, _request)): Query<(Entity, &Request)>,
        mut commands: Commands,
    ) {
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
