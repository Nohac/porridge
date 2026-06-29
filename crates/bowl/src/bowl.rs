use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures::{channel::oneshot, lock::Mutex};
use variadics_please::all_tuples;

use crate::{
    Component, Entity, Ephemeral, IntoSystem, QueryResult,
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
///
/// The handle remembers the generation that includes the inserted bundle. A
/// follow-up [`InsertedEntity::query`] waits for that generation before reading,
/// which makes request-style flows deterministic:
///
/// ```text
/// insert request -> generation G
/// wait for G
/// query only the inserted entity's outputs
/// ```
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

    /// Waits for this insert's generation and queries rows bound to this
    /// inserted entity.
    ///
    /// This is the current request-query bridge. It filters query rows by the
    /// entity key produced by [`QueryParam::keys`], so it is suitable for
    /// request outputs that are written back onto the request entity. It is not
    /// a final replacement for the planned `BoundEntity`/`Take<T>` capability.
    pub async fn query<Q>(&self) -> QueryResult<Q>
    where
        Q: QueryParam,
    {
        self.bowl.ensure_evaluated(self.generation).await;
        let result = self.bowl.query_entity::<Q>(self.entity).await;
        self.bowl.cleanup_ephemeral_entities().await;
        result
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
        let snapshot = self.snapshot().await;
        QueryResult::new(snapshot)
    }

    async fn query_entity<Q>(&self, entity: Entity) -> QueryResult<Q>
    where
        Q: QueryParam,
    {
        self.settle().await;
        let snapshot = self.snapshot().await;
        QueryResult::new_for_entity(snapshot, entity)
    }

    /// Removes ephemeral request entities and outputs that were derived from
    /// them.
    ///
    /// Cleanup is intentionally live-world only. Inserted request queries build
    /// their [`QueryResult`] snapshot first, so callers can still read the
    /// answer while the bowl is ready for later generations.
    async fn cleanup_ephemeral_entities(&self) {
        let mut state = self.inner.state.lock().await;
        if state.world.entities_with::<Ephemeral>().is_empty() {
            return;
        }

        let mut frontier: HashSet<_> = state
            .world
            .entities_with::<Ephemeral>()
            .into_iter()
            .collect();
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

        let ephemeral_entities: HashSet<_> = state
            .world
            .entities_with::<Ephemeral>()
            .into_iter()
            .collect();
        let mut removed_owners = Vec::new();

        for entity in &ephemeral_entities {
            removed_owners.extend(state.world.remove_entity(*entity));
        }

        remove_memo_touched_by(&mut state.memo, &ephemeral_entities);

        for owner in removed_owners {
            state.world.remove_derived_owned(&owner);
            state.memo.remove(&owner);
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

    use crate::{Bowl, Commands, Component, Entity, Ephemeral, Query, View};

    struct A(u32);
    struct B(u32);
    struct C(u32);
    struct Count(usize);
    struct Request;
    struct Answer(u32);

    impl Component for A {}
    impl Component for B {}
    impl Component for C {}
    impl Component for Count {}
    impl Component for Request {}
    impl Component for Answer {}

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
            let result = inserted.query::<(Entity, &B)>().await;
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

            let inserted = bowl.insert((A(1),)).await;
            bowl.insert((B(10),)).await;
            bowl.insert((B(20),)).await;

            let result = inserted.query::<(Entity, &Count)>().await;
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
    fn inserted_query_cleans_up_ephemeral_request_outputs() {
        block_on(async {
            let bowl = Bowl::new();
            bowl.add_system(answer_request).await;

            let request = bowl.insert((Ephemeral, Request)).await;
            let result = request.query::<(Entity, &Answer)>().await;
            let rows = result.collect();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, request.entity());
            assert_eq!(rows[0].1.0, 42);
            assert_eq!(bowl.query::<(Entity, &Answer)>().await.len(), 0);
        });
    }
}
