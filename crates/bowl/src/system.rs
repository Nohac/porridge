use std::{collections::HashMap, future::Future, marker::PhantomData, sync::Arc};

use async_fn_traits::{
    AsyncFn1, AsyncFn2, AsyncFn3, AsyncFn4, AsyncFn5, AsyncFn6, AsyncFn7, AsyncFn8,
};
use futures::future::{BoxFuture, FutureExt, join_all};

use crate::{
    Bowl, Commands, DerivedFrom, Entity, Query, View,
    commands::CommandOp,
    query::{Access, Dep, QueryFilter, QueryParam, filtered_access, filtered_deps, filtered_rows},
    world::{Snapshot, SystemId, SystemInvocation},
};
use variadics_please::all_tuples;

/// Coarse phase in which a system runs during one evaluation generation.
///
/// Systems registered without configuration run during [`Phase::Evaluate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Runs once before the first evaluate phase.
    Startup,
    /// Default phase for ordinary fact-producing systems.
    Evaluate,
    /// Runs after evaluate systems in the same generation.
    Complete,
    /// Runs after complete systems in the same generation.
    Cleanup,
}

impl Phase {
    pub(crate) const fn ordered(startup: bool) -> &'static [Phase] {
        if startup {
            &[Phase::Startup, Phase::Evaluate, Phase::Complete]
        } else {
            &[Phase::Evaluate, Phase::Complete]
        }
    }
}

/// Memoized dependency record for one system invocation.
///
/// Invocation identity lives in [`SystemInvocation`]; this entry records the
/// tracked component revisions observed the last time that invocation ran.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MemoEntry {
    deps: Vec<Dep>,
}

/// Buffered output from one system invocation.
///
/// The runner uses `owner` to remove old derived facts for that invocation
/// before applying `commands`.
pub(crate) struct SystemOutput {
    pub(crate) owner: SystemInvocation,
    pub(crate) commands: Vec<Box<dyn CommandOp>>,
}

/// Outputs and memo writes produced by one system for one generation.
///
/// Systems read the previous memo table immutably while they run. Each system
/// returns the memo entries it wants to publish, and the bowl merges them after
/// all concurrent system futures complete.
pub(crate) struct SystemRun {
    pub(crate) completed: bool,
    pub(crate) outputs: Vec<SystemOutput>,
    pub(crate) memo_updates: Vec<(SystemInvocation, MemoEntry)>,
}

impl SystemRun {
    fn empty() -> Self {
        Self {
            completed: false,
            outputs: Vec::new(),
            memo_updates: Vec::new(),
        }
    }
}

impl MemoEntry {
    pub(crate) fn is_current(&self, snapshot: &Snapshot) -> bool {
        self.deps.iter().all(|dep| dep.is_current(snapshot))
    }
}

pub(crate) struct PlannedSystemRun<'a> {
    pub(crate) owner: SystemInvocation,
    pub(crate) access: Vec<Access>,
    pub(crate) run: BoxFuture<'a, SystemRun>,
}

struct PlannedRun<State> {
    completed: bool,
    invocations: Vec<PlannedInvocation<State>>,
}

struct PlannedInvocation<State> {
    state: State,
    owner: SystemInvocation,
    deps: Vec<Dep>,
    access: Vec<Access>,
}

/// A value that can be used as a system function parameter.
///
/// Params control invocation behavior through their state set:
///
/// ```text
/// Query
///   one state per matching row
///
/// View / Commands
///   one unit state
/// ```
///
/// Tuple params form a cartesian product of their state sets. This lets `Query`
/// drive per-row execution while ambient params like `View` and `Commands`
/// participate in the same machinery without special role flags.
pub(crate) trait SystemParam {
    type State: Clone + Send;
    type Item<'a>: Send;

    fn states(snapshot: &Snapshot) -> Vec<Self::State>;
    fn keys(state: &Self::State) -> Vec<Entity>;
    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep>;
    fn access(snapshot: &Snapshot, state: &Self::State) -> Vec<Access>;
    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Self::State,
        commands: &Commands,
    ) -> Self::Item<'a>;
    fn always_run() -> bool {
        false
    }
}

impl<Q, Filter> SystemParam for Query<Q, Filter>
where
    Q: QueryParam,
    Filter: QueryFilter<Q> + Send,
{
    type State = Q::State;
    type Item<'a> = Query<Q::Item<'a>, Filter>;

    fn states(snapshot: &Snapshot) -> Vec<Self::State> {
        filtered_rows::<Q, Filter>(snapshot)
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        Q::keys(state)
    }

    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
        filtered_deps::<Q, Filter>(snapshot, state)
    }

    fn access(snapshot: &Snapshot, state: &Self::State) -> Vec<Access> {
        filtered_access::<Q, Filter>(snapshot, state)
    }

    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Self::State,
        _commands: &Commands,
    ) -> Self::Item<'a> {
        let mut guards = Vec::new();
        let item = Q::fetch(bowl, snapshot, state, &mut guards);
        Query::new_guarded(item, guards)
    }
}

impl<'view, Q, Filter> SystemParam for View<'view, Q, Filter>
where
    Q: QueryParam + Send,
    Filter: QueryFilter<Q> + Send + Sync,
{
    type State = ();
    type Item<'a> = View<'a, Q, Filter>;

    fn states(_snapshot: &Snapshot) -> Vec<Self::State> {
        vec![()]
    }

    fn keys(_state: &Self::State) -> Vec<Entity> {
        Vec::new()
    }

    fn deps(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Dep> {
        Vec::new()
    }

    fn access(snapshot: &Snapshot, _state: &Self::State) -> Vec<Access> {
        filtered_rows::<Q, Filter>(snapshot)
            .into_iter()
            .flat_map(|state| filtered_access::<Q, Filter>(snapshot, &state))
            .collect()
    }

    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        _state: &Self::State,
        _commands: &Commands,
    ) -> Self::Item<'a> {
        View::new(bowl.clone(), snapshot)
    }
}

impl SystemParam for Commands {
    type State = ();
    type Item<'a> = Commands;

    fn states(_snapshot: &Snapshot) -> Vec<Self::State> {
        vec![()]
    }

    fn keys(_state: &Self::State) -> Vec<Entity> {
        Vec::new()
    }

    fn deps(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Dep> {
        Vec::new()
    }

    fn access(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Access> {
        Vec::new()
    }

    fn fetch<'a>(
        _bowl: &Bowl,
        _snapshot: &'a Snapshot,
        _state: &Self::State,
        commands: &Commands,
    ) -> Self::Item<'a> {
        commands.clone()
    }
}

/// Read-only world metadata available to systems.
///
/// This is intentionally narrower than a full world reference. It exposes
/// metadata needed by infrastructure-like systems, such as revision-scoped
/// cleanup, without allowing component reads outside `Query`/`View`.
pub struct WorldMetaView<'a> {
    snapshot: &'a Snapshot,
}

impl WorldMetaView<'_> {
    /// Returns whether `derived_from` still matches its owner entity's current
    /// revision in this snapshot.
    pub fn is_current(&self, derived_from: &DerivedFrom) -> bool {
        derived_from.is_current_revision(|entity| self.snapshot.entity_revision(entity))
    }
}

impl SystemParam for WorldMetaView<'_> {
    type State = ();
    type Item<'a> = WorldMetaView<'a>;

    fn states(_snapshot: &Snapshot) -> Vec<Self::State> {
        vec![()]
    }

    fn keys(_state: &Self::State) -> Vec<Entity> {
        Vec::new()
    }

    fn deps(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Dep> {
        Vec::new()
    }

    fn access(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Access> {
        Vec::new()
    }

    fn fetch<'a>(
        _bowl: &Bowl,
        snapshot: &'a Snapshot,
        _state: &Self::State,
        _commands: &Commands,
    ) -> Self::Item<'a> {
        WorldMetaView { snapshot }
    }

    fn always_run() -> bool {
        true
    }
}

macro_rules! impl_system_param_tuple {
    ($($P:ident),*) => {
        impl<$($P: SystemParam),*> SystemParam for ($($P,)*)
        {
            type State = ($($P::State,)*);
            type Item<'a> = ($($P::Item<'a>,)*);

            fn states(snapshot: &Snapshot) -> Vec<Self::State> {
                let mut states = Vec::new();
                $(
                    let $P = $P::states(snapshot);
                )*

                for_each_state!(states, (); $($P),*);

                states
            }

            fn keys(state: &Self::State) -> Vec<Entity> {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                let mut keys = Vec::new();
                $(keys.extend($P::keys($P));)*
                keys
            }

            fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                let mut deps = Vec::new();
                $(deps.extend($P::deps(snapshot, $P));)*
                deps
            }

            fn access(snapshot: &Snapshot, state: &Self::State) -> Vec<Access> {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                let mut access = Vec::new();
                $(access.extend($P::access(snapshot, $P));)*
                access
            }

            fn fetch<'a>(
                bowl: &Bowl,
                snapshot: &'a Snapshot,
                state: &Self::State,
                commands: &Commands,
            ) -> Self::Item<'a> {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                ($($P::fetch(bowl, snapshot, $P, commands),)*)
            }

            fn always_run() -> bool {
                false $(|| $P::always_run())*
            }
        }
    };
}

macro_rules! for_each_state {
    ($out:ident, ($($picked:expr,)*);) => {
        $out.push(($($picked.clone(),)*));
    };
    ($out:ident, ($($picked:expr,)*); $head:ident $(, $tail:ident)*) => {
        for state in &$head {
            for_each_state!($out, ($($picked,)* state,); $($tail),*);
        }
    };
}

all_tuples!(impl_system_param_tuple, 1, 8, P);

fn plan_invocations<Params>(
    system: SystemId,
    snapshot: &Snapshot,
    memo: &HashMap<SystemInvocation, MemoEntry>,
) -> PlannedRun<Params::State>
where
    Params: SystemParam,
{
    let states = Params::states(snapshot);
    let completed = !states.is_empty();
    let invocations = states
        .into_iter()
        .filter_map(|state| {
            let owner = SystemInvocation {
                system,
                keys: Params::keys(&state),
            };
            let deps = Params::deps(snapshot, &state);
            let access = Params::access(snapshot, &state);

            (Params::always_run() || memo.get(&owner).is_none_or(|entry| entry.deps != deps))
                .then_some(PlannedInvocation {
                    state,
                    owner,
                    deps,
                    access,
                })
        })
        .collect();

    PlannedRun {
        completed,
        invocations,
    }
}

fn finish_invocation(
    owner: SystemInvocation,
    deps: Vec<Dep>,
    commands: Commands,
) -> (SystemOutput, (SystemInvocation, MemoEntry)) {
    let output = SystemOutput {
        owner: owner.clone(),
        commands: commands.take(),
    };
    let memo_update = (owner, MemoEntry { deps });

    (output, memo_update)
}

async fn collect_invocations<Fut>(futures: Vec<Fut>) -> SystemRun
where
    Fut: Future<Output = (SystemOutput, (SystemInvocation, MemoEntry))>,
{
    let rows = join_all(futures).await;
    let mut run = SystemRun::empty();

    for (output, memo_update) in rows {
        run.outputs.push(output);
        run.memo_updates.push(memo_update);
    }

    run
}

/// Type-erased executable system.
///
/// `Runnable` receives a structural snapshot plus an immutable view of the memo
/// table. It returns command buffers and memo updates that need to be committed
/// for this generation.
///
/// Returned futures are `Send`, so external bowl operations can be spawned on a
/// multi-threaded executor. Borrowed query data remains valid because the query
/// wrappers own read guards for the duration of each system invocation.
pub(crate) trait Runnable: Send + Sync {
    fn has_work(&self, _snapshot: &Snapshot, _memo: &HashMap<SystemInvocation, MemoEntry>) -> bool {
        false
    }

    fn run<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun>;

    fn stream_runs<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &Arc<HashMap<SystemInvocation, MemoEntry>>,
    ) -> Vec<PlannedSystemRun<'a>>;

    fn run_settled<'a>(
        &'a self,
        _bowl: Bowl,
        _snapshot: &'a Snapshot,
        _memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        async { SystemRun::empty() }.boxed()
    }
}

/// Type-erased registered system.
#[derive(Clone)]
pub struct BoxedSystem {
    pub(crate) runnable: Arc<dyn Runnable>,
    pub(crate) phase: Phase,
}

impl BoxedSystem {
    fn new(runnable: Arc<dyn Runnable>) -> Self {
        Self {
            runnable,
            phase: Phase::Evaluate,
        }
    }

    fn run_during(mut self, phase: Phase) -> Self {
        self.phase = phase;
        self
    }

    fn has_work(&self, snapshot: &Snapshot, memo: &HashMap<SystemInvocation, MemoEntry>) -> bool {
        self.runnable.has_work(snapshot, memo)
    }
}

/// Converts a user function into a registered system.
///
/// The `Marker` parameter is the usual Rust trick used to distinguish function
/// shapes without overlapping trait impls. Users do not name it directly.
///
pub trait IntoSystem<Marker>: Send + Sync + 'static {
    fn into_system(self, id: SystemId) -> BoxedSystem;
}

pub struct FunctionSystemMarker;
pub struct OnStartMarker;
pub struct OnCompleteMarker;
pub struct OnSettledMarker;

/// Callback run around system batches.
pub trait SystemCallback: Send + Sync + 'static {
    fn run(&self, commands: Commands);
}

impl<F> SystemCallback for F
where
    F: Fn(Commands) + Send + Sync + 'static,
{
    fn run(&self, commands: Commands) {
        self(commands);
    }
}

/// Extension methods for system configuration.
pub trait SystemExt: Sized {
    /// Runs `callback` once before this system starts processing invocations
    /// that are invalid for the current snapshot.
    fn on_start<C>(self, callback: C) -> OnStart<Self, C>
    where
        C: SystemCallback,
    {
        OnStart {
            system: self,
            callback,
        }
    }

    /// Runs `callback` once after this system has completed all invocations that
    /// were invalid for the current snapshot.
    fn on_complete<C>(self, callback: C) -> OnComplete<Self, C>
    where
        C: SystemCallback,
    {
        OnComplete {
            system: self,
            callback,
        }
    }

    /// Runs `callback` after normal evaluation has stopped producing tracked
    /// changes, but before cleanup and before the caller observes results.
    ///
    /// Settled hooks may run more than once while the bowl tries to settle.
    /// Keep them idempotent: a hook that writes tracked changes every time will
    /// keep the bowl alive until the commit limit is reached, unless the limit
    /// is disabled.
    fn on_settled<C>(self, callback: C) -> OnSettled<Self, C>
    where
        C: SystemCallback,
    {
        OnSettled {
            system: self,
            callback,
        }
    }

    /// Runs this system during `phase` instead of the default
    /// [`Phase::Evaluate`] phase.
    fn run_during(self, phase: Phase) -> RunDuring<Self> {
        RunDuring {
            system: self,
            phase,
        }
    }
}

impl<S> SystemExt for S {}

/// System wrapper produced by [`SystemExt::on_start`].
pub struct OnStart<S, C> {
    system: S,
    callback: C,
}

/// System wrapper produced by [`SystemExt::on_complete`].
pub struct OnComplete<S, C> {
    system: S,
    callback: C,
}

/// System wrapper produced by [`SystemExt::on_settled`].
pub struct OnSettled<S, C> {
    system: S,
    callback: C,
}

/// System wrapper produced by [`SystemExt::run_during`].
pub struct RunDuring<S> {
    system: S,
    phase: Phase,
}

struct OnCompleteSystem<C> {
    id: SystemId,
    system: BoxedSystem,
    callback: C,
}

struct OnStartSystem<C> {
    id: SystemId,
    system: BoxedSystem,
    callback: C,
}

struct OnSettledSystem<C> {
    id: SystemId,
    system: BoxedSystem,
    callback: C,
}

impl<C> Runnable for OnStartSystem<C>
where
    C: SystemCallback,
{
    fn has_work(&self, snapshot: &Snapshot, memo: &HashMap<SystemInvocation, MemoEntry>) -> bool {
        self.system.has_work(snapshot, memo)
    }

    fn run<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        async move {
            let has_work = self.system.has_work(snapshot, memo);
            let start_commands = has_work.then(|| {
                let commands = Commands::new();
                self.callback.run(commands.clone());
                SystemOutput {
                    owner: SystemInvocation {
                        system: self.id,
                        keys: Vec::new(),
                    },
                    commands: commands.take(),
                }
            });
            let mut run = self.system.run(bowl, snapshot, memo).await;

            if let Some(output) = start_commands {
                run.outputs.insert(0, output);
            }

            run
        }
        .boxed()
    }

    fn stream_runs<'a>(
        &'a self,
        _bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &Arc<HashMap<SystemInvocation, MemoEntry>>,
    ) -> Vec<PlannedSystemRun<'a>> {
        if !self.has_work(&snapshot, memo) {
            return Vec::new();
        }

        let owner = SystemInvocation {
            system: self.id,
            keys: Vec::new(),
        };
        let memo = Arc::clone(memo);
        let run = async move { self.run(_bowl, &snapshot, &memo).await }.boxed();

        vec![PlannedSystemRun {
            owner,
            access: Vec::new(),
            run,
        }]
    }
}

impl<C> Runnable for OnCompleteSystem<C>
where
    C: SystemCallback,
{
    fn has_work(&self, snapshot: &Snapshot, memo: &HashMap<SystemInvocation, MemoEntry>) -> bool {
        self.system.has_work(snapshot, memo)
    }

    fn run<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        async move {
            let mut run = self.system.run(bowl, snapshot, memo).await;
            let owner = SystemInvocation {
                system: self.id,
                keys: Vec::new(),
            };

            if !run.outputs.is_empty() {
                let commands = Commands::new();
                self.callback.run(commands.clone());
                run.outputs.push(SystemOutput {
                    owner,
                    commands: commands.take(),
                });
            }

            run
        }
        .boxed()
    }

    fn stream_runs<'a>(
        &'a self,
        _bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &Arc<HashMap<SystemInvocation, MemoEntry>>,
    ) -> Vec<PlannedSystemRun<'a>> {
        if !self.has_work(&snapshot, memo) {
            return Vec::new();
        }

        let owner = SystemInvocation {
            system: self.id,
            keys: Vec::new(),
        };
        let memo = Arc::clone(memo);
        let run = async move { self.run(_bowl, &snapshot, &memo).await }.boxed();

        vec![PlannedSystemRun {
            owner,
            access: Vec::new(),
            run,
        }]
    }
}

impl<C> Runnable for OnSettledSystem<C>
where
    C: SystemCallback,
{
    fn has_work(&self, snapshot: &Snapshot, memo: &HashMap<SystemInvocation, MemoEntry>) -> bool {
        self.system.has_work(snapshot, memo)
    }

    fn run<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        self.system.run(bowl, snapshot, memo)
    }

    fn stream_runs<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &Arc<HashMap<SystemInvocation, MemoEntry>>,
    ) -> Vec<PlannedSystemRun<'a>> {
        self.system.stream_runs(bowl, snapshot, memo)
    }

    fn run_settled<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        async move {
            let check_bowl = bowl.clone();
            let run = self.system.run(bowl, snapshot, memo).await;
            let owner = SystemInvocation {
                system: self.id,
                keys: Vec::new(),
            };
            // Ownership lives with the live world, not snapshots; the settle
            // runner does not commit while settled hooks run, so this check
            // matches the snapshot the hook planned from.
            let should_emit_settled = run.completed
                && run.outputs.is_empty()
                && !check_bowl.has_derived_owned(&owner).await;

            if !should_emit_settled {
                return SystemRun::empty();
            }

            let commands = Commands::new();
            self.callback.run(commands.clone());

            SystemRun {
                completed: true,
                outputs: vec![SystemOutput {
                    owner,
                    commands: commands.take(),
                }],
                memo_updates: Vec::new(),
            }
        }
        .boxed()
    }
}

impl<S, C, M> IntoSystem<(OnCompleteMarker, M)> for OnComplete<S, C>
where
    S: IntoSystem<M>,
    C: SystemCallback,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        let system = self.system.into_system(id);
        let phase = system.phase;
        BoxedSystem {
            runnable: Arc::new(OnCompleteSystem {
                id,
                system,
                callback: self.callback,
            }),
            phase,
        }
    }
}

impl<S, C, M> IntoSystem<(OnStartMarker, M)> for OnStart<S, C>
where
    S: IntoSystem<M>,
    C: SystemCallback,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        let system = self.system.into_system(id);
        let phase = system.phase;
        BoxedSystem {
            runnable: Arc::new(OnStartSystem {
                id,
                system,
                callback: self.callback,
            }),
            phase,
        }
    }
}

impl<S, C, M> IntoSystem<(OnSettledMarker, M)> for OnSettled<S, C>
where
    S: IntoSystem<M>,
    C: SystemCallback,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        let system = self.system.into_system(id);
        let phase = system.phase;
        BoxedSystem {
            runnable: Arc::new(OnSettledSystem {
                id,
                system,
                callback: self.callback,
            }),
            phase,
        }
    }
}

impl<S, M> IntoSystem<(RunDuringMarker, M)> for RunDuring<S>
where
    S: IntoSystem<M>,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        self.system.into_system(id).run_during(self.phase)
    }
}

pub struct RunDuringMarker;

impl BoxedSystem {
    pub(crate) fn run<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        self.runnable.run(bowl, snapshot, memo)
    }

    pub(crate) fn stream_runs<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &Arc<HashMap<SystemInvocation, MemoEntry>>,
    ) -> Vec<PlannedSystemRun<'a>> {
        self.runnable.stream_runs(bowl, snapshot, memo)
    }

    pub(crate) fn run_settled<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        self.runnable.run_settled(bowl, snapshot, memo)
    }
}

/// Builds a completion callback that inserts `component` on `entity`.
pub fn insert_on<T>(entity: Entity, component: T) -> impl SystemCallback
where
    T: crate::Component + Clone,
{
    move |mut commands: Commands| {
        commands.entity(entity).insert(component.clone());
    }
}

/// Cleanup system for entities tagged with [`DerivedFrom`].
///
/// Register this during [`Phase::Cleanup`] to remove derived entities whose
/// owner entity has changed since insertion:
///
/// ```text
/// bowl.add_system(cleanup_stale_derived.run_during(Phase::Cleanup));
/// ```
pub async fn cleanup_stale_derived(
    query: Query<(Entity, &DerivedFrom)>,
    meta: WorldMetaView<'_>,
    mut commands: Commands,
) {
    let (entity, derived_from) = query.item();

    if !meta.is_current(derived_from) {
        commands.remove(entity);
    }
}

struct FunctionSystem<F, Marker> {
    id: SystemId,
    function: F,
    _marker: PhantomData<Marker>,
}

impl<F, Marker> Runnable for FunctionSystem<F, Marker>
where
    Marker: Send + Sync + 'static,
    F: SystemParamFunction<Marker>,
    F::Param: Send + Sync + 'static,
{
    fn has_work(&self, snapshot: &Snapshot, memo: &HashMap<SystemInvocation, MemoEntry>) -> bool {
        !plan_invocations::<F::Param>(self.id, snapshot, memo)
            .invocations
            .is_empty()
    }

    fn run<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        async move {
            let planned = plan_invocations::<F::Param>(self.id, snapshot, memo);
            let row_futures = planned
                .invocations
                .into_iter()
                .map(|invocation| {
                    let bowl = bowl.clone();
                    async move {
                        let commands = Commands::new();
                        let params = F::Param::fetch(&bowl, snapshot, &invocation.state, &commands);
                        self.function.run(params).await;

                        finish_invocation(invocation.owner, invocation.deps, commands)
                    }
                })
                .collect::<Vec<_>>();

            let mut run = collect_invocations(row_futures).await;
            run.completed = planned.completed;
            run
        }
        .boxed()
    }

    fn stream_runs<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &Arc<HashMap<SystemInvocation, MemoEntry>>,
    ) -> Vec<PlannedSystemRun<'a>> {
        plan_invocations::<F::Param>(self.id, &snapshot, memo)
            .invocations
            .into_iter()
            .map(|invocation| {
                let owner = invocation.owner.clone();
                let access = invocation.access.clone();
                let bowl = bowl.clone();
                let snapshot = Arc::clone(&snapshot);
                let run = async move {
                    let commands = Commands::new();
                    let params = F::Param::fetch(&bowl, &snapshot, &invocation.state, &commands);
                    self.function.run(params).await;

                    let (output, memo_update) =
                        finish_invocation(invocation.owner, invocation.deps, commands);

                    SystemRun {
                        completed: true,
                        outputs: vec![output],
                        memo_updates: vec![memo_update],
                    }
                }
                .boxed();

                PlannedSystemRun { owner, access, run }
            })
            .collect()
    }
}

pub(crate) trait SystemParamFunction<Marker>: Send + Sync + 'static {
    type Param: SystemParam;

    fn run<'a>(&'a self, params: <Self::Param as SystemParam>::Item<'a>) -> BoxFuture<'a, ()>;
}

macro_rules! impl_system_param_function {
    ($AsyncFnN:ident; $($P:ident),*) => {
        impl<F, $($P),*> SystemParamFunction<fn($($P),*)> for F
        where
            F: Send + Sync + 'static,
            $($P: SystemParam + 'static,)*
            F: AsyncFn($($P),*),
            for<'a> F: $AsyncFnN<$($P::Item<'a>,)* Output = ()>,
            for<'a> <F as $AsyncFnN<$($P::Item<'a>,)*>>::OutputFuture: Send,
        {
            type Param = ($($P,)*);

            fn run<'a>(
                &'a self,
                params: <Self::Param as SystemParam>::Item<'a>,
            ) -> BoxFuture<'a, ()> {
                #[allow(non_snake_case)]
                let ($($P,)*) = params;
                (self)($($P),*).boxed()
            }
        }
    };
}

impl_system_param_function!(AsyncFn1; P0);
impl_system_param_function!(AsyncFn2; P0, P1);
impl_system_param_function!(AsyncFn3; P0, P1, P2);
impl_system_param_function!(AsyncFn4; P0, P1, P2, P3);
impl_system_param_function!(AsyncFn5; P0, P1, P2, P3, P4);
impl_system_param_function!(AsyncFn6; P0, P1, P2, P3, P4, P5);
impl_system_param_function!(AsyncFn7; P0, P1, P2, P3, P4, P5, P6);
impl_system_param_function!(AsyncFn8; P0, P1, P2, P3, P4, P5, P6, P7);

impl<F, Marker> IntoSystem<(FunctionSystemMarker, Marker)> for F
where
    Marker: Send + Sync + 'static,
    F: SystemParamFunction<Marker>,
    F::Param: Send + Sync + 'static,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        BoxedSystem::new(Arc::new(FunctionSystem {
            id,
            function: self,
            _marker: PhantomData::<Marker>,
        }))
    }
}
