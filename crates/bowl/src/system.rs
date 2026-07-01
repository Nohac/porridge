use std::{collections::HashMap, future::Future, marker::PhantomData, sync::Arc};

use futures::future::{FutureExt, LocalBoxFuture, join_all};

use crate::{
    Commands, Entity, Query, View,
    commands::CommandOp,
    query::{Dep, QueryFilter, QueryParam, filtered_deps, filtered_rows},
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
    pub(crate) completion_only: bool,
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

struct PlannedRun<State> {
    completed: bool,
    invocations: Vec<PlannedInvocation<State>>,
}

struct PlannedInvocation<State> {
    state: State,
    owner: SystemInvocation,
    deps: Vec<Dep>,
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
    type State: Clone;
    type Item<'a>;

    fn states(snapshot: &Snapshot) -> Vec<Self::State>;
    fn keys(state: &Self::State) -> Vec<Entity>;
    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep>;
    fn fetch<'a>(
        snapshot: &'a Snapshot,
        state: &Self::State,
        commands: &Commands,
    ) -> Self::Item<'a>;
}

impl<Q, Filter> SystemParam for Query<Q, Filter>
where
    Q: QueryParam,
    Filter: QueryFilter<Q>,
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

    fn fetch<'a>(
        snapshot: &'a Snapshot,
        state: &Self::State,
        _commands: &Commands,
    ) -> Self::Item<'a> {
        Query::new(Q::fetch(snapshot, state))
    }
}

impl<'view, Q, Filter> SystemParam for View<'view, Q, Filter>
where
    Q: QueryParam,
    Filter: QueryFilter<Q>,
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

    fn fetch<'a>(
        snapshot: &'a Snapshot,
        _state: &Self::State,
        _commands: &Commands,
    ) -> Self::Item<'a> {
        View::new(snapshot)
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

    fn fetch<'a>(
        _snapshot: &'a Snapshot,
        _state: &Self::State,
        commands: &Commands,
    ) -> Self::Item<'a> {
        commands.clone()
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

            fn fetch<'a>(
                snapshot: &'a Snapshot,
                state: &Self::State,
                commands: &Commands,
            ) -> Self::Item<'a> {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                ($($P::fetch(snapshot, $P, commands),)*)
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

            memo.get(&owner)
                .is_none_or(|entry| entry.deps != deps)
                .then_some(PlannedInvocation { state, owner, deps })
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
        completion_only: false,
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
/// `Runnable` receives an immutable snapshot plus an immutable view of the memo
/// table. It returns command buffers and memo updates that need to be committed
/// for this generation.
///
/// The returned future is local rather than `Send`. This lets ordinary async
/// functions borrow snapshot data across `.await` without forcing the first
/// implementation to solve cross-thread spawning. The bowl can still be shared;
/// this only constrains where the evaluation future may be polled.
pub(crate) trait Runnable: Send + Sync {
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, SystemRun>;
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
pub struct OnCompleteMarker;

/// Callback run once after a system has finished iterating its driving query.
pub trait CompleteCallback: Send + Sync + 'static {
    fn run(&self, commands: Commands);
}

impl<F> CompleteCallback for F
where
    F: Fn(Commands) + Send + Sync + 'static,
{
    fn run(&self, commands: Commands) {
        self(commands);
    }
}

/// Extension methods for system configuration.
pub trait SystemExt: Sized {
    /// Runs `callback` once after this system has completed all invocations that
    /// were invalid for the current snapshot.
    fn on_complete<C>(self, callback: C) -> OnComplete<Self, C>
    where
        C: CompleteCallback,
    {
        OnComplete {
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

/// System wrapper produced by [`SystemExt::on_complete`].
pub struct OnComplete<S, C> {
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

impl<C> Runnable for OnCompleteSystem<C>
where
    C: CompleteCallback,
{
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, SystemRun> {
        async move {
            let mut run = self.system.run(snapshot, memo).await;
            let owner = SystemInvocation {
                system: self.id,
                keys: Vec::new(),
            };
            let should_emit_completion =
                run.completed && (!run.outputs.is_empty() || !snapshot.has_derived_owned(&owner));

            if should_emit_completion {
                let commands = Commands::new();
                self.callback.run(commands.clone());
                run.outputs.push(SystemOutput {
                    owner,
                    commands: commands.take(),
                    completion_only: true,
                });
            }

            run
        }
        .boxed_local()
    }
}

impl<S, C, M> IntoSystem<(OnCompleteMarker, M)> for OnComplete<S, C>
where
    S: IntoSystem<M>,
    C: CompleteCallback,
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
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, SystemRun> {
        self.runnable.run(snapshot, memo)
    }
}

/// Builds a completion callback that inserts `component` on `entity`.
pub fn insert_on<T>(entity: Entity, component: T) -> impl CompleteCallback
where
    T: crate::Component + Clone,
{
    move |mut commands: Commands| {
        commands.entity(entity).insert(component.clone());
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
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, SystemRun> {
        async move {
            let planned = plan_invocations::<F::Param>(self.id, snapshot, memo);
            let row_futures = planned
                .invocations
                .into_iter()
                .map(|invocation| async move {
                    let commands = Commands::new();
                    let params = F::Param::fetch(snapshot, &invocation.state, &commands);
                    self.function.run(params).await;

                    finish_invocation(invocation.owner, invocation.deps, commands)
                })
                .collect::<Vec<_>>();

            let mut run = collect_invocations(row_futures).await;
            run.completed = planned.completed;
            run
        }
        .boxed_local()
    }
}

pub(crate) trait SystemParamFunction<Marker>: Send + Sync + 'static {
    type Param: SystemParam;

    fn run<'a>(&'a self, params: <Self::Param as SystemParam>::Item<'a>) -> LocalBoxFuture<'a, ()>;
}

macro_rules! impl_system_param_function {
    ($($P:ident),*) => {
        impl<F, $($P),*> SystemParamFunction<fn($($P),*)> for F
        where
            F: Send + Sync + 'static,
            $($P: SystemParam + 'static,)*
            for<'a> F: AsyncFn($($P),*) + AsyncFn($($P::Item<'a>),*),
        {
            type Param = ($($P,)*);

            fn run<'a>(
                &'a self,
                params: <Self::Param as SystemParam>::Item<'a>,
            ) -> LocalBoxFuture<'a, ()> {
                #[allow(non_snake_case)]
                let ($($P,)*) = params;
                async move {
                    (self)($($P),*).await;
                }
                .boxed_local()
            }
        }
    };
}

all_tuples!(impl_system_param_function, 1, 8, P);

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
