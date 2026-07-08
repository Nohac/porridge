use std::{any::TypeId, collections::HashMap, future::Future, marker::PhantomData, sync::Arc};

use async_fn_traits::{
    AsyncFn1, AsyncFn2, AsyncFn3, AsyncFn4, AsyncFn5, AsyncFn6, AsyncFn7, AsyncFn8,
};
use futures::future::{BoxFuture, FutureExt, join_all};

use crate::{
    Bowl, Commands, DerivedFrom, Entity, Query, View,
    commands::CommandOp,
    query::{
        Access, AccessKind, Dep, GuardStore, QueryFilter, QueryParam, filtered_access,
        filtered_deps, filtered_rows, store_watermark_dep,
    },
    world::{Snapshot, SystemId, SystemInvocation},
};
use variadics_please::all_tuples;

/// Coarse phase in which a system runs during one evaluation generation.
///
/// Systems registered without configuration run during [`Phase::Evaluate`].
///
/// `Startup`, `Evaluate`, and `Complete` are the forward-derivation phases
/// of a generation. `Settle` runs once per settle, at convergence, and is
/// not a further derivation stage: its removals apply immediately (reaping
/// stale facts before settled reads return) while its inserts are *queued
/// as inputs for the next run* — a settle-phase system can seed the next
/// state-machine step, but it cannot drive the current settle forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Runs once before the first evaluate phase, and again after a
    /// preemption restart — the retraction slot for ephemeral facts.
    Startup,
    /// Default phase for ordinary fact-producing systems.
    Evaluate,
    /// Runs after evaluate systems in the same generation. The phase
    /// boundary is the barrier that makes ambient (`View`) consumption of
    /// evaluate-phase output deterministic.
    Complete,
    /// Runs at settle time, after the last generation converges. Removals
    /// apply within the settle; inserts defer to the next run.
    Settle,
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
    /// World revision of the snapshot this invocation last planned from.
    /// Compared against viewed-store watermarks by `explain` to surface
    /// ambient staleness (views moved, nothing reran).
    pub(crate) planned_revision: u64,
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
    /// Rows the invocations mutated in place through `MutRef` access.
    ///
    /// The commit reconciles their fingerprints/revisions and refreshes the
    /// matching memo deps so an invocation's own write does not invalidate
    /// its own memo entry.
    pub(crate) writes: Vec<(TypeId, Entity)>,
}

impl SystemRun {
    fn empty() -> Self {
        Self {
            completed: false,
            outputs: Vec::new(),
            memo_updates: Vec::new(),
            writes: Vec::new(),
        }
    }
}

impl MemoEntry {
    pub(crate) fn is_current(&self, snapshot: &Snapshot) -> bool {
        self.deps.iter().all(|dep| dep.is_current(snapshot))
    }

    /// Refreshes deps for rows this invocation wrote itself, absorbing the
    /// post-commit revisions ("my write is my output, not a changed input").
    pub(crate) fn refresh_written(&mut self, world: &Snapshot, writes: &[(TypeId, Entity)]) {
        for dep in &mut self.deps {
            dep.refresh_written(world, writes);
        }
    }
}

/// Extracts the rows an invocation has exclusive write access to.
fn written_rows(access: &[Access]) -> Vec<(TypeId, Entity)> {
    access
        .iter()
        .filter(|access| access.kind == AccessKind::Write)
        .filter_map(|access| access.entity.map(|entity| (access.component, entity)))
        .collect()
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
#[doc(hidden)]
pub trait SystemParam {
    type State: Clone + Send;
    type Item<'a>: Send;

    fn states(snapshot: &Snapshot) -> Vec<Self::State>;
    fn keys(state: &Self::State) -> Vec<Entity>;
    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep>;
    fn access(snapshot: &Snapshot, state: &Self::State) -> Vec<Access>;
    /// Builds the user-facing parameter value.
    ///
    /// `guards` is owned by the running invocation and dropped only after the
    /// system function returns, so component read locks taken here outlive
    /// every borrow handed to user code.
    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Self::State,
        commands: &Commands,
        guards: &mut GuardStore,
    ) -> Self::Item<'a>;
    fn always_run() -> bool {
        false
    }
    /// Join keys a bound `Where` filter on this param requires, with the
    /// row's stamped fingerprint per key. Compound filters return several;
    /// every key must match its provider for the row to join.
    fn bound_keys(_snapshot: &Snapshot, _state: &Self::State) -> Vec<(TypeId, Option<u64>)> {
        Vec::new()
    }
    /// Static form of [`SystemParam::bound_keys`] (key types and names).
    fn bound_key_types() -> Vec<(TypeId, &'static str)> {
        Vec::new()
    }
    /// Component types this param reads *ambiently* (without contributing
    /// memo deps). Used by the same-phase production flag and by
    /// `explain`'s stale-view detection.
    fn view_types(_out: &mut Vec<TypeId>) {}
    /// For outer-join params: the bound key types this state must verify
    /// have *no* matching row. Nonempty only for the absent placeholder
    /// state of an `Option<Query<..>>`.
    fn absent_keys(_state: &Self::State) -> Vec<TypeId> {
        Vec::new()
    }
    /// For outer-join params: whether no row of the inner query carries
    /// `key` with an equal fingerprint — i.e. the absent placeholder is
    /// legitimate for this provider.
    fn absent_binding_matches(_snapshot: &Snapshot, _key: TypeId, _fingerprint: u64) -> bool {
        true
    }
    /// Whether this param's item reads component `key` (bound join provider).
    fn provides_key(_key: TypeId) -> bool {
        false
    }
    /// Stamped fingerprint of component `key` on this param's row when the
    /// param's item reads `key`.
    fn provided_key(
        _snapshot: &Snapshot,
        _state: &Self::State,
        _key: TypeId,
    ) -> Option<Option<u64>> {
        None
    }
    /// Whether a tuple state satisfies every bound `Where` filter binding.
    fn binding_matches(_snapshot: &Snapshot, _state: &Self::State) -> bool {
        true
    }
    /// Param-local rejection of unsupported filter shapes, checked before
    /// tuple-level binding validation.
    fn validate_local() -> Result<(), String> {
        Ok(())
    }
    /// Validates bound `Where` filters against sibling params at
    /// registration time.
    fn validate_bindings() -> Result<(), String> {
        Self::validate_local()?;
        match Self::bound_key_types().first() {
            Some((_, name)) => Err(format!(
                "bound `Where<Eq<{name}>>` needs exactly one sibling query param reading `&{name}`; found none"
            )),
            None => Ok(()),
        }
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
        guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        Query::new(Q::fetch(bowl, snapshot, state, guards))
    }

    fn bound_keys(snapshot: &Snapshot, state: &Self::State) -> Vec<(TypeId, Option<u64>)> {
        Filter::bound_keys(snapshot, state)
    }

    fn bound_key_types() -> Vec<(TypeId, &'static str)> {
        Filter::bound_key_types()
    }

    fn provides_key(key: TypeId) -> bool {
        Q::provides_key(key)
    }

    fn provided_key(
        snapshot: &Snapshot,
        state: &Self::State,
        key: TypeId,
    ) -> Option<Option<u64>> {
        Q::provided_key(snapshot, state, key)
    }
}

/// Outer join: like the bound inner join, but a provider row with *zero*
/// matching rows still runs — exactly once, with `None` — instead of being
/// silently dropped. The unfiltered "else branch" system an inner join
/// forces (seed a fallback when nothing matched) folds into the join.
impl<Q, Filter> SystemParam for Option<Query<Q, Filter>>
where
    Q: QueryParam,
    Filter: QueryFilter<Q> + Send,
{
    type State = Option<Q::State>;
    type Item<'a> = Option<Query<Q::Item<'a>, Filter>>;

    fn states(snapshot: &Snapshot) -> Vec<Self::State> {
        // The absent placeholder always enumerates; `binding_matches`
        // prunes it from tuples whose provider has a matching row.
        let mut states = filtered_rows::<Q, Filter>(snapshot)
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        states.push(None);
        states
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        match state {
            Some(state) => Q::keys(state),
            None => Vec::new(),
        }
    }

    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
        match state {
            Some(state) => filtered_deps::<Q, Filter>(snapshot, state),
            // The None invocation observed "no partner": record the joined
            // stores' watermarks so partner churn (appear, change,
            // disappear) reruns the unmatched row. Store-scoped and
            // therefore coarse, but correct.
            None => {
                let mut deps = Vec::new();
                for access in Q::access_all() {
                    deps.push(store_watermark_dep(snapshot, access.component));
                }
                for (key, _name) in Filter::bound_key_types() {
                    deps.push(store_watermark_dep(snapshot, key));
                }
                deps
            }
        }
    }

    fn access(snapshot: &Snapshot, state: &Self::State) -> Vec<Access> {
        match state {
            Some(state) => filtered_access::<Q, Filter>(snapshot, state),
            // Component-level read access: writers creating a matching
            // partner must serialize against the unmatched invocation.
            None => {
                let mut access = Q::access_all();
                access.extend(Filter::access_all());
                access
            }
        }
    }

    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Self::State,
        _commands: &Commands,
        guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        state
            .as_ref()
            .map(|state| Query::new(Q::fetch(bowl, snapshot, state, guards)))
    }

    fn bound_keys(snapshot: &Snapshot, state: &Self::State) -> Vec<(TypeId, Option<u64>)> {
        match state {
            Some(state) => Filter::bound_keys(snapshot, state),
            None => Vec::new(),
        }
    }

    fn bound_key_types() -> Vec<(TypeId, &'static str)> {
        Filter::bound_key_types()
    }

    fn absent_keys(state: &Self::State) -> Vec<TypeId> {
        match state {
            Some(_) => Vec::new(),
            None => Filter::bound_key_types()
                .into_iter()
                .map(|(key, _name)| key)
                .collect(),
        }
    }

    fn absent_binding_matches(snapshot: &Snapshot, key: TypeId, fingerprint: u64) -> bool {
        // Legitimate only when no inner row carries an equal key.
        !filtered_rows::<Q, Filter>(snapshot).iter().any(|state| {
            Filter::bound_keys(snapshot, state)
                .into_iter()
                .any(|(row_key, row_fingerprint)| {
                    row_key == key && row_fingerprint == Some(fingerprint)
                })
        })
    }

    fn validate_local() -> Result<(), String> {
        if Filter::bound_key_types().is_empty() {
            return Err(
                "`Option<Query<..>>` is an outer join and needs a bound `Where<Eq<..>>` \
                 filter; for a maybe-present component on the row itself use `Option<&T>` \
                 in the tuple instead"
                    .to_string(),
            );
        }
        Ok(())
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

    fn access(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Access> {
        // A view reads whatever rows exist when it is built, so it declares
        // component-level access instead of enumerating rows at plan time.
        let mut access = Q::access_all();
        access.extend(Filter::access_all());
        access
    }

    fn view_types(out: &mut Vec<TypeId>) {
        out.extend(Q::access_all().iter().map(|access| access.component));
    }

    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        _state: &Self::State,
        _commands: &Commands,
        _guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        View::new(bowl.clone(), snapshot)
    }

    fn validate_local() -> Result<(), String> {
        // A view has no row state to bind a join key from; accepting the
        // filter would silently degrade `Eq` to `With` semantics.
        match Filter::bound_key_types().first() {
            Some((_, name)) => Err(format!(
                "`View` does not support bound `Where<Eq<{name}>>` yet; bind on a `Query` and filter view rows inside the system"
            )),
            None => Ok(()),
        }
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
        _guards: &mut GuardStore,
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
        _guards: &mut GuardStore,
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

                let has_bound = false $(|| !$P::bound_key_types().is_empty())*;
                if has_bound {
                    states.retain(|state| Self::binding_matches(snapshot, state));
                }

                states
            }

            fn binding_matches(snapshot: &Snapshot, state: &Self::State) -> bool {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;

                let mut bound: Vec<(usize, TypeId, Option<u64>)> = Vec::new();
                let mut index = 0usize;
                $(
                    for (key, fingerprint) in $P::bound_keys(snapshot, $P) {
                        bound.push((index, key, fingerprint));
                    }
                    index += 1;
                )*
                let _ = index;

                for (bound_index, key, fingerprint) in bound {
                    let Some(fingerprint) = fingerprint else {
                        panic!(
                            "bound `Where<Eq<..>>` key component must be \
                             `#[component(hash)]` so rows can join on fingerprints"
                        );
                    };

                    let mut provided: Option<Option<u64>> = None;
                    let mut index = 0usize;
                    $(
                        if index != bound_index && provided.is_none() {
                            provided = $P::provided_key(snapshot, $P, key);
                        }
                        index += 1;
                    )*
                    let _ = index;

                    match provided {
                        Some(Some(provider)) if provider == fingerprint => {}
                        Some(Some(_)) => return false,
                        Some(None) => panic!(
                            "bound `Where<Eq<..>>` provider component must be \
                             `#[component(hash)]` so rows can join on fingerprints"
                        ),
                        // Unreachable: providers are validated at registration.
                        None => return false,
                    }
                }

                // Outer joins: the absent placeholder survives only when no
                // row of its inner query carries the provider's key.
                let mut absent: Vec<(usize, TypeId)> = Vec::new();
                let mut index = 0usize;
                $(
                    for key in $P::absent_keys($P) {
                        absent.push((index, key));
                    }
                    index += 1;
                )*
                let _ = index;

                for (absent_index, key) in absent {
                    let mut provided: Option<Option<u64>> = None;
                    let mut index = 0usize;
                    $(
                        if index != absent_index && provided.is_none() {
                            provided = $P::provided_key(snapshot, $P, key);
                        }
                        index += 1;
                    )*
                    let _ = index;

                    let Some(Some(fingerprint)) = provided else {
                        panic!(
                            "outer-join provider component must be \
                             `#[component(hash)]` so rows can join on fingerprints"
                        );
                    };

                    let mut index = 0usize;
                    $(
                        if index == absent_index
                            && !$P::absent_binding_matches(snapshot, key, fingerprint)
                        {
                            return false;
                        }
                        index += 1;
                    )*
                    let _ = index;
                }

                true
            }

            fn validate_bindings() -> Result<(), String> {
                $( $P::validate_local()?; )*

                let mut bound: Vec<(usize, TypeId, &'static str)> = Vec::new();
                let mut index = 0usize;
                $(
                    for (key, name) in $P::bound_key_types() {
                        bound.push((index, key, name));
                    }
                    index += 1;
                )*
                let _ = index;

                for (bound_index, key, name) in bound {
                    let mut providers = 0usize;
                    let mut index = 0usize;
                    $(
                        if index != bound_index && $P::provides_key(key) {
                            providers += 1;
                        }
                        index += 1;
                    )*
                    let _ = index;

                    if providers != 1 {
                        return Err(format!(
                            "bound `Where<Eq<{name}>>` needs exactly one sibling query \
                             param reading `&{name}`; found {providers}"
                        ));
                    }
                }

                Ok(())
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
                guards: &mut GuardStore,
            ) -> Self::Item<'a> {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                ($($P::fetch(bowl, snapshot, $P, commands, guards),)*)
            }

            fn always_run() -> bool {
                false $(|| $P::always_run())*
            }

            fn view_types(out: &mut Vec<TypeId>) {
                $($P::view_types(out);)*
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

/// Counts a system's rows for [`crate::Bowl::explain`]: `(matched,
/// memoized)`, where memoized rows are matched rows the planner would skip
/// because their recorded deps are unchanged.
fn count_rows<Params>(
    system: SystemId,
    snapshot: &Snapshot,
    memo: &HashMap<SystemInvocation, MemoEntry>,
) -> (usize, usize)
where
    Params: SystemParam,
{
    let states = Params::states(snapshot);
    let matched = states.len();
    let memoized = states
        .into_iter()
        .filter(|state| {
            let owner = SystemInvocation {
                system,
                keys: Params::keys(state),
            };
            let deps = Params::deps(snapshot, state);
            !Params::always_run() && memo.get(&owner).is_some_and(|entry| entry.deps == deps)
        })
        .count();

    (matched, memoized)
}

fn finish_invocation(
    owner: SystemInvocation,
    deps: Vec<Dep>,
    planned_revision: u64,
    commands: Commands,
) -> (SystemOutput, (SystemInvocation, MemoEntry)) {
    let output = SystemOutput {
        owner: owner.clone(),
        commands: commands.take(),
    };
    let memo_update = (
        owner,
        MemoEntry {
            deps,
            planned_revision,
        },
    );

    (output, memo_update)
}

async fn collect_invocations<Fut>(futures: Vec<Fut>) -> SystemRun
where
    Fut: Future<
        Output = (
            SystemOutput,
            (SystemInvocation, MemoEntry),
            Vec<(TypeId, Entity)>,
        ),
    >,
{
    let rows = join_all(futures).await;
    let mut run = SystemRun::empty();

    for (output, memo_update, writes) in rows {
        run.outputs.push(output);
        run.memo_updates.push(memo_update);
        run.writes.extend(writes);
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

    /// `(matched, memoized)` row counts for [`crate::Bowl::explain`].
    fn row_counts(
        &self,
        _snapshot: &Snapshot,
        _memo: &HashMap<SystemInvocation, MemoEntry>,
    ) -> (usize, usize) {
        (0, 0)
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
    /// Full type path of the registered function, for `explain` lookups.
    pub(crate) name: &'static str,
    /// Component types the system's params read ambiently (`View`s), for
    /// the same-phase production flag and `explain`'s stale-view report.
    pub(crate) view_types: Arc<Vec<TypeId>>,
}

impl BoxedSystem {
    fn new(runnable: Arc<dyn Runnable>, name: &'static str, view_types: Vec<TypeId>) -> Self {
        Self {
            runnable,
            phase: Phase::Evaluate,
            name,
            view_types: Arc::new(view_types),
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

    fn row_counts(
        &self,
        snapshot: &Snapshot,
        memo: &HashMap<SystemInvocation, MemoEntry>,
    ) -> (usize, usize) {
        self.system.runnable.row_counts(snapshot, memo)
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
                let owner = SystemInvocation {
                    system: self.id,
                    keys: Vec::new(),
                };
                let commands =
                    Commands::new(snapshot.spawn_slots(&owner), snapshot.entity_allocator());
                self.callback.run(commands.clone());
                SystemOutput {
                    owner,
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

    fn row_counts(
        &self,
        snapshot: &Snapshot,
        memo: &HashMap<SystemInvocation, MemoEntry>,
    ) -> (usize, usize) {
        self.system.runnable.row_counts(snapshot, memo)
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
                let commands =
                    Commands::new(snapshot.spawn_slots(&owner), snapshot.entity_allocator());
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

    fn row_counts(
        &self,
        snapshot: &Snapshot,
        memo: &HashMap<SystemInvocation, MemoEntry>,
    ) -> (usize, usize) {
        self.system.runnable.row_counts(snapshot, memo)
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

            let commands =
                Commands::new(snapshot.spawn_slots(&owner), snapshot.entity_allocator());
            self.callback.run(commands.clone());

            SystemRun {
                completed: true,
                outputs: vec![SystemOutput {
                    owner,
                    commands: commands.take(),
                }],
                memo_updates: Vec::new(),
                writes: Vec::new(),
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
        let name = system.name;
        let view_types = Arc::clone(&system.view_types);
        BoxedSystem {
            runnable: Arc::new(OnCompleteSystem {
                id,
                system,
                callback: self.callback,
            }),
            phase,
            name,
            view_types,
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
        let name = system.name;
        let view_types = Arc::clone(&system.view_types);
        BoxedSystem {
            runnable: Arc::new(OnStartSystem {
                id,
                system,
                callback: self.callback,
            }),
            phase,
            name,
            view_types,
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
        let name = system.name;
        let view_types = Arc::clone(&system.view_types);
        BoxedSystem {
            runnable: Arc::new(OnSettledSystem {
                id,
                system,
                callback: self.callback,
            }),
            phase,
            name,
            view_types,
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
/// Register this during [`Phase::Settle`] to remove derived entities whose
/// owner entity has changed since insertion — removal-only, so it reaps at
/// convergence without driving the settle forward:
///
/// ```text
/// bowl.add_system(cleanup_stale_derived.run_during(Phase::Settle));
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

    fn row_counts(
        &self,
        snapshot: &Snapshot,
        memo: &HashMap<SystemInvocation, MemoEntry>,
    ) -> (usize, usize) {
        count_rows::<F::Param>(self.id, snapshot, memo)
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
                        let writes = written_rows(&invocation.access);
                        let commands = Commands::new(
                            snapshot.spawn_slots(&invocation.owner),
                            snapshot.entity_allocator(),
                        );
                        // Read guards live here, in the invocation frame, so
                        // borrows handed to the system stay locked until the
                        // system function returns.
                        let mut guards = GuardStore::new();
                        let params = F::Param::fetch(
                            &bowl,
                            snapshot,
                            &invocation.state,
                            &commands,
                            &mut guards,
                        );
                        self.function.run(params).await;
                        drop(guards);

                        let (output, memo_update) =
                            finish_invocation(invocation.owner, invocation.deps, snapshot.revision_raw(), commands);
                        (output, memo_update, writes)
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
                    let writes = written_rows(&invocation.access);
                    let commands = Commands::new(
                        snapshot.spawn_slots(&invocation.owner),
                        snapshot.entity_allocator(),
                    );
                    // Read guards live here, in the invocation frame, so
                    // borrows handed to the system stay locked until the
                    // system function returns.
                    let mut guards = GuardStore::new();
                    let params = F::Param::fetch(
                        &bowl,
                        &snapshot,
                        &invocation.state,
                        &commands,
                        &mut guards,
                    );
                    self.function.run(params).await;
                    drop(guards);

                    let (output, memo_update) =
                        finish_invocation(invocation.owner, invocation.deps, snapshot.revision_raw(), commands);

                    SystemRun {
                        completed: true,
                        outputs: vec![output],
                        memo_updates: vec![memo_update],
                        writes,
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
        if let Err(message) = F::Param::validate_bindings() {
            panic!(
                "cannot register system `{}`: {message}",
                std::any::type_name::<F>()
            );
        }

        let mut view_types = Vec::new();
        F::Param::view_types(&mut view_types);
        BoxedSystem::new(
            Arc::new(FunctionSystem {
                id,
                function: self,
                _marker: PhantomData::<Marker>,
            }),
            std::any::type_name::<F>(),
            view_types,
        )
    }
}
