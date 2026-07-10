use std::{any::TypeId, collections::HashMap, future::Future, marker::PhantomData, sync::Arc};

use async_fn_traits::{
    AsyncFn1, AsyncFn2, AsyncFn3, AsyncFn4, AsyncFn5, AsyncFn6, AsyncFn7, AsyncFn8,
};
use futures::future::{BoxFuture, FutureExt, join_all};

use crate::{
    Bowl, Commands, DerivedFrom, Entity, Query, View,
    commands::CommandOp,
    declare::{Anything, DeclarationList},
    query::{
        Access, AccessKind, Dep, GuardStore, QueryFilter, QueryParam, filtered_access,
        filtered_deps, filtered_rows, filtered_rows_from_candidates, store_watermark_dep,
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

pub(crate) struct PlannedSystemRun {
    pub(crate) owner: SystemInvocation,
    pub(crate) access: Vec<Access>,
    /// Owned and `Send`: the future captures `Arc`s (system function,
    /// snapshot, callbacks), never borrows the registry — so the runner
    /// may spawn it onto worker threads.
    pub(crate) run: BoxFuture<'static, SystemRun>,
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
/// How a param relates to delta (dirty-entity) planning.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeltaShape {
    /// Contributes a singleton state, never entity rows (`View`,
    /// `Commands`): compatible with a sibling driver's hint.
    Inert,
    /// Drives entity rows and can enumerate them from a dirty-entity hint
    /// (a plain tracked `Query`).
    Driver,
    /// Cannot be hinted (joins, outer joins, always-run params, custom
    /// params): the owning system always plans fully.
    Opaque,
}

pub trait SystemParam {
    type State: Clone + Send;
    type Item<'a>: Send;

    fn states(snapshot: &Snapshot) -> Vec<Self::State>;
    /// `states` restricted to candidate entities whose stores were written
    /// since the system's last plan. Only consulted when the whole param
    /// tuple is delta-eligible (exactly one `Driver`, rest `Inert`).
    fn states_hinted(snapshot: &Snapshot, hint: &[Entity]) -> Vec<Self::State> {
        let _ = hint;
        Self::states(snapshot)
    }
    /// This param's relation to delta planning; conservative default.
    fn delta_shape() -> DeltaShape {
        DeltaShape::Opaque
    }
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
        commands: &Commands<Anything>,
        guards: &mut GuardStore,
    ) -> Self::Item<'a>;
    fn always_run() -> bool {
        false
    }
    /// Component stores whose watermark movement can change this param's
    /// planned rows or deps. `None` means unbounded: the system is planned
    /// every wave. Ambient params (`View`, `Commands`) contribute nothing
    /// without poisoning the set.
    fn interest_types() -> Option<Vec<TypeId>> {
        None
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
    /// Membership join keys a `Where<In<T>>` filter on this param requires:
    /// the set component's type and this row's entity, matched against the
    /// sibling-provided member list.
    fn in_keys(_snapshot: &Snapshot, _state: &Self::State) -> Vec<(TypeId, Entity)> {
        Vec::new()
    }
    /// Static form of [`SystemParam::in_keys`] (the provider rule is the
    /// same as `Eq`: exactly one sibling reading `&T`).
    fn in_key_types() -> Vec<(TypeId, &'static str)> {
        Vec::new()
    }
    /// Member list of the maintained inverse `key` on this param's row,
    /// when the param's item reads `key` (`Where<In<..>>` provider side).
    fn provided_members(
        _snapshot: &Snapshot,
        _state: &Self::State,
        _key: TypeId,
    ) -> Option<Vec<Entity>> {
        None
    }
    /// Whether planning may enumerate this param's rows from a sibling
    /// provider's pair list (single-key bound joins) instead of forming the
    /// full product and pruning.
    fn pair_expandable() -> bool {
        false
    }
    /// Row states restricted to `candidates` (pair-driven planning).
    fn states_from_candidates(snapshot: &Snapshot, _candidates: Vec<Entity>) -> Vec<Self::State> {
        Self::states(snapshot)
    }
    /// Component sets this param reads *ambiently* (without contributing
    /// memo deps) — one entry per `View`, listing the components an entity
    /// must carry to appear in it. Used by the same-phase production flag
    /// (a written entity races a view only if it carries the whole set)
    /// and by `explain`'s stale-view detection.
    fn view_sets(_out: &mut Vec<Vec<TypeId>>) {}
    /// Component types this param may emit. `Some(empty)` for non-writers
    /// (the default), `Some(list)` for a typed `Commands<S>`, `None` for
    /// the wildcard (bare `Commands`).
    fn declared_outputs() -> Option<Vec<TypeId>> {
        Some(Vec::new())
    }
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

    fn interest_types() -> Option<Vec<TypeId>> {
        let mut interest = Q::interest_types()?;
        interest.extend(<Filter as QueryFilter<Q>>::interest_types()?);
        Some(interest)
    }

    fn states(snapshot: &Snapshot) -> Vec<Self::State> {
        filtered_rows::<Q, Filter>(snapshot)
    }

    fn states_hinted(snapshot: &Snapshot, hint: &[Entity]) -> Vec<Self::State> {
        filtered_rows_from_candidates::<Q, Filter>(snapshot, hint.to_vec())
    }

    fn delta_shape() -> DeltaShape {
        // Bound joins enumerate from providers, not hints; everything else
        // about a plain tracked query is hintable.
        if <Filter as QueryFilter<Q>>::bound_key_types().is_empty()
            && <Filter as QueryFilter<Q>>::in_key_types().is_empty()
        {
            DeltaShape::Driver
        } else {
            DeltaShape::Opaque
        }
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
        _commands: &Commands<Anything>,
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

    fn in_keys(snapshot: &Snapshot, state: &Self::State) -> Vec<(TypeId, Entity)> {
        Filter::in_keys(snapshot, state)
    }

    fn in_key_types() -> Vec<(TypeId, &'static str)> {
        Filter::in_key_types()
    }

    fn pair_expandable() -> bool {
        Filter::in_key_types().len() + Filter::bound_key_types().len() == 1
    }

    fn states_from_candidates(snapshot: &Snapshot, candidates: Vec<Entity>) -> Vec<Self::State> {
        filtered_rows_from_candidates::<Q, Filter>(snapshot, candidates)
    }

    fn provides_key(key: TypeId) -> bool {
        Q::provides_key(key)
    }

    fn provided_key(snapshot: &Snapshot, state: &Self::State, key: TypeId) -> Option<Option<u64>> {
        Q::provided_key(snapshot, state, key)
    }

    fn provided_members(
        snapshot: &Snapshot,
        state: &Self::State,
        key: TypeId,
    ) -> Option<Vec<Entity>> {
        Q::provided_members(snapshot, state, key)
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

    fn interest_types() -> Option<Vec<TypeId>> {
        <Query<Q, Filter> as SystemParam>::interest_types()
    }

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
        _commands: &Commands<Anything>,
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

    fn delta_shape() -> DeltaShape {
        DeltaShape::Inert
    }

    // Ambient by design: view movement never invalidates, so it never
    // requires replanning either.
    fn interest_types() -> Option<Vec<TypeId>> {
        Some(Vec::new())
    }

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

    fn view_sets(out: &mut Vec<Vec<TypeId>>) {
        out.push(
            Q::access_all()
                .iter()
                .map(|access| access.component)
                .collect(),
        );
    }

    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        _state: &Self::State,
        _commands: &Commands<Anything>,
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

impl<S> SystemParam for Commands<S>
where
    S: DeclarationList + Send + Sync + 'static,
{
    type State = ();
    type Item<'a> = Commands<S>;

    fn delta_shape() -> DeltaShape {
        DeltaShape::Inert
    }

    fn interest_types() -> Option<Vec<TypeId>> {
        Some(Vec::new())
    }

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

    fn declared_outputs() -> Option<Vec<TypeId>> {
        // `None` is the wildcard (bare `Commands`); a typed declaration
        // enumerates its component set for the system graph.
        S::declared_types()
    }

    fn fetch<'a>(
        _bowl: &Bowl,
        _snapshot: &'a Snapshot,
        _state: &Self::State,
        commands: &Commands<Anything>,
        _guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        commands.retype()
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
        _commands: &Commands<Anything>,
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

            fn interest_types() -> Option<Vec<TypeId>> {
                let mut interest = Vec::new();
                $(interest.extend($P::interest_types()?);)*
                Some(interest)
            }

            fn delta_shape() -> DeltaShape {
                let mut drivers = 0usize;
                $(match $P::delta_shape() {
                    DeltaShape::Opaque => return DeltaShape::Opaque,
                    DeltaShape::Driver => drivers += 1,
                    DeltaShape::Inert => {}
                })*
                if drivers == 1 {
                    DeltaShape::Driver
                } else {
                    DeltaShape::Opaque
                }
            }

            fn states_hinted(snapshot: &Snapshot, hint: &[Entity]) -> Vec<Self::State> {
                // Only reachable when delta-eligible: exactly one driver,
                // no pair-expandable params, no bound pruning needed.
                let mut states = Vec::new();
                $(
                    let $P = $P::states_hinted(snapshot, hint);
                )*
                for_each_state!(snapshot, states, []; $($P),*);
                states
            }

            fn states(snapshot: &Snapshot) -> Vec<Self::State> {
                let mut states = Vec::new();
                // Pair-expandable params (single-key bound joins) are not
                // enumerated independently: their rows come from the
                // already-picked provider's pair list during product
                // construction, so planning is O(pairs), not O(product).
                $(
                    let $P = if $P::pair_expandable() {
                        ::std::vec::Vec::new()
                    } else {
                        $P::states(snapshot)
                    };
                )*

                for_each_state!(snapshot, states, []; $($P),*);

                // Pair-expanded params are key-equal by construction (their
                // rows come from the provider's member list or fingerprint
                // bucket), so only *non-expandable* bound params — compound
                // multi-key joins — still need the product prune.
                let needs_prune = false
                    $(|| (!$P::bound_key_types().is_empty() || !$P::in_key_types().is_empty())
                        && !$P::pair_expandable())*;
                if needs_prune {
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

                // Membership joins: a `Where<In<T>>` row pairs only when its
                // entity is in the sibling-provided member list.
                let mut membership: Vec<(usize, TypeId, Entity)> = Vec::new();
                let mut index = 0usize;
                $(
                    for (key, member) in $P::in_keys(snapshot, $P) {
                        membership.push((index, key, member));
                    }
                    index += 1;
                )*
                let _ = index;

                for (in_index, key, member) in membership {
                    let mut provided: Option<Vec<Entity>> = None;
                    let mut index = 0usize;
                    $(
                        if index != in_index && provided.is_none() {
                            provided = $P::provided_members(snapshot, $P, key);
                        }
                        index += 1;
                    )*
                    let _ = index;

                    match provided {
                        Some(members) => {
                            if members.binary_search(&member).is_err() {
                                return false;
                            }
                        }
                        None => panic!(
                            "`Where<In<..>>` provider must read a maintained \
                             relationship inverse (`relationship_members` returned \
                             nothing for the bound set component)"
                        ),
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
                    // Membership joins share the provider rule: exactly one
                    // sibling reading `&T`.
                    for (key, name) in $P::in_key_types() {
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
                commands: &Commands<Anything>,
                guards: &mut GuardStore,
            ) -> Self::Item<'a> {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                ($($P::fetch(bowl, snapshot, $P, commands, guards),)*)
            }

            fn always_run() -> bool {
                false $(|| $P::always_run())*
            }

            fn view_sets(out: &mut Vec<Vec<TypeId>>) {
                $($P::view_sets(out);)*
            }

            fn declared_outputs() -> Option<Vec<TypeId>> {
                let mut out = Vec::new();
                $(
                    match $P::declared_outputs() {
                        Some(types) => out.extend(types),
                        // Any wildcard writer makes the whole system a
                        // wildcard.
                        None => return None,
                    }
                )*
                Some(out)
            }
        }
    };
}

macro_rules! for_each_state {
    ($snapshot:ident, $out:ident, [$(($PickedTy:ident, $picked:expr)),*];) => {
        $out.push(($($picked.clone(),)*));
    };
    ($snapshot:ident, $out:ident, [$(($PickedTy:ident, $picked:expr)),*]; $head:ident $(, $tail:ident)*) => {
        // Pair-driven expansion: a single-key bound join enumerates its
        // rows from the already-picked provider's pair list — the member
        // list for `In`, the fingerprint index bucket for `Eq` — instead
        // of the full product.
        let __pair_rows = if $head::pair_expandable() {
            let mut __rows: ::std::option::Option<::std::vec::Vec<_>> =
                ::std::option::Option::None;
            if let ::std::option::Option::Some((__key, _)) =
                $head::in_key_types().into_iter().next()
            {
                $(
                    if __rows.is_none() {
                        if let ::std::option::Option::Some(__members) =
                            $PickedTy::provided_members($snapshot, $picked, __key)
                        {
                            __rows = ::std::option::Option::Some(
                                $head::states_from_candidates($snapshot, __members),
                            );
                        }
                    }
                )*
            } else if let ::std::option::Option::Some((__key, __name)) =
                $head::bound_key_types().into_iter().next()
            {
                $(
                    if __rows.is_none() {
                        if let ::std::option::Option::Some(__provided) =
                            $PickedTy::provided_key($snapshot, $picked, __key)
                        {
                            let ::std::option::Option::Some(__fingerprint) = __provided else {
                                panic!(
                                    "bound `Where<Eq<{__name}>>` provider component must be \
                                     `#[component(hash)]` so rows can join on fingerprints"
                                );
                            };
                            __rows = ::std::option::Option::Some(
                                $head::states_from_candidates(
                                    $snapshot,
                                    $snapshot.fingerprint_candidates(__key, __fingerprint),
                                ),
                            );
                        }
                    }
                )*
            }
            match __rows {
                ::std::option::Option::Some(rows) => rows,
                ::std::option::Option::None => panic!(
                    "bound join provider must precede the joined query in the \
                     system's parameter list (pair-driven planning reads the \
                     provider's pair list while building the product)"
                ),
            }
        } else {
            ::std::vec::Vec::new()
        };
        let __iter = if $head::pair_expandable() { &__pair_rows } else { &$head };
        for state in __iter {
            for_each_state!(
                $snapshot, $out,
                [$(($PickedTy, $picked),)* ($head, state)];
                $($tail),*
            );
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
    plan_invocations_hinted::<Params>(system, snapshot, memo, None)
}

fn plan_invocations_hinted<Params>(
    system: SystemId,
    snapshot: &Snapshot,
    memo: &HashMap<SystemInvocation, MemoEntry>,
    hint: Option<&[Entity]>,
) -> PlannedRun<Params::State>
where
    Params: SystemParam,
{
    let states = match hint {
        Some(hint) => Params::states_hinted(snapshot, hint),
        None => Params::states(snapshot),
    };
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
    commands: Commands<Anything>,
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

    fn stream_runs(
        &self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &HashMap<SystemInvocation, MemoEntry>,
        hint: Option<&[Entity]>,
    ) -> Vec<PlannedSystemRun>;

    fn run_settled<'a>(
        &'a self,
        _bowl: Bowl,
        _snapshot: &'a Snapshot,
        _memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        async { SystemRun::empty() }.boxed()
    }
}

/// Per-system profiling counters (nanoseconds; poll-time attribution, so
/// concurrent awaiting of *other* systems is not charged here).
#[derive(Default)]
pub struct SystemStats {
    pub plan_nanos: std::sync::atomic::AtomicU64,
    pub run_nanos: std::sync::atomic::AtomicU64,
    pub runs: std::sync::atomic::AtomicU64,
}

/// Type-erased registered system.
#[derive(Clone)]
pub struct BoxedSystem {
    pub(crate) runnable: Arc<dyn Runnable>,
    pub(crate) phase: Phase,
    /// Full type path of the registered function, for `explain` lookups.
    pub(crate) name: &'static str,
    /// One component set per `View` the system's params read ambiently:
    /// the components an entity must carry to appear in that view. For the
    /// same-phase production flag and `explain`'s stale-view report.
    pub(crate) view_sets: Arc<Vec<Vec<TypeId>>>,
    /// Component types the system declared it may emit (`Commands<S>`);
    /// `None` is the wildcard (bare `Commands` or hook-driven writers).
    pub(crate) declared_outputs: Option<Arc<Vec<TypeId>>>,
    /// Planner interest: the stores whose watermark movement can change
    /// this system's plan. `None` = unbounded (planned every wave).
    pub(crate) interest: Option<Arc<Vec<TypeId>>>,
    /// Highest interest-store watermark this system was last planned
    /// against; planning is skipped while no interested store moves past
    /// it. Reset on conflict deferral and stale commits.
    pub(crate) planned_mark: Arc<std::sync::atomic::AtomicU64>,
    /// Profiling counters, surfaced by [`crate::Bowl::profile_all`].
    pub(crate) stats: Arc<SystemStats>,
    /// Whether the system can plan from a dirty-entity hint (exactly one
    /// plain tracked query driving rows, bounded interest).
    pub(crate) delta_eligible: bool,
    /// The plan epoch this system's `log_pos` belongs to; `u64::MAX`
    /// forces the next plan to be full (fresh registration, resets).
    pub(crate) plan_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Cursor into the settle-scoped write log: entries before this were
    /// covered by the system's last plan.
    pub(crate) log_pos: Arc<std::sync::atomic::AtomicU64>,
}

impl BoxedSystem {
    fn new(
        runnable: Arc<dyn Runnable>,
        name: &'static str,
        view_sets: Vec<Vec<TypeId>>,
        declared_outputs: Option<Vec<TypeId>>,
        interest: Option<Vec<TypeId>>,
        delta_eligible: bool,
    ) -> Self {
        Self {
            runnable,
            phase: Phase::Evaluate,
            name,
            view_sets: Arc::new(view_sets),
            declared_outputs: declared_outputs.map(Arc::new),
            delta_eligible: delta_eligible && interest.is_some(),
            interest: interest.map(Arc::new),
            planned_mark: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            stats: Arc::new(SystemStats::default()),
            plan_epoch: Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX)),
            log_pos: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Whether the planner must (re)consider this system: unbounded
    /// interest always plans; scoped interest plans only when some
    /// interested store's watermark moved past the last planned mark.
    /// Advances the mark to the snapshot's level when planning proceeds.
    /// Pure form of [`BoxedSystem::needs_planning`]: no mark advance.
    pub(crate) fn peek_needs_planning(&self, snapshot: &Snapshot) -> bool {
        let Some(interest) = &self.interest else {
            return true;
        };
        let mark = interest
            .iter()
            .map(|type_id| snapshot.store_watermark(*type_id))
            .max()
            .unwrap_or(0);
        mark > self.planned_mark.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn needs_planning(&self, snapshot: &Snapshot) -> bool {
        if !self.peek_needs_planning(snapshot) {
            return false;
        }
        if let Some(interest) = &self.interest {
            let mark = interest
                .iter()
                .map(|type_id| snapshot.store_watermark(*type_id))
                .max()
                .unwrap_or(0);
            self.planned_mark
                .store(mark, std::sync::atomic::Ordering::Relaxed);
        }
        true
    }

    /// Forces the next wave to replan this system (conflict deferrals and
    /// stale commits invalidate the planned mark). Delta cursors are
    /// invalidated too: a deferred or discarded row's entity may have no
    /// new writes, so only a full plan is guaranteed to see it again.
    pub(crate) fn reset_planned_mark(&self) {
        self.planned_mark
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.plan_epoch
            .store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
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

/// Callback run around system batches. The callback declares its outputs
/// through its `Commands<S>` parameter type, exactly like a system.
pub trait SystemCallback<S>: Send + Sync + 'static {
    fn run(&self, commands: Commands<S>);
}

impl<F, S> SystemCallback<S> for F
where
    F: Fn(Commands<S>) + Send + Sync + 'static,
{
    fn run(&self, commands: Commands<S>) {
        self(commands);
    }
}

/// Extension methods for system configuration.
pub trait SystemExt: Sized {
    /// Runs `callback` once before this system starts processing invocations
    /// that are invalid for the current snapshot. The callback's
    /// `Commands<S>` parameter type declares its outputs, like a system's.
    fn on_start<C, D>(self, callback: C) -> OnStart<Self, C, D>
    where
        C: SystemCallback<D>,
    {
        OnStart {
            system: self,
            callback,
            _declares: PhantomData,
        }
    }

    /// Runs `callback` once after this system has completed all invocations that
    /// were invalid for the current snapshot.
    fn on_complete<C, D>(self, callback: C) -> OnComplete<Self, C, D>
    where
        C: SystemCallback<D>,
    {
        OnComplete {
            system: self,
            callback,
            _declares: PhantomData,
        }
    }

    /// Runs `callback` after normal evaluation has stopped producing tracked
    /// changes, but before cleanup and before the caller observes results.
    ///
    /// Settled hooks may run more than once while the bowl tries to settle.
    /// Keep them idempotent: a hook that writes tracked changes every time will
    /// keep the bowl alive until the commit limit is reached, unless the limit
    /// is disabled.
    fn on_settled<C, D>(self, callback: C) -> OnSettled<Self, C, D>
    where
        C: SystemCallback<D>,
    {
        OnSettled {
            system: self,
            callback,
            _declares: PhantomData,
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
pub struct OnStart<S, C, D> {
    system: S,
    callback: C,
    _declares: PhantomData<fn() -> D>,
}

/// System wrapper produced by [`SystemExt::on_complete`].
pub struct OnComplete<S, C, D> {
    system: S,
    callback: C,
    _declares: PhantomData<fn() -> D>,
}

/// System wrapper produced by [`SystemExt::on_settled`].
pub struct OnSettled<S, C, D> {
    system: S,
    callback: C,
    _declares: PhantomData<fn() -> D>,
}

/// System wrapper produced by [`SystemExt::run_during`].
pub struct RunDuring<S> {
    system: S,
    phase: Phase,
}

struct OnCompleteSystem<C, D> {
    _declares: ::std::marker::PhantomData<fn() -> D>,
    id: SystemId,
    system: BoxedSystem,
    callback: Arc<C>,
}

struct OnStartSystem<C, D> {
    _declares: ::std::marker::PhantomData<fn() -> D>,
    id: SystemId,
    system: BoxedSystem,
    callback: Arc<C>,
}

struct OnSettledSystem<C, D> {
    _declares: ::std::marker::PhantomData<fn() -> D>,
    id: SystemId,
    system: BoxedSystem,
    callback: Arc<C>,
}

impl<C, D> Runnable for OnStartSystem<C, D>
where
    C: SystemCallback<D>,
    D: Send + Sync + 'static,
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
                self.callback.run(commands.retype());
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

    fn stream_runs(
        &self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &HashMap<SystemInvocation, MemoEntry>,
        hint: Option<&[Entity]>,
    ) -> Vec<PlannedSystemRun> {
        // Pre-plan the inner system now (planning is deterministic over the
        // captured snapshot + memo), so the run future carries the planned
        // rows instead of a memo snapshot — this is what lets the runner
        // stop cloning the memo per wave.
        let inner = self.system.stream_runs(bowl, Arc::clone(&snapshot), memo, hint);
        if inner.is_empty() {
            return Vec::new();
        }

        let owner = SystemInvocation {
            system: self.id,
            keys: Vec::new(),
        };
        let hook_owner = owner.clone();
        let callback = Arc::clone(&self.callback);
        let run = async move {
            // The hook fires before the batch, its output prepended.
            let commands = Commands::new(
                snapshot.spawn_slots(&hook_owner),
                snapshot.entity_allocator(),
            );
            callback.run(commands.retype());
            let start_output = SystemOutput {
                owner: hook_owner,
                commands: commands.take(),
            };

            let runs = join_all(inner.into_iter().map(|planned| planned.run)).await;
            let mut merged = SystemRun::empty();
            merged.completed = true;
            for inner_run in runs {
                merged.completed &= inner_run.completed;
                merged.outputs.extend(inner_run.outputs);
                merged.memo_updates.extend(inner_run.memo_updates);
                merged.writes.extend(inner_run.writes);
            }
            merged.outputs.insert(0, start_output);
            merged
        }
        .boxed();

        vec![PlannedSystemRun {
            owner,
            access: Vec::new(),
            run,
        }]
    }
}

impl<C, D> Runnable for OnCompleteSystem<C, D>
where
    C: SystemCallback<D>,
    D: Send + Sync + 'static,
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
                self.callback.run(commands.retype());
                run.outputs.push(SystemOutput {
                    owner,
                    commands: commands.take(),
                });
            }

            run
        }
        .boxed()
    }

    fn stream_runs(
        &self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &HashMap<SystemInvocation, MemoEntry>,
        hint: Option<&[Entity]>,
    ) -> Vec<PlannedSystemRun> {
        // Pre-planned like `OnStart`; the completion hook appends its
        // output after the batch, only when the batch produced outputs.
        let inner = self.system.stream_runs(bowl, Arc::clone(&snapshot), memo, hint);
        if inner.is_empty() {
            return Vec::new();
        }

        let owner = SystemInvocation {
            system: self.id,
            keys: Vec::new(),
        };
        let hook_owner = owner.clone();
        let callback = Arc::clone(&self.callback);
        let run = async move {
            let runs = join_all(inner.into_iter().map(|planned| planned.run)).await;
            let mut merged = SystemRun::empty();
            merged.completed = true;
            for inner_run in runs {
                merged.completed &= inner_run.completed;
                merged.outputs.extend(inner_run.outputs);
                merged.memo_updates.extend(inner_run.memo_updates);
                merged.writes.extend(inner_run.writes);
            }

            if !merged.outputs.is_empty() {
                let commands = Commands::new(
                    snapshot.spawn_slots(&hook_owner),
                    snapshot.entity_allocator(),
                );
                callback.run(commands.retype());
                merged.outputs.push(SystemOutput {
                    owner: hook_owner,
                    commands: commands.take(),
                });
            }

            merged
        }
        .boxed();

        vec![PlannedSystemRun {
            owner,
            access: Vec::new(),
            run,
        }]
    }
}

impl<C, D> Runnable for OnSettledSystem<C, D>
where
    C: SystemCallback<D>,
    D: Send + Sync + 'static,
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

    fn stream_runs(
        &self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &HashMap<SystemInvocation, MemoEntry>,
        hint: Option<&[Entity]>,
    ) -> Vec<PlannedSystemRun> {
        self.system.stream_runs(bowl, snapshot, memo, hint)
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

            let commands = Commands::new(snapshot.spawn_slots(&owner), snapshot.entity_allocator());
            self.callback.run(commands.retype());

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

impl<S, C, D, M> IntoSystem<(OnCompleteMarker, M)> for OnComplete<S, C, D>
where
    S: IntoSystem<M>,
    C: SystemCallback<D>,
    D: DeclarationList + Send + Sync + 'static,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        let system = self.system.into_system(id);
        let phase = system.phase;
        let name = system.name;
        let view_sets = Arc::clone(&system.view_sets);
        // The hook declares its outputs too; merge them into the
        // system's entry.
        let declared_outputs =
            merge_declarations(system.declared_outputs.clone(), D::declared_types());
        let interest = system.interest.clone();
        let system_delta_eligible = system.delta_eligible;
        BoxedSystem {
            runnable: Arc::new(OnCompleteSystem {
                _declares: PhantomData,
                id,
                system,
                callback: Arc::new(self.callback),
            }),
            phase,
            name,
            view_sets,
            declared_outputs,
            delta_eligible: system_delta_eligible,
            interest,
            planned_mark: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            stats: Arc::new(SystemStats::default()),
            plan_epoch: Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX)),
            log_pos: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl<S, C, D, M> IntoSystem<(OnStartMarker, M)> for OnStart<S, C, D>
where
    S: IntoSystem<M>,
    C: SystemCallback<D>,
    D: DeclarationList + Send + Sync + 'static,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        let system = self.system.into_system(id);
        let phase = system.phase;
        let name = system.name;
        let view_sets = Arc::clone(&system.view_sets);
        // The hook declares its outputs too; merge them into the
        // system's entry.
        let declared_outputs =
            merge_declarations(system.declared_outputs.clone(), D::declared_types());
        let interest = system.interest.clone();
        let system_delta_eligible = system.delta_eligible;
        BoxedSystem {
            runnable: Arc::new(OnStartSystem {
                _declares: PhantomData,
                id,
                system,
                callback: Arc::new(self.callback),
            }),
            phase,
            name,
            view_sets,
            declared_outputs,
            delta_eligible: system_delta_eligible,
            interest,
            planned_mark: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            stats: Arc::new(SystemStats::default()),
            plan_epoch: Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX)),
            log_pos: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl<S, C, D, M> IntoSystem<(OnSettledMarker, M)> for OnSettled<S, C, D>
where
    S: IntoSystem<M>,
    C: SystemCallback<D>,
    D: DeclarationList + Send + Sync + 'static,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        let system = self.system.into_system(id);
        let phase = system.phase;
        let name = system.name;
        let view_sets = Arc::clone(&system.view_sets);
        // The hook declares its outputs too; merge them into the
        // system's entry.
        let declared_outputs =
            merge_declarations(system.declared_outputs.clone(), D::declared_types());
        let interest = system.interest.clone();
        let system_delta_eligible = system.delta_eligible;
        BoxedSystem {
            runnable: Arc::new(OnSettledSystem {
                _declares: PhantomData,
                id,
                system,
                callback: Arc::new(self.callback),
            }),
            phase,
            name,
            view_sets,
            declared_outputs,
            delta_eligible: system_delta_eligible,
            interest,
            planned_mark: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            stats: Arc::new(SystemStats::default()),
            plan_epoch: Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX)),
            log_pos: Arc::new(std::sync::atomic::AtomicU64::new(0)),
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

/// Merges a hook callback's declaration into the wrapped system's entry.
/// A wildcard on either side makes the whole system a wildcard.
fn merge_declarations(
    inner: Option<Arc<Vec<TypeId>>>,
    hook: Option<Vec<TypeId>>,
) -> Option<Arc<Vec<TypeId>>> {
    match (inner, hook) {
        (Some(inner), Some(hook)) => Some(Arc::new(inner.iter().copied().chain(hook).collect())),
        _ => None,
    }
}

impl BoxedSystem {
    pub(crate) fn run<'a>(
        &'a self,
        bowl: Bowl,
        snapshot: &'a Snapshot,
        memo: &'a HashMap<SystemInvocation, MemoEntry>,
    ) -> BoxFuture<'a, SystemRun> {
        self.runnable.run(bowl, snapshot, memo)
    }

    pub(crate) fn stream_runs(
        &self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &HashMap<SystemInvocation, MemoEntry>,
        hint: Option<&[Entity]>,
    ) -> Vec<PlannedSystemRun> {
        self.runnable.stream_runs(bowl, snapshot, memo, hint)
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
pub fn insert_on<T>(entity: Entity, component: T) -> impl SystemCallback<(T,)>
where
    T: crate::Component + Clone,
{
    move |mut commands: Commands<(T,)>| {
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
    // A batch sweep, deliberately not a tracked per-row query: cleanup
    // memoizes at 0% by nature (its work only exists when anchors moved),
    // so per-row invocations are pure overhead — one invocation iterates
    // the whole store instead. `WorldMetaView` keeps it always-run; the
    // pattern is user-land on purpose, so plugins can build the same
    // sweep shape without an engine feature.
    derived: View<'_, (Entity, &DerivedFrom)>,
    meta: WorldMetaView<'_>,
    // Removal-only: the empty declaration says so.
    mut commands: Commands<()>,
) {
    for (entity, derived_from) in derived.iter() {
        if !meta.is_current(derived_from) {
            commands.remove(entity);
        }
    }
}

struct FunctionSystem<F, Marker> {
    id: SystemId,
    /// Behind `Arc` so planned run futures own their callee and stay
    /// `'static` (spawnable).
    function: Arc<F>,
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

                        let (output, memo_update) = finish_invocation(
                            invocation.owner,
                            invocation.deps,
                            snapshot.revision_raw(),
                            commands,
                        );
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

    fn stream_runs(
        &self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &HashMap<SystemInvocation, MemoEntry>,
        hint: Option<&[Entity]>,
    ) -> Vec<PlannedSystemRun> {
        plan_invocations_hinted::<F::Param>(self.id, &snapshot, memo, hint)
            .invocations
            .into_iter()
            .map(|invocation| {
                let owner = invocation.owner.clone();
                let access = invocation.access.clone();
                let bowl = bowl.clone();
                let snapshot = Arc::clone(&snapshot);
                let function = Arc::clone(&self.function);
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
                    function.run(params).await;
                    drop(guards);

                    let (output, memo_update) = finish_invocation(
                        invocation.owner,
                        invocation.deps,
                        snapshot.revision_raw(),
                        commands,
                    );

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

        let mut view_sets = Vec::new();
        F::Param::view_sets(&mut view_sets);
        BoxedSystem::new(
            Arc::new(FunctionSystem {
                id,
                function: Arc::new(self),
                _marker: PhantomData::<Marker>,
            }),
            std::any::type_name::<F>(),
            view_sets,
            F::Param::declared_outputs(),
            F::Param::interest_types(),
            F::Param::delta_shape() == DeltaShape::Driver,
        )
    }
}
