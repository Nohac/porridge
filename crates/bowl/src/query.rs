use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    marker::PhantomData,
};

use variadics_please::all_tuples;

use crate::{
    Component, Entity,
    world::{Revision, Snapshot, World},
};

/// A tracked system input.
///
/// `Query<T, F>` is what makes a system row-addressable and memoizable. Each
/// row discovered by `T` and accepted by filter `F` contributes:
///
/// ```text
/// keys -> invocation identity
/// deps -> memoized input revisions
/// item -> values passed to the system
/// ```
///
/// Query items borrow from an immutable snapshot through normal Rust lifetimes.
pub struct Query<T, F = ()> {
    item: T,
    _filter: PhantomData<F>,
}

impl<T, F> Query<T, F> {
    pub(crate) fn new(item: T) -> Self {
        Self {
            item,
            _filter: PhantomData,
        }
    }

    /// Returns the row data selected by this query.
    pub fn item(self) -> T {
        self.item
    }

    /// Borrows the row data selected by this query.
    pub fn as_item(&self) -> &T {
        &self.item
    }
}

/// Assigns a type-level name to an external scoop query.
///
/// Named queries let one scoop request bind different runtime args of the same
/// component type:
///
/// ```text
/// struct Imports;
/// struct Diagnostics;
///
/// bowl.scoop::<(
///     Named<Imports, Query<(Entity, &Import), Where<Eq<FilePath>>>>,
///     Named<Diagnostics, Query<(Entity, &Diagnostic), Where<Eq<FilePath>>>>,
/// )>()
/// .args_for::<Imports>(FilePath("main.por"))
/// .args_for::<Diagnostics>(FilePath("lib.por"))
/// .await
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Named<Tag, S>(PhantomData<fn() -> (Tag, S)>);

/// Marker query filter that requires `T` to be present without fetching it.
///
/// This is useful for components that act only as tags:
///
/// ```text
/// Query<(Entity, &FilePath), With<HoverRequest>>
/// ```
#[derive(Debug, Clone, Copy)]
pub struct With<T>(PhantomData<T>);

/// Query filter wrapper used by external bowl queries.
///
/// Unlike system-side [`Query<T, F>`] filters, `Where<F>` can use runtime
/// arguments supplied through `Bowl::scoop(...).args(...)`.
#[derive(Debug, Clone, Copy)]
pub struct Where<F>(PhantomData<F>);

/// Requires component `T` on the row entity to equal the bound query argument.
#[derive(Debug, Clone, Copy)]
pub struct Eq<T>(PhantomData<T>);

/// Requires component `T` on the row entity to be greater than or equal to the
/// bound query argument.
#[derive(Debug, Clone, Copy)]
pub struct Gte<T>(PhantomData<T>);

/// Boolean conjunction of two query filter expressions.
#[derive(Debug, Clone, Copy)]
pub struct And<A, B>(PhantomData<(A, B)>);

/// Boolean disjunction of two query filter expressions.
#[derive(Debug, Clone, Copy)]
pub struct Or<A, B>(PhantomData<(A, B)>);

/// Boolean negation of one query filter expression.
#[derive(Debug, Clone, Copy)]
pub struct Not<F>(PhantomData<F>);

/// Requires component `T` to be absent from the row entity.
#[derive(Debug, Clone, Copy)]
pub struct Without<T>(PhantomData<T>);

/// Mutable component projection for external mutation queries.
///
/// `Mut<T>` does not represent a long-lived mutable borrow. It is only valid in
/// `Bowl::scoop(...).for_each(...)`, where the mutable borrow is scoped to one
/// synchronous closure call while the live world is locked.
#[derive(Debug, Clone, Copy)]
pub struct Mut<T>(PhantomData<T>);

/// Runtime values bound to external query filters.
#[derive(Default)]
pub struct QueryArgs {
    shared: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    scoped: HashMap<(TypeId, TypeId), Box<dyn Any + Send + Sync>>,
}

impl QueryArgs {
    pub(crate) fn insert<T: Component>(&mut self, scope: Option<TypeId>, value: T) {
        let component = TypeId::of::<T>();
        let previous = match scope {
            Some(scope) => self.scoped.insert((scope, component), Box::new(value)),
            None => self.shared.insert(component, Box::new(value)),
        };

        if previous.is_some() {
            panic!("duplicate query argument {}", type_name::<T>());
        }
    }

    fn get<T: Component>(&self, scope: Option<TypeId>) -> &T {
        let component = TypeId::of::<T>();
        let value = scope
            .and_then(|scope| self.scoped.get(&(scope, component)))
            .or_else(|| self.shared.get(&component))
            .unwrap_or_else(|| panic!("missing query argument {}", type_name::<T>()));

        value
            .downcast_ref()
            .expect("query argument stored with wrong type")
    }
}

/// One or more runtime arguments for external `Where` filters.
pub trait ArgBundle: Send + 'static {
    #[doc(hidden)]
    fn insert_into(self, args: &mut QueryArgs, scope: Option<TypeId>);
}

impl<T> ArgBundle for T
where
    T: Component,
{
    fn insert_into(self, args: &mut QueryArgs, scope: Option<TypeId>) {
        args.insert(scope, self);
    }
}

macro_rules! impl_arg_bundle_tuple {
    ($($T:ident),*) => {
        impl<$($T: ArgBundle),*> ArgBundle for ($($T,)*)
        {
            #[allow(non_snake_case)]
            fn insert_into(self, args: &mut QueryArgs, scope: Option<TypeId>) {
                let ($($T,)*) = self;
                $($T.insert_into(args, scope);)*
            }
        }
    };
}

all_tuples!(impl_arg_bundle_tuple, 1, 8, T);

/// Ambient read-only snapshot access.
///
/// A `View<T>` is built from the same immutable snapshot as the driving
/// [`Query`], but it is intentionally not part of the invocation memo key.
///
/// ```text
/// Query<T, F = ()>
///   tracked dependency
///
/// View<T, F = ()>
///   current snapshot context
///   no automatic invalidation
/// ```
///
/// This is useful for checks that need to inspect surrounding facts but should
/// only rerun when their driving row changes.
pub struct View<'a, T, F = ()>
where
    T: QueryParam,
    F: QueryFilter<T>,
{
    snapshot: &'a Snapshot,
    rows: Vec<<T as QueryParam>::State>,
    _marker: PhantomData<(T, F)>,
}

impl<'a, T, F> View<'a, T, F>
where
    T: QueryParam,
    F: QueryFilter<T>,
{
    /// Builds an ambient view over `snapshot`.
    ///
    /// This records row states eagerly, then fetches borrowed items lazily while
    /// iterating. The snapshot is immutable for the whole system invocation, so
    /// rows cannot disappear between enumeration and fetch.
    pub(crate) fn new(snapshot: &'a Snapshot) -> Self {
        Self {
            snapshot,
            rows: filtered_rows::<T, F>(snapshot),
            _marker: PhantomData,
        }
    }

    /// Iterates rows visible in this snapshot.
    pub fn iter(&'a self) -> impl Iterator<Item = T::Item<'a>> + 'a {
        self.rows.iter().map(|row| T::fetch(self.snapshot, row))
    }

    /// Number of rows in the view.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` when the view has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Materialized result of a database query.
///
/// The result owns the snapshot it was read from. Borrowed rows returned by
/// [`QueryResult::collect`] are therefore tied to `&self`, not to the live bowl.
pub struct QueryResult<Q, F = ()>
where
    Q: QueryParam,
{
    snapshot: Snapshot,
    rows: Vec<Q::State>,
    _marker: PhantomData<(Q, F)>,
}

impl<Q, F> QueryResult<Q, F>
where
    Q: QueryParam,
    F: ExternalQueryFilter<Q>,
{
    /// Creates a result over every row of `Q` in `snapshot`.
    pub(crate) fn new(snapshot: Snapshot, args: &QueryArgs, scope: Option<TypeId>) -> Self {
        let rows = external_filtered_rows::<Q, F>(&snapshot, args, scope);
        Self {
            snapshot,
            rows,
            _marker: PhantomData,
        }
    }

    /// Fetches all rows from the owned snapshot.
    ///
    /// This returns borrowed values. Keep the [`QueryResult`] alive while using
    /// the collected rows.
    pub fn collect(&self) -> Vec<Q::Item<'_>> {
        self.rows
            .iter()
            .map(|row| Q::fetch(&self.snapshot, row))
            .collect()
    }

    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` when the result has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct Dep {
    type_id: TypeId,
    entity: Entity,
    revision: Revision,
}

/// Describes how a query-shaped type is enumerated and fetched from a snapshot.
///
/// This is the low-level trait behind both [`Query`] and [`View`].
///
/// The four pieces have distinct roles:
///
/// ```text
/// State
///   cheap row handle, usually Entity
///
/// Item<'a>
///   borrowed data passed to user code
///
/// keys
///   entity keys used to identify the system invocation
///
/// deps
///   tracked component revisions used for memoization
/// ```
///
/// `fetch` is safe because it reads from an immutable snapshot and row states
/// are produced from that same snapshot.
pub trait QueryParam {
    type State: Clone;
    type Item<'a>;

    /// Enumerates all row states in `snapshot`.
    fn rows(snapshot: &Snapshot) -> Vec<Self::State>;
    /// Returns entity keys that identify the invocation for this row.
    fn keys(state: &Self::State) -> Vec<Entity>;
    /// Returns tracked component revisions that should invalidate this row.
    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep>;
    /// Fetches the user-facing item for a previously enumerated row.
    fn fetch<'a>(snapshot: &'a Snapshot, state: &Self::State) -> Self::Item<'a>;
}

/// Query-shaped mutable projection over the live world.
pub trait MutQueryParam {
    type State: Clone + EntityQueryState;
    type Item<'a>;

    /// Enumerates candidate row states from the current live world.
    fn rows(snapshot: &Snapshot) -> Vec<Self::State>;
    /// Mutates one previously-enumerated row.
    fn for_each_mut<F>(world: &mut World, state: &Self::State, f: F) -> bool
    where
        F: for<'a> FnOnce(Self::Item<'a>);
}

impl<T> MutQueryParam for (Mut<T>,)
where
    T: Component + Clone,
{
    type State = Entity;
    type Item<'a> = &'a mut T;

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        (0..snapshot.next_entity_raw())
            .map(Entity)
            .filter(|entity| snapshot.has::<T>(*entity))
            .collect()
    }

    fn for_each_mut<F>(world: &mut World, state: &Self::State, f: F) -> bool
    where
        F: for<'a> FnOnce(Self::Item<'a>),
    {
        world
            .update_component::<T, _, _>(*state, |component| f(component))
            .unwrap_or(false)
    }
}

impl<T> MutQueryParam for (Entity, Mut<T>)
where
    T: Component + Clone,
{
    type State = Entity;
    type Item<'a> = (Entity, &'a mut T);

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        (0..snapshot.next_entity_raw())
            .map(Entity)
            .filter(|entity| snapshot.has::<T>(*entity))
            .collect()
    }

    fn for_each_mut<F>(world: &mut World, state: &Self::State, f: F) -> bool
    where
        F: for<'a> FnOnce(Self::Item<'a>),
    {
        world
            .update_component::<T, _, _>(*state, |component| f((*state, component)))
            .unwrap_or(false)
    }
}

impl QueryParam for Entity {
    type State = Entity;
    type Item<'a> = Entity;

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        (0..snapshot.next_entity_raw()).map(Entity).collect()
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Dep> {
        Vec::new()
    }

    fn fetch<'a>(_snapshot: &'a Snapshot, state: &Self::State) -> Self::Item<'a> {
        *state
    }
}

impl<T: Component> QueryParam for &T {
    type State = Entity;
    type Item<'a> = &'a T;

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        (0..snapshot.next_entity_raw())
            .map(Entity)
            .filter(|entity| snapshot.has::<T>(*entity))
            .collect()
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
        component_dep_if_tracked::<T>(snapshot, *state)
            .into_iter()
            .collect()
    }

    fn fetch<'a>(snapshot: &'a Snapshot, state: &Self::State) -> Self::Item<'a> {
        snapshot
            .get::<T>(*state)
            .expect("query row referenced a missing component")
    }
}

/// One entry in an entity query tuple.
#[doc(hidden)]
pub trait QueryPart {
    type Item<'a>;

    fn matches(snapshot: &Snapshot, entity: Entity) -> bool;
    fn deps(snapshot: &Snapshot, entity: Entity) -> Vec<Dep>;
    fn fetch<'a>(snapshot: &'a Snapshot, entity: Entity) -> Self::Item<'a>;
}

impl<T: Component> QueryPart for &T {
    type Item<'a> = &'a T;

    fn matches(snapshot: &Snapshot, entity: Entity) -> bool {
        snapshot.has::<T>(entity)
    }

    fn deps(snapshot: &Snapshot, entity: Entity) -> Vec<Dep> {
        component_dep_if_tracked::<T>(snapshot, entity)
            .into_iter()
            .collect()
    }

    fn fetch<'a>(snapshot: &'a Snapshot, entity: Entity) -> Self::Item<'a> {
        snapshot
            .get::<T>(entity)
            .expect("query row referenced a missing component")
    }
}

macro_rules! impl_entity_query_param {
    ($($P:ident),*) => {
        impl<$($P: QueryPart),*> QueryParam for (Entity, $($P,)*)
        {
            type State = Entity;
            type Item<'a> = (Entity, $(<$P as QueryPart>::Item<'a>,)*);

            fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
                (0..snapshot.next_entity_raw())
                    .map(Entity)
                    .filter(|entity| true $(&& $P::matches(snapshot, *entity))*)
                    .collect()
            }

            fn keys(state: &Self::State) -> Vec<Entity> {
                vec![*state]
            }

            fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
                let mut deps = Vec::new();
                $(deps.extend($P::deps(snapshot, *state));)*
                deps
            }

            fn fetch<'a>(snapshot: &'a Snapshot, state: &Self::State) -> Self::Item<'a> {
                (
                    *state,
                    $($P::fetch(snapshot, *state),)*
                )
            }
        }
    };
}

all_tuples!(impl_entity_query_param, 1, 8, P);

#[doc(hidden)]
pub trait EntityQueryState {
    fn entity(&self) -> Entity;
}

impl EntityQueryState for Entity {
    fn entity(&self) -> Entity {
        *self
    }
}

/// Additional predicate applied to query data without changing the returned row.
///
/// Filters can contribute dependencies, so a memoized system row can be
/// invalidated when a tracked marker component changes even though that marker
/// is not part of the returned item.
pub trait QueryFilter<Q: QueryParam> {
    fn matches(snapshot: &Snapshot, state: &Q::State) -> bool;
    fn deps(snapshot: &Snapshot, state: &Q::State) -> Vec<Dep>;
}

impl<Q: QueryParam> QueryFilter<Q> for () {
    fn matches(_snapshot: &Snapshot, _state: &Q::State) -> bool {
        true
    }

    fn deps(_snapshot: &Snapshot, _state: &Q::State) -> Vec<Dep> {
        Vec::new()
    }
}

impl<Q, T> QueryFilter<Q> for With<T>
where
    Q: QueryParam,
    Q::State: EntityQueryState,
    T: Component,
{
    fn matches(snapshot: &Snapshot, state: &Q::State) -> bool {
        snapshot.has::<T>(state.entity())
    }

    fn deps(snapshot: &Snapshot, state: &Q::State) -> Vec<Dep> {
        component_dep_if_tracked::<T>(snapshot, state.entity())
            .into_iter()
            .collect()
    }
}

impl<Q, T> QueryFilter<Q> for Without<T>
where
    Q: QueryParam,
    Q::State: EntityQueryState,
    T: Component,
{
    fn matches(snapshot: &Snapshot, state: &Q::State) -> bool {
        !snapshot.has::<T>(state.entity())
    }

    fn deps(_snapshot: &Snapshot, _state: &Q::State) -> Vec<Dep> {
        Vec::new()
    }
}

/// Filter over an external query row state.
pub trait ExternalFilter<State>: 'static {
    fn matches(snapshot: &Snapshot, args: &QueryArgs, scope: Option<TypeId>, state: &State)
    -> bool;
}

impl<State> ExternalFilter<State> for () {
    fn matches(
        _snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
        _state: &State,
    ) -> bool {
        true
    }
}

impl<State, F> ExternalFilter<State> for Where<F>
where
    State: EntityQueryState,
    F: FilterExpr,
{
    fn matches(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
        state: &State,
    ) -> bool {
        F::matches(state.entity(), snapshot, args, scope)
    }
}

impl<State, T> ExternalFilter<State> for With<T>
where
    State: EntityQueryState,
    T: Component,
{
    fn matches(
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
        state: &State,
    ) -> bool {
        snapshot.has::<T>(state.entity())
    }
}

impl<State, T> ExternalFilter<State> for Without<T>
where
    State: EntityQueryState,
    T: Component,
{
    fn matches(
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
        state: &State,
    ) -> bool {
        !snapshot.has::<T>(state.entity())
    }
}

/// Filter used by external read-only bowl queries.
pub trait ExternalQueryFilter<Q: QueryParam>: 'static {
    fn matches(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
        state: &Q::State,
    ) -> bool;
}

impl<Q, F> ExternalQueryFilter<Q> for F
where
    Q: QueryParam,
    F: ExternalFilter<Q::State>,
{
    fn matches(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
        state: &Q::State,
    ) -> bool {
        F::matches(snapshot, args, scope, state)
    }
}

/// Runtime-argument filter expression used inside [`Where`].
pub trait FilterExpr: 'static {
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool;
}

impl<T> FilterExpr for Eq<T>
where
    T: Component + PartialEq,
{
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        snapshot
            .get::<T>(entity)
            .is_some_and(|value| value == args.get::<T>(scope))
    }
}

impl<T> FilterExpr for Gte<T>
where
    T: Component + PartialOrd,
{
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        snapshot
            .get::<T>(entity)
            .is_some_and(|value| value >= args.get::<T>(scope))
    }
}

impl<T: Component> FilterExpr for With<T> {
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> bool {
        snapshot.has::<T>(entity)
    }
}

impl<T: Component> FilterExpr for Without<T> {
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> bool {
        !snapshot.has::<T>(entity)
    }
}

impl<A, B> FilterExpr for And<A, B>
where
    A: FilterExpr,
    B: FilterExpr,
{
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        A::matches(entity, snapshot, args, scope) && B::matches(entity, snapshot, args, scope)
    }
}

impl<A, B> FilterExpr for Or<A, B>
where
    A: FilterExpr,
    B: FilterExpr,
{
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        A::matches(entity, snapshot, args, scope) || B::matches(entity, snapshot, args, scope)
    }
}

impl<F: FilterExpr> FilterExpr for Not<F> {
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        !F::matches(entity, snapshot, args, scope)
    }
}

pub(crate) fn filtered_rows<Q, F>(snapshot: &Snapshot) -> Vec<Q::State>
where
    Q: QueryParam,
    F: QueryFilter<Q>,
{
    Q::rows(snapshot)
        .into_iter()
        .filter(|state| F::matches(snapshot, state))
        .collect()
}

pub(crate) fn external_filtered_rows<Q, F>(
    snapshot: &Snapshot,
    args: &QueryArgs,
    scope: Option<TypeId>,
) -> Vec<Q::State>
where
    Q: QueryParam,
    F: ExternalQueryFilter<Q>,
{
    Q::rows(snapshot)
        .into_iter()
        .filter(|state| F::matches(snapshot, args, scope, state))
        .collect()
}

pub(crate) fn external_filtered_mut_rows<Q, F>(
    snapshot: &Snapshot,
    args: &QueryArgs,
    scope: Option<TypeId>,
) -> Vec<Q::State>
where
    Q: MutQueryParam,
    F: ExternalFilter<Q::State>,
{
    Q::rows(snapshot)
        .into_iter()
        .filter(|state| F::matches(snapshot, args, scope, state))
        .collect()
}

pub(crate) fn filtered_deps<Q, F>(snapshot: &Snapshot, state: &Q::State) -> Vec<Dep>
where
    Q: QueryParam,
    F: QueryFilter<Q>,
{
    let mut deps = Q::deps(snapshot, state);
    deps.extend(F::deps(snapshot, state));
    deps
}

/// Produces a dependency for `T` on `entity` when `T` participates in revision
/// tracking.
fn component_dep_if_tracked<T: Component>(snapshot: &Snapshot, entity: Entity) -> Option<Dep> {
    T::tracked().then(|| Dep {
        type_id: TypeId::of::<T>(),
        entity,
        revision: snapshot
            .revision::<T>(entity)
            .expect("query dependency referenced a missing component"),
    })
}
