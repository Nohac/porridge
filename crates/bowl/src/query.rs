use std::{any::TypeId, marker::PhantomData};

use variadics_please::all_tuples;

use crate::{
    Component, Entity,
    world::{Revision, Snapshot},
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

/// Marker query filter that requires `T` to be present without fetching it.
///
/// This is useful for components that act only as tags:
///
/// ```text
/// Query<(Entity, &FilePath), With<HoverRequest>>
/// ```
#[derive(Debug, Clone, Copy)]
pub struct With<T>(PhantomData<T>);

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
    F: QueryFilter<Q>,
{
    snapshot: Snapshot,
    rows: Vec<Q::State>,
    _marker: PhantomData<(Q, F)>,
}

impl<Q, F> QueryResult<Q, F>
where
    Q: QueryParam,
    F: QueryFilter<Q>,
{
    /// Creates a result over every row of `Q` in `snapshot`.
    pub(crate) fn new(snapshot: Snapshot) -> Self {
        let rows = filtered_rows::<Q, F>(&snapshot);
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
