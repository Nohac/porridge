use std::{any::TypeId, marker::PhantomData};

use variadics_please::all_tuples;

use crate::{
    Component, Entity,
    world::{Revision, Snapshot},
};

/// A tracked system input.
///
/// `Query<T>` is what makes a system row-addressable and memoizable. Each row
/// discovered by `T` contributes:
///
/// ```text
/// keys -> invocation identity
/// deps -> memoized input revisions
/// item -> values passed to the system
/// ```
///
/// Query items borrow from an immutable snapshot through normal Rust lifetimes.
pub struct Query<T>(pub T);

/// Ambient read-only snapshot access.
///
/// A `View<T>` is built from the same immutable snapshot as the driving
/// [`Query`], but it is intentionally not part of the invocation memo key.
///
/// ```text
/// Query<T>
///   tracked dependency
///
/// View<T>
///   current snapshot context
///   no automatic invalidation
/// ```
///
/// This is useful for checks that need to inspect surrounding facts but should
/// only rerun when their driving row changes.
pub struct View<'a, T: QueryParam> {
    snapshot: &'a Snapshot,
    rows: Vec<<T as QueryParam>::State>,
    _marker: PhantomData<T>,
}

impl<'a, T: QueryParam> View<'a, T> {
    /// Builds an ambient view over `snapshot`.
    ///
    /// This records row states eagerly, then fetches borrowed items lazily while
    /// iterating. The snapshot is immutable for the whole system invocation, so
    /// rows cannot disappear between enumeration and fetch.
    pub(crate) fn new(snapshot: &'a Snapshot) -> Self {
        Self {
            snapshot,
            rows: T::rows(snapshot),
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
pub struct QueryResult<Q: QueryParam> {
    snapshot: Snapshot,
    rows: Vec<Q::State>,
    _marker: PhantomData<Q>,
}

impl<Q: QueryParam> QueryResult<Q> {
    /// Creates a result over every row of `Q` in `snapshot`.
    pub(crate) fn new(snapshot: Snapshot) -> Self {
        let rows = Q::rows(&snapshot);
        Self {
            snapshot,
            rows,
            _marker: PhantomData,
        }
    }

    /// Creates a result scoped to rows whose invocation keys contain `entity`.
    ///
    /// This is a temporary bridge for request-style inserted entity queries.
    /// The future bound-entity API should make this capability explicit.
    pub(crate) fn new_for_entity(snapshot: Snapshot, entity: Entity) -> Self {
        let rows = Q::rows(&snapshot)
            .into_iter()
            .filter(|row| Q::keys(row).contains(&entity))
            .collect();

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

macro_rules! impl_entity_query_param {
    ($($T:ident),*) => {
        impl<$($T: Component),*> QueryParam for (Entity, $(& $T,)*)
        {
            type State = Entity;
            type Item<'a> = (Entity, $(&'a $T,)*);

            fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
                (0..snapshot.next_entity_raw())
                    .map(Entity)
                    .filter(|entity| true $(&& snapshot.has::<$T>(*entity))*)
                    .collect()
            }

            fn keys(state: &Self::State) -> Vec<Entity> {
                vec![*state]
            }

            fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
                let mut deps = Vec::new();
                $(deps.extend(component_dep_if_tracked::<$T>(snapshot, *state));)*
                deps
            }

            fn fetch<'a>(snapshot: &'a Snapshot, state: &Self::State) -> Self::Item<'a> {
                (
                    *state,
                    $(snapshot.get::<$T>(*state).expect("query row referenced a missing component"),)*
                )
            }
        }
    };
}

all_tuples!(impl_entity_query_param, 1, 8, T);

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
