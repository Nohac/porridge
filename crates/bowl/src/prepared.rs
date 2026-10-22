//! Snapshot-local prepared query execution. No row borrows or locks are cached.
use std::marker::PhantomData;
use variadics_please::all_tuples;

use crate::declare::{Anything, FacetKind};
use crate::query::{
    Dep, GuardStore, QueryFilter, QueryParam, QueryPart, store_read_guard, store_write_guard,
};
use crate::system::SystemParam;
use crate::world::{ResolvedStore, Snapshot};
use crate::{Bowl, Commands, Entity, MutRef, Query};

/// Prepared system parameter. Custom parameters retain their existing methods.
#[doc(hidden)]
pub trait PreparedParam<P: SystemParam>: Send + Sync {
    fn deps(&self, snapshot: &Snapshot, state: &P::State, out: &mut Vec<Dep>);

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &P::State,
        commands: &Commands<Anything>,
        guards: &mut GuardStore,
    ) -> P::Item<'a>;
}

/// Prepared query. Handles belong to the same snapshot used to enumerate rows.
#[doc(hidden)]
pub trait PreparedQuery<Q: QueryParam>: Send + Sync {
    fn deps(&self, snapshot: &Snapshot, state: &Q::State, out: &mut Vec<Dep>);

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Q::State,
        guards: &mut GuardStore,
    ) -> Q::Item<'a>;
}

/// Prepared part of an entity query.
#[doc(hidden)]
pub trait PreparedPart<P: QueryPart>: Send + Sync {
    fn deps(&self, snapshot: &Snapshot, entity: Entity, out: &mut Vec<Dep>);

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        entity: Entity,
        guards: &mut GuardStore,
    ) -> P::Item<'a>;
}

pub(crate) struct Fallback<P>(PhantomData<fn() -> P>);

impl<P> Fallback<P> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P: SystemParam> PreparedParam<P> for Fallback<P> {
    fn deps(&self, snapshot: &Snapshot, state: &P::State, out: &mut Vec<Dep>) {
        out.extend(P::deps(snapshot, state));
    }

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &P::State,
        commands: &Commands<Anything>,
        guards: &mut GuardStore,
    ) -> P::Item<'a> {
        P::fetch(bowl, snapshot, state, commands, guards)
    }
}

impl<Q: QueryParam> PreparedQuery<Q> for Fallback<Q> {
    fn deps(&self, snapshot: &Snapshot, state: &Q::State, out: &mut Vec<Dep>) {
        out.extend(Q::deps(snapshot, state));
    }

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Q::State,
        guards: &mut GuardStore,
    ) -> Q::Item<'a> {
        Q::fetch(bowl, snapshot, state, guards)
    }
}

impl<P: QueryPart> PreparedPart<P> for Fallback<P> {
    fn deps(&self, snapshot: &Snapshot, entity: Entity, out: &mut Vec<Dep>) {
        out.extend(P::deps(snapshot, entity));
    }

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        entity: Entity,
        guards: &mut GuardStore,
    ) -> P::Item<'a> {
        P::fetch(bowl, snapshot, entity, guards)
    }
}

impl<T: crate::Component> PreparedPart<&T> for ResolvedStore<T> {
    fn deps(&self, _: &Snapshot, entity: Entity, out: &mut Vec<Dep>) {
        out.extend(self.dependency(entity, false));
    }

    fn fetch<'a>(
        &'a self,
        _: &Bowl,
        _: &'a Snapshot,
        entity: Entity,
        guards: &mut GuardStore,
    ) -> &'a T {
        store_read_guard(
            self.get(entity).expect("prepared row missing component"),
            guards,
        )
    }
}

impl<T: crate::Component> PreparedPart<Option<&T>> for ResolvedStore<T> {
    fn deps(&self, _: &Snapshot, entity: Entity, out: &mut Vec<Dep>) {
        out.extend(self.dependency(entity, true));
    }

    fn fetch<'a>(
        &'a self,
        _: &Bowl,
        _: &'a Snapshot,
        entity: Entity,
        guards: &mut GuardStore,
    ) -> Option<&'a T> {
        self.get(entity)
            .map(|guard| store_read_guard(guard, guards))
    }
}

impl<T: crate::Component> PreparedPart<MutRef<'_, T>> for ResolvedStore<T> {
    fn deps(&self, _: &Snapshot, entity: Entity, out: &mut Vec<Dep>) {
        out.extend(self.dependency(entity, false));
    }

    fn fetch<'a>(
        &'a self,
        _: &Bowl,
        _: &'a Snapshot,
        entity: Entity,
        guards: &mut GuardStore,
    ) -> MutRef<'a, T> {
        let value = store_write_guard(
            self.get_mut(entity)
                .expect("prepared row missing component"),
            guards,
        );
        MutRef::new(entity, value)
    }
}

pub(crate) struct Scalar<P, S>(pub(crate) S, pub(crate) PhantomData<fn() -> P>);

impl<T: crate::Component, S: PreparedPart<&'static T>> PreparedQuery<&T> for Scalar<T, S> {
    fn deps(&self, snapshot: &Snapshot, state: &Entity, out: &mut Vec<Dep>) {
        self.0.deps(snapshot, *state, out);
    }

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Entity,
        guards: &mut GuardStore,
    ) -> &'a T {
        self.0.fetch(bowl, snapshot, *state, guards)
    }
}

impl<T: crate::Component, S: PreparedPart<MutRef<'static, T>>> PreparedQuery<(MutRef<'_, T>,)>
    for Scalar<T, S>
{
    fn deps(&self, snapshot: &Snapshot, state: &Entity, out: &mut Vec<Dep>) {
        self.0.deps(snapshot, *state, out);
    }

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Entity,
        guards: &mut GuardStore,
    ) -> MutRef<'a, T> {
        self.0.fetch(bowl, snapshot, *state, guards)
    }
}

pub(crate) struct Filtered<S>(pub(crate) S);

impl<Q: QueryParam, F: QueryFilter<Q> + Send, S: PreparedQuery<Q>> PreparedParam<Query<Q, F>>
    for Filtered<S>
{
    fn deps(&self, snapshot: &Snapshot, state: &Q::State, out: &mut Vec<Dep>) {
        self.0.deps(snapshot, state, out);
        out.extend(F::deps(snapshot, state));
    }

    fn fetch<'a>(
        &'a self,
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Q::State,
        _: &Commands<Anything>,
        guards: &mut GuardStore,
    ) -> Query<Q::Item<'a>, F> {
        Query::new(self.0.fetch(bowl, snapshot, state, guards))
    }
}

pub(crate) struct EntityParts<S>(pub(crate) S);

macro_rules! prepared_tuples {
    ($(($P:ident, $S:ident)),*) => {
        impl<$($P: SystemParam, $S: PreparedParam<$P>),*> PreparedParam<($($P,)*)> for ($($S,)*) {
            fn deps(&self, snapshot: &Snapshot, state: &($($P::State,)*), out: &mut Vec<Dep>) {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                #[allow(non_snake_case)]
                let ($($S,)*) = self;
                $($S.deps(snapshot, $P, out);)*
            }

            fn fetch<'a>(&'a self, bowl: &Bowl, snapshot: &'a Snapshot, state: &($($P::State,)*),
                commands: &Commands<Anything>, guards: &mut GuardStore) -> ($($P::Item<'a>,)*) {
                #[allow(non_snake_case)]
                let ($($P,)*) = state;
                #[allow(non_snake_case)]
                let ($($S,)*) = self;
                ($($S.fetch(bowl, snapshot, $P, commands, guards),)*)
            }
        }

        impl<H: FacetKind, $($P: QueryPart, $S: PreparedPart<$P>),*> PreparedQuery<(Entity<H>, $($P,)*)> for EntityParts<($($S,)*)> {
            fn deps(&self, snapshot: &Snapshot, state: &Entity, out: &mut Vec<Dep>) {
                #[allow(non_snake_case)]
                let ($($S,)*) = &self.0;
                $($S.deps(snapshot, *state, out);)*
            }

            fn fetch<'a>(&'a self, bowl: &Bowl, snapshot: &'a Snapshot, state: &Entity,
                guards: &mut GuardStore) -> (Entity<H>, $($P::Item<'a>,)*) {
                #[allow(non_snake_case)]
                let ($($S,)*) = &self.0;
                (state.retype(), $($S.fetch(bowl, snapshot, *state, guards),)*)
            }
        }
    };
}

all_tuples!(prepared_tuples, 1, 8, P, S);

#[cfg(test)]
mod tests;
