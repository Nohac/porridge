//! Snapshot-local candidate collection. Prepared values never hold row guards.
use std::{any::TypeId, marker::PhantomData};
use variadics_please::all_tuples;
use crate::{
    Component, Entity, MutRef, Query,
    declare::FacetKind,
    query::{PresenceReq, QueryFilter, QueryParam, QueryPart},
    system::SystemParam,
    world::{ResolvedStore, Snapshot},
};

#[doc(hidden)]
pub trait PreparedRows<Q: QueryParam> {
    fn extend(&self, snapshot: &Snapshot, candidates: impl Iterator<Item = Entity>,
        out: &mut impl Extend<Q::State>);
}

#[doc(hidden)]
pub trait PreparedStates<P: SystemParam> {
    fn extend(&self, snapshot: &Snapshot, candidates: impl Iterator<Item = Entity>,
        out: &mut impl Extend<P::State>);
}

pub(crate) struct Fallback<P>(PhantomData<fn() -> P>);

impl<P> Fallback<P> {
    pub(crate) fn new() -> Self { Self(PhantomData) }
}

impl<Q: QueryParam> PreparedRows<Q> for Fallback<Q> {
    fn extend(&self, snapshot: &Snapshot, candidates: impl Iterator<Item = Entity>,
        out: &mut impl Extend<Q::State>) {
        out.extend(Q::rows_hinted(snapshot, Some(candidates.collect())));
    }
}

impl<P: SystemParam> PreparedStates<P> for Fallback<P> {
    fn extend(&self, snapshot: &Snapshot, candidates: impl Iterator<Item = Entity>,
        out: &mut impl Extend<P::State>) {
        out.extend(P::states_from_candidates(snapshot, candidates.collect()));
    }
}

pub(crate) struct Scalar<T>(pub(crate) ResolvedStore<T>);

impl<T: Component> PreparedRows<&T> for Scalar<T> {
    fn extend(&self, _: &Snapshot, candidates: impl Iterator<Item = Entity>,
        out: &mut impl Extend<Entity>) {
        out.extend(candidates.filter(|entity| self.0.contains(*entity)));
    }
}

impl<T: Component> PreparedRows<(MutRef<'_, T>,)> for Scalar<T> {
    fn extend(&self, _: &Snapshot, candidates: impl Iterator<Item = Entity>,
        out: &mut impl Extend<Entity>) {
        out.extend(candidates.filter(|entity| self.0.contains(*entity)));
    }
}

pub(crate) trait Parts {
    fn required(out: &mut Vec<TypeId>) -> bool;
    fn matches(snapshot: &Snapshot, entity: Entity) -> bool;
}

pub(crate) struct EntityRows<H, P> {
    required: Vec<TypeId>,
    mask: Option<Vec<u64>>,
    marker: PhantomData<fn() -> (H, P)>,
}

impl<H: FacetKind, P: Parts> EntityRows<H, P> {
    pub(crate) fn new(snapshot: &Snapshot) -> Self {
        let mut required: Vec<_> = H::parts().into_iter()
            .filter(|part| !part.optional).map(|part| part.type_id).collect();
        let facet_len = required.len();
        let pure_presence = P::required(&mut required);
        let mask = if pure_presence {
            snapshot.presence().and_then(|presence| presence.mask(&required))
        } else { None };
        // Opaque/schema-less fallback checks parts through their predicates;
        // retain only facet requirements to avoid probing parts twice.
        required.truncate(facet_len);
        Self { required, mask, marker: PhantomData }
    }

    fn matches(&self, snapshot: &Snapshot, entity: Entity) -> bool {
        match (&self.mask, snapshot.presence()) {
            (Some(mask), Some(presence)) => presence.matches(entity, mask),
            _ => self.required.iter().all(|ty| snapshot.has_dyn(*ty, entity))
                && P::matches(snapshot, entity),
        }
    }
}

macro_rules! entity_rows {
    ($($P:ident),*) => {
        impl<$($P: QueryPart),*> Parts for ($($P,)*) {
            fn required(out: &mut Vec<TypeId>) -> bool {
                let mut pure = true;
                $(match $P::presence_requirement() {
                    PresenceReq::Requires(ty) => out.push(ty),
                    PresenceReq::Free => {},
                    PresenceReq::Opaque => pure = false,
                })*
                pure
            }

            fn matches(snapshot: &Snapshot, entity: Entity) -> bool {
                true $(&& $P::matches(snapshot, entity))*
            }
        }

        impl<H: FacetKind, $($P: QueryPart),*> PreparedRows<(Entity<H>, $($P,)*)>
            for EntityRows<H, ($($P,)*)> {
            fn extend(&self, snapshot: &Snapshot, candidates: impl Iterator<Item = Entity>,
                out: &mut impl Extend<Entity>) {
                out.extend(candidates.filter(|entity| self.matches(snapshot, *entity)));
            }
        }
    };
}

all_tuples!(entity_rows, 1, 8, P);

pub(crate) struct Filtered<Q, F, R> {
    rows: R,
    marker: PhantomData<fn() -> (Q, F)>,
}

impl<Q, F, R> Filtered<Q, F, R> {
    pub(crate) fn new(rows: R) -> Self { Self { rows, marker: PhantomData } }
}

impl<Q: QueryParam, F: QueryFilter<Q> + Send, R: PreparedRows<Q>> PreparedStates<Query<Q, F>>
    for Filtered<Q, F, R> {
    fn extend(&self, snapshot: &Snapshot, candidates: impl Iterator<Item = Entity>,
        out: &mut impl Extend<Q::State>) {
        self.rows.extend(snapshot, candidates, &mut FilterSink::<Q, F, _> {
            snapshot, out, marker: PhantomData,
        });
    }
}

struct FilterSink<'a, Q: QueryParam, F, O> {
    snapshot: &'a Snapshot,
    out: &'a mut O,
    marker: PhantomData<fn() -> (Q, F)>,
}

impl<Q: QueryParam, F: QueryFilter<Q>, O: Extend<Q::State>> Extend<Q::State> for FilterSink<'_, Q, F, O> {
    fn extend<I: IntoIterator<Item = Q::State>>(&mut self, iter: I) {
        self.out.extend(iter.into_iter().filter(|state| F::matches(self.snapshot, state)));
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Binding {
    Unbound,
    Membership(TypeId),
    Equality(TypeId, &'static str),
    Invalid,
}

pub(crate) struct Buffer<S, P> {
    pub(crate) rows: Vec<S>,
    pub(crate) binding: Binding,
    pub(crate) prepared: Option<P>,
}

pub(crate) fn buffer<P: SystemParam>(snapshot: &Snapshot, rows: Vec<P::State>)
    -> Buffer<P::State, impl PreparedStates<P> + use<P>> {
    let bound = P::pair_expandable();
    let binding = if !bound { Binding::Unbound }
    else if let Some((key, _)) = P::in_key_types().into_iter().next() { Binding::Membership(key) }
    else if let Some((key, name)) = P::bound_key_types().into_iter().next() { Binding::Equality(key, name) }
    else { Binding::Invalid };
    Buffer { rows, binding, prepared: bound.then(|| P::prepare_rows(snapshot)) }
}

#[cfg(test)]
mod tests;
