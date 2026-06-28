use std::{any::TypeId, collections::HashMap, marker::PhantomData};

use crate::{Component, Db, Dep, Entity, QueryParam, World};

pub struct Where<F>(pub PhantomData<F>);
pub struct And<A, B>(pub PhantomData<(A, B)>);
pub struct Or<A, B>(pub PhantomData<(A, B)>);
pub struct Not<F>(pub PhantomData<F>);
pub struct Eq<T>(pub PhantomData<T>);
pub struct Gte<T>(pub PhantomData<T>);
pub struct With<T>(pub PhantomData<T>);
pub struct Without<T>(pub PhantomData<T>);

#[derive(Default)]
pub(crate) struct Bindings {
    values: HashMap<TypeId, Box<dyn std::any::Any>>,
}

impl Bindings {
    pub(crate) fn insert<T: Component>(&mut self, value: T) {
        self.values.insert(TypeId::of::<T>(), Box::new(value));
    }

    pub(crate) fn get<T: Component>(&self) -> &T {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref())
            .expect("missing query binding")
    }
}

pub trait FilterExpr: 'static {
    fn matches(entity: Entity, world: &World, bindings: &Bindings) -> bool;
    fn deps(entity: Entity, world: &World) -> Vec<Dep>;
}

impl<T> FilterExpr for Eq<T>
where
    T: Component + PartialEq,
{
    fn matches(entity: Entity, world: &World, bindings: &Bindings) -> bool {
        world
            .get::<T>(entity)
            .is_some_and(|value| value == bindings.get::<T>())
    }

    fn deps(entity: Entity, world: &World) -> Vec<Dep> {
        crate::component_dep_if_present::<T>(world, entity)
            .into_iter()
            .collect()
    }
}

impl<T> FilterExpr for Gte<T>
where
    T: Component + PartialOrd,
{
    fn matches(entity: Entity, world: &World, bindings: &Bindings) -> bool {
        world
            .get::<T>(entity)
            .is_some_and(|value| value >= bindings.get::<T>())
    }

    fn deps(entity: Entity, world: &World) -> Vec<Dep> {
        crate::component_dep_if_present::<T>(world, entity)
            .into_iter()
            .collect()
    }
}

impl<T: Component> FilterExpr for With<T> {
    fn matches(entity: Entity, world: &World, _bindings: &Bindings) -> bool {
        world.has::<T>(entity)
    }

    fn deps(entity: Entity, world: &World) -> Vec<Dep> {
        crate::component_dep_if_present::<T>(world, entity)
            .into_iter()
            .collect()
    }
}

impl<T: Component> FilterExpr for Without<T> {
    fn matches(entity: Entity, world: &World, _bindings: &Bindings) -> bool {
        !world.has::<T>(entity)
    }

    fn deps(_entity: Entity, _world: &World) -> Vec<Dep> {
        Vec::new()
    }
}

impl<A, B> FilterExpr for And<A, B>
where
    A: FilterExpr,
    B: FilterExpr,
{
    fn matches(entity: Entity, world: &World, bindings: &Bindings) -> bool {
        A::matches(entity, world, bindings) && B::matches(entity, world, bindings)
    }

    fn deps(entity: Entity, world: &World) -> Vec<Dep> {
        [A::deps(entity, world), B::deps(entity, world)].concat()
    }
}

impl<A, B> FilterExpr for Or<A, B>
where
    A: FilterExpr,
    B: FilterExpr,
{
    fn matches(entity: Entity, world: &World, bindings: &Bindings) -> bool {
        A::matches(entity, world, bindings) || B::matches(entity, world, bindings)
    }

    fn deps(entity: Entity, world: &World) -> Vec<Dep> {
        [A::deps(entity, world), B::deps(entity, world)].concat()
    }
}

impl<F: FilterExpr> FilterExpr for Not<F> {
    fn matches(entity: Entity, world: &World, bindings: &Bindings) -> bool {
        !F::matches(entity, world, bindings)
    }

    fn deps(entity: Entity, world: &World) -> Vec<Dep> {
        F::deps(entity, world)
    }
}

pub struct QueryBuilder<'db, Q> {
    pub(crate) db: &'db mut Db,
    pub(crate) bindings: Bindings,
    pub(crate) _query: PhantomData<Q>,
}

impl<'db, Q> QueryBuilder<'db, Q>
where
    Q: QueryParam,
{
    pub fn bind<T: Component>(mut self, value: T) -> Self {
        self.bindings.insert(value);
        self
    }

    pub fn collect(self) -> Vec<Q::Item> {
        self.db.materialize();
        let rows = Q::rows_with(&self.db.world, &self.bindings);
        let world = &self.db.world as *const World;
        rows.into_iter()
            // SAFETY: `rows` was produced from `self.db.world` immediately
            // above, and no mutation occurs before the query result values are
            // fetched. The resulting references are prototype-lifetime widened;
            // callers must not mutate this `Db` while holding them.
            .map(|row| unsafe { Q::fetch(world, &row) })
            .collect()
    }
}
