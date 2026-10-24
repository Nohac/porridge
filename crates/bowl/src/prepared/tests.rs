use super::*;
use crate::world::World;
use crate::{Component, Tracked};
use std::any::TypeId;

struct A(u64);

struct B(u64);

struct Untracked;

impl Component for A {}

impl Component for B {}

impl Component for Untracked {
    fn tracked() -> bool {
        false
    }
}

#[test]
fn prepared_rows_preserve_snapshot_values_and_observe_optional_transitions() {
    type Row = (Entity, &'static A, Option<&'static B>);
    let entity = Entity::from_raw(0);
    let mut world = World::new();
    world.init_presence(vec![TypeId::of::<A>(), TypeId::of::<B>()]);
    world.insert_base(entity, A(10));
    let snapshot = world.clone();
    let prepared = Row::prepare(&snapshot);
    let mut deps = Vec::new();
    prepared.deps(&snapshot, &entity, &mut deps);
    assert_eq!(deps, Row::deps(&snapshot, &entity));
    assert!(deps.iter().all(|dep| dep.is_current(&world)));

    world.insert_base(entity, B(20));
    world.insert_base(entity, A(30));
    assert!(deps.iter().all(|dep| !dep.is_current(&world)));
    let bowl = Bowl::builder().build();
    let mut guards = GuardStore::new();
    let (_, a, b) = prepared.fetch(&bowl, &snapshot, &entity, &mut guards);
    assert_eq!(a.0, 10);
    assert!(b.is_none());
    drop(guards);

    let next = world.clone();
    let prepared = Row::prepare(&next);
    let mut guards = GuardStore::new();
    let (_, a, b) = prepared.fetch(&bowl, &next, &entity, &mut guards);
    assert_eq!((a.0, b.unwrap().0), (30, 20));
    drop(guards);
    let mut deps = Vec::new();
    prepared.deps(&next, &entity, &mut deps);
    world.remove_component::<B>(entity, None);
    assert!(!deps[1].is_current(&world));
    world.remove_component::<A>(entity, None);
    assert!(!deps[0].is_current(&world));
}

#[test]
fn prepared_mutation_uses_existing_cells_and_reconciliation() {
    type Row = (Entity, MutRef<'static, A>, &'static Untracked);
    let entity = Entity::from_raw(0);
    let mut world = World::new();
    world.init_presence(vec![TypeId::of::<A>(), TypeId::of::<Untracked>()]);
    world.insert_base(entity, A(10));
    world.insert_base(entity, Untracked);
    let snapshot = world.clone();
    let prepared = Row::prepare(&snapshot);
    let mut deps = Vec::new();
    prepared.deps(&snapshot, &entity, &mut deps);
    assert_eq!(deps.len(), 1);
    let bowl = Bowl::builder().build();
    let mut guards = GuardStore::new();
    let (_, mut value, _) = prepared.fetch(&bowl, &snapshot, &entity, &mut guards);
    value.0 = 40;
    drop(guards);

    assert_eq!(world.get::<A>(entity).unwrap().0, 40);
    world.reconcile_written(&[(TypeId::of::<A>(), entity)]);
    assert!(!deps[0].is_current(&world));
}

#[test]
fn schema_slots_handle_absence_fallback_and_different_layouts() {
    let entity = Entity::from_raw(0);
    let mut first = World::new();
    first.init_presence(vec![TypeId::of::<A>(), TypeId::of::<B>()]);
    let absent = first.resolve_store::<B>().dependency(entity, true).unwrap();
    first.insert_base(entity, B(1));
    assert!(!absent.is_current(&first));
    first.insert_base(entity, Untracked); // undeclared engine/internal fallback
    assert!(first.get::<Untracked>(entity).is_some());
    assert!(first.clone().get::<Untracked>(entity).is_some());

    let mut second = World::new();
    second.init_presence(vec![TypeId::of::<B>(), TypeId::of::<A>()]);
    second.insert_base(entity, A(1));
    second.insert_base(entity, B(2));
    assert_eq!(
        second.revision_by_slot(0, TypeId::of::<A>(), entity),
        second.revision::<A>(entity)
    );
    assert_eq!(
        second.revision_by_slot(1, TypeId::of::<B>(), entity),
        second.revision::<B>(entity)
    );
}

#[test]
fn complex_parts_keep_their_dependency_fallback() {
    type Row = (Entity, Tracked<(A, Option<B>, Option<Untracked>)>);
    let mut world = World::new();
    world.init_presence(vec![
        TypeId::of::<A>(),
        TypeId::of::<B>(),
        TypeId::of::<Untracked>(),
    ]);
    let entity = Entity::from_raw(0);
    world.insert_base(entity, A(1));
    let prepared = Row::prepare(&world);
    let mut deps = Vec::new();
    prepared.deps(&world, &entity, &mut deps);
    assert_eq!(deps, Row::deps(&world, &entity));
}

#[test]
fn refreshing_written_dependency_checks_cached_slot_layout_and_fallback() {
    let entity = Entity::from_raw(0);
    let mut original = World::new();
    original.init_presence(vec![TypeId::of::<A>(), TypeId::of::<B>()]);
    original.insert_base(entity, A(1));
    let mut dep = original.resolve_store::<A>().dependency(entity, false).unwrap();
    original.insert_base(entity, A(2));
    dep.refresh_written(&original, &[(TypeId::of::<B>(), entity)]);
    assert!(!dep.is_current(&original));
    dep.refresh_written(&original, &[(TypeId::of::<A>(), entity)]);
    assert!(dep.is_current(&original));

    let mut reordered = World::new();
    reordered.init_presence(vec![TypeId::of::<B>(), TypeId::of::<A>()]);
    reordered.insert_base(entity, A(3));
    reordered.insert_base(entity, B(4));
    dep.refresh_written(&reordered, &[(TypeId::of::<A>(), entity)]);
    assert!(dep.is_current(&reordered));

    let mut fallback = World::new();
    fallback.insert_base(entity, A(5));
    let mut dep = fallback.resolve_store::<A>().dependency(entity, false).unwrap();
    fallback.insert_base(entity, A(6));
    dep.refresh_written(&fallback, &[(TypeId::of::<A>(), entity)]);
    assert!(dep.is_current(&fallback));
}
