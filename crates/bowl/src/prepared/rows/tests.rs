use super::*;
use crate::{And, Eq, Where, Without};

struct Key(u64);
impl Component for Key { fn fingerprint(&self) -> Option<u64> { Some(self.0) } }
struct Request;
impl Component for Request {}
struct Value;
impl Component for Value {}
struct Flag;
impl Component for Flag {}
struct Anchor;
impl Component for Anchor {}

#[test]
fn pair_buffers_clear_between_providers_and_refresh_masks_each_snapshot() {
    type Params = (
        Query<(Entity, &'static Request, &'static Key)>,
        Query<(Entity<(Value, Anchor)>, &'static Value, Option<&'static Flag>),
            And<Where<Eq<Key>>, Without<Flag>>>,
    );
    let mut world = Snapshot::new();
    world.init_presence(vec![TypeId::of::<Key>(), TypeId::of::<Request>(),
        TypeId::of::<Value>(), TypeId::of::<Flag>(), TypeId::of::<Anchor>()]);
    let p1 = Entity::from_raw(1);
    let p2 = Entity::from_raw(2);
    let a = Entity::from_raw(10);
    let b = Entity::from_raw(11);
    let c = Entity::from_raw(12);
    for (id, key) in [(p1, 7), (p2, 8)] {
        world.insert_base(id, Request);
        world.insert_base(id, Key(key));
    }
    for id in [a, b, c] {
        world.insert_base(id, Value);
        world.insert_base(id, Key(7));
    }
    world.insert_base(a, Anchor);
    world.insert_base(b, Anchor);
    world.insert_base(b, Flag);
    // c lacks the facet's Anchor even though the fetched Value is present.
    assert_eq!(Params::states(&world), [(p1, a)]);

    let snapshot = world.clone();
    world.remove_component::<Flag>(b, None);
    world.insert_base(b, Key(8));
    world.insert_base(c, Anchor);
    assert_eq!(Params::states(&world), [(p1, a), (p1, c), (p2, b)]);
    assert_eq!(Params::states(&snapshot), [(p1, a)]);
    world.remove_component::<Value>(b, None);
    assert_eq!(Params::states(&world), [(p1, a), (p1, c)]);
}

struct OddRows;

impl QueryParam for OddRows {
    type State = Entity;
    type Item<'a> = ();

    fn rows(_: &Snapshot) -> Vec<Entity> { panic!("candidate hints must reach custom rows"); }

    fn rows_hinted(_: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Entity> {
        hint.unwrap().into_iter().filter(|id| id.raw() % 2 == 1).collect()
    }

    fn keys(state: &Entity) -> Vec<Entity> { vec![*state] }

    fn deps(_: &Snapshot, _: &Entity) -> Vec<crate::query::Dep> { Vec::new() }

    fn access(_: &Snapshot, _: &Entity) -> Vec<crate::query::Access> { Vec::new() }

    fn access_all() -> Vec<crate::query::Access> { Vec::new() }

    fn fetch<'a>(_: &crate::Bowl, _: &'a Snapshot, _: &Entity,
        _: &mut crate::query::GuardStore) {}
}

#[test]
fn custom_candidate_fallback_keeps_its_filtering_and_append_contract() {
    let world = Snapshot::new();
    let prepared = OddRows::prepare_rows(&world);
    let mut result = vec![Entity::from_raw(99)];
    prepared.extend(&world, (0..5).map(Entity::from_raw), &mut result);
    assert_eq!(result.into_iter().map(|id| id.raw()).collect::<Vec<_>>(), [99, 1, 3]);
}

impl QueryPart for OddRows {
    type Item<'a> = ();

    fn store_len(_: &Snapshot) -> usize { usize::MAX }

    fn candidates(_: &Snapshot) -> Vec<Entity> { panic!("prepared matching does not enumerate custom parts"); }

    fn matches(_: &Snapshot, entity: Entity) -> bool { entity.raw() % 2 == 1 }

    fn presence_requirement() -> PresenceReq { PresenceReq::Opaque }

    fn deps(_: &Snapshot, _: Entity) -> Vec<crate::query::Dep> { Vec::new() }

    fn access(_: &Snapshot, _: Entity) -> Vec<crate::query::Access> { Vec::new() }

    fn access_all() -> crate::query::Access { crate::query::Access::read_all::<Value>() }

    fn fetch<'a>(_: &crate::Bowl, _: &'a Snapshot, _: Entity,
        _: &mut crate::query::GuardStore) {}
}

#[test]
fn opaque_parts_are_not_replaced_by_presence_matching() {
    type Row = (Entity, OddRows, Option<&'static Flag>);
    let mut world = Snapshot::new();
    world.init_presence(vec![TypeId::of::<Flag>()]);
    let prepared = Row::prepare_rows(&world);
    let mut rows = Vec::new();
    prepared.extend(&world, (0..5).map(Entity::from_raw), &mut rows);
    assert_eq!(rows.into_iter().map(|id| id.raw()).collect::<Vec<_>>(), [1, 3]);
}
