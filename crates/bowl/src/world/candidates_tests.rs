use super::*;

struct Key(u64);

impl Component for Key {
    fn fingerprint(&self) -> Option<u64> { Some(self.0) }
}

struct Missing;
impl Component for Missing {}

#[test]
fn borrowed_candidates_keep_snapshot_order_and_live_index_updates() {
    let mut world = World::new();
    let a = Entity::from_raw(1);
    let b = Entity::from_raw(2);
    world.insert_base(b, Key(7));
    world.insert_base(a, Key(7));
    let snapshot = world.clone();
    let mut candidates = snapshot.fingerprint_candidates_iter(TypeId::of::<Key>(), 7);
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates.next(), Some(a));
    assert_eq!(candidates.len(), 1);

    world.insert_base(a, Key(8));
    world.remove_component::<Key>(b, None);
    assert_eq!(candidates.collect::<Vec<_>>(), [b]);
    assert_eq!(world.fingerprint_candidates_iter(TypeId::of::<Key>(), 7).len(), 0);
    assert_eq!(world.fingerprint_candidates_iter(TypeId::of::<Key>(), 8).collect::<Vec<_>>(), [a]);
    assert_eq!(snapshot.fingerprint_candidates_iter(TypeId::of::<Key>(), 7).collect::<Vec<_>>(), [a, b]);
    assert_eq!(world.fingerprint_candidates_iter(TypeId::of::<Missing>(), 7).len(), 0);
}
