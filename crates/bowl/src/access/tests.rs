use super::*;
use crate::{Component, world::SystemId};

impl Reservations {
    fn insert(&mut self, owner: SystemInvocation, accesses: Vec<Access>) {
        assert!(!self.owners.contains_key(&owner));
        let accesses: Vec<_> = accesses.into_iter().map(|a| self.components.resolve(a)).collect();
        let writer = accesses.iter().any(|a| a.kind == AccessKind::Write);
        for access in &accesses { self.components.slots[access.component].add(access); }
        let id = self.records.insert(Record { owner: owner.clone(), accesses, writer });
        self.owners.insert(owner, id);
    }

    fn remove_owner(&mut self, owner: &SystemInvocation) {
        if let Some(&id) = self.owners.get(owner) { self.remove(id); }
    }

    fn conflicts(&mut self, accesses: &[Access]) -> bool {
        accesses.iter().any(|access| {
            let resolved = self.components.resolve(*access);
            self.components.slots[resolved.component].conflicts(&resolved)
        })
    }
}

struct A;

impl Component for A {}

struct B;

impl Component for B {}

fn owner(id: usize) -> SystemInvocation {
    SystemInvocation {
        system: SystemId(id % 3),
        keys: vec![Entity::from_raw(id as u64)].into(),
    }
}

fn candidates() -> Vec<Access> {
    let a = Entity::from_raw(1);
    let b = Entity::from_raw(2);
    vec![
        Access::read::<A>(a),
        Access::write::<A>(a),
        Access::read::<A>(b),
        Access::write::<A>(b),
        Access::read_all::<A>(),
        Access::write_all::<A>(),
        Access::read::<B>(a),
        Access::write::<B>(a),
        Access::read_all::<B>(),
        Access::write_all::<B>(),
    ]
}

#[test]
fn indexed_reservations_match_pairwise_conflicts_and_release() {
    let candidates = candidates();
    for first in &candidates {
        for second in &candidates {
            let mut index = Reservations::default();
            index.insert(owner(0), vec![*first, *first]);
            index.insert(owner(1), vec![*second]);
            for candidate in &candidates {
                assert_eq!(
                    index.conflicts(&[*candidate]),
                    candidate.conflicts(*first) || candidate.conflicts(*second)
                );
            }

            index.remove_owner(&owner(0));
            for candidate in &candidates {
                assert_eq!(index.conflicts(&[*candidate]), candidate.conflicts(*second));
            }
            index.remove_owner(&owner(0));
            index.remove_owner(&owner(1));
            assert!(index.components.slots.iter().all(|slot| !slot.total.any()));
            assert!(!index.conflicts(&candidates));
        }
    }
}

#[test]
fn multiple_readers_keep_writer_blocked_until_the_last_release() {
    let row = Entity::from_raw(7);
    let mut index = Reservations::default();
    index.insert(owner(0), vec![Access::read::<A>(row)]);
    index.insert(owner(1), vec![Access::read_all::<A>()]);
    index.remove_owner(&owner(0));
    assert!(index.conflicts(&[Access::write::<A>(row)]));
    assert!(!index.conflicts(&[Access::read::<A>(row)]));
    index.remove_owner(&owner(1));
    assert!(!index.conflicts(&[Access::write::<A>(row)]));
}

#[test]
fn mixed_reservation_lifecycles_match_pairwise_oracle() {
    fn next(seed: &mut u64, limit: usize) -> usize {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((*seed >> 32) as usize) % limit
    }

    let candidates = candidates();
    let mut seed = 17_u64;
    let mut index = Reservations::default();
    let mut reference = HashMap::<SystemInvocation, Vec<Access>>::new();
    for _ in 0..1000 {
        let owner = owner(next(&mut seed, 16));
        // Release/reuse identities, including absent owners and empty accesses.
        index.remove_owner(&owner);
        reference.remove(&owner);
        if next(&mut seed, 3) != 0 {
            let accesses = (0..next(&mut seed, 5))
                .map(|_| candidates[next(&mut seed, candidates.len())])
                .collect::<Vec<_>>();
            index.insert(owner.clone(), accesses.clone());
            reference.insert(owner, accesses);
        }

        for first in &candidates {
            for second in &candidates {
                let requested = [*first, *second];
                let expected = reference.values().any(|active| {
                    requested.iter().any(|candidate| {
                        active.iter().any(|access| candidate.conflicts(*access))
                    })
                });
                assert_eq!(index.conflicts(&requested), expected);
            }
        }
        assert!(!index.conflicts(&[]));
    }

    for owner in reference.keys() {
        index.remove_owner(owner);
    }
    assert!(index.owners.is_empty());
    assert!(index.components.slots.iter().all(|slot| !slot.total.any()));
    assert!(!index.conflicts(&candidates));
}

#[test]
fn admission_reuse_rejects_stale_ids_and_conflicts_without_leaking_access() {
    let row = Entity::from_raw(1);
    let mut index = Reservations::new(Arc::new(HashMap::from([(TypeId::of::<A>(), 0)])));
    let first = index.admit(owner(1), vec![Access::read::<A>(row)]).unwrap();
    assert!(matches!(index.admit(owner(1), vec![]), Err(Rejected::Duplicate)));
    assert!(matches!(index.admit(owner(2), vec![Access::write::<A>(row), Access::write::<B>(row)]), Err(Rejected::Conflict(_))));
    // Rejection did not reserve the undeclared fallback component.
    let other = index.admit(owner(3), vec![Access::write::<B>(row)]).unwrap();
    index.remove(first).unwrap();
    let reused = index.admit(owner(2), vec![Access::write::<A>(row)]).unwrap();
    assert_eq!(first.slot, reused.slot);
    assert_ne!(first, reused);
    assert!(index.remove(first).is_none());
    assert!(index.contains(reused));
    assert!(matches!(index.admit(owner(4), vec![Access::read::<A>(row)]), Err(Rejected::Conflict(_))));
    index.remove(reused).unwrap();
    index.remove(other).unwrap();
    for _ in 0..1000 {
        let id = index.admit(owner(1), vec![Access::write_all::<A>()]).unwrap();
        index.remove(id).unwrap();
    }
    assert_eq!(index.records.slots.len(), 2);
    assert!(index.owners.is_empty());
}

#[test]
fn cancelling_readers_keeps_writer_reservations_and_generation_does_not_wrap() {
    let row = Entity::from_raw(2);
    let mut index = Reservations::default();
    let reader = index.admit(owner(0), vec![Access::read::<A>(row)]).unwrap();
    let writer = index.admit(owner(1), vec![Access::write::<B>(row)]).unwrap();
    index.cancel_readers();
    assert!(!index.contains(reader));
    assert!(index.contains(writer));
    assert!(matches!(index.admit(owner(2), vec![Access::read::<B>(row)]), Err(Rejected::Conflict(_))));
    index.remove(writer).unwrap();

    let mut records = Records::default();
    let id = records.insert(Record { owner: owner(0), accesses: vec![], writer: false });
    records.slots[id.slot as usize].generation = u32::MAX;
    let last = RunId { slot: id.slot, generation: u32::MAX };
    records.remove(last).unwrap();
    let next = records.insert(Record { owner: owner(1), accesses: vec![], writer: false });
    assert_ne!(next.slot, last.slot);
    assert!(records.get(last).is_none());
}
