//! Row/store access reservations for one evaluator phase.
//! Candidate cost follows its declared accesses, not the number of running rows.
use crate::{
    Entity,
    query::{Access, AccessKind},
    world::SystemInvocation,
};
use std::{any::TypeId, collections::HashMap};

#[derive(Default)]
struct Counts {
    readers: usize,
    writers: usize,
}

impl Counts {
    fn any(&self) -> bool {
        self.readers != 0 || self.writers != 0
    }

    fn conflicts(&self, kind: AccessKind) -> bool {
        match kind {
            AccessKind::Read => self.writers != 0,
            AccessKind::Write => self.any(),
        }
    }

    fn add(&mut self, kind: AccessKind) {
        match kind {
            AccessKind::Read => self.readers += 1,
            AccessKind::Write => self.writers += 1,
        }
    }

    fn remove(&mut self, kind: AccessKind) {
        match kind {
            AccessKind::Read => self.readers -= 1,
            AccessKind::Write => self.writers -= 1,
        }
    }
}

#[derive(Default)]
struct ComponentAccess {
    total: Counts,
    whole_store: Counts,
    rows: HashMap<Entity, Counts>,
}

/// Ownership metadata permits exact release after completion or preemption.
/// Repeated accesses are counted, including overlapping row/store declarations.
/// The phase driver owns this index; workers never access it. These are scheduling
/// reservations, not component locks. Completed runs keep them until publication;
/// cancelled readers can release them when their work is discarded.
#[derive(Default)]
pub(crate) struct Reservations {
    owners: HashMap<SystemInvocation, Vec<Access>>,
    components: HashMap<TypeId, ComponentAccess>,
}

impl Reservations {
    pub(crate) fn conflicts(&self, accesses: &[Access]) -> bool {
        accesses.iter().any(|access| {
            let Some(component) = self.components.get(&access.component) else {
                return false;
            };
            match access.entity {
                None => component.total.conflicts(access.kind),
                Some(entity) => {
                    component.whole_store.conflicts(access.kind)
                        || component
                            .rows
                            .get(&entity)
                            .is_some_and(|row| row.conflicts(access.kind))
                }
            }
        })
    }

    pub(crate) fn insert(&mut self, owner: SystemInvocation, accesses: Vec<Access>) {
        assert!(
            !self.owners.contains_key(&owner),
            "invocation already reserved"
        );
        for access in &accesses {
            let component = self.components.entry(access.component).or_default();
            component.total.add(access.kind);
            let counts = match access.entity {
                Some(entity) => component.rows.entry(entity).or_default(),
                None => &mut component.whole_store,
            };
            counts.add(access.kind);
        }
        self.owners.insert(owner, accesses);
    }

    pub(crate) fn remove(&mut self, owner: &SystemInvocation) {
        let Some(accesses) = self.owners.remove(owner) else {
            return;
        };
        for access in accesses {
            let component = self
                .components
                .get_mut(&access.component)
                .expect("reserved component");
            component.total.remove(access.kind);
            match access.entity {
                None => component.whole_store.remove(access.kind),
                Some(entity) => {
                    let counts = component.rows.get_mut(&entity).expect("reserved row");
                    counts.remove(access.kind);
                    if !counts.any() {
                        component.rows.remove(&entity);
                    }
                }
            }
            if !component.total.any() {
                self.components.remove(&access.component);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Component, world::SystemId};

    struct A;

    impl Component for A {}

    struct B;

    impl Component for B {}

    fn owner(id: usize) -> SystemInvocation {
        SystemInvocation {
            system: SystemId(id % 3),
            keys: vec![Entity::from_raw(id as u64)],
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

                index.remove(&owner(0));
                for candidate in &candidates {
                    assert_eq!(index.conflicts(&[*candidate]), candidate.conflicts(*second));
                }
                index.remove(&owner(0));
                index.remove(&owner(1));
                assert!(index.components.is_empty());
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
        index.remove(&owner(0));
        assert!(index.conflicts(&[Access::write::<A>(row)]));
        assert!(!index.conflicts(&[Access::read::<A>(row)]));
        index.remove(&owner(1));
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
            index.remove(&owner);
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
            index.remove(owner);
        }
        assert!(index.owners.is_empty());
        assert!(index.components.is_empty());
        assert!(!index.conflicts(&candidates));
    }
}
