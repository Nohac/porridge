use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::{Component, Entity};

/// Monotonic revision assigned to tracked component writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Revision(pub(crate) u64);

/// Stable id assigned to a registered system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[doc(hidden)]
pub struct SystemId(pub(crate) usize);

/// Identity of one memoized system invocation.
///
/// ```text
/// system id + query entity keys
/// ```
///
/// Derived outputs written by an invocation are owned by this value. On rerun,
/// the runner removes old outputs for the owner before applying new commands.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SystemInvocation {
    pub(crate) system: SystemId,
    pub(crate) keys: Vec<Entity>,
}

/// Whether a component was inserted by a caller or derived by a system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    Base,
    Derived,
}

/// Stored component value plus tracking metadata.
///
/// Values live behind `Arc` so snapshots can clone stores cheaply without
/// cloning user component data.
struct ComponentEntry<T> {
    value: Arc<T>,
    revision: Revision,
    fingerprint: Option<u64>,
    origin: Origin,
    owner: Option<SystemInvocation>,
}

impl<T> Clone for ComponentEntry<T> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            revision: self.revision,
            fingerprint: self.fingerprint,
            origin: self.origin,
            owner: self.owner.clone(),
        }
    }
}

/// Type-erased component store.
///
/// Each component type has its own concrete `Store<T>`, kept behind this trait
/// in the world's `TypeId` map.
trait StoreDyn: Send + Sync {
    fn clone_box(&self) -> Box<dyn StoreDyn>;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn remove_derived_owned(&mut self, owner: &SystemInvocation, revision: &mut Revision);
}

/// Concrete storage for one component type.
struct Store<T> {
    entries: BTreeMap<Entity, ComponentEntry<T>>,
}

impl<T> Clone for Store<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<T> Default for Store<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<T: Component> StoreDyn for Store<T> {
    fn clone_box(&self) -> Box<dyn StoreDyn> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn remove_derived_owned(&mut self, owner: &SystemInvocation, revision: &mut Revision) {
        let before = self.entries.len();
        self.entries.retain(|_, entry| {
            entry.origin != Origin::Derived || entry.owner.as_ref() != Some(owner)
        });

        if T::tracked() && self.entries.len() != before {
            bump(revision);
        }
    }
}

impl Clone for Box<dyn StoreDyn> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Clone)]
#[doc(hidden)]
/// Component storage for the live world and immutable snapshots.
///
/// `World` is currently public only because [`crate::QueryParam`] exposes it in
/// low-level trait methods. It should be treated as an implementation detail.
pub struct World {
    next_entity: u64,
    revision: Revision,
    stores: HashMap<TypeId, Box<dyn StoreDyn>>,
}

/// Immutable read source for one generation.
///
/// A snapshot is a structural clone of `World`; component payloads are shared by
/// `Arc`, and the snapshot is not mutated while systems read from it.
pub(crate) type Snapshot = World;

impl World {
    /// Creates an empty world.
    pub(crate) fn new() -> Self {
        Self {
            next_entity: 0,
            revision: Revision(0),
            stores: HashMap::new(),
        }
    }

    /// Allocates a fresh entity id in the live world.
    pub(crate) fn spawn_empty(&mut self) -> Entity {
        let entity = Entity(self.next_entity);
        self.next_entity += 1;
        entity
    }

    /// Inserts a base input component.
    pub(crate) fn insert_base<T: Component>(&mut self, entity: Entity, value: T) {
        self.insert(entity, value, Origin::Base, None);
    }

    /// Inserts a system-derived component owned by `owner`.
    pub(crate) fn insert_derived<T: Component>(
        &mut self,
        entity: Entity,
        value: T,
        owner: SystemInvocation,
    ) {
        self.insert(entity, value, Origin::Derived, Some(owner));
    }

    /// Inserts a component and updates revision metadata.
    ///
    /// If a component provides a fingerprint and the fingerprint is unchanged,
    /// the existing revision is reused. Otherwise tracked components bump the
    /// world's revision counter.
    fn insert<T: Component>(
        &mut self,
        entity: Entity,
        value: T,
        origin: Origin,
        owner: Option<SystemInvocation>,
    ) {
        let fingerprint = value.fingerprint();
        let revision = if T::tracked() {
            let old_revision = self
                .store::<T>()
                .and_then(|store| store.entries.get(&entity))
                .and_then(|entry| {
                    (entry.fingerprint.is_some() && entry.fingerprint == fingerprint)
                        .then_some(entry.revision)
                });

            match old_revision {
                Some(revision) => revision,
                None => {
                    bump(&mut self.revision);
                    self.revision
                }
            }
        } else {
            self.revision
        };

        self.store_mut::<T>().entries.insert(
            entity,
            ComponentEntry {
                value: Arc::new(value),
                revision,
                fingerprint,
                origin,
                owner,
            },
        );
    }

    /// Borrows a component from the world/snapshot.
    pub(crate) fn get<T: Component>(&self, entity: Entity) -> Option<&T> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .map(|entry| entry.value.as_ref())
    }

    /// Returns the tracked revision for a component on an entity.
    pub(crate) fn revision<T: Component>(&self, entity: Entity) -> Option<Revision> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .map(|entry| entry.revision)
    }

    /// Returns whether an entity has a component of type `T`.
    pub(crate) fn has<T: Component>(&self, entity: Entity) -> bool {
        self.store::<T>()
            .is_some_and(|store| store.entries.contains_key(&entity))
    }

    /// Removes every derived component currently owned by `owner`.
    ///
    /// This is what makes system outputs replaceable: rerunning the same
    /// invocation clears its previous facts before applying the new command
    /// buffer.
    pub(crate) fn remove_derived_owned(&mut self, owner: &SystemInvocation) {
        for store in self.stores.values_mut() {
            store.remove_derived_owned(owner, &mut self.revision);
        }
    }

    /// Upper bound used for simple entity scans.
    pub(crate) fn next_entity_raw(&self) -> u64 {
        self.next_entity
    }

    /// Returns the typed component store for `T`, if it exists.
    fn store<T: Component>(&self) -> Option<&Store<T>> {
        self.stores
            .get(&TypeId::of::<T>())
            .and_then(|store| store.as_any().downcast_ref())
    }

    /// Returns the typed component store for `T`, creating it if needed.
    fn store_mut<T: Component>(&mut self) -> &mut Store<T> {
        self.stores
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::<Store<T>>::default())
            .as_any_mut()
            .downcast_mut()
            .expect("component store has the wrong concrete type")
    }
}

/// Advances a revision counter.
fn bump(revision: &mut Revision) {
    revision.0 += 1;
}
