use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use crate::{
    Component, Entity,
    component::{ComponentHookContext, DerivedFrom},
};

/// Monotonic revision assigned to tracked component writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[doc(hidden)]
pub struct SystemInvocation {
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
    fn revision_for_entity(&self, entity: Entity) -> Option<Revision>;
    fn remove_derived_owned(&mut self, owner: &SystemInvocation, revision: &mut Revision);
    fn has_derived_owned(&self, owner: &SystemInvocation) -> bool;
    fn remove_derived_touched_by(
        &mut self,
        keys: &HashSet<Entity>,
        revision: &mut Revision,
    ) -> Vec<Entity>;
    fn remove_entity(&mut self, entity: Entity, revision: &mut Revision) -> Vec<SystemInvocation>;
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

    fn revision_for_entity(&self, entity: Entity) -> Option<Revision> {
        self.entries.get(&entity).map(|entry| entry.revision)
    }

    fn remove_derived_owned(&mut self, owner: &SystemInvocation, revision: &mut Revision) {
        let before = self.entries.len();
        self.entries.retain(|entity, entry| {
            let remove = entry.origin == Origin::Derived && entry.owner.as_ref() == Some(owner);

            if remove {
                T::on_remove(ComponentHookContext::new(*entity));
            }

            !remove
        });

        if T::tracked() && self.entries.len() != before {
            bump(revision);
        }
    }

    fn has_derived_owned(&self, owner: &SystemInvocation) -> bool {
        self.entries
            .values()
            .any(|entry| entry.origin == Origin::Derived && entry.owner.as_ref() == Some(owner))
    }

    fn remove_derived_touched_by(
        &mut self,
        keys: &HashSet<Entity>,
        revision: &mut Revision,
    ) -> Vec<Entity> {
        let mut removed = Vec::new();

        self.entries.retain(|entity, entry| {
            let remove = entry.origin == Origin::Derived
                && entry
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.keys.iter().any(|key| keys.contains(key)));

            if remove {
                removed.push(*entity);
                T::on_remove(ComponentHookContext::new(*entity));
            }

            !remove
        });

        if T::tracked() && !removed.is_empty() {
            bump(revision);
        }

        removed
    }

    fn remove_entity(&mut self, entity: Entity, revision: &mut Revision) -> Vec<SystemInvocation> {
        let Some(removed) = self.entries.remove(&entity) else {
            return Vec::new();
        };

        let context = ComponentHookContext::new(entity);
        T::on_entity_remove(context);
        T::on_remove(context);

        if T::tracked() {
            bump(revision);
        }

        removed.owner.into_iter().collect()
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
    singleton_entities: HashMap<TypeId, Entity>,
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
            singleton_entities: HashMap::new(),
        }
    }

    /// Allocates a fresh entity id in the live world.
    pub(crate) fn spawn_empty(&mut self) -> Entity {
        let entity = Entity(self.next_entity);
        self.next_entity += 1;
        entity
    }

    /// Returns the entity for a singleton key, allocating one if needed.
    pub(crate) fn singleton_entity_or_spawn(&mut self, key: TypeId) -> Entity {
        if let Some(entity) = self.singleton_entities.get(&key) {
            return *entity;
        }

        let entity = self.spawn_empty();
        self.singleton_entities.insert(key, entity);
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
        mut value: T,
        origin: Origin,
        owner: Option<SystemInvocation>,
    ) {
        if let Some(derived_from) = (&mut value as &mut dyn Any).downcast_mut::<DerivedFrom>() {
            derived_from.capture(|entity| self.entity_revision(entity));
        }

        if let Some(key) = T::singleton_key() {
            match self.singleton_entities.get(&key) {
                Some(existing) if *existing != entity => {
                    panic!(
                        "singleton component {} is already registered on entity {}",
                        std::any::type_name::<T>(),
                        existing.raw(),
                    );
                }
                Some(_) => {}
                None => {
                    self.singleton_entities.insert(key, entity);
                }
            }
        }

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

        T::on_insert(ComponentHookContext::new(entity));
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

    /// Returns the current entity revision.
    ///
    /// Entity revisions are computed as the newest component revision currently
    /// attached to the entity. This keeps the storage model simple while giving
    /// revision-scoped relations a stable "owner changed" signal.
    pub(crate) fn entity_revision(&self, entity: Entity) -> Option<Revision> {
        self.stores
            .values()
            .filter_map(|store| store.revision_for_entity(entity))
            .max()
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

    /// Returns whether any derived component is currently owned by `owner`.
    pub(crate) fn has_derived_owned(&self, owner: &SystemInvocation) -> bool {
        self.stores
            .values()
            .any(|store| store.has_derived_owned(owner))
    }

    /// Removes derived components whose owner key set intersects `keys`.
    ///
    /// The returned entities form the next cleanup frontier: a derived entity
    /// touched by a bound request may itself own more derived outputs.
    pub(crate) fn remove_derived_touched_by(&mut self, keys: &HashSet<Entity>) -> Vec<Entity> {
        self.stores
            .values_mut()
            .flat_map(|store| store.remove_derived_touched_by(keys, &mut self.revision))
            .collect()
    }

    /// Removes every component attached to `entity`.
    ///
    /// If removed components were themselves derived, their owners are returned
    /// so the caller can clear any remaining outputs for those invocations.
    pub(crate) fn remove_entity(&mut self, entity: Entity) -> Vec<SystemInvocation> {
        let mut owners = Vec::new();

        for store in self.stores.values_mut() {
            owners.extend(store.remove_entity(entity, &mut self.revision));
        }

        self.singleton_entities
            .retain(|_, singleton_entity| *singleton_entity != entity);

        owners
    }

    /// Removes one typed component and returns the stored shared value.
    ///
    /// Component payloads are stored behind `Arc` so immutable snapshots can
    /// keep reading old generations. Removing from the live world therefore
    /// transfers the live world's `Arc<T>` handle rather than cloning or moving
    /// `T` itself.
    pub(crate) fn remove_component<T>(&mut self, entity: Entity) -> Option<Arc<T>>
    where
        T: Component,
    {
        let removed = self.store_mut_existing::<T>()?.entries.remove(&entity)?;

        T::on_remove(ComponentHookContext::new(entity));

        if T::tracked() {
            bump(&mut self.revision);
        }

        if let Some(key) = T::singleton_key() {
            if self
                .singleton_entities
                .get(&key)
                .is_some_and(|singleton_entity| *singleton_entity == entity)
            {
                self.singleton_entities.remove(&key);
            }
        }

        Some(removed.value)
    }

    /// Mutates one component in the live world and updates revision metadata.
    ///
    /// If `T` provides a fingerprint and the fingerprint is unchanged after the
    /// closure runs, the component keeps its existing revision. Components
    /// without fingerprints conservatively bump on every mutable access.
    pub(crate) fn update_component<T, F, R>(&mut self, entity: Entity, f: F) -> Option<bool>
    where
        T: Component + Clone,
        F: FnOnce(&mut T) -> R,
    {
        let next_revision = Revision(self.revision.0 + 1);
        let changed = {
            let store = self.store_mut_existing::<T>()?;
            let entry = store.entries.get_mut(&entity)?;
            let before_fingerprint = entry.value.fingerprint();

            f(Arc::make_mut(&mut entry.value));

            let after_fingerprint = entry.value.fingerprint();
            entry.fingerprint = after_fingerprint;

            let changed = T::tracked()
                && (before_fingerprint.is_none() || before_fingerprint != after_fingerprint);

            if changed {
                entry.revision = next_revision;
            }

            changed
        };

        if changed {
            self.revision = next_revision;
        }

        Some(changed)
    }

    /// Upper bound used for simple entity scans.
    pub(crate) fn next_entity_raw(&self) -> u64 {
        self.next_entity
    }

    /// Current global revision.
    pub(crate) fn revision_raw(&self) -> u64 {
        self.revision.0
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

    /// Returns the typed component store for `T`, if it already exists.
    fn store_mut_existing<T: Component>(&mut self) -> Option<&mut Store<T>> {
        self.stores
            .get_mut(&TypeId::of::<T>())
            .and_then(|store| store.as_any_mut().downcast_mut())
    }
}

/// Advances a revision counter.
fn bump(revision: &mut Revision) {
    revision.0 += 1;
}
