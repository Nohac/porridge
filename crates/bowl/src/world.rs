use std::{
    any::{Any, TypeId},
    cell::UnsafeCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::{Deref, DerefMut},
    sync::{Arc, Condvar, Mutex},
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
/// Values live in a shared lock cell so snapshots can clone stores cheaply
/// without cloning user component data, while live mutation can wait for active
/// read guards instead of requiring unique `Arc<T>` ownership.
struct ComponentEntry<T> {
    value: Arc<ComponentCell<T>>,
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

struct ComponentCell<T> {
    state: Mutex<ComponentLockState>,
    available: Condvar,
    value: UnsafeCell<T>,
}

#[derive(Default)]
struct ComponentLockState {
    readers: usize,
    writer: bool,
}

impl<T> ComponentCell<T> {
    fn new(value: T) -> Self {
        Self {
            state: Mutex::new(ComponentLockState::default()),
            available: Condvar::new(),
            value: UnsafeCell::new(value),
        }
    }

    fn read(self: &Arc<Self>) -> ComponentReadGuard<T> {
        let mut state = self.state.lock().expect("component lock poisoned");
        while state.writer {
            state = self.available.wait(state).expect("component lock poisoned");
        }
        state.readers += 1;
        drop(state);

        ComponentReadGuard {
            cell: Arc::clone(self),
        }
    }

    fn write(self: &Arc<Self>) -> ComponentWriteGuard<T> {
        let mut state = self.state.lock().expect("component lock poisoned");
        while state.writer || state.readers != 0 {
            state = self.available.wait(state).expect("component lock poisoned");
        }
        state.writer = true;
        drop(state);

        ComponentWriteGuard {
            cell: Arc::clone(self),
        }
    }

    /// Acquires the write guard only if the cell is currently uncontended.
    ///
    /// Callers holding the bowl state lock must use this instead of `write`
    /// so they never block on a cell while other tasks may need the state
    /// lock to release their guards.
    fn try_write(self: &Arc<Self>) -> Option<ComponentWriteGuard<T>> {
        let mut state = self.state.lock().expect("component lock poisoned");
        if state.writer || state.readers != 0 {
            return None;
        }
        state.writer = true;
        drop(state);

        Some(ComponentWriteGuard {
            cell: Arc::clone(self),
        })
    }

    fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

// SAFETY: ComponentCell only exposes `T` through read/write guards. The lock
// state ensures many readers or one writer, and `Component: Send + Sync`.
unsafe impl<T: Send> Send for ComponentCell<T> {}
// SAFETY: shared access to `T` is guarded by the lock state. Readers require
// `T: Sync`, writers require unique access, and `Component: Send + Sync`.
unsafe impl<T: Send + Sync> Sync for ComponentCell<T> {}

struct ComponentReadGuard<T> {
    cell: Arc<ComponentCell<T>>,
}

impl<T> Deref for ComponentReadGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: the guard increments the reader count while no writer is
        // active. Writers wait for the reader count to reach zero before
        // obtaining `&mut T`.
        unsafe { &*self.cell.value.get() }
    }
}

impl<T> Drop for ComponentReadGuard<T> {
    fn drop(&mut self) {
        let mut state = self.cell.state.lock().expect("component lock poisoned");
        state.readers -= 1;
        if state.readers == 0 {
            self.cell.available.notify_all();
        }
    }
}

// SAFETY: the guard owns an `Arc` to the cell and unlocks by updating shared
// lock state on drop, so it does not rely on thread-affine OS guard semantics.
unsafe impl<T: Send + Sync> Send for ComponentReadGuard<T> {}
unsafe impl<T: Sync> Sync for ComponentReadGuard<T> {}

struct ComponentWriteGuard<T> {
    cell: Arc<ComponentCell<T>>,
}

impl<T> Deref for ComponentWriteGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: the guard owns the exclusive writer slot, so no mutable alias
        // or active writer can exist. Shared references are allowed from `&mut`.
        unsafe { &*self.cell.value.get() }
    }
}

impl<T> DerefMut for ComponentWriteGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: the guard owns the exclusive writer slot and all readers are
        // excluded until it drops.
        unsafe { &mut *self.cell.value.get() }
    }
}

impl<T> Drop for ComponentWriteGuard<T> {
    fn drop(&mut self) {
        let mut state = self.cell.state.lock().expect("component lock poisoned");
        state.writer = false;
        self.cell.available.notify_all();
    }
}

// SAFETY: like ComponentReadGuard, this unlocks through shared lock state on
// drop rather than through thread-affine OS guard ownership.
unsafe impl<T: Send + Sync> Send for ComponentWriteGuard<T> {}
unsafe impl<T: Sync> Sync for ComponentWriteGuard<T> {}

/// Read guard returned by query fetches.
///
/// This is the guarded replacement for borrowing directly from a shared
/// immutable payload. It dereferences to `T`, so most query code can keep using
/// field access like `component.field`.
#[doc(hidden)]
pub struct ComponentRef<'a, T> {
    guard: ComponentReadGuard<T>,
    _marker: std::marker::PhantomData<&'a T>,
}

impl<T> Deref for ComponentRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

/// Write guard returned by system `Mut<T>` fetches.
///
/// Exclusivity is guaranteed twice over: the cell's writer slot, and the
/// planner's access scheduling which never runs conflicting invocations
/// concurrently.
#[doc(hidden)]
pub struct ComponentMut<'a, T> {
    guard: ComponentWriteGuard<T>,
    _marker: std::marker::PhantomData<&'a mut T>,
}

impl<T> Deref for ComponentMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> DerefMut for ComponentMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
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
    /// Removes one derived entry if it is still owned by `owner`.
    ///
    /// Returns whether the component type is tracked.
    fn remove_entry_owned(&mut self, entity: Entity, owner: &SystemInvocation) -> Option<bool>;
    /// Recomputes one entry's fingerprint after an in-place `Mut` write and
    /// bumps revisions if the tracked value changed.
    fn reconcile_entry(&mut self, entity: Entity, revision: &mut Revision);
    /// Removes one entry as part of whole-entity removal.
    ///
    /// The outer `Option` reports whether an entry existed; the inner value is
    /// the derived owner, if any.
    fn remove_entity(
        &mut self,
        entity: Entity,
        revision: &mut Revision,
    ) -> Option<Option<SystemInvocation>>;
}

/// Concrete storage for one component type.
struct Store<T> {
    entries: BTreeMap<Entity, ComponentEntry<T>>,
    /// Fingerprint → entities index for equality lookups.
    ///
    /// Only entries with a fingerprint participate. The map is shared with
    /// snapshots and copied on the first live write after a clone, so
    /// snapshot clones stay cheap while external `Where<Eq<T>>` queries can
    /// resolve candidates without scanning.
    by_fingerprint: Arc<HashMap<u64, BTreeSet<Entity>>>,
}

impl<T> Store<T> {
    fn index_fingerprint(&mut self, entity: Entity, fingerprint: Option<u64>) {
        let Some(fingerprint) = fingerprint else {
            return;
        };

        Arc::make_mut(&mut self.by_fingerprint)
            .entry(fingerprint)
            .or_default()
            .insert(entity);
    }

    fn unindex_fingerprint(&mut self, entity: Entity, fingerprint: Option<u64>) {
        let Some(fingerprint) = fingerprint else {
            return;
        };

        let index = Arc::make_mut(&mut self.by_fingerprint);
        if let Some(entities) = index.get_mut(&fingerprint) {
            entities.remove(&entity);
            if entities.is_empty() {
                index.remove(&fingerprint);
            }
        }
    }
}

impl<T> Clone for Store<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            by_fingerprint: Arc::clone(&self.by_fingerprint),
        }
    }
}

impl<T> Default for Store<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            by_fingerprint: Arc::new(HashMap::new()),
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

    fn reconcile_entry(&mut self, entity: Entity, revision: &mut Revision) {
        let (before, after, changed) = {
            let Some(entry) = self.entries.get_mut(&entity) else {
                return;
            };

            let before = entry.fingerprint;
            let after = entry.value.read().fingerprint();
            entry.fingerprint = after;

            let changed = T::tracked() && (before.is_none() || before != after);
            if changed {
                bump(revision);
                entry.revision = *revision;
            }

            (before, after, changed)
        };

        let _ = changed;
        if before != after {
            self.unindex_fingerprint(entity, before);
            self.index_fingerprint(entity, after);
        }
    }

    fn remove_entry_owned(&mut self, entity: Entity, owner: &SystemInvocation) -> Option<bool> {
        let entry = self.entries.get(&entity)?;
        if entry.origin != Origin::Derived || entry.owner.as_ref() != Some(owner) {
            return None;
        }

        let removed = self.entries.remove(&entity)?;
        self.unindex_fingerprint(entity, removed.fingerprint);
        T::on_remove(ComponentHookContext::new(entity));

        Some(T::tracked())
    }

    fn remove_entity(
        &mut self,
        entity: Entity,
        revision: &mut Revision,
    ) -> Option<Option<SystemInvocation>> {
        let removed = self.entries.remove(&entity)?;
        self.unindex_fingerprint(entity, removed.fingerprint);

        let context = ComponentHookContext::new(entity);
        T::on_entity_remove(context);
        T::on_remove(context);

        if T::tracked() {
            bump(revision);
        }

        Some(removed.owner)
    }
}

impl Clone for Box<dyn StoreDyn> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[doc(hidden)]
/// Component storage for the live world and structural snapshots.
///
/// `World` is currently public only because [`crate::QueryParam`] exposes it in
/// low-level trait methods. It should be treated as an implementation detail.
pub struct World {
    next_entity: u64,
    revision: Revision,
    /// Bumped on every structural change (entry added or removed, or a value
    /// replaced without a matching fingerprint), including untracked
    /// components. Unlike `revision`, this signals "the world is different"
    /// without implying dependency invalidation.
    mutations: u64,
    stores: HashMap<TypeId, Box<dyn StoreDyn>>,
    singleton_entities: HashMap<TypeId, Entity>,
    /// Derived outputs currently owned by each system invocation.
    ///
    /// Keeps output replacement and ownership checks proportional to an
    /// invocation's own outputs instead of every stored component entry.
    derived_owners: HashMap<SystemInvocation, HashSet<(TypeId, Entity)>>,
    /// Entities spawned by each invocation, in spawn order.
    ///
    /// Reruns reuse these ids slot by slot so idempotent spawn output keeps
    /// its entity identity (and its component revisions) instead of growing
    /// the id space on every rerun.
    derived_spawns: HashMap<SystemInvocation, Vec<Entity>>,
    /// Per-owner spawn cursor for the commit currently being applied.
    spawn_cursors: HashMap<SystemInvocation, usize>,
    /// Entities removed since the runner last drained this list.
    ///
    /// The commit path uses it to purge memo entries keyed by entities that
    /// no longer exist.
    removed_entities: Vec<Entity>,
}

impl Clone for World {
    /// Structural clone used for snapshots.
    ///
    /// The derived-owner index is intentionally left empty: it is runner
    /// bookkeeping for replacing outputs in the live world, and cloning it per
    /// snapshot would make every planning wave pay for the whole ownership
    /// table. Ownership checks against a snapshot must go through the live
    /// bowl instead.
    fn clone(&self) -> Self {
        Self {
            next_entity: self.next_entity,
            revision: self.revision,
            mutations: self.mutations,
            stores: self.stores.clone(),
            singleton_entities: self.singleton_entities.clone(),
            derived_owners: HashMap::new(),
            derived_spawns: HashMap::new(),
            spawn_cursors: HashMap::new(),
            removed_entities: Vec::new(),
        }
    }
}

/// Structural read source for one generation.
///
/// A snapshot is a structural clone of `World`; component cells are shared and
/// reads are protected by component read guards.
pub(crate) type Snapshot = World;

/// Outcome of a non-blocking component mutation attempt.
pub(crate) enum TryUpdate<R, F> {
    /// The mutation ran; `changed` reflects fingerprint-based tracking.
    Applied { changed: bool, result: R },
    /// The component or entity does not exist.
    Missing,
    /// The cell is currently held; the closure is handed back for retry.
    Busy(F),
}

impl World {
    /// Creates an empty world.
    pub(crate) fn new() -> Self {
        Self {
            next_entity: 0,
            revision: Revision(0),
            mutations: 0,
            stores: HashMap::new(),
            singleton_entities: HashMap::new(),
            derived_owners: HashMap::new(),
            derived_spawns: HashMap::new(),
            spawn_cursors: HashMap::new(),
            removed_entities: Vec::new(),
        }
    }

    /// Drains the entities removed since the last drain.
    pub(crate) fn take_removed_entities(&mut self) -> Vec<Entity> {
        std::mem::take(&mut self.removed_entities)
    }

    /// Returns the entity for an invocation's next spawn slot.
    ///
    /// Slots are matched by spawn order within one commit; a rerun that spawns
    /// the same outputs reuses the same entity ids.
    pub(crate) fn spawn_derived(&mut self, owner: &SystemInvocation) -> Entity {
        let cursor = self.spawn_cursors.entry(owner.clone()).or_default();
        let slot = *cursor;
        *cursor += 1;

        let spawns = self.derived_spawns.entry(owner.clone()).or_default();
        if let Some(entity) = spawns.get(slot) {
            return *entity;
        }

        let entity = Entity(self.next_entity);
        self.next_entity += 1;
        self.derived_spawns
            .get_mut(owner)
            .expect("spawn list inserted above")
            .push(entity);
        entity
    }

    /// Ends the spawn phase of one commit, dropping unused trailing slots.
    pub(crate) fn finish_derived_spawns(&mut self, owner: &SystemInvocation) {
        let cursor = self.spawn_cursors.remove(owner).unwrap_or(0);
        if let Some(spawns) = self.derived_spawns.get_mut(owner) {
            spawns.truncate(cursor);
            if spawns.is_empty() {
                self.derived_spawns.remove(owner);
            }
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
        let previous_fingerprint = self
            .store::<T>()
            .and_then(|store| store.entries.get(&entity))
            .map(|entry| entry.fingerprint);
        let fingerprint_matches = previous_fingerprint
            .is_some_and(|previous| previous.is_some() && previous == fingerprint);

        let revision = if T::tracked() {
            let old_revision = self
                .store::<T>()
                .and_then(|store| store.entries.get(&entity))
                .and_then(|entry| fingerprint_matches.then_some(entry.revision));

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

        if !fingerprint_matches {
            self.mutations += 1;
        }

        let new_owner = owner.clone();
        let store = self.store_mut::<T>();
        let previous = store.entries.insert(
            entity,
            ComponentEntry {
                value: Arc::new(ComponentCell::new(value)),
                revision,
                fingerprint,
                origin,
                owner,
            },
        );

        if let Some(previous_entry) = &previous {
            if previous_entry.fingerprint != fingerprint {
                store.unindex_fingerprint(entity, previous_entry.fingerprint);
                store.index_fingerprint(entity, fingerprint);
            }
        } else {
            store.index_fingerprint(entity, fingerprint);
        }

        let type_id = TypeId::of::<T>();
        if let Some(previous_owner) = previous.and_then(|entry| entry.owner) {
            if Some(&previous_owner) != new_owner.as_ref() {
                self.unindex_derived(&previous_owner, type_id, entity);
            }
        }
        if origin == Origin::Derived {
            if let Some(new_owner) = new_owner {
                self.derived_owners
                    .entry(new_owner)
                    .or_default()
                    .insert((type_id, entity));
            }
        }

        T::on_insert(ComponentHookContext::new(entity));
    }

    /// Drops one output from an invocation's ownership index.
    fn unindex_derived(&mut self, owner: &SystemInvocation, type_id: TypeId, entity: Entity) {
        let Some(outputs) = self.derived_owners.get_mut(owner) else {
            return;
        };

        outputs.remove(&(type_id, entity));
        if outputs.is_empty() {
            self.derived_owners.remove(owner);
        }
    }

    /// Borrows a component from the world/snapshot.
    pub(crate) fn get<T: Component>(&self, entity: Entity) -> Option<ComponentRef<'_, T>> {
        let guard = self.store::<T>()?.entries.get(&entity)?.value.read();

        Some(ComponentRef {
            guard,
            _marker: std::marker::PhantomData,
        })
    }

    /// Exclusively borrows a component for a system `Mut<T>` row.
    ///
    /// Waits for outstanding read guards (external result holders) to
    /// release; the caller must not hold the bowl state lock. Revision and
    /// fingerprint bookkeeping happens later, at commit, via
    /// [`World::reconcile_written`].
    pub(crate) fn get_mut<T: Component>(&self, entity: Entity) -> Option<ComponentMut<'_, T>> {
        let guard = self.store::<T>()?.entries.get(&entity)?.value.write();

        Some(ComponentMut {
            guard,
            _marker: std::marker::PhantomData,
        })
    }

    /// Reconciles rows mutated in place by a finished invocation.
    ///
    /// Recomputes each written row's fingerprint; tracked value changes bump
    /// the row and world revisions, and the fingerprint index is updated.
    pub(crate) fn reconcile_written(&mut self, writes: &[(TypeId, Entity)]) {
        for (type_id, entity) in writes {
            let Some(store) = self.stores.get_mut(type_id) else {
                continue;
            };

            store.reconcile_entry(*entity, &mut self.revision);
        }
    }

    /// Returns the tracked revision for a component on an entity.
    pub(crate) fn revision<T: Component>(&self, entity: Entity) -> Option<Revision> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .map(|entry| entry.revision)
    }

    /// Returns the tracked revision for a component type id on an entity.
    pub(crate) fn revision_by_type(&self, type_id: TypeId, entity: Entity) -> Option<Revision> {
        self.stores
            .get(&type_id)
            .and_then(|store| store.revision_for_entity(entity))
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
        self.derived_spawns.remove(owner);
        let Some(outputs) = self.derived_owners.remove(owner) else {
            return;
        };

        for (type_id, entity) in outputs {
            self.remove_derived_entry(type_id, entity, owner);
        }
    }

    /// Takes the outputs currently owned by `owner`, clearing its index entry.
    ///
    /// Commands applied afterwards rebuild the index with exactly the pairs
    /// the rerun re-emits, which is what makes the stale diff correct for
    /// spawned outputs that move to different entities.
    pub(crate) fn take_derived_outputs(
        &mut self,
        owner: &SystemInvocation,
    ) -> HashSet<(TypeId, Entity)> {
        self.derived_owners.remove(owner).unwrap_or_default()
    }

    /// Removes outputs in `previous` that `owner` no longer owns.
    ///
    /// This is the second half of output replacement by diffing: commands were
    /// applied over the invocation's old outputs (so unchanged fingerprints
    /// kept their revisions), and whatever the rerun did not re-emit is
    /// removed here.
    pub(crate) fn remove_derived_stale(
        &mut self,
        owner: &SystemInvocation,
        previous: HashSet<(TypeId, Entity)>,
    ) {
        for (type_id, entity) in previous {
            let still_owned = self
                .derived_owners
                .get(owner)
                .is_some_and(|outputs| outputs.contains(&(type_id, entity)));

            if !still_owned {
                self.remove_derived_entry(type_id, entity, owner);
            }
        }
    }

    /// Removes one derived store entry if `owner` still owns it.
    fn remove_derived_entry(
        &mut self,
        type_id: TypeId,
        entity: Entity,
        owner: &SystemInvocation,
    ) -> bool {
        let Some(store) = self.stores.get_mut(&type_id) else {
            return false;
        };

        let Some(tracked) = store.remove_entry_owned(entity, owner) else {
            return false;
        };

        self.mutations += 1;
        if tracked {
            bump(&mut self.revision);
        }

        true
    }

    /// Returns whether any derived component is currently owned by `owner`.
    pub(crate) fn has_derived_owned(&self, owner: &SystemInvocation) -> bool {
        self.derived_owners
            .get(owner)
            .is_some_and(|outputs| !outputs.is_empty())
    }

    /// Removes derived components whose owner key set intersects `keys`.
    ///
    /// The returned entities form the next cleanup frontier: a derived entity
    /// touched by a bound request may itself own more derived outputs.
    pub(crate) fn remove_derived_touched_by(&mut self, keys: &HashSet<Entity>) -> Vec<Entity> {
        let owners = self
            .derived_owners
            .keys()
            .filter(|owner| owner.keys.iter().any(|key| keys.contains(key)))
            .cloned()
            .collect::<Vec<_>>();

        let mut removed = Vec::new();
        for owner in owners {
            self.derived_spawns.remove(&owner);
            let Some(outputs) = self.derived_owners.remove(&owner) else {
                continue;
            };

            for (type_id, entity) in outputs {
                if self.remove_derived_entry(type_id, entity, &owner) {
                    removed.push(entity);
                }
            }
        }

        removed
    }

    /// Removes every component attached to `entity`.
    ///
    /// If removed components were themselves derived, their owners are returned
    /// so the caller can clear any remaining outputs for those invocations.
    pub(crate) fn remove_entity(&mut self, entity: Entity) -> Vec<SystemInvocation> {
        let mut owners = Vec::new();
        let mut unindex = Vec::new();

        for (type_id, store) in self.stores.iter_mut() {
            let Some(owner) = store.remove_entity(entity, &mut self.revision) else {
                continue;
            };

            self.mutations += 1;
            if let Some(owner) = owner {
                unindex.push((owner.clone(), *type_id));
                owners.push(owner);
            }
        }

        for (owner, type_id) in unindex {
            self.unindex_derived(&owner, type_id, entity);
        }

        self.singleton_entities
            .retain(|_, singleton_entity| *singleton_entity != entity);
        self.removed_entities.push(entity);

        owners
    }

    /// Removes one typed component and returns the stored value behind `Arc`.
    ///
    /// Taking still avoids `T: Clone`, but it now requires that no structural
    /// snapshot keeps the removed component cell alive. Normal bound request
    /// lifetimes satisfy that; a caller holding an old query result for the same
    /// component can make this return `None`.
    pub(crate) fn remove_component<T>(&mut self, entity: Entity) -> Option<Arc<T>>
    where
        T: Component,
    {
        let store = self.store_mut_existing::<T>()?;
        let removed = store.entries.remove(&entity)?;
        store.unindex_fingerprint(entity, removed.fingerprint);

        T::on_remove(ComponentHookContext::new(entity));

        self.mutations += 1;
        if T::tracked() {
            bump(&mut self.revision);
        }

        if let Some(owner) = &removed.owner {
            let owner = owner.clone();
            self.unindex_derived(&owner, TypeId::of::<T>(), entity);
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

        let value = Arc::try_unwrap(removed.value).ok()?.into_inner();

        Some(Arc::new(value))
    }

    /// Mutates one component in the live world and updates revision metadata.
    ///
    /// If `T` provides a fingerprint and the fingerprint is unchanged after the
    /// closure runs, the component keeps its existing revision. Components
    /// without fingerprints conservatively bump on every mutable access.
    pub(crate) fn update_component<T, F, R>(&mut self, entity: Entity, f: F) -> Option<(bool, R)>
    where
        T: Component + Clone,
        F: FnOnce(&mut T) -> R,
    {
        self.update_component_live(entity, f)
    }

    /// Attempts to mutate one component without waiting on its cell.
    ///
    /// Returns `Busy(f)` when the cell is held by a reader or writer, handing
    /// the closure back so the caller can retry after yielding — without ever
    /// blocking while it holds the bowl state lock.
    pub(crate) fn try_update_component_live<T, F, R>(
        &mut self,
        entity: Entity,
        f: F,
    ) -> TryUpdate<R, F>
    where
        T: Component,
        F: FnOnce(&mut T) -> R,
    {
        let next_revision = Revision(self.revision.0 + 1);
        let (changed, result, before_fingerprint, after_fingerprint) = {
            let Some(store) = self.store_mut_existing::<T>() else {
                return TryUpdate::Missing;
            };
            let Some(entry) = store.entries.get_mut(&entity) else {
                return TryUpdate::Missing;
            };
            let before_fingerprint = entry.fingerprint;

            let Some(mut value) = entry.value.try_write() else {
                return TryUpdate::Busy(f);
            };
            let result = f(&mut value);

            let after_fingerprint = value.fingerprint();
            entry.fingerprint = after_fingerprint;

            let changed = T::tracked()
                && (before_fingerprint.is_none() || before_fingerprint != after_fingerprint);

            if changed {
                entry.revision = next_revision;
            }

            (changed, result, before_fingerprint, after_fingerprint)
        };

        if before_fingerprint != after_fingerprint {
            if let Some(store) = self.store_mut_existing::<T>() {
                store.unindex_fingerprint(entity, before_fingerprint);
                store.index_fingerprint(entity, after_fingerprint);
            }
        }

        if changed {
            self.revision = next_revision;
        }

        TryUpdate::Applied { changed, result }
    }

    /// Mutates one component in the live world without cloning the payload.
    pub(crate) fn update_component_live<T, F, R>(
        &mut self,
        entity: Entity,
        f: F,
    ) -> Option<(bool, R)>
    where
        T: Component,
        F: FnOnce(&mut T) -> R,
    {
        let next_revision = Revision(self.revision.0 + 1);
        let (changed, result, before_fingerprint, after_fingerprint) = {
            let store = self.store_mut_existing::<T>()?;
            let entry = store.entries.get_mut(&entity)?;
            let before_fingerprint = entry.fingerprint;

            let mut value = entry.value.write();
            let result = f(&mut value);

            let after_fingerprint = value.fingerprint();
            entry.fingerprint = after_fingerprint;

            let changed = T::tracked()
                && (before_fingerprint.is_none() || before_fingerprint != after_fingerprint);

            if changed {
                entry.revision = next_revision;
            }

            (changed, result, before_fingerprint, after_fingerprint)
        };

        if before_fingerprint != after_fingerprint {
            if let Some(store) = self.store_mut_existing::<T>() {
                store.unindex_fingerprint(entity, before_fingerprint);
                store.index_fingerprint(entity, after_fingerprint);
            }
        }

        if changed {
            self.revision = next_revision;
        }

        Some((changed, result))
    }

    /// Upper bound used for simple entity scans.
    pub(crate) fn next_entity_raw(&self) -> u64 {
        self.next_entity
    }

    /// Entities that currently have a component of type `T`, ascending.
    pub(crate) fn entities_with<T: Component>(&self) -> Vec<Entity> {
        self.store::<T>()
            .map(|store| store.entries.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Number of entities that currently have a component of type `T`.
    pub(crate) fn store_len<T: Component>(&self) -> usize {
        self.store::<T>().map_or(0, |store| store.entries.len())
    }

    /// Entities whose `T` component currently has this fingerprint, ascending.
    pub(crate) fn entities_with_fingerprint<T: Component>(&self, fingerprint: u64) -> Vec<Entity> {
        self.store::<T>()
            .and_then(|store| store.by_fingerprint.get(&fingerprint))
            .map(|entities| entities.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Current global revision.
    pub(crate) fn revision_raw(&self) -> u64 {
        self.revision.0
    }

    /// Current structural mutation counter.
    pub(crate) fn mutations_raw(&self) -> u64 {
        self.mutations
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
