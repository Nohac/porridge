use std::{
    any::{Any, TypeId},
    cell::UnsafeCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::{Deref, DerefMut},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    Component, Entity,
    component::{
        ComponentHookContext, DerivedFrom, RelationshipEdge, RelationshipRetraction,
        RelationshipTarget,
    },
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
    /// Number of entries in the store.
    fn len(&self) -> usize;
    /// Entities present in the store, ascending.
    fn entities(&self) -> Vec<Entity>;
    /// Whether this store's component type participates in revision tracking.
    fn tracked(&self) -> bool;
    /// Highest revision at which this store changed.
    fn watermark(&self) -> u64;
    /// Raises the change watermark (used by removal paths that bump the
    /// world revision outside the store).
    fn bump_watermark(&mut self, revision: u64);
    /// Entities whose stored fingerprint equals `fingerprint`, ascending —
    /// the pair candidates for index-driven `Eq` join planning.
    fn fingerprint_entities(&self, fingerprint: u64) -> Vec<Entity>;
    /// Relationship maintenance info for `entity`'s stored value, read
    /// before a removal path drops the entry: the edge it carries (if it is
    /// a relationship source) and the retractions it implies (if it is a
    /// maintained inverse).
    fn relationship_ops(
        &self,
        entity: Entity,
    ) -> (Option<RelationshipEdge>, Vec<RelationshipRetraction>);
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
    /// Entry map behind `Arc`: snapshots clone the handle (O(1) per
    /// store), and live mutation copies the map on the first write after
    /// a clone — the same copy-on-write pattern as the fingerprint index
    /// and presence bits, making `World::clone` O(#stores) instead of
    /// O(total entries).
    entries: Arc<BTreeMap<Entity, ComponentEntry<T>>>,
    /// Fingerprint → entities index for equality lookups.
    ///
    /// Only entries with a fingerprint participate. The map is shared with
    /// snapshots and copied on the first live write after a clone, so
    /// snapshot clones stay cheap while external `Where<Eq<T>>` queries can
    /// resolve candidates without scanning.
    by_fingerprint: Arc<HashMap<u64, BTreeSet<Entity>>>,
    /// Highest revision at which this store changed (inserts, removals —
    /// including whole-entity and sweep paths — and in-place writes).
    /// Compared against memoized `planned_revision`s by `explain`'s
    /// stale-view detection and by outer-join absence deps.
    watermark: u64,
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
            entries: Arc::clone(&self.entries),
            by_fingerprint: Arc::clone(&self.by_fingerprint),
            watermark: self.watermark,
        }
    }
}

impl<T> Default for Store<T> {
    fn default() -> Self {
        Self {
            entries: Arc::new(BTreeMap::new()),
            by_fingerprint: Arc::new(HashMap::new()),
            watermark: 0,
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

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn entities(&self) -> Vec<Entity> {
        self.entries.keys().copied().collect()
    }

    fn tracked(&self) -> bool {
        T::tracked()
    }

    fn watermark(&self) -> u64 {
        self.watermark
    }

    fn bump_watermark(&mut self, revision: u64) {
        self.watermark = self.watermark.max(revision);
    }

    fn fingerprint_entities(&self, fingerprint: u64) -> Vec<Entity> {
        self.by_fingerprint
            .get(&fingerprint)
            .map(|entities| entities.iter().copied().collect())
            .unwrap_or_default()
    }

    fn relationship_ops(
        &self,
        entity: Entity,
    ) -> (Option<RelationshipEdge>, Vec<RelationshipRetraction>) {
        self.entries
            .get(&entity)
            .map(|entry| {
                let value = entry.value.read();
                (value.relationship_edge(), value.relationship_retractions())
            })
            .unwrap_or((None, Vec::new()))
    }

    fn reconcile_entry(&mut self, entity: Entity, revision: &mut Revision) {
        let (before, after, changed) = {
            let Some(entry) = Arc::make_mut(&mut self.entries).get_mut(&entity) else {
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

        if changed {
            self.watermark = self.watermark.max(revision.0);
        }
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

        let removed = Arc::make_mut(&mut self.entries).remove(&entity)?;
        self.unindex_fingerprint(entity, removed.fingerprint);
        T::on_remove(ComponentHookContext::new(entity));

        Some(T::tracked())
    }

    fn remove_entity(
        &mut self,
        entity: Entity,
        revision: &mut Revision,
    ) -> Option<Option<SystemInvocation>> {
        let removed = Arc::make_mut(&mut self.entries).remove(&entity)?;
        self.unindex_fingerprint(entity, removed.fingerprint);

        let context = ComponentHookContext::new(entity);
        T::on_entity_remove(context);
        T::on_remove(context);

        if T::tracked() {
            bump(revision);
            self.watermark = self.watermark.max(revision.0);
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
    /// Shared monotonic id allocator. Shared (not copied) into snapshots so
    /// command buffers can reserve real entity ids at buffer time.
    next_entity: Arc<AtomicU64>,
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
    /// the id space on every rerun. Shared (not copied) into snapshots so
    /// `Commands` buffers reserve the previous run's slot ids at buffer
    /// time; the map itself is only mutated at commit, under the state
    /// lock, and one invocation is never in flight twice.
    derived_spawns: Arc<Mutex<HashMap<SystemInvocation, Vec<Entity>>>>,
    /// Per-owner spawn cursor for the commit currently being applied.
    spawn_cursors: HashMap<SystemInvocation, usize>,
    /// Entities removed since the runner last drained this list.
    ///
    /// The commit path uses it to purge memo entries keyed by entities that
    /// no longer exist.
    removed_entities: Vec<Entity>,
    /// `DerivedFrom` inserts deferred until the current command buffer has
    /// fully applied.
    ///
    /// Anchor revisions must be resolved at buffer end: capturing them in
    /// command-application order makes a derived fact stale on arrival
    /// whenever a *later* command in the same buffer writes to its anchor
    /// entity — and cleanup then silently reaps it.
    pending_derived_from: Vec<(Entity, DerivedFrom, Origin, Option<SystemInvocation>)>,
    /// True while `flush_derived_from` re-enters `insert`, so the deferral
    /// branch steps aside and the anchors capture for real.
    flushing_anchors: bool,
    /// Derived component writes since the runner last drained this list:
    /// which type landed on which entity. Debug builds use it to flag
    /// same-phase ambient consumption — a written entity races a view only
    /// if it ends up carrying *all* the components the view requires.
    written_derived: Vec<(TypeId, Entity, &'static str)>,
    /// Presence bitmaps over the schema's closed component universe (bowls
    /// constructed with [`Bowl::of`](crate::Bowl::of) only): one bit per
    /// (entity, component-in-universe), maintained at the same chokepoints
    /// as watermarks and the fingerprint index. Row matching consults this
    /// instead of probing stores. Copy-on-write against snapshots.
    presence: Option<PresenceIndex>,
    /// Settle-scoped write log: every (store, entity) touched since the
    /// current plan epoch began, in write order — the delta-planning
    /// source. Cleared by the runner at settle start; per-system cursors
    /// slice it. Behind `Arc` so planning waves take a cheap handle; not
    /// carried into snapshots.
    write_log: Arc<Vec<(TypeId, Entity)>>,
    /// Bumped when the write log is cleared, so per-system cursors from a
    /// previous settle are recognized as stale (forcing one full plan).
    plan_epoch: u64,
}

/// Dense presence bits over a schema-closed component universe.
///
/// Bit positions are assigned once, at bowl construction, so every store
/// and entity carries the layout from birth. `bits` is a flat row-major
/// array (`entity id × stride` words) behind an `Arc`: snapshots share it
/// structurally and live mutation copies on first write per generation,
/// like the fingerprint index.
pub(crate) struct PresenceIndex {
    bit_for: Arc<HashMap<TypeId, u32>>,
    /// Words per entity row.
    stride: usize,
    bits: Arc<Vec<u64>>,
}

impl Clone for PresenceIndex {
    fn clone(&self) -> Self {
        Self {
            bit_for: Arc::clone(&self.bit_for),
            stride: self.stride,
            bits: Arc::clone(&self.bits),
        }
    }
}

impl PresenceIndex {
    fn new(universe: Vec<TypeId>) -> Self {
        let bit_for: HashMap<TypeId, u32> = universe
            .into_iter()
            .enumerate()
            .map(|(index, type_id)| (type_id, index as u32))
            .collect();
        let stride = bit_for.len().div_ceil(64).max(1);
        Self {
            bit_for: Arc::new(bit_for),
            stride,
            bits: Arc::new(Vec::new()),
        }
    }

    fn set(&mut self, type_id: TypeId, entity: Entity, present: bool) {
        let Some(&bit) = self.bit_for.get(&type_id) else {
            return;
        };
        let row = entity.raw() as usize * self.stride;
        let bits = Arc::make_mut(&mut self.bits);
        if bits.len() < row + self.stride {
            if !present {
                return;
            }
            bits.resize(row + self.stride, 0);
        }
        let word = row + (bit / 64) as usize;
        let flag = 1u64 << (bit % 64);
        if present {
            bits[word] |= flag;
        } else {
            bits[word] &= !flag;
        }
    }

    fn clear_entity(&mut self, entity: Entity) {
        let row = entity.raw() as usize * self.stride;
        if row >= self.bits.len() {
            return;
        }
        let stride = self.stride;
        let bits = Arc::make_mut(&mut self.bits);
        bits[row..row + stride].fill(0);
    }

    /// The mask for a conjunction of required components, or `None` when a
    /// type is outside the universe (caller falls back to store probing).
    pub(crate) fn mask(&self, type_ids: &[TypeId]) -> Option<Vec<u64>> {
        let mut mask = vec![0u64; self.stride];
        for type_id in type_ids {
            let bit = *self.bit_for.get(type_id)?;
            mask[(bit / 64) as usize] |= 1u64 << (bit % 64);
        }
        Some(mask)
    }

    pub(crate) fn matches(&self, entity: Entity, mask: &[u64]) -> bool {
        let row = entity.raw() as usize * self.stride;
        if row + self.stride > self.bits.len() {
            return mask.iter().all(|word| *word == 0);
        }
        mask.iter()
            .zip(&self.bits[row..row + self.stride])
            .all(|(mask_word, bits_word)| bits_word & mask_word == *mask_word)
    }

    /// All entities whose bits satisfy `mask`, scanning row-major words.
    /// Sound only for non-empty masks: entities that never carried a
    /// universe component have no row, which cannot matter when at least
    /// one bit is required.
    pub(crate) fn entities_matching(&self, mask: &[u64]) -> Vec<Entity> {
        let rows = self.bits.len() / self.stride;
        let mut out = Vec::new();
        for row in 0..rows {
            let start = row * self.stride;
            let hit = mask
                .iter()
                .zip(&self.bits[start..start + self.stride])
                .all(|(mask_word, bits_word)| bits_word & mask_word == *mask_word);
            if hit {
                out.push(Entity::from_raw(row as u64));
            }
        }
        out
    }
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
        debug_assert!(
            self.pending_derived_from.is_empty(),
            "snapshot taken with unflushed DerivedFrom anchors; \
             a command-application site is missing flush_derived_from()"
        );
        Self {
            next_entity: Arc::clone(&self.next_entity),
            revision: self.revision,
            mutations: self.mutations,
            stores: self.stores.clone(),
            singleton_entities: self.singleton_entities.clone(),
            derived_owners: HashMap::new(),
            derived_spawns: Arc::clone(&self.derived_spawns),
            spawn_cursors: HashMap::new(),
            removed_entities: Vec::new(),
            pending_derived_from: Vec::new(),
            flushing_anchors: false,
            written_derived: Vec::new(),
            presence: self.presence.clone(),
            write_log: Arc::new(Vec::new()),
            plan_epoch: self.plan_epoch,
        }
    }
}

/// Structural read source for one generation.
///
/// A snapshot is a structural clone of `World`; component cells are shared and
/// reads are protected by component read guards.
#[doc(hidden)]
pub type Snapshot = World;

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
            next_entity: Arc::new(AtomicU64::new(0)),
            revision: Revision(0),
            mutations: 0,
            stores: HashMap::new(),
            singleton_entities: HashMap::new(),
            derived_owners: HashMap::new(),
            derived_spawns: Arc::new(Mutex::new(HashMap::new())),
            spawn_cursors: HashMap::new(),
            removed_entities: Vec::new(),
            pending_derived_from: Vec::new(),
            flushing_anchors: false,
            written_derived: Vec::new(),
            presence: None,
            write_log: Arc::new(Vec::new()),
            plan_epoch: 0,
        }
    }

    /// Rolls the plan epoch when the write log has grown past its budget:
    /// clears the log and invalidates every per-system delta cursor (each
    /// then full-plans once and rejoins the deltas). Called by the runner
    /// at evaluation start, so cursors stay valid across generations and
    /// settles while activity is modest.
    pub(crate) fn maybe_roll_plan_epoch(&mut self) {
        if self.write_log.len() > 4096 {
            self.plan_epoch += 1;
            self.write_log = Arc::new(Vec::new());
        }
    }

    /// The current write log and plan epoch, as a cheap shared handle.
    pub(crate) fn plan_log(&self) -> (Arc<Vec<(TypeId, Entity)>>, u64) {
        (Arc::clone(&self.write_log), self.plan_epoch)
    }

    fn log_write(&mut self, type_id: TypeId, entity: Entity) {
        Arc::make_mut(&mut self.write_log).push((type_id, entity));
    }

    /// Installs the presence index over a schema-closed component universe.
    /// Called once, at bowl construction, before any store exists.
    pub(crate) fn init_presence(&mut self, universe: Vec<TypeId>) {
        debug_assert!(
            self.stores.is_empty(),
            "presence index must be installed before any component is stored"
        );
        self.presence = Some(PresenceIndex::new(universe));
    }

    /// The presence index, when this bowl was constructed over a schema.
    pub(crate) fn presence(&self) -> Option<&PresenceIndex> {
        self.presence.as_ref()
    }

    fn presence_set(&mut self, type_id: TypeId, entity: Entity, present: bool) {
        if let Some(presence) = &mut self.presence {
            presence.set(type_id, entity, present);
        }
    }

    fn presence_clear_entity(&mut self, entity: Entity) {
        if let Some(presence) = &mut self.presence {
            presence.clear_entity(entity);
        }
    }

    /// Type-erased store size (`usize::MAX` for an absent store, so it is
    /// never picked as a probing driver over a present one).
    pub(crate) fn store_len_dyn(&self, type_id: TypeId) -> usize {
        self.stores
            .get(&type_id)
            .map_or(usize::MAX, |store| store.len())
    }

    /// Type-erased store row listing, ascending; empty for an absent store.
    pub(crate) fn entities_with_dyn(&self, type_id: TypeId) -> Vec<Entity> {
        self.stores
            .get(&type_id)
            .map_or_else(Vec::new, |store| store.entities())
    }

    /// Type-erased revision lookup for facet-part deps.
    pub(crate) fn revision_for_entity_dyn(
        &self,
        type_id: TypeId,
        entity: Entity,
    ) -> Option<Revision> {
        self.stores
            .get(&type_id)
            .and_then(|store| store.revision_for_entity(entity))
    }

    /// Drains the derived component writes recorded since the last drain.
    pub(crate) fn take_written_derived(&mut self) -> Vec<(TypeId, Entity, &'static str)> {
        std::mem::take(&mut self.written_derived)
    }

    /// Whether `entity` currently carries a component of type `type_id`.
    pub(crate) fn has_dyn(&self, type_id: TypeId, entity: Entity) -> bool {
        self.stores
            .get(&type_id)
            .is_some_and(|store| store.revision_for_entity(entity).is_some())
    }

    /// Adds `source` to the maintained inverse `T` on `target`, keeping the
    /// member list sorted by entity id. Idempotent. The inverse is written
    /// as a base fact with no owner, so derived-output diffing never
    /// touches it (spec/joins.md, "Authoring shape").
    pub(crate) fn relationship_add_member<T: RelationshipTarget>(
        &mut self,
        source: Entity,
        target: Entity,
    ) {
        let mut members = self
            .store::<T>()
            .and_then(|store| store.entries.get(&target))
            .map(|entry| entry.value.read().members().to_vec())
            .unwrap_or_default();

        match members.binary_search(&source) {
            Ok(_) => return,
            Err(index) => members.insert(index, source),
        }
        self.insert_base(target, T::from_members(members));
    }

    /// Removes `source` from the maintained inverse `T` on `target`,
    /// removing the inverse outright when it empties. Tolerates a missing
    /// inverse (the target may be mid-removal).
    pub(crate) fn relationship_remove_member<T: RelationshipTarget>(
        &mut self,
        source: Entity,
        target: Entity,
    ) {
        let Some(mut members) = self
            .store::<T>()
            .and_then(|store| store.entries.get(&target))
            .map(|entry| entry.value.read().members().to_vec())
        else {
            return;
        };

        let Ok(index) = members.binary_search(&source) else {
            return;
        };
        members.remove(index);

        if members.is_empty() {
            self.remove_component::<T>(target);
        } else {
            self.insert_base(target, T::from_members(members));
        }
    }

    /// Drains the entities removed since the last drain.
    pub(crate) fn take_removed_entities(&mut self) -> Vec<Entity> {
        std::mem::take(&mut self.removed_entities)
    }

    /// Applies the `DerivedFrom` inserts deferred from the command buffer
    /// that just finished applying, capturing anchor revisions against the
    /// fully written world.
    ///
    /// Must run before anything reads the world (planning, snapshots,
    /// stale-output removal), so the deferral is never observable.
    pub(crate) fn flush_derived_from(&mut self) {
        if self.pending_derived_from.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut self.pending_derived_from);
        self.flushing_anchors = true;
        for (entity, value, origin, owner) in pending {
            self.insert(entity, value, origin, owner);
        }
        self.flushing_anchors = false;
    }

    /// Records an invocation's spawn into its next slot at commit time.
    ///
    /// The entity id itself was reserved at buffer time ([`Commands`] hands
    /// out the previous run's slot ids in spawn order, allocating fresh ids
    /// only for new slots), so a rerun that spawns the same outputs reuses
    /// the same entity ids. Recording keeps the slot list current for the
    /// next reservation.
    pub(crate) fn record_derived_spawn(&mut self, owner: &SystemInvocation, entity: Entity) {
        let cursor = self.spawn_cursors.entry(owner.clone()).or_default();
        let slot = *cursor;
        *cursor += 1;

        let mut map = self
            .derived_spawns
            .lock()
            .expect("derived spawn map lock poisoned");
        let spawns = map.entry(owner.clone()).or_default();
        if slot < spawns.len() {
            spawns[slot] = entity;
        } else {
            spawns.push(entity);
        }
    }

    /// The previous run's spawn slots for `owner`, in spawn order.
    ///
    /// Read at buffer time by [`Commands`]; safe because the map is only
    /// mutated at commit and one invocation is never in flight twice.
    pub(crate) fn spawn_slots(&self, owner: &SystemInvocation) -> Vec<Entity> {
        self.derived_spawns
            .lock()
            .expect("derived spawn map lock poisoned")
            .get(owner)
            .cloned()
            .unwrap_or_default()
    }

    /// The shared monotonic entity id allocator.
    pub(crate) fn entity_allocator(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.next_entity)
    }

    /// Ends the spawn phase of one commit, dropping unused trailing slots.
    pub(crate) fn finish_derived_spawns(&mut self, owner: &SystemInvocation) {
        let cursor = self.spawn_cursors.remove(owner).unwrap_or(0);
        let mut map = self
            .derived_spawns
            .lock()
            .expect("derived spawn map lock poisoned");
        if let Some(spawns) = map.get_mut(owner) {
            spawns.truncate(cursor);
            if spawns.is_empty() {
                map.remove(owner);
            }
        }
    }

    /// Allocates a fresh entity id in the live world.
    pub(crate) fn spawn_empty(&mut self) -> Entity {
        Entity::from_raw(self.next_entity.fetch_add(1, Ordering::Relaxed))
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

    /// Returns the entity for a singleton key, registering `candidate` as
    /// the singleton entity if none exists yet.
    ///
    /// Used by spawn commands whose id was reserved at buffer time: the
    /// reservation stands when this spawn *creates* the singleton, and is
    /// superseded by the existing entity otherwise.
    pub(crate) fn singleton_entity_or_register(&mut self, key: TypeId, candidate: Entity) -> Entity {
        if let Some(entity) = self.singleton_entities.get(&key) {
            return *entity;
        }

        self.singleton_entities.insert(key, candidate);
        candidate
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
            // Anchor revisions resolve at buffer end, not in command order:
            // a later command in the same buffer may still write to an
            // anchor entity, and capturing now would make this derived fact
            // stale on arrival. Defer the whole insert until
            // `flush_derived_from` runs after the buffer has applied.
            if !self.flushing_anchors {
                let deferred = std::mem::replace(derived_from, DerivedFrom::many([]));
                self.pending_derived_from
                    .push((entity, deferred, origin, owner));
                return;
            }
            derived_from.capture(|entity| self.entity_revision(entity));
        }

        let edge = value.relationship_edge();

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

        if cfg!(debug_assertions) && origin == Origin::Derived {
            self.written_derived
                .push((TypeId::of::<T>(), entity, std::any::type_name::<T>()));
        }

        let new_owner = owner.clone();
        self.presence_set(TypeId::of::<T>(), entity, true);
        self.log_write(TypeId::of::<T>(), entity);
        let store = self.store_mut::<T>();
        store.watermark = store.watermark.max(revision.0);
        let previous = Arc::make_mut(&mut store.entries).insert(
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

        let previous_edge = previous
            .as_ref()
            .and_then(|entry| entry.value.read().relationship_edge());

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

        // Relationship maintenance: keep the target's inverse current. An
        // unchanged target (idempotent re-emission) touches nothing, so the
        // fingerprint cutoff on the inverse keeps holding.
        let previous_target = previous_edge.as_ref().map(|edge| edge.target);
        let new_target = edge.as_ref().map(|edge| edge.target);
        if previous_target != new_target {
            if let Some(previous_edge) = previous_edge {
                (previous_edge.remove)(self, entity, previous_edge.target);
            }
            if let Some(edge) = edge {
                (edge.add)(self, entity, edge.target);
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
            Arc::make_mut(&mut self.write_log).push((*type_id, *entity));
        }
    }

    /// Returns the tracked revision for a component on an entity.
    pub(crate) fn revision<T: Component>(&self, entity: Entity) -> Option<Revision> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .map(|entry| entry.revision)
    }

    /// Returns the stamped fingerprint for a component on an entity.
    ///
    /// `None` when the entity lacks the component or the component type has
    /// no fingerprint (not `#[component(hash)]`).
    pub(crate) fn fingerprint<T: Component>(&self, entity: Entity) -> Option<u64> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .and_then(|entry| entry.fingerprint)
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
        // Untracked components re-stamp their entry with the current global
        // revision on every write; including them would lift entity revisions
        // (and so retire `DerivedFrom`-anchored facts) without any tracked
        // change having happened.
        self.stores
            .values()
            .filter(|store| store.tracked())
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
        self.derived_spawns
            .lock()
            .expect("derived spawn map lock poisoned")
            .remove(owner);
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

        let (edge, _) = store.relationship_ops(entity);
        let Some(tracked) = store.remove_entry_owned(entity, owner) else {
            return false;
        };
        self.presence_set(type_id, entity, false);
        self.log_write(type_id, entity);

        self.mutations += 1;
        if tracked {
            bump(&mut self.revision);
            let watermark = self.revision.0;
            if let Some(store) = self.stores.get_mut(&type_id) {
                store.bump_watermark(watermark);
            }
        }

        // Sweeps are a removal path like any other: a swept edge retracts
        // its membership from the target's inverse.
        if let Some(edge) = edge {
            (edge.remove)(self, entity, edge.target);
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
            self.derived_spawns
                .lock()
                .expect("derived spawn map lock poisoned")
                .remove(&owner);
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
        // Collect relationship maintenance before anything is removed, and
        // apply it only after the whole entity is gone, so store iteration
        // order never matters: this entity's edges retract from their
        // targets, and if this entity was itself a target, every source's
        // edge component is retracted (edge consistency, no cascade).
        let mut edges = Vec::new();
        let mut retractions = Vec::new();
        for store in self.stores.values() {
            let (edge, mut retract) = store.relationship_ops(entity);
            if let Some(edge) = edge {
                edges.push(edge);
            }
            retractions.append(&mut retract);
        }

        let mut owners = Vec::new();
        let mut unindex = Vec::new();

        let mut removed_types = Vec::new();
        for (type_id, store) in self.stores.iter_mut() {
            let Some(owner) = store.remove_entity(entity, &mut self.revision) else {
                continue;
            };

            self.mutations += 1;
            removed_types.push(*type_id);
            if let Some(owner) = owner {
                unindex.push((owner.clone(), *type_id));
                owners.push(owner);
            }
        }
        for type_id in removed_types {
            self.log_write(type_id, entity);
        }

        for (owner, type_id) in unindex {
            self.unindex_derived(&owner, type_id, entity);
        }

        self.singleton_entities
            .retain(|_, singleton_entity| *singleton_entity != entity);
        self.removed_entities.push(entity);
        self.presence_clear_entity(entity);

        for edge in edges {
            (edge.remove)(self, entity, edge.target);
        }
        for retraction in retractions {
            (retraction.remove_edge)(self, retraction.source);
        }

        owners
    }

    /// Whether taking `T` from `entity` would currently fail because a live
    /// snapshot or query result still shares the component cell.
    ///
    /// Callers hold the state lock while checking, and snapshot creation also
    /// requires that lock, so a `false` answer cannot be invalidated before
    /// the caller's matching `remove_component`.
    pub(crate) fn component_pinned<T: Component>(&self, entity: Entity) -> bool {
        self.store::<T>().is_some_and(|store| {
            store.entries.get(&entity).is_some_and(|entry| {
                // A shared entries map pins every cell in it: removing
                // under COW would clone the map first, leaving the removed
                // cell alive in the sharer and the taken value
                // unrecoverable. Same coarseness as the pre-COW behavior,
                // where every snapshot bumped every cell.
                Arc::strong_count(&store.entries) > 1 || Arc::strong_count(&entry.value) > 1
            })
        })
    }

    /// Removes one typed component and returns the stored value behind `Arc`.
    ///
    /// Taking still avoids `T: Clone`, but it requires that no structural
    /// snapshot keeps the removed component cell alive; check
    /// [`World::component_pinned`] first, because a pinned removal loses the
    /// value.
    pub(crate) fn remove_component<T>(&mut self, entity: Entity) -> Option<Arc<T>>
    where
        T: Component,
    {
        let store = self.store_mut_existing::<T>()?;
        let removed = Arc::make_mut(&mut store.entries).remove(&entity)?;
        store.unindex_fingerprint(entity, removed.fingerprint);
        self.presence_set(TypeId::of::<T>(), entity, false);
        self.log_write(TypeId::of::<T>(), entity);
        let edge = removed.value.read().relationship_edge();

        T::on_remove(ComponentHookContext::new(entity));

        self.mutations += 1;
        if T::tracked() {
            bump(&mut self.revision);
            let watermark = self.revision.0;
            if let Some(store) = self.store_mut_existing::<T>() {
                store.watermark = store.watermark.max(watermark);
            }
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

        if let Some(edge) = edge {
            (edge.remove)(self, entity, edge.target);
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
            let Some(entry) = Arc::make_mut(&mut store.entries).get_mut(&entity) else {
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
            // External mutation moves the store for planner gating and
            // delta hints, exactly like the insert and removal paths.
            if let Some(store) = self.store_mut_existing::<T>() {
                store.watermark = store.watermark.max(next_revision.0);
            }
            self.log_write(TypeId::of::<T>(), entity);
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
            let entry = Arc::make_mut(&mut store.entries).get_mut(&entity)?;
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
            // External mutation moves the store for planner gating and
            // delta hints, exactly like the insert and removal paths.
            if let Some(store) = self.store_mut_existing::<T>() {
                store.watermark = store.watermark.max(next_revision.0);
            }
            self.log_write(TypeId::of::<T>(), entity);
            self.revision = next_revision;
        }

        Some((changed, result))
    }

    /// Upper bound used for simple entity scans.
    pub(crate) fn next_entity_raw(&self) -> u64 {
        self.next_entity.load(Ordering::Relaxed)
    }

    /// Pair candidates for an index-driven `Eq` join: entities whose stored
    /// fingerprint for `type_id` equals `fingerprint`.
    pub(crate) fn fingerprint_candidates(&self, type_id: TypeId, fingerprint: u64) -> Vec<Entity> {
        self.stores
            .get(&type_id)
            .map(|store| store.fingerprint_entities(fingerprint))
            .unwrap_or_default()
    }

    /// Highest revision at which the store for `type_id` changed; zero when
    /// no such store exists.
    pub(crate) fn store_watermark(&self, type_id: TypeId) -> u64 {
        self.stores
            .get(&type_id)
            .map(|store| store.watermark())
            .unwrap_or(0)
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
