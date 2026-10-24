//! Dense, phase-local execution records and component reservations.
use crate::{
    Entity,
    query::{Access, AccessKind},
    world::SystemInvocation,
};
use std::{any::TypeId, collections::{HashMap, hash_map::Entry}, sync::Arc};

/// Only valid within its owning evaluator phase. Reused slots change generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RunId {
    pub(crate) slot: u32,
    generation: u32,
}

impl RunId {
    #[cfg(test)]
    pub(crate) fn test(slot: u64) -> Self {
        Self { slot: u32::try_from(slot).unwrap(), generation: 0 }
    }
}

#[derive(Debug)]
pub(crate) enum Rejected {
    Duplicate,
    Conflict(SystemInvocation),
}

#[derive(Default)]
struct Counts {
    readers: usize,
    writers: usize,
}

impl Counts {
    fn any(&self) -> bool { self.readers != 0 || self.writers != 0 }

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

impl ComponentAccess {
    fn conflicts(&self, access: &ResolvedAccess) -> bool {
        match access.entity {
            None => self.total.conflicts(access.kind),
            Some(entity) => self.whole_store.conflicts(access.kind)
                || self.rows.get(&entity).is_some_and(|row| row.conflicts(access.kind)),
        }
    }

    fn add(&mut self, access: &ResolvedAccess) {
        self.total.add(access.kind);
        match access.entity {
            Some(entity) => self.rows.entry(entity).or_default().add(access.kind),
            None => self.whole_store.add(access.kind),
        }
    }

    fn remove(&mut self, access: ResolvedAccess) {
        self.total.remove(access.kind);
        match access.entity {
            None => self.whole_store.remove(access.kind),
            Some(entity) => {
                if let Entry::Occupied(mut entry) = self.rows.entry(entity) {
                    entry.get_mut().remove(access.kind);
                    if !entry.get().any() { entry.remove(); }
                } else {
                    panic!("reserved row missing");
                }
            }
        }
    }
}

struct ResolvedAccess {
    component: usize,
    entity: Option<Entity>,
    kind: AccessKind,
    // Preserve Access's allocation width when replacing TypeId with an index.
    // Vec's in-place collection otherwise shrinks every row's allocation. This
    // padding is only a performance aid; conversion remains safe if a target's
    // layout needs the standard library's allocating fallback.
    _allocation_width: [u8; std::mem::size_of::<TypeId>() - std::mem::size_of::<usize>()],
}

#[derive(Default)]
struct Components {
    schema: Arc<HashMap<TypeId, u32>>,
    extra: HashMap<TypeId, usize>,
    slots: Vec<ComponentAccess>,
}

impl Components {
    fn resolve(&mut self, access: Access) -> ResolvedAccess {
        let component = if let Some(&slot) = self.schema.get(&access.component) {
            slot as usize
        } else {
            *self.extra.entry(access.component).or_insert_with(|| {
                let slot = self.slots.len();
                self.slots.push(ComponentAccess::default());
                slot
            })
        };
        ResolvedAccess { component, entity: access.entity, kind: access.kind,
            _allocation_width: [0; std::mem::size_of::<TypeId>() - std::mem::size_of::<usize>()] }
    }
}

struct Record {
    owner: SystemInvocation,
    accesses: Vec<ResolvedAccess>,
    writer: bool,
}

struct Slot {
    generation: u32,
    record: Option<Record>,
}

#[derive(Default)]
struct Records {
    slots: Vec<Slot>,
    free: Vec<u32>,
}

impl Records {
    fn insert(&mut self, record: Record) -> RunId {
        let index = if let Some(index) = self.free.pop() {
            index
        } else {
            let index = u32::try_from(self.slots.len()).expect("execution slot space exhausted");
            self.slots.push(Slot { generation: 0, record: None });
            index
        };
        let slot = &mut self.slots[index as usize];
        debug_assert!(slot.record.is_none());
        slot.record = Some(record);
        RunId { slot: index, generation: slot.generation }
    }

    fn get(&self, id: RunId) -> Option<&Record> {
        let slot = self.slots.get(id.slot as usize)?;
        (slot.generation == id.generation).then_some(slot.record.as_ref()).flatten()
    }

    fn remove(&mut self, id: RunId) -> Option<Record> {
        let slot = self.slots.get_mut(id.slot as usize)?;
        if slot.generation != id.generation { return None; }
        let record = slot.record.take()?;
        // Never wrap a generation and make a retired completion valid again.
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            self.free.push(id.slot);
        }
        Some(record)
    }
}

/// One logical-key index for duplicate detection. Completion and release use IDs.
/// Exact row/store reservations remain held through publication. Component
/// counters and record slots retain capacity only for the active phase.
#[derive(Default)]
pub(crate) struct Reservations {
    owners: HashMap<SystemInvocation, RunId>,
    records: Records,
    components: Components,
}

impl Reservations {
    pub(crate) fn new(schema: Arc<HashMap<TypeId, u32>>) -> Self {
        let mut slots = Vec::with_capacity(schema.len());
        slots.resize_with(schema.len(), ComponentAccess::default);
        Self { owners: HashMap::new(), records: Records::default(), components: Components { schema, slots, extra: HashMap::new() } }
    }

    pub(crate) fn admit(&mut self, owner: SystemInvocation, accesses: Vec<Access>) -> Result<RunId, Rejected> {
        let Entry::Vacant(entry) = self.owners.entry(owner) else {
            return Err(Rejected::Duplicate);
        };
        let accesses: Vec<_> = accesses.into_iter().map(|access| self.components.resolve(access)).collect();
        if accesses.iter().any(|access| self.components.slots[access.component].conflicts(access)) {
            return Err(Rejected::Conflict(entry.into_key()));
        }
        let writer = accesses.iter().any(|access| access.kind == AccessKind::Write);
        for access in &accesses { self.components.slots[access.component].add(access); }
        let id = self.records.insert(Record { owner: entry.key().clone(), accesses, writer });
        entry.insert(id);
        Ok(id)
    }

    pub(crate) fn contains(&self, id: RunId) -> bool { self.records.get(id).is_some() }

    pub(crate) fn is_writer(&self, id: RunId) -> bool {
        self.records.get(id).expect("admitted invocation").writer
    }

    pub(crate) fn remove(&mut self, id: RunId) -> Option<SystemInvocation> {
        let record = self.records.remove(id)?;
        let previous = self.owners.remove(&record.owner);
        debug_assert_eq!(previous, Some(id));
        for access in record.accesses { self.components.slots[access.component].remove(access); }
        Some(record.owner)
    }

    pub(crate) fn cancel_readers(&mut self) {
        // Cancellation is exceptional; ordinary completion goes straight by ID.
        let readers: Vec<_> = self.records.slots.iter().enumerate().filter_map(|(index, slot)| {
            slot.record.as_ref().filter(|record| !record.writer)
                .map(|_| RunId { slot: index as u32, generation: slot.generation })
        }).collect();
        for id in readers { self.remove(id); }
    }
}

#[cfg(test)]
mod tests;
