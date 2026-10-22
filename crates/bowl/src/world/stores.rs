//! Schema slots share the presence-bit universe. Undeclared internal stores keep
//! a TypeId fallback; component payload storage and guarded cells are unchanged.
use super::*;

#[derive(Clone, Default)]
pub(super) struct Stores {
    slots: Vec<(TypeId, Option<Box<dyn StoreDyn>>)>,
    registry: Arc<HashMap<TypeId, u32>>,
    fallback: HashMap<TypeId, Box<dyn StoreDyn>>,
}

impl Stores {
    pub(super) fn with_registry(registry: Arc<HashMap<TypeId, u32>>) -> Self {
        let mut types: Vec<_> = registry.iter().map(|(&ty, &slot)| (slot, ty)).collect();
        types.sort_unstable_by_key(|(slot, _)| *slot);
        Self {
            slots: types.into_iter().map(|(_, ty)| (ty, None)).collect(),
            registry,
            fallback: HashMap::new(),
        }
    }

    pub(super) fn slot(&self, ty: TypeId) -> Option<u32> {
        self.registry.get(&ty).copied()
    }

    pub(super) fn get_slot(&self, slot: u32, ty: TypeId) -> Option<&dyn StoreDyn> {
        match self.slots.get(slot as usize) {
            Some((actual, store)) if *actual == ty => store.as_deref(),
            // A handle from another layout must never read the wrong type.
            _ => self.get(&ty),
        }
    }

    pub(super) fn get(&self, ty: &TypeId) -> Option<&dyn StoreDyn> {
        match self.registry.get(ty) {
            Some(&slot) => self.slots[slot as usize].1.as_deref(),
            None => self.fallback.get(ty).map(|store| store.as_ref()),
        }
    }

    pub(super) fn get_mut(&mut self, ty: &TypeId) -> Option<&mut Box<dyn StoreDyn>> {
        match self.registry.get(ty) {
            Some(&slot) => self.slots[slot as usize].1.as_mut(),
            None => self.fallback.get_mut(ty),
        }
    }

    pub(super) fn get_or_insert<T: Component>(&mut self) -> &mut Box<dyn StoreDyn> {
        let ty = TypeId::of::<T>();
        match self.registry.get(&ty) {
            Some(&slot) => self.slots[slot as usize]
                .1
                .get_or_insert_with(|| Box::<Store<T>>::default()),
            None => self
                .fallback
                .entry(ty)
                .or_insert_with(|| Box::<Store<T>>::default()),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.slots.iter().all(|(_, store)| store.is_none()) && self.fallback.is_empty()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &Box<dyn StoreDyn>> {
        self.slots
            .iter()
            .filter_map(|(_, store)| store.as_ref())
            .chain(self.fallback.values())
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&TypeId, &mut Box<dyn StoreDyn>)> {
        self.slots
            .iter_mut()
            .filter_map(|(ty, store)| store.as_mut().map(|store| (&*ty, store)))
            .chain(self.fallback.iter_mut())
    }
}
