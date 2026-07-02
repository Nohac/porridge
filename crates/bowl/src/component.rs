use std::{
    any::TypeId,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use crate::{Entity, world::Revision};

/// Context passed to component lifecycle hooks.
#[derive(Debug, Clone, Copy)]
pub struct ComponentHookContext {
    entity: Entity,
}

impl ComponentHookContext {
    pub(crate) const fn new(entity: Entity) -> Self {
        Self { entity }
    }

    /// Entity whose component lifecycle changed.
    pub fn entity(self) -> Entity {
        self.entity
    }
}

/// Data that can be attached to an [`Entity`](crate::Entity).
///
/// Components must be `Send + Sync` because `Bowl` is designed to be shared
/// across async tasks and eventually evaluated in parallel.
pub trait Component: Send + Sync + 'static {
    /// Whether this component participates in revision-based invalidation.
    fn tracked() -> bool {
        true
    }

    /// Optional stable fingerprint for avoiding revision bumps on equal writes.
    ///
    /// Returning `Some(hash)` means inserting the same fingerprint again keeps
    /// the previous revision. Returning `None` means every tracked insert is
    /// considered a new value.
    fn fingerprint(&self) -> Option<u64> {
        None
    }

    #[doc(hidden)]
    fn singleton_key() -> Option<TypeId> {
        None
    }

    /// Runs after this component type is inserted or replaced on an entity.
    fn on_insert(_context: ComponentHookContext) {}

    /// Runs when this component type is removed from an entity.
    fn on_remove(_context: ComponentHookContext) {}

    /// Runs before this component type is removed as part of removing the whole
    /// entity.
    fn on_entity_remove(_context: ComponentHookContext) {}
}

/// Marker component that routes a bundle through the singleton index for `T`.
///
/// This is the manual MVP shape:
///
/// ```rust
/// # use bowl::{Component, Singleton};
/// # struct ProjectConfig;
/// # impl Component for ProjectConfig {}
/// let marker = Singleton::<ProjectConfig>::new();
/// ```
///
/// Inserting a bundle containing `Singleton<T>` inserts the whole bundle onto
/// the unique singleton entity for `T`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Singleton<T> {
    _marker: PhantomData<fn() -> T>,
}

impl<T> Singleton<T> {
    /// Creates a singleton marker keyed by component type `T`.
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Component for Singleton<T>
where
    T: Component,
{
    fn tracked() -> bool {
        false
    }

    fn singleton_key() -> Option<TypeId> {
        Some(TypeId::of::<T>())
    }
}

/// Marks an entity as derived from one or more source entities.
///
/// This is useful for facts that should disappear when the inputs they were
/// derived from change. For example, a diagnostic derived from an import and a
/// project import database can be attached to both:
///
/// ```text
/// let derived = DerivedFrom::many([import, import_db]);
/// ```
///
/// Internally, the bowl captures the current revision of each source entity
/// when this component is inserted. [`crate::cleanup_stale_derived`] removes
/// the derived entity if any source entity changes or is removed.
///
/// ```text
/// diagnostic
///   DerivedFrom([import @ rev 10, import_db @ rev 20])
///
/// import_db changes to rev 21
///   cleanup removes diagnostic
/// ```
///
/// This is intentionally entity-scoped. Changing any tracked component on the
/// source entity invalidates the derived entity.
#[derive(Debug, Clone)]
pub struct DerivedFrom {
    anchors: Vec<DerivedAnchor>,
}

#[derive(Debug, Clone, Copy)]
struct DerivedAnchor {
    entity: Entity,
    revision: Option<Revision>,
}

impl DerivedFrom {
    /// Marks this entity as derived from one source entity.
    ///
    /// The actual revision is resolved by the bowl when the component is
    /// inserted, so callers do not need to traffic in revision values.
    pub fn new(entity: Entity) -> Self {
        Self::many([entity])
    }

    /// Marks this entity as derived from every source entity in `entities`.
    ///
    /// The derived entity remains current only while every captured source
    /// entity stays at the same revision.
    pub fn many(entities: impl IntoIterator<Item = Entity>) -> Self {
        Self {
            anchors: entities
                .into_iter()
                .map(|entity| DerivedAnchor {
                    entity,
                    revision: None,
                })
                .collect(),
        }
    }

    /// Source entities this derived output is attached to.
    pub fn entities(&self) -> impl Iterator<Item = Entity> + '_ {
        self.anchors.iter().map(|anchor| anchor.entity)
    }

    pub(crate) fn capture(&mut self, mut revision: impl FnMut(Entity) -> Option<Revision>) {
        for anchor in &mut self.anchors {
            anchor.revision = revision(anchor.entity);
        }
    }

    pub(crate) fn is_current_revision(
        &self,
        mut revision: impl FnMut(Entity) -> Option<Revision>,
    ) -> bool {
        !self.anchors.is_empty()
            && self.anchors.iter().all(|anchor| {
                anchor.revision.is_some() && anchor.revision == revision(anchor.entity)
            })
    }
}

impl Component for DerivedFrom {
    fn tracked() -> bool {
        false
    }
}

/// Convenience helper for implementing [`Component::fingerprint`] with `Hash`.
pub fn hash_component<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
