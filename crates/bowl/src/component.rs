use std::{
    any::TypeId,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

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

/// Convenience helper for implementing [`Component::fingerprint`] with `Hash`.
pub fn hash_component<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
