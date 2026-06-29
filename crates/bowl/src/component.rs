use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
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
}

/// Convenience helper for implementing [`Component::fingerprint`] with `Hash`.
pub fn hash_component<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
