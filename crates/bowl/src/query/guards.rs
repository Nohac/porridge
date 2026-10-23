//! Invocation/result-owned lock tokens. Component payloads remain in their cells.
use std::{any::Any, sync::Arc};

pub(crate) trait GuardCell: Send + Sync {
    fn release_read(&self);

    fn release_write(&self);
}

pub(crate) enum HeldGuard {
    Read(Arc<dyn GuardCell>),
    Write(Arc<dyn GuardCell>),
    Custom(Box<dyn Any + Send>),
}

impl Drop for HeldGuard {
    fn drop(&mut self) {
        match self {
            Self::Read(cell) => cell.release_read(),
            Self::Write(cell) => cell.release_write(),
            // The custom box drops normally after this method returns.
            Self::Custom(guard) => {
                let _ = guard;
            }
        }
    }
}

/// Holds locks until all references fetched for an invocation/result are dead.
/// Tokens own the existing cell Arcs; there is no heap box per component.
/// Moving or growing this store never moves a payload or releases a lock.
#[doc(hidden)]
#[derive(Default)]
pub struct GuardStore {
    guards: Vec<HeldGuard>,
}

impl GuardStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compatibility for custom low-level query parameters owning boxed guards.
    pub fn push(&mut self, guard: Box<dyn Any + Send>) {
        self.hold(HeldGuard::Custom(guard));
    }

    /// Only after all fetched items are dead: release locks, retaining capacity.
    pub(crate) fn clear(&mut self) {
        self.guards.clear();
    }

    pub(crate) fn capacity(&self) -> usize {
        self.guards.capacity()
    }

    pub(crate) fn hold(&mut self, guard: HeldGuard) {
        self.guards.push(guard);
    }
}
