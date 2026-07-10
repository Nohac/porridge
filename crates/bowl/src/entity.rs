use std::marker::PhantomData;

/// Marker for an entity handle with no shape claim: plain identity.
pub struct Untyped;

/// Copyable identity for a row of components.
///
/// `Entity` is intentionally just an id. It does not grant ownership or
/// destructive permissions. APIs such as future `BoundEntity` handles should
/// carry those stronger capabilities.
///
/// The type parameter is a *facet*: `Entity<H>` claims the entity conforms
/// at least to shape `H` (its required components are present). Entities
/// are multi-faceted — one id may satisfy several shapes, and typed handles
/// to different facets of the same entity coexist. Bare `Entity` is the
/// untyped handle (`H = Untyped`), used for cross-shape vocabulary reads
/// and at the dynamic boundary. Facet handles come from strict spawns
/// (`Commands::insert` returns `Entity<H>` for the matched shape) and
/// facet-anchored queries; the claim is made against the invocation's
/// snapshot, with the debug-build shape conformance check as the runtime
/// backstop.
pub struct Entity<H = Untyped>(pub(crate) u64, pub(crate) PhantomData<fn() -> H>);

impl<H> Entity<H> {
    pub(crate) fn from_raw(raw: u64) -> Self {
        Entity(raw, PhantomData)
    }

    /// Returns the underlying numeric id for debugging and display.
    pub fn raw(self) -> u64 {
        self.0
    }

    /// Drops the facet claim, yielding the plain identity handle.
    pub fn untyped(self) -> Entity {
        Entity(self.0, PhantomData)
    }

    /// Reinterprets the handle under a different facet claim.
    ///
    /// Crate-internal: user-facing facets are established by strict spawns
    /// and facet queries, never asserted.
    pub(crate) fn retype<H2>(self) -> Entity<H2> {
        Entity(self.0, PhantomData)
    }
}

// Manual impls: the facet parameter is phantom, so none of these may
// require bounds on `H` (derives would add them).
impl<H> Clone for Entity<H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H> Copy for Entity<H> {}

impl<H1, H2> PartialEq<Entity<H2>> for Entity<H1> {
    fn eq(&self, other: &Entity<H2>) -> bool {
        self.0 == other.0
    }
}

impl<H> Eq for Entity<H> {}

impl<H> PartialOrd for Entity<H> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<H> Ord for Entity<H> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<H> std::hash::Hash for Entity<H> {
    fn hash<Hasher: std::hash::Hasher>(&self, state: &mut Hasher) {
        self.0.hash(state);
    }
}

impl<H> std::fmt::Debug for Entity<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Entity").field(&self.0).finish()
    }
}
