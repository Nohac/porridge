/// Copyable identity for a row of components.
///
/// `Entity` is intentionally just an id. It does not grant ownership or
/// destructive permissions. APIs such as future `BoundEntity` handles should
/// carry those stronger capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Entity(pub(crate) u64);

impl Entity {
    /// Returns the underlying numeric id for debugging and display.
    pub fn raw(self) -> u64 {
        self.0
    }
}
