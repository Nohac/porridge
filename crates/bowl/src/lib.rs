//! Async-first fact evaluation.
//!
//! `bowl` is the async implementation track for Porridge. A [`Bowl`] owns a
//! set of component facts and a list of systems that derive more facts from
//! them.
//!
//! The central API split is:
//!
//! ```text
//! Query<T, F = ()>
//!   tracked input
//!   drives system invocation identity and memo dependencies
//!   optional filter F affects matching without changing the item
//!
//! View<T, F = ()>
//!   ambient snapshot read
//!   visible when a system runs, but not part of memo dependencies
//!
//! Commands
//!   buffered writes
//!   applied only after the current snapshot tick completes
//! ```
//!
//! External queries use the same row/filter split:
//!
//! ```text
//! bowl.scoop::<Query<(Entity, &Diagnostic), Where<Gte<Severity>>>>()
//!   .args(Severity::Warning)
//!   .await
//! ```
//!
//! `.args(...)` supplies typed runtime arguments for `Where` expressions. The
//! `()` filter selects all rows.
//!
//! Clone-on-write external queries use [`Cow<T>`](Cow) and run through a
//! synchronous closure while the live world is locked:
//!
//! ```text
//! bowl.scoop::<Query<(Entity, Cow<FileText>), Where<Eq<FilePath>>>>()
//!   .args(FilePath(path))
//!   .for_each(|(_entity, text)| text.apply_delta(delta))
//!   .await
//! ```
//!
//! `Cow<T>` requires `T: Clone` so live updates can preserve immutable
//! snapshots with clone-on-write storage.
//!
//! Scoped external mutation uses [`Mut<T>`](Mut). A `Mut<T>` handle is inert
//! until `.with_original(...)` or `.with_latest(...)` runs a synchronous
//! closure, so ordinary async code cannot hold live mutable access across
//! `.await`.
//!
//! Inside systems, mutation uses [`MutRef<'_, T>`](MutRef) instead: the
//! scheduler grants the invocation exclusive row access, so the system gets a
//! plain in-place `&mut T` and revision bookkeeping happens when the
//! invocation commits.
//!
//! Evaluation is generation based:
//!
//! ```text
//! pending inputs
//!      |
//!      v
//! +------------+      immutable       +-------------+
//! | World N    | ---> snapshot N ---> | systems run |
//! +------------+                      +-------------+
//!      ^                                    |
//!      |                                    v
//! +------------+      buffered        +-------------+
//! | World N+1  | <--- commands <----- | outputs     |
//! +------------+                      +-------------+
//! ```
//!
//! `Bowl` is an internally shared handle. Clone it into tasks instead of
//! wrapping it in another `Arc`. Internally, only one evaluation runner is
//! active at a time; concurrent readers subscribe to the same in-flight
//! generation instead of starting duplicate work.
//!
//! Evaluation normally drives until the bowl settles. [`CommitLimit`] is a
//! configurable guardrail for accidental non-convergence; set it to
//! `CommitLimit::None` when a caller intentionally wants to drive an
//! open-ended system and handle cancellation externally.
//!
//! Systems registered with [`Bowl::add_system`] are async functions. The first
//! implementation uses local async concurrency: systems and invalid query rows
//! are polled together, but they are not spawned onto worker threads.
//!
//! This crate is intentionally small right now and is the primary runtime for
//! the prototype.

mod bowl;
mod commands;
mod component;
mod entity;
mod query;
mod system;
mod world;

pub use bowl::{
    BoundEntity, Bowl, BowlEntity, Bundle, CommitLimit, ExplainReport, ExternalScoop,
    InsertBuilder, InsertedEntity, RemoveBuilder, ScoopBuilder, TakeBundle, TakeError,
};
pub use commands::Commands;
pub use component::{
    Component, ComponentHookContext, DerivedFrom, RelationshipEdge, RelationshipRetraction,
    RelationshipTarget, Singleton, hash_component, relationship_retractions_for,
};
pub use entity::Entity;
pub use macros::Component;
pub use query::{
    And, ArgBundle, Cow, CowQueryParam, EntityMutResult, Eq, ExternalFilter, ExternalQueryFilter,
    FilterExpr, Gte, In, Mut, MutRef, MutResult, Named, Not, Or, Query, QueryFilter, QueryParam,
    QueryResult, View, Where, With, Without,
};
pub use system::{
    IntoSystem, Phase, SystemCallback, SystemExt, WorldMetaView, cleanup_stale_derived, insert_on,
};
#[doc(hidden)]
pub use world::ComponentRef;

pub use macros::SystemParam;

/// Support surface for the `#[derive(SystemParam)]` macro. Not public API.
#[doc(hidden)]
pub mod __derive {
    pub use crate::query::{Access, Dep, GuardStore};
    pub use crate::system::SystemParam;
    pub use crate::world::Snapshot;
    pub use crate::{Bowl, Commands, Entity};
}
