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

pub use bowl::{BoundEntity, Bowl, Bundle, InsertedEntity, TakeBundle, TakeError};
pub use commands::Commands;
pub use component::{Component, ComponentHookContext, Singleton, hash_component};
pub use entity::Entity;
pub use macros::Component;
pub use query::{Query, QueryFilter, QueryParam, QueryResult, View, With};
pub use system::{CompleteCallback, IntoSystem, Phase, SystemExt, insert_on};
