//! The toy language, assembled from language entities.
//!
//! - `grammar/` — syntax only: lexer and lelwel-generated parser.
//! - `facts` — cross-cutting fact components (diagnostics, spans, markers).
//! - `entity` — the language-entity contract and its stage traits.
//! - `entities/` — one vertical slice per language concept.
//! - `service/` — request/response facts external callers drive.

pub(crate) mod entities;
pub(crate) mod entity;
pub(crate) mod facts;
pub(crate) mod grammar;
pub(crate) mod schema;
pub(crate) mod service;

use bowl::{Bowl, Phase, SystemExt, cleanup_stale_derived};

use crate::lang::{
    entities::{
        definition::Definition, document::Document, import::Import, namespace::Namespace,
    },
    entity::register_entity,
};

/// Assemble the language: every entity, the shared lowering walk, the
/// services, and the cleanup system the derived-fact conventions rely on.
/// The bowl is expected to be constructed over the language schema
/// (`Bowl::of::<schema::LangSchema>()`) so registration-time analyses can
/// consult the shapes.
pub(crate) async fn register_language(db: &Bowl) {
    register_entity::<Document>(db).await;
    register_entity::<Import>(db).await;
    register_entity::<Definition>(db).await;
    register_entity::<Namespace>(db).await;

    db.add_system(entities::generate_ast).await;

    service::register_services(db).await;

    db.add_system(cleanup_stale_derived.run_during(Phase::Settle))
        .await;
}
