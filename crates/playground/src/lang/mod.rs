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

use bowl::{Phase, Plugin, Registrar, Schema, ShapeDesc, SystemExt, cleanup_stale_derived};

use crate::lang::{
    entities::{
        definition::Definition, document::Document, import::Import, namespace::Namespace,
    },
    entity::register_entity,
};

/// The toy language as a bowl plugin: its schema fragment and every
/// entity, the shared lowering walk, the services, and the cleanup system
/// the derived-fact conventions rely on — shapes and systems travel
/// together, so installing the plugin cannot desync them.
pub(crate) struct LangPlugin;

impl Plugin for LangPlugin {
    fn shapes(&self) -> Vec<ShapeDesc> {
        schema::LangSchema::shapes()
    }

    fn build(&self, reg: &mut Registrar<'_>) {
        register_entity::<Document>(reg);
        register_entity::<Import>(reg);
        register_entity::<Definition>(reg);
        register_entity::<Namespace>(reg);

        reg.system(entities::generate_ast);

        service::register_services(reg);

        reg.system(cleanup_stale_derived.run_during(Phase::Settle));
    }
}
