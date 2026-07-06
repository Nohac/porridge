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
pub(crate) mod service;

use bowl::{
    Bowl, Commands, Entity, Phase, Query, Singleton, SystemExt, With, cleanup_stale_derived,
};

use crate::lang::{
    entities::{definition::Definition, document::Document, import::Import},
    entity::register_entity,
    facts::{AstAvailable, Ephemeral},
};

/// Assemble the language: every entity, the shared lowering walk, the
/// services, and the cleanup systems the derived-fact conventions rely on.
pub(crate) async fn register_language(db: &Bowl) {
    register_entity::<Document>(db).await;
    register_entity::<Import>(db).await;
    register_entity::<Definition>(db).await;

    db.add_system(entities::generate_ast.on_settled(|mut commands: Commands| {
        commands.insert((Singleton::<AstAvailable>::new(), AstAvailable, Ephemeral));
    }))
    .await;

    service::register_services(db).await;

    db.add_system(cleanup_stale_derived.run_during(Phase::Cleanup))
        .await;
    db.add_system(cleanup_ephemeral.run_during(Phase::Cleanup))
        .await;
}

async fn cleanup_ephemeral(query: Query<Entity, With<Ephemeral>>, mut commands: Commands) {
    crate::short_sleep().await;

    let entity = query.item();
    commands.remove(entity);
}
