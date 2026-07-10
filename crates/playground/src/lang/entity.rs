//! The language-entity contract.
//!
//! A language entity is a vertical slice of one language concept: a single
//! file co-locates the entity's fact components, how they lower from syntax,
//! the checks that validate them, and how they present to services.
//!
//! The stage traits below form the coverage contract. [`register_entity`]
//! bounds on every stage, so a new entity does not compile until it has
//! declared each one — where a stage does not apply, the declaration is an
//! explicit no-effect impl (see `Document`).
//!
//! Dispatch is hand-written and exhaustive instead of registry-driven:
//! grammar rules are claimed in `entities::lower_rule` (a `match` on `Rule`,
//! so a new grammar rule fails to compile until an entity claims it).
//! Service answers flow through the candidate-fact pipeline instead of an
//! aggregating system: each entity registers its own request-answering
//! systems (see [`HoverStage`]) and a service finalizer picks the winning
//! candidate by priority (`service::hover`).

use bowl::{Bowl, Commands, DerivedFrom, Entity};
use tracing::info;

use crate::lang::{
    entities::{
        definition::AstDef,
        import::ImportDecl,
        namespace::{NamespaceDecl, NamespacePath},
    },
    facts::BelongsToFile,
    grammar::parser::{CstData, NodeRef},
};

/// Everything the lowering walk may emit — the shared output declaration
/// for [`LowerStage`] and `generate_ast` (spec/declared-outputs.md).
pub(crate) type AstFacts = (
    AstDef,
    ImportDecl,
    NamespaceDecl,
    NamespacePath,
    BelongsToFile,
    DerivedFrom,
);

pub(crate) trait LanguageEntity {
    const NAME: &'static str;

    /// Register the entity's derivation and check systems on the bowl.
    async fn register(db: &Bowl);
}

/// Context handed to [`LowerStage::lower`] for one owned CST rule node.
pub(crate) struct LowerCtx<'a> {
    pub(crate) cst: &'a CstData,
    pub(crate) source: &'a str,
    pub(crate) file: Entity,
    /// Fully qualified path of the enclosing namespace, if any. The walk in
    /// `entities` scopes it when descending into a namespace body.
    pub(crate) namespace: Option<String>,
}

/// Syntax stage: lower an owned CST rule node into fact components.
/// Rule ownership is assigned in `entities::lower_rule`.
pub(crate) trait LowerStage: LanguageEntity {
    fn lower(ctx: &LowerCtx<'_>, node: NodeRef, commands: &mut Commands<AstFacts>);
}

/// Service stage: register systems that answer hover requests by inserting
/// `HoverCandidate` facts (see `service::hover` for the pipeline). Each
/// entity's systems read only the enriched request plus the entity's own
/// facts, so param lists stay small no matter how many entities exist.
/// Entities without hover behavior register nothing (explicit empty impl).
pub(crate) trait HoverStage: LanguageEntity {
    async fn register_hover(db: &Bowl);
}

/// The compile-time coverage contract: an entity only registers once it has
/// declared every stage.
pub(crate) async fn register_entity<E>(db: &Bowl)
where
    E: LanguageEntity + LowerStage + HoverStage,
{
    info!(entity = E::NAME, "register language entity");
    E::register(db).await;
    E::register_hover(db).await;
}
