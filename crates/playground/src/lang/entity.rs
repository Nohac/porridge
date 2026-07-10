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

use bowl::{Commands, Entity, Registrar};
use tracing::info;

use crate::lang::{
    grammar::parser::{CstData, NodeRef},
    schema::lang_schema,
};

/// Everything the lowering walk may emit — the shared output declaration
/// for [`LowerStage`] and `generate_ast`: a subset selection out of the
/// bowl schema, where the shapes are defined (spec/declared-outputs.md).
pub(crate) type AstFacts = (
    lang_schema::AstDef,
    lang_schema::Import,
    lang_schema::Namespace,
);

pub(crate) trait LanguageEntity {
    const NAME: &'static str;

    /// Register the entity's derivation and check systems.
    fn register(reg: &mut Registrar<'_>);
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
    fn register_hover(reg: &mut Registrar<'_>);
}

/// The compile-time coverage contract: an entity only registers once it has
/// declared every stage.
pub(crate) fn register_entity<E>(reg: &mut Registrar<'_>)
where
    E: LanguageEntity + LowerStage + HoverStage,
{
    info!(entity = E::NAME, "register language entity");
    E::register(reg);
    E::register_hover(reg);
}
