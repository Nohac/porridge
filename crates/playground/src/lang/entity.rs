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
//! so a new grammar rule fails to compile until an entity claims it) and
//! hover arbitration asks each entity in turn in `service::hover`. The trade
//! for skipping registries is that the shared contexts below name concrete
//! entity facts; an entity that grows new service behavior may extend them.

use bowl::{Bowl, Commands, Entity};
use tracing::info;

use crate::lang::{
    entities::{
        definition::AstDef,
        import::{ImportDecl, SystemImportDb},
        namespace::QualifiedName,
    },
    facts::BelongsToFile,
    grammar::parser::{CstData, NodeRef},
};

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
    fn lower(ctx: &LowerCtx<'_>, node: NodeRef, commands: &mut Commands);
}

/// Context handed to [`HoverStage::hover`] for one hover request.
pub(crate) struct HoverCtx<'a> {
    pub(crate) file: Entity,
    pub(crate) offset: usize,
    pub(crate) word: Option<&'a str>,
    pub(crate) defs: &'a [(Entity, &'a AstDef)],
    pub(crate) imports: &'a [(Entity, &'a BelongsToFile, &'a ImportDecl)],
    pub(crate) known_imports: Option<&'a SystemImportDb>,
    pub(crate) qualified: &'a [(Entity, &'a QualifiedName)],
}

/// Service stage: contribute hover content for a position. Return `None`
/// when the entity has nothing to say; the service supplies the fallback.
pub(crate) trait HoverStage: LanguageEntity {
    fn hover(ctx: &HoverCtx<'_>) -> Option<String>;
}

/// The compile-time coverage contract: an entity only registers once it has
/// declared every stage.
pub(crate) async fn register_entity<E>(db: &Bowl)
where
    E: LanguageEntity + LowerStage + HoverStage,
{
    info!(entity = E::NAME, "register language entity");
    E::register(db).await;
}
