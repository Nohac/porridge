//! Definition entity: named definitions (functions, types), the definition
//! index, and the duplicate-name check.

use std::fmt;

use bowl::{
    Bowl, Commands, Component, DerivedFrom, Entity, Phase, Query, Singleton, SystemExt, View,
    With,
};
use tracing::info;

use crate::lang::{
    entities::{first_token_text, namespace::NamespacePath, node_span},
    entity::{HoverStage, LanguageEntity, LowerCtx, LowerStage},
    facts::{AstAvailable, BelongsToFile, Severity, Span, emit_diagnostic},
    grammar::{lexer::Token, parser::NodeRef, parser::Rule},
    service::{HoverCandidate, HoverRequest, HoverWord, priority},
};

#[derive(Debug, Component, Hash)]
#[component(hash)]
pub(crate) enum AstDef {
    Function(FunctionDef),
    Type(TypeDef),
}

impl AstDef {
    pub(crate) fn name(&self) -> &str {
        match self {
            AstDef::Function(def) => &def.name,
            AstDef::Type(def) => &def.name,
        }
    }

    pub(crate) fn kind(&self) -> DefKind {
        match self {
            AstDef::Function(_) => DefKind::Function,
            AstDef::Type(_) => DefKind::Type,
        }
    }

    pub(crate) fn span(&self) -> Span {
        match self {
            AstDef::Function(def) => def.span,
            AstDef::Type(def) => def.span,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DefKind {
    Function,
    Type,
}

impl fmt::Display for DefKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DefKind::Function => f.write_str("function"),
            DefKind::Type => f.write_str("type"),
        }
    }
}

#[derive(Debug, Hash)]
pub(crate) struct FunctionDef {
    pub(crate) name: String,
    pub(crate) span: Span,
}

#[derive(Debug, Hash)]
pub(crate) struct TypeDef {
    pub(crate) name: String,
    pub(crate) span: Span,
}

/// Fingerprint of the full definition set, maintained by `index_defs`.
/// Checks that must react to *other* definitions appearing or disappearing
/// take this singleton as a tracked input: its revision moves only when the
/// set actually changes, so idempotent reruns invalidate nothing.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct DefIndex(Vec<(String, Option<String>, u64)>);

pub(crate) struct Definition;

impl LanguageEntity for Definition {
    const NAME: &'static str = "definition";

    async fn register(db: &Bowl) {
        db.add_system(index_defs).await;
        db.add_system(check_duplicate_defs).await;
    }
}

impl LowerStage for Definition {
    fn lower(ctx: &LowerCtx<'_>, node: NodeRef, commands: &mut Commands) {
        let Some(name) = first_token_text(ctx.cst, ctx.source, node, Token::Name) else {
            return;
        };

        let span = node_span(ctx.cst, node);
        let def = if ctx.cst.match_rule(node, Rule::FunctionDef) {
            AstDef::Function(FunctionDef { name, span })
        } else {
            AstDef::Type(TypeDef { name, span })
        };

        // Definitions inside a namespace carry its path as their membership
        // join key (see `namespace::qualify_members`).
        match &ctx.namespace {
            Some(path) => commands.insert((
                DerivedFrom::new(ctx.file),
                BelongsToFile(ctx.file),
                def,
                NamespacePath(path.clone()),
            )),
            None => commands.insert((DerivedFrom::new(ctx.file), BelongsToFile(ctx.file), def)),
        };
    }
}

impl HoverStage for Definition {
    async fn register_hover(db: &Bowl) {
        db.add_system(hover_definitions.run_during(Phase::Complete))
            .await;
    }
}

/// Answers hover requests whose word names a definition.
async fn hover_definitions(
    query: Query<(Entity, &HoverWord), With<HoverRequest>>,
    defs: View<'_, (Entity, &AstDef)>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    let (request, word) = query.item();

    let Some((definition, def)) = defs.iter().find(|(_, def)| def.name() == word.0) else {
        return;
    };

    commands.insert((
        DerivedFrom::new(request),
        HoverCandidate {
            request,
            priority: priority::NAME,
            text: format!(
                "`{}` is a {} definition on entity {}",
                word.0,
                def.kind(),
                definition.raw()
            ),
        },
    ));
}

/// Aggregate the definition set into the `DefIndex` singleton after each
/// wave where the AST regenerated (the `AstAvailable` gate marker). Entries
/// carry the namespace so definitions moving between namespaces change the
/// fingerprint too.
async fn index_defs(
    _: Query<Entity, With<AstAvailable>>,
    defs: View<'_, (Entity, &AstDef)>,
    paths: View<'_, (Entity, &NamespacePath)>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    let namespaces = paths.iter().collect::<Vec<_>>();
    let mut entries = defs
        .iter()
        .map(|(entity, def)| {
            let namespace = namespaces
                .iter()
                .find(|(owner, _)| *owner == entity)
                .map(|(_, path)| path.0.clone());
            (def.name().to_string(), namespace, entity.raw())
        })
        .collect::<Vec<_>>();
    entries.sort();

    info!(defs = entries.len(), "index_defs");
    commands.insert((Singleton::<DefIndex>::new(), DefIndex(entries)));
}

/// The `DefIndex` query keeps this check honest: the `View` of other
/// definitions contributes no memo deps, so without a tracked input over the
/// definition *set*, a row would never rerun when an unrelated definition is
/// added or removed — a surviving duplicate could go unreported.
pub(crate) async fn check_duplicate_defs(
    query: Query<(Entity, &AstDef)>,
    _index: Query<(Entity, &DefIndex)>,
    defs: View<'_, (Entity, &AstDef)>,
    paths: View<'_, (Entity, &NamespacePath)>,
    mut commands: Commands,
) {
    let (entity, def) = query.item();

    crate::short_sleep().await;

    info!(entity = entity.raw(), "check_duplicate_defs");

    // Duplicates are scoped: two definitions collide only within the same
    // namespace (or both at the top level).
    let namespaces = paths.iter().collect::<Vec<_>>();
    let namespace_of = |target: Entity| {
        namespaces
            .iter()
            .find(|(owner, _)| *owner == target)
            .map(|(_, path)| path.0.as_str())
    };

    let scope = namespace_of(entity);
    let Some((previous, previous_def)) = defs.iter().find(|(other, other_def)| {
        *other < entity && other_def.name() == def.name() && namespace_of(*other) == scope
    }) else {
        return;
    };

    emit_diagnostic(
        &mut commands,
        DerivedFrom::many([entity, previous]),
        Severity::Error,
        format!(
            "duplicate definition `{}`; previous {} is entity {}",
            def.name(),
            previous_def.kind(),
            previous.raw()
        ),
    );
}
