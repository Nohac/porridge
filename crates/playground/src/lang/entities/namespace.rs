//! Namespace entity: `namespace a.b { ... }` declarations, the namespace
//! membership key, and the join-driven qualified names of member definitions.

use bowl::{Bowl, Commands, Component, DerivedFrom, Entity, Eq, Query, Where};
use tracing::info;

use crate::lang::{
    entities::{definition::AstDef, node_span, token_texts},
    entity::{HoverCtx, HoverStage, LanguageEntity, LowerCtx, LowerStage},
    facts::{BelongsToFile, Span},
    grammar::{
        lexer::Token,
        parser::{CstData, NodeRef, Rule},
    },
};

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct NamespaceDecl {
    pub(crate) path: String,
    pub(crate) span: Span,
}

/// Join key for namespace membership. Present on the namespace entity (its
/// own path) and on every definition lowered inside the namespace body, so
/// bound `Where<Eq<NamespacePath>>` queries pair namespaces with exactly
/// their members.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct NamespacePath(pub(crate) String);

/// Derived per (namespace, member definition) pair by `qualify_members`.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct QualifiedName {
    pub(crate) definition: Entity,
    pub(crate) name: String,
}

pub(crate) struct Namespace;

impl LanguageEntity for Namespace {
    const NAME: &'static str = "namespace";

    async fn register(db: &Bowl) {
        db.add_system(qualify_members).await;
    }
}

impl LowerStage for Namespace {
    fn lower(ctx: &LowerCtx<'_>, node: NodeRef, commands: &mut Commands) {
        let Some((path, path_node)) = declared_path(ctx, node) else {
            return;
        };

        commands.insert((
            DerivedFrom::new(ctx.file),
            BelongsToFile(ctx.file),
            NamespaceDecl {
                path: path.clone(),
                span: node_span(ctx.cst, path_node),
            },
            NamespacePath(path),
        ));
    }
}

impl HoverStage for Namespace {
    // Namespaces answer for their members: a definition with a qualified
    // name is described by that name, shadowing the definition entity's own
    // plainer answer further down the arbitration chain.
    fn hover(ctx: &HoverCtx<'_>) -> Option<String> {
        let word = ctx.word?;
        let (definition, def) = ctx.defs.iter().find(|(_, def)| def.name() == word)?;
        let qualified = ctx
            .qualified
            .iter()
            .find(|(_, qualified)| qualified.definition == *definition)?;

        Some(format!(
            "`{word}` is a {} definition on entity {}, known as `{}`",
            def.kind(),
            definition.raw(),
            qualified.1.name
        ))
    }
}

/// The fully qualified path a `namespace_decl` node declares, honoring the
/// enclosing namespace in `ctx`, plus the path node for span reporting.
pub(crate) fn declared_path(ctx: &LowerCtx<'_>, node: NodeRef) -> Option<(String, NodeRef)> {
    let path_node = first_rule_child(ctx.cst, node, Rule::Path)?;
    let names = token_texts(ctx.cst, ctx.source, path_node, Token::Name);
    if names.is_empty() {
        return None;
    }

    let declared = names.join(".");
    let full = match &ctx.namespace {
        Some(parent) => format!("{parent}.{declared}"),
        None => declared,
    };
    Some((full, path_node))
}

fn first_rule_child(cst: &CstData, node: NodeRef, rule: Rule) -> Option<NodeRef> {
    cst.children(node).find(|child| cst.match_rule(*child, rule))
}

/// Join: one invocation per (namespace, member definition) pair. Members are
/// definitions whose `NamespacePath` equals the namespace's — the bound
/// `Where<Eq<..>>` binds to the namespace query's key automatically.
pub(crate) async fn qualify_members(
    namespaces: Query<(Entity, &NamespaceDecl, &NamespacePath)>,
    members: Query<(Entity, &AstDef), Where<Eq<NamespacePath>>>,
    mut commands: Commands,
) {
    let (namespace, decl, _path) = namespaces.item();
    let (definition, def) = members.item();

    crate::short_sleep().await;

    info!(
        namespace = namespace.raw(),
        definition = definition.raw(),
        "qualify_members"
    );

    commands.insert((
        DerivedFrom::many([namespace, definition]),
        QualifiedName {
            definition,
            name: format!("{}.{}", decl.path, def.name()),
        },
    ));
}
