//! Language entities and the rule-ownership dispatch.
//!
//! `generate_ast` is the one generic CST walk: it visits every rule node
//! once and hands it to the entity that owns the rule. Ownership lives in
//! [`lower_rule`] — an exhaustive `match`, so adding a rule to `syntax.llw`
//! fails to compile here until an entity claims it or it is explicitly
//! listed as structural.

pub(crate) mod definition;
pub(crate) mod document;
pub(crate) mod import;
pub(crate) mod namespace;

use bowl::{Commands, Entity, Query};
use tracing::info;

use crate::lang::{
    entity::{AstFacts, LowerCtx, LowerStage},
    facts::Span,
    grammar::{
        lexer::Token,
        parser::{CstData, Node, NodeRef, Rule},
    },
};
use definition::Definition;
use document::{Document, FileText, ParsedFile};
use import::Import;
use namespace::Namespace;

fn lower_rule(ctx: &LowerCtx<'_>, rule: Rule, node: NodeRef, commands: &mut Commands<AstFacts>) {
    match rule {
        Rule::File => Document::lower(ctx, node, commands),
        Rule::ImportDecl => Import::lower(ctx, node, commands),
        Rule::NamespaceDecl => Namespace::lower(ctx, node, commands),
        Rule::FunctionDef | Rule::TypeDef | Rule::StructDef => {
            Definition::lower(ctx, node, commands)
        }
        // Structural rules: unowned; their content is consumed by the
        // entities owning their ancestors. Parse errors surface through the
        // document's parse diagnostics, not through lowering.
        Rule::Item
        | Rule::Path
        | Rule::Field
        | Rule::Param
        | Rule::ParamList
        | Rule::TypeRef
        | Rule::Block
        | Rule::Stmt
        | Rule::ReturnStmt
        | Rule::ExprStmt
        | Rule::Expr
        | Rule::Error => {}
    }
}

pub(crate) async fn generate_ast(
    query: Query<(Entity, &ParsedFile, &FileText)>,
    mut commands: Commands<AstFacts>,
) {
    let (file, parsed, text) = query.item();

    crate::short_sleep().await;

    info!(entity = file.raw(), "generate_ast");

    let ctx = LowerCtx {
        cst: &parsed.cst,
        source: &text.0,
        file,
        namespace: None,
    };
    walk(&ctx, NodeRef::ROOT, &mut commands);
}

fn walk(ctx: &LowerCtx<'_>, node: NodeRef, commands: &mut Commands<AstFacts>) {
    if let Node::Rule(rule, _) = ctx.cst.get(node) {
        lower_rule(ctx, rule, node, commands);

        // Namespace bodies scope everything beneath them: descend with the
        // fully qualified path in context so member entities pick it up as
        // their join key.
        if matches!(rule, Rule::NamespaceDecl) {
            if let Some((path, _)) = namespace::declared_path(ctx, node) {
                let scoped = LowerCtx {
                    cst: ctx.cst,
                    source: ctx.source,
                    file: ctx.file,
                    namespace: Some(path),
                };
                for child in ctx.cst.children(node) {
                    walk(&scoped, child, commands);
                }
                return;
            }
        }
    }

    for child in ctx.cst.children(node) {
        walk(ctx, child, commands);
    }
}

// CST helpers shared by entity lowerings.

pub(crate) fn token_texts(cst: &CstData, source: &str, node: NodeRef, token: Token) -> Vec<String> {
    let mut values = Vec::new();
    collect_token_texts(cst, source, node, token, &mut values);
    values
}

pub(crate) fn first_token_text(
    cst: &CstData,
    source: &str,
    node: NodeRef,
    token: Token,
) -> Option<String> {
    token_texts(cst, source, node, token).into_iter().next()
}

fn collect_token_texts(
    cst: &CstData,
    source: &str,
    node: NodeRef,
    token: Token,
    values: &mut Vec<String>,
) {
    if let Some(span) = cst.match_token(node, token) {
        values.push(source[span].to_string());
    }

    if matches!(cst.get(node), Node::Rule(..)) {
        for child in cst.children(node) {
            collect_token_texts(cst, source, child, token, values);
        }
    }
}

pub(crate) fn node_span(cst: &CstData, node: NodeRef) -> Span {
    let span = cst.span(node);
    Span {
        start: span.start,
        end: span.end,
    }
}
