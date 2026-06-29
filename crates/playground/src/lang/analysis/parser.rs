use crate::lang::grammar::{
    AstDef, BelongsToFile, Diagnostic, FileText, FunctionDef, ImportDecl, ParsedFile, Span,
    TypeDef,
    lexer::Token,
    parser::{CstData, Node, NodeRef, Parser, Rule},
};
use bowl::{Commands, Entity, Query};

pub(crate) async fn parse_file(query: Query<(Entity, &FileText)>, mut commands: Commands) {
    let (file, text) = query.item();

    println!("parse_file({})", file.raw());

    let mut diags = Vec::new();
    let cst = Parser::new(&text.0, &mut diags).parse(&mut diags);

    commands.entity(file).insert(ParsedFile {
        cst: cst.into_data(),
    });

    for diag in diags {
        commands.entity(file).insert(Diagnostic(diag.message));
    }
}

pub(crate) async fn generate_ast(
    query: Query<(Entity, &ParsedFile, &FileText)>,
    mut commands: Commands,
) {
    let (file, parsed, text) = query.item();

    println!("generate_ast({})", file.raw());

    for fact in ast_facts(&parsed.cst, &text.0) {
        match fact {
            AstFact::Import(import) => commands.insert((BelongsToFile(file), import)),
            AstFact::Def(def) => commands.insert((BelongsToFile(file), def)),
        }
    }
}

enum AstFact {
    Import(ImportDecl),
    Def(AstDef),
}

fn ast_facts(cst: &CstData, source: &str) -> Vec<AstFact> {
    let mut facts = Vec::new();

    facts.extend(
        rule_nodes(cst, Rule::ImportDecl)
            .into_iter()
            .filter_map(|node| {
                let names = token_texts(cst, source, node, Token::Name);
                (!names.is_empty()).then(|| {
                    AstFact::Import(ImportDecl {
                        path: names.join("."),
                        span: span(cst, node),
                    })
                })
            }),
    );

    facts.extend(
        rule_nodes(cst, Rule::FunctionDef)
            .into_iter()
            .filter_map(|node| {
                first_token_text(cst, source, node, Token::Name).map(|name| {
                    AstFact::Def(AstDef::Function(FunctionDef {
                        name,
                        span: span(cst, node),
                    }))
                })
            }),
    );

    facts.extend(
        rule_nodes(cst, Rule::TypeDef)
            .into_iter()
            .chain(rule_nodes(cst, Rule::StructDef))
            .filter_map(|node| {
                first_token_text(cst, source, node, Token::Name).map(|name| {
                    AstFact::Def(AstDef::Type(TypeDef {
                        name,
                        span: span(cst, node),
                    }))
                })
            }),
    );

    facts
}

fn rule_nodes(cst: &CstData, rule: Rule) -> Vec<NodeRef> {
    let mut nodes = Vec::new();
    collect_rule_nodes(cst, NodeRef::ROOT, rule, &mut nodes);
    nodes
}

fn collect_rule_nodes(cst: &CstData, node: NodeRef, rule: Rule, nodes: &mut Vec<NodeRef>) {
    if cst.match_rule(node, rule) {
        nodes.push(node);
    }

    for child in cst.children(node) {
        collect_rule_nodes(cst, child, rule, nodes);
    }
}

fn token_texts(cst: &CstData, source: &str, node: NodeRef, token: Token) -> Vec<String> {
    let mut values = Vec::new();
    collect_token_texts(cst, source, node, token, &mut values);
    values
}

fn first_token_text(cst: &CstData, source: &str, node: NodeRef, token: Token) -> Option<String> {
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

fn span(cst: &CstData, node: NodeRef) -> Span {
    let span = cst.span(node);
    Span {
        start: span.start,
        end: span.end,
    }
}
