//! Import entity: `import a.b` declarations, the known-import database, and
//! the check that flags imports the system does not know.

use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use bowl::{Bowl, Commands, Component, DerivedFrom, Entity, Phase, Query, SystemExt, View, With};
use tracing::info;

use crate::lang::{
    entities::{node_span, token_texts},
    entity::{AstFacts, HoverStage, LanguageEntity, LowerCtx, LowerStage},
    facts::{BelongsToFile, DiagnosticParts, DiagnosticsDemand, Severity, Span, emit_diagnostic},
    grammar::{lexer::Token, parser::NodeRef},
    service::{CandidateParts, HoverCandidate, HoverFile, HoverRequest, Position, RequestKey, priority},
};

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct ImportDecl {
    pub(crate) path: String,
    pub(crate) span: Span,
}

#[derive(Clone)]
pub(crate) struct SystemImportDb(pub(crate) HashSet<String>);

impl Component for SystemImportDb {
    fn fingerprint(&self) -> Option<u64> {
        let mut imports = self.0.iter().collect::<Vec<_>>();
        imports.sort();

        let mut hasher = DefaultHasher::new();
        imports.hash(&mut hasher);
        Some(hasher.finish())
    }
}

impl Default for SystemImportDb {
    fn default() -> Self {
        let mut imports = HashSet::new();
        imports.insert("std.io".to_string());
        Self(imports)
    }
}

pub(crate) struct Import;

impl LanguageEntity for Import {
    const NAME: &'static str = "import";

    async fn register(db: &Bowl) {
        db.add_system(check_imports).await;
    }
}

impl LowerStage for Import {
    fn lower(ctx: &LowerCtx<'_>, node: NodeRef, commands: &mut Commands<AstFacts>) {
        let names = token_texts(ctx.cst, ctx.source, node, Token::Name);
        if names.is_empty() {
            return;
        }

        commands.insert((
            DerivedFrom::new(ctx.file),
            BelongsToFile(ctx.file),
            ImportDecl {
                path: names.join("."),
                span: node_span(ctx.cst, node),
            },
        ));
    }
}

impl HoverStage for Import {
    async fn register_hover(db: &Bowl) {
        db.add_system(hover_imports.run_during(Phase::Complete)).await;
    }
}

/// Answers hover requests whose position falls inside an import declaration
/// of the request's file.
async fn hover_imports(
    query: Query<(Entity, &HoverFile, &Position), With<HoverRequest>>,
    imports: View<'_, (Entity, &BelongsToFile, &ImportDecl)>,
    import_db: View<'_, (Entity, &SystemImportDb)>,
    mut commands: Commands<(CandidateParts,)>,
) {
    crate::short_sleep().await;

    let (request, file, position) = query.item();

    let Some((_, _, import)) = imports.iter().find(|(_, belongs, import)| {
        belongs.0 == file.0
            && import.span.start <= position.offset
            && position.offset < import.span.end
    }) else {
        return;
    };

    let known = import_db
        .iter()
        .next()
        .is_some_and(|(_, imports)| imports.0.contains(&import.path));
    let text = if known {
        format!("`{}` is a known import", import.path)
    } else {
        format!("`{}` is an unknown import", import.path)
    };

    commands.insert((
        DerivedFrom::new(request),
        RequestKey(request),
        HoverCandidate {
            priority: priority::POSITION,
            text,
        },
    ));
}

pub(crate) async fn check_imports(
    _: Query<Entity, With<DiagnosticsDemand>>,
    query: Query<(Entity, &ImportDecl)>,
    system_imports: View<'_, (Entity, &SystemImportDb)>,
    mut commands: Commands<(DiagnosticParts,)>,
) {
    crate::short_sleep().await;

    info!("check_imports");
    let (import, import_decl) = query.item();
    let (system_entity, system) = system_imports.iter().next().unwrap();

    if !system.0.contains(&import_decl.path) {
        emit_diagnostic(
            &mut commands,
            DerivedFrom::many([import, system_entity]),
            Severity::Warning,
            format!("unknown import `{}`", import_decl.path),
        );
    }
}
