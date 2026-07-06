//! Import entity: `import a.b` declarations, the known-import database, and
//! the check that flags imports the system does not know.

use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use bowl::{Bowl, Commands, Component, DerivedFrom, Entity, Query, View};
use tracing::info;

use crate::lang::{
    entities::{node_span, token_texts},
    entity::{HoverCtx, HoverStage, LanguageEntity, LowerCtx, LowerStage},
    facts::{BelongsToFile, Severity, Span, emit_diagnostic},
    grammar::{lexer::Token, parser::NodeRef},
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
    fn lower(ctx: &LowerCtx<'_>, node: NodeRef, commands: &mut Commands) {
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
    fn hover(ctx: &HoverCtx<'_>) -> Option<String> {
        let (_, _, import) = ctx.imports.iter().find(|(_, belongs, import)| {
            belongs.0 == ctx.file && import.span.start <= ctx.offset && ctx.offset < import.span.end
        })?;

        let known = ctx
            .known_imports
            .is_some_and(|imports| imports.0.contains(&import.path));
        Some(if known {
            format!("`{}` is a known import", import.path)
        } else {
            format!("`{}` is an unknown import", import.path)
        })
    }
}

pub(crate) async fn check_imports(
    query: Query<(Entity, &ImportDecl)>,
    system_imports: View<'_, (Entity, &SystemImportDb)>,
    mut commands: Commands,
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
