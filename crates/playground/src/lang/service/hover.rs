//! Hover service: request components plus arbitration across the entities.

use bowl::{Commands, Component, Entity, Query, View, With};
use tracing::info;

use crate::lang::{
    entities::{
        definition::{AstDef, Definition},
        document::{Document, FilePath, FileText},
        import::{Import, ImportDecl, SystemImportDb},
    },
    entity::{HoverCtx, HoverStage},
    facts::{AstAvailable, BelongsToFile},
};

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverRequest;

#[derive(Debug, Component, Hash)]
#[component(hash)]
pub(crate) struct Position {
    pub(crate) offset: usize,
}

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverInfo(pub(crate) String);

pub(crate) async fn hover_info(
    _: Query<Entity, With<AstAvailable>>,
    query: Query<(Entity, &FilePath, &Position), With<HoverRequest>>,
    files: View<'_, (Entity, &FilePath, &FileText)>,
    defs: View<'_, (Entity, &AstDef)>,
    imports: View<'_, (Entity, &BelongsToFile, &ImportDecl)>,
    import_db: View<'_, (Entity, &SystemImportDb)>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    info!("hover_info");
    let (request, path, position) = query.item();

    let Some((file, _path, text)) = files.iter().find(|(_, file_path, _)| *file_path == path)
    else {
        commands
            .entity(request)
            .insert(HoverInfo("unknown file".to_string()));
        return;
    };

    let defs = defs.iter().collect::<Vec<_>>();
    let imports = imports.iter().collect::<Vec<_>>();
    let ctx = HoverCtx {
        file,
        offset: position.offset,
        word: word_at(&text.0, position.offset),
        defs: &defs,
        imports: &imports,
        known_imports: import_db.iter().next().map(|(_, imports)| imports),
    };

    // Exhaustive arbitration: ask every entity, most specific first. An
    // entity that grows hover behavior gets added here.
    let answer = Import::hover(&ctx)
        .or_else(|| Definition::hover(&ctx))
        .or_else(|| Document::hover(&ctx));

    let message = answer.unwrap_or_else(|| match ctx.word {
        Some(word) => format!("unresolved symbol `{word}`"),
        None => "no symbol at position".to_string(),
    });
    commands.entity(request).insert(HoverInfo(message));
}

fn word_at(text: &str, offset: usize) -> Option<&str> {
    if offset >= text.len() || !text.is_char_boundary(offset) {
        return None;
    }

    let is_word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let bytes = text.as_bytes();

    if !is_word(bytes[offset]) {
        return None;
    }

    let start = bytes[..offset]
        .iter()
        .rposition(|byte| !is_word(*byte))
        .map_or(0, |index| index + 1);
    let end = bytes[offset..]
        .iter()
        .position(|byte| !is_word(*byte))
        .map_or(text.len(), |index| offset + index);

    Some(&text[start..end])
}
