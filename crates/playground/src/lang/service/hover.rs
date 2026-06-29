use crate::lang::grammar::{
    AstAvailable, AstDef, FilePath, FileText, HoverInfo, HoverRequest, Position,
};
use bowl::{Commands, Entity, Query, View, With};

pub(crate) async fn hover_info(
    _: Query<Entity, With<AstAvailable>>,
    query: Query<(Entity, &FilePath, &Position), With<HoverRequest>>,
    files: View<'_, (Entity, &FilePath, &FileText)>,
    defs: View<'_, (Entity, &AstDef)>,
    mut commands: Commands,
) {
    println!("hover_info");
    let (request, path, position) = query.item();

    let Some((_file, _path, text)) = files.iter().find(|(_, file_path, _)| *file_path == path)
    else {
        commands
            .entity(request)
            .insert(HoverInfo("unknown file".to_string()));
        return;
    };

    let Some(word) = word_at(&text.0, position.offset) else {
        commands
            .entity(request)
            .insert(HoverInfo("no symbol at position".to_string()));
        return;
    };

    let Some((definition, def)) = defs.iter().find(|(_, def)| def.name() == word) else {
        commands
            .entity(request)
            .insert(HoverInfo(format!("unresolved symbol `{word}`")));
        return;
    };

    commands.entity(request).insert(HoverInfo(format!(
        "`{word}` is a {} definition on entity {}",
        def.kind(),
        definition.raw()
    )));
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
