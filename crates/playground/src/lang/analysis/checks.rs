use crate::lang::grammar::{
    AstAvailable, AstDef, BelongsToFile, Diagnostic, FilePath, ImportDecl, Severity, SourceFile,
    SystemImportDb,
};
use pipeline::{Commands, Entity, Query, View};

fn emit_diagnostic(
    commands: &mut Commands,
    entity: Entity,
    severity: Severity,
    message: impl Into<String>,
) {
    let mut entity = commands.entity(entity);
    entity.insert(severity);
    entity.insert(Diagnostic(message.into()));
}

pub(crate) fn check_imports(
    _: Query<(Entity, &AstAvailable)>,
    imports: View<(Entity, &ImportDecl, &BelongsToFile)>,
    system_imports: View<&SystemImportDb>,
    files: View<(Entity, &SourceFile)>,
    mut commands: Commands,
) {
    println!("check_imports");
    let system = system_imports.iter().next().unwrap();

    for (import, import_decl, f) in imports.iter() {
        if !system.0.contains(&import_decl.path) {
            let file = files
                .get(f.0)
                .map(|source| source.path.as_str())
                .unwrap_or("<unknown>");
            commands.entity(*import).insert(FilePath(file.to_string()));
            emit_diagnostic(
                &mut commands,
                *import,
                Severity::Warning,
                format!("unknown import `{}` in file {}", import_decl.path, file),
            );
        }
    }
}

pub(crate) fn check_duplicate_defs(
    Query((entity, def)): Query<(Entity, &AstDef)>,
    ast_available: View<(Entity, &AstAvailable)>,
    defs: View<(Entity, &AstDef)>,
    mut commands: Commands,
) {
    if ast_available.iter().next().is_none() {
        return;
    }

    println!("check_duplicate_defs({})", entity.raw());

    let Some((previous, previous_def)) = defs
        .iter()
        .find(|(other, other_def)| *other < entity && other_def.name() == def.name())
    else {
        return;
    };

    emit_diagnostic(
        &mut commands,
        entity,
        Severity::Error,
        format!(
            "duplicate definition `{}`; previous {} is entity {}",
            def.name(),
            previous_def.kind(),
            previous.raw()
        ),
    );
}
