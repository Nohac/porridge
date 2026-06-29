use crate::lang::grammar::{
    AstAvailable, AstDef, Diagnostic, ImportDecl, Severity, SystemImportDb,
};
use bowl::{Commands, Entity, Query, View};

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

pub(crate) async fn check_imports(
    query: Query<(Entity, &ImportDecl)>,
    system_imports: View<'_, &SystemImportDb>,
    mut commands: Commands,
) {
    println!("check_imports");
    let (import, import_decl) = query.item();
    let system = system_imports.iter().next().unwrap();

    if !system.0.contains(&import_decl.path) {
        emit_diagnostic(
            &mut commands,
            import,
            Severity::Warning,
            format!("unknown import `{}`", import_decl.path),
        );
    }
}

pub(crate) async fn check_duplicate_defs(
    query: Query<(Entity, &AstDef)>,
    ast_available: View<'_, (Entity, &AstAvailable)>,
    defs: View<'_, (Entity, &AstDef)>,
    mut commands: Commands,
) {
    let (entity, def) = query.item();

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
