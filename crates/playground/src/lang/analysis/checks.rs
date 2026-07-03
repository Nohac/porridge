use crate::lang::grammar::{AstDef, Diagnostic, ImportDecl, Severity, SystemImportDb};
use bowl::{Commands, DerivedFrom, Entity, Query, View};

fn emit_diagnostic(
    commands: &mut Commands,
    derived_from: DerivedFrom,
    severity: Severity,
    message: impl Into<String>,
) {
    commands.insert((derived_from, severity, Diagnostic(message.into())));
}

pub(crate) async fn check_imports(
    query: Query<(Entity, &ImportDecl)>,
    system_imports: View<'_, (Entity, &SystemImportDb)>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    println!("check_imports");
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

pub(crate) async fn check_duplicate_defs(
    query: Query<(Entity, &AstDef)>,
    defs: View<'_, (Entity, &AstDef)>,
    mut commands: Commands,
) {
    let (entity, def) = query.item();

    crate::short_sleep().await;

    println!("check_duplicate_defs({})", entity.raw());

    let Some((previous, previous_def)) = defs
        .iter()
        .find(|(other, other_def)| *other < entity && other_def.name() == def.name())
    else {
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
