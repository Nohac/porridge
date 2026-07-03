mod lang;

use bowl::{
    Bowl, Commands, Entity, Gte, Mut, Phase, Query, Singleton, SystemExt, Where, With,
    cleanup_stale_derived,
};

use crate::lang::{
    analysis::{check_duplicate_defs, check_imports, generate_ast, parse_file},
    grammar::{
        AstAvailable, AstDef, CstAvailable, Diagnostic, Ephemeral, FilePath, FileText, HoverInfo,
        HoverRequest, Position, Severity, SystemImportDb,
    },
    service::hover_info,
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let db = Bowl::new();

    db.add_system(parse_file.on_settled(|mut commands: Commands| {
        commands.insert((Singleton::<CstAvailable>::new(), CstAvailable, Ephemeral));
    }))
    .await;
    db.add_system(generate_ast.on_settled(|mut commands: Commands| {
        commands.insert((Singleton::<AstAvailable>::new(), AstAvailable, Ephemeral));
    }))
    .await;
    db.add_system(check_imports.run_during(Phase::Complete))
        .await;
    db.add_system(check_duplicate_defs.run_during(Phase::Complete))
        .await;
    db.add_system(hover_info.run_during(Phase::Complete)).await;
    db.add_system(cleanup_stale_derived.run_during(Phase::Cleanup))
        .await;
    db.add_system(cleanup_ephemeral.run_during(Phase::Cleanup))
        .await;

    db.insert((
        Singleton::<SystemImportDb>::new(),
        SystemImportDb::default(),
    ))
    .await;

    println!("\nregister std.net import with Mut<SystemImportDb>");
    db.scoop::<Query<(Entity, Mut<SystemImportDb>)>>()
        .for_each(|(entity, imports)| {
            println!("mutating import database entity {}", entity.raw());
            imports.0.insert("std.net".to_string());
        })
        .await;

    db.insert((
        FilePath("main.porridge".to_string()),
        FileText(
            "import std.io\nimport std.net\nfn main() -> UserId { return 1; }\ntype UserId"
                .to_string(),
        ),
    ))
    .await;

    db.insert((
        FilePath("lib.porridge".to_string()),
        FileText("import std.fs\nstruct Widget {}\nfn main() { return 2; }".to_string()),
    ))
    .await;

    println!("query diagnostics");
    let diagnostics = db.scoop::<Query<(Entity, &Diagnostic)>>().await;
    for (entity, diagnostic) in diagnostics.collect() {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\ndefinitions");
    let definitions = db.scoop::<Query<(Entity, &AstDef)>>().await;
    for (entity, def) in definitions.collect() {
        println!(
            "entity {}: {} `{}` at {:?}",
            entity.raw(),
            def.kind(),
            def.name(),
            def.span()
        );
    }

    println!("query diagnostics again");
    let diagnostics = db.scoop::<Query<(Entity, &Diagnostic)>>().await;
    for (entity, diagnostic) in diagnostics.collect() {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\ndiagnostics at warning or above");
    let diagnostics = db
        .scoop::<Query<(Entity, &Diagnostic), Where<Gte<Severity>>>>()
        .arg(Severity::Warning)
        .await;
    for (entity, diagnostic) in diagnostics.collect() {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\nast available markers");
    let ast_available = db.scoop::<Query<(Entity, &AstAvailable)>>().await;
    for (entity, _) in ast_available.collect() {
        println!("entity {}", entity.raw());
    }

    println!("\nhover request");
    if let Ok(info) = db
        .insert((
            HoverRequest,
            FilePath("main.porridge".to_string()),
            Position {
                offset: "import std.io\nimport std.net\nfn ".len(),
            },
        ))
        .await
        .bind()
        .take::<HoverInfo>()
        .await
    {
        println!("{}", info.0);
    }

    db.insert((
        FilePath("foo.porridge".to_string()),
        FileText("import derp.fs\nstruct Widget {}\nfn other() { return 2; }".to_string()),
    ))
    .await;

    println!("\nhover request");
    if let Ok(info) = db
        .insert((
            HoverRequest,
            FilePath("foo.porridge".to_string()),
            Position {
                offset: "import derp.fs\nstruct Widget {}\nfn ".len(),
            },
        ))
        .await
        .bind()
        .take::<HoverInfo>()
        .await
    {
        println!("{}", info.0);
    }

    let hover_facts = db.scoop::<Query<(Entity, &HoverInfo)>>().await;
    println!("hover facts after request: {}", hover_facts.collect().len());
}

async fn cleanup_ephemeral(query: Query<Entity, With<Ephemeral>>, mut commands: Commands) {
    let entity = query.item();
    commands.remove(entity);
}
