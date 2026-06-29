mod lang;

use bowl::{Bowl, Entity, Ephemeral};

use crate::lang::{
    analysis::{check_duplicate_defs, check_imports, generate_ast, parse_file},
    grammar::{
        AstAvailable, AstDef, Diagnostic, FilePath, FileText, HoverInfo, HoverRequest, Position,
        Project, SystemImportDb,
    },
    service::hover_info,
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let db = Bowl::new();

    db.add_system(parse_file).await;
    db.add_system(generate_ast).await;
    db.add_system(check_imports).await;
    db.add_system(check_duplicate_defs).await;
    db.add_system(hover_info).await;

    db.insert((Project,)).await;
    db.insert((SystemImportDb::default(),)).await;

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
    let diagnostics = db.query::<(Entity, &Diagnostic)>().await;
    for (entity, diagnostic) in diagnostics.collect() {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\ndefinitions");
    let definitions = db.query::<(Entity, &AstDef)>().await;
    for (entity, def) in definitions.collect() {
        println!(
            "entity {}: {} `{}` at {:?}",
            entity.raw(),
            def.kind(),
            def.name(),
            def.span()
        );
    }

    db.insert((
        FilePath("foo.porridge".to_string()),
        FileText("import derp.fs\nstruct Widget {}\nfn other() { return 2; }".to_string()),
    ))
    .await;

    println!("query diagnostics again");
    let diagnostics = db.query::<(Entity, &Diagnostic)>().await;
    for (entity, diagnostic) in diagnostics.collect() {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\nast available markers");
    let ast_available = db.query::<(Entity, &AstAvailable)>().await;
    for (entity, _) in ast_available.collect() {
        println!("entity {}", entity.raw());
    }

    println!("\nhover request");
    let request = db
        .insert((
            Ephemeral,
            HoverRequest,
            FilePath("main.porridge".to_string()),
            Position {
                offset: "import std.io\nimport std.net\nfn ".len(),
            },
        ))
        .await;

    let hover = request.query::<(Entity, &HoverInfo)>().await;
    if let Some((_, info)) = hover.collect().into_iter().next() {
        println!("{}", info.0);
    }

    let hover_facts = db.query::<(Entity, &HoverInfo)>().await;
    println!("hover facts after request: {}", hover_facts.collect().len());
}
