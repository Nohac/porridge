mod lang;

use pipeline::{Db, Entity, Ephemeral, SystemExt, Take};

use crate::lang::{
    analysis::{ast_available, check_duplicate_defs, check_imports, generate_ast, parse_file},
    grammar::{
        AstAvailable, AstDef, Diagnostic, FilePath, FileText, HoverInfo, HoverRequest, Position,
        Project, SystemImportDb,
    },
    service::hover_info,
};

fn main() {
    let mut db = Db::new();
    let project = db.insert((Project,)).entity();

    db.add_system(parse_file);
    db.add_system(generate_ast.on_complete(ast_available(project)));
    db.add_system(check_imports);
    db.add_system(check_duplicate_defs);
    db.add_system(hover_info);

    db.insert((SystemImportDb::default(),));

    db.insert((
        FilePath("main.porridge".to_string()),
        FileText(
            "import std.io\nimport std.net\nfn main() -> UserId { return 1; }\ntype UserId"
                .to_string(),
        ),
    ));

    db.insert((
        FilePath("lib.porridge".to_string()),
        FileText("import std.fs\nstruct Widget {}\nfn main() { return 2; }".to_string()),
    ));

    println!("query diagnostics");
    for (entity, diagnostic) in db.query::<(Entity, &Diagnostic)>().collect() {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\ndefinitions");
    for (entity, def) in db.query::<(Entity, &AstDef)>().collect() {
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
    ));

    println!("query diagnostics again");
    for (entity, diagnostic) in db.query::<(Entity, &Diagnostic)>().collect() {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\nast available markers");
    for (entity, _) in db.query::<(Entity, &AstAvailable)>().collect() {
        println!("entity {}", entity.raw());
    }

    println!("\nhover request");
    let hover = db
        .insert((
            Ephemeral,
            HoverRequest,
            FilePath("main.porridge".to_string()),
            Position {
                offset: "import std.io\nimport std.net\nfn ".len(),
            },
        ))
        .query::<Take<HoverInfo>>()
        .one();

    if let Some(info) = hover {
        println!("{}", info.0);
    }

    println!(
        "hover facts after take: {}",
        db.query::<(Entity, &HoverInfo)>().collect().len()
    );
}
