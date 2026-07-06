mod lang;
#[cfg(test)]
mod tests;

use std::collections::HashSet;

use bowl::{
    Bowl, Commands, Component, Entity, Eq, Gte, Mut, MutRef, Named, Query, Singleton, Where,
    Without,
};
use futures::{StreamExt, stream::FuturesUnordered};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::lang::{
    entities::{
        definition::AstDef,
        document::{FilePath, FileText},
        import::SystemImportDb,
        namespace::QualifiedName,
    },
    facts::{AstAvailable, Diagnostic, Severity},
    service::{HoverInfo, HoverRequest, Position},
};

struct StressTouched;
impl Component for StressTouched {}

struct ImportDbStressTouched;
impl Component for ImportDbStressTouched {}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    init_tracing();

    let db = Bowl::new();

    lang::register_language(&db).await;
    db.add_system(touch_file_text_once).await;
    db.add_system(seed_extra_imports_once).await;

    db.insert((
        Singleton::<SystemImportDb>::new(),
        SystemImportDb::default(),
    ))
    .await;

    info!("register std.net import with Mut<SystemImportDb>");
    for (entity, imports) in db
        .scoop::<Query<(Entity, Mut<SystemImportDb>)>>()
        .await
        .collect()
    {
        imports
            .with_latest(|imports| {
                info!(entity = entity.raw(), "mutating import database");
                imports.0.insert("std.net".to_string());
            })
            .await;
    }

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

    db.insert((
        FilePath("core.porridge".to_string()),
        FileText(
            "namespace app.core {\nfn boot() { return 1; }\ntype Config\n}".to_string(),
        ),
    ))
    .await;

    info!("named multi-scoop by file path");
    struct MainSource;
    struct LibSource;
    let (main_source, lib_source) = db
        .scoop::<(
            Named<MainSource, Query<(Entity, &FileText), Where<Eq<FilePath>>>>,
            Named<LibSource, Query<(Entity, &FileText), Where<Eq<FilePath>>>>,
        )>()
        .args_for::<MainSource>(FilePath("main.porridge".to_string()))
        .args_for::<LibSource>(FilePath("lib.porridge".to_string()))
        .await;
    info!(
        main = main_source.len(),
        lib = lib_source.len(),
        "source matches"
    );

    info!("query diagnostics");
    let diagnostics = db.scoop::<Query<(Entity, &Diagnostic)>>().await;
    for (entity, diagnostic) in diagnostics.collect() {
        info!(entity = entity.raw(), diagnostic = %diagnostic.0);
    }

    info!("definitions");
    let definitions = db.scoop::<Query<(Entity, &AstDef)>>().await;
    for (entity, def) in definitions.collect() {
        info!(
            entity = entity.raw(),
            kind = %def.kind(),
            name = def.name(),
            span = ?def.span(),
            "definition"
        );
    }

    info!("query diagnostics again");
    let diagnostics = db.scoop::<Query<(Entity, &Diagnostic)>>().await;
    for (entity, diagnostic) in diagnostics.collect() {
        info!(entity = entity.raw(), diagnostic = %diagnostic.0);
    }

    info!("diagnostics at warning or above");
    let diagnostics = db
        .scoop::<Query<(Entity, &Diagnostic), Where<Gte<Severity>>>>()
        .args(Severity::Warning)
        .await;
    for (entity, diagnostic) in diagnostics.collect() {
        info!(entity = entity.raw(), diagnostic = %diagnostic.0);
    }

    info!("ast available markers");
    let ast_available = db.scoop::<Query<(Entity, &AstAvailable)>>().await;
    for (entity, _) in ast_available.collect() {
        info!(entity = entity.raw(), "ast available");
    }

    info!("hover request");
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
        info!(hover = %info.0);
    }

    info!("hover request in a namespace");
    if let Ok(info) = db
        .insert((
            HoverRequest,
            FilePath("core.porridge".to_string()),
            Position {
                offset: "namespace app.core {\nfn ".len(),
            },
        ))
        .await
        .bind()
        .take::<HoverInfo>()
        .await
    {
        info!(hover = %info.0);
    }

    info!("qualified names derived by the namespace join");
    let qualified = db.scoop::<Query<(Entity, &QualifiedName)>>().await;
    for (entity, name) in qualified.collect() {
        info!(
            entity = entity.raw(),
            definition = name.definition.raw(),
            qualified = %name.name,
        );
    }

    db.insert((
        FilePath("foo.porridge".to_string()),
        FileText("import derp.fs\nstruct Widget {}\nfn other() { return 2; }".to_string()),
    ))
    .await;

    info!("spawn a contrived async task storm");
    run_task_storm(db.clone()).await;

    info!("hover request");
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
        info!(hover = %info.0);
    }

    let hover_facts = db.scoop::<Query<(Entity, &HoverInfo)>>().await;
    info!(
        count = hover_facts.collect().len(),
        "hover facts after request"
    );
}

fn init_tracing() {
    if std::env::var_os("RUST_LOG").is_none() {
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

async fn touch_file_text_once(
    query: Query<(Entity, MutRef<'_, FileText>), Without<StressTouched>>,
    mut commands: Commands,
) {
    short_sleep().await;

    let (entity, mut text) = query.item();
    if !text.0.contains("type StressTouchedBySystem") {
        text.0.push_str("\ntype StressTouchedBySystem");
    }

    info!(entity = entity.raw(), "touch_file_text_once");
    commands.entity(entity).insert(StressTouched);
}

async fn seed_extra_imports_once(
    query: Query<(Entity, MutRef<'_, SystemImportDb>), Without<ImportDbStressTouched>>,
    mut commands: Commands,
) {
    short_sleep().await;

    let (entity, mut imports) = query.item();
    imports.0.insert("std.fs".to_string());
    imports.0.insert("derp.fs".to_string());
    imports.0.insert("storm.lib".to_string());

    info!(entity = entity.raw(), "seed_extra_imports_once");
    commands.entity(entity).insert(ImportDbStressTouched);
}

async fn run_task_storm(db: Bowl) {
    let paths = [
        "main.porridge",
        "lib.porridge",
        "foo.porridge",
        "storm-a.porridge",
        "storm-b.porridge",
        "storm-c.porridge",
    ];

    for (index, path) in paths.iter().enumerate().skip(3) {
        db.insert((
            FilePath((*path).to_string()),
            FileText(format!(
                "import storm.lib\nfn storm_{index}() {{ return {index}; }}\ntype Storm{index}"
            )),
        ))
        .await;
    }

    let mut spawned = FuturesUnordered::new();

    for path in paths {
        let db = db.clone();
        spawned.push(tokio::spawn(async move {
            short_sleep().await;
            mutate_file_by_path(db, path.to_string()).await;
            format!("mutated {path}")
        }));
    }

    for path in paths {
        let db = db.clone();
        spawned.push(tokio::spawn(async move {
            let row_count = {
                let result = db
                    .scoop::<Query<(Entity, &FileText), Where<Eq<FilePath>>>>()
                    .args(FilePath(path.to_string()))
                    .await;
                result.collect().len()
            };
            short_sleep().await;
            format!("read {row_count} file rows for {path}")
        }));
    }

    for offset in [0, 8, 24, 40, 64, 128] {
        let db = db.clone();
        spawned.push(tokio::spawn(async move {
            let target = if offset % 2 == 0 {
                "main.porridge"
            } else {
                "foo.porridge"
            };
            let response = db
                .insert((
                    HoverRequest,
                    FilePath(target.to_string()),
                    Position { offset },
                ))
                .await
                .bind()
                .take::<HoverInfo>()
                .await
                .map(|info| info.0.clone())
                .unwrap_or_else(|error| format!("hover miss: {error}"));
            format!("hover {target}@{offset}: {response}")
        }));
    }

    for name in ["std.fs", "storm.extra", "derp.fs", "std.net"] {
        let db = db.clone();
        spawned.push(tokio::spawn(async move {
            let imports = db.scoop::<Query<(Entity, Mut<SystemImportDb>)>>().await;
            for (entity, imports) in imports.collect() {
                imports
                    .with_latest(|imports| {
                        imports.0.insert(name.to_string());
                    })
                    .await;
                info!(name, entity = entity.raw(), "task import mut");
            }
            format!("registered import {name}")
        }));
    }

    while let Some(result) = spawned.next().await {
        match result {
            Ok(message) => info!(%message, "task completed"),
            Err(error) => warn!(%error, "task failed"),
        }
    }

    info!("concurrent multi-scoop fanout");
    let mut fanout = FuturesUnordered::new();
    for threshold in [Severity::Warning, Severity::Error] {
        let db = db.clone();
        fanout.push(async move {
            let (diagnostics, defs, files) = db
                .scoop::<(
                    Query<(Entity, &Diagnostic), Where<Gte<Severity>>>,
                    Query<(Entity, &AstDef)>,
                    Query<(Entity, &FilePath)>,
                )>()
                .args(threshold)
                .await;
            let diagnostic_count = diagnostics.collect().len();
            let def_names = defs
                .collect()
                .into_iter()
                .map(|(_, def)| def.name().to_string())
                .collect::<Vec<_>>();
            let file_count = files.collect().len();
            (diagnostic_count, def_names, file_count)
        });
    }

    while let Some((diagnostics, defs, files)) = fanout.next().await {
        info!(
            diagnostics,
            defs = %defs.join(","),
            files,
            "fanout"
        );
    }

    let final_imports = db.scoop::<Query<(Entity, &SystemImportDb)>>().await;
    for (entity, imports) in final_imports.collect() {
        let sorted = imports.0.iter().cloned().collect::<HashSet<_>>();
        info!(
            entity = entity.raw(),
            entries = sorted.len(),
            "final import db"
        );
    }
}

async fn mutate_file_by_path(db: Bowl, path: String) {
    let files = db
        .scoop::<Query<(Entity, Mut<FileText>), Where<Eq<FilePath>>>>()
        .args(FilePath(path.clone()))
        .await;

    for (entity, text) in files.collect() {
        let path = path.clone();
        let updated = text
            .with_latest(|text| {
                text.0
                    .push_str(&format!("\nfn generated_for_{}() {{ return 0; }}", path));
            })
            .await;
        info!(
            entity = entity.raw(),
            path,
            updated = updated.is_some(),
            "external file mut"
        );
    }
}

pub(crate) async fn short_sleep() {
    // let millis = 50 + rand::random::<u64>() % 751;
    // tokio::time::sleep(Duration::from_millis(millis)).await;
}
