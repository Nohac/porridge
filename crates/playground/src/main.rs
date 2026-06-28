use std::collections::{HashMap, HashSet};

use pipeline::{
    And, Commands, Component, Db, Entity, Eq, Gte, Query, SystemExt, View, Where, insert,
};

#[derive(Component)]
struct SourceFile {
    path: String,
}

#[derive(Component, Hash, Clone, PartialEq, Eq)]
#[component(hash)]
struct FilePath(String);

#[derive(Component, Hash)]
#[component(hash)]
struct FileText(String);

#[derive(Component, Clone)]
struct SystemImportDb(HashSet<String>);

impl Default for SystemImportDb {
    fn default() -> Self {
        let mut imports = HashSet::new();
        imports.insert("std.io".to_string());
        imports.insert("std.io".to_string());
        Self(imports)
    }
}

#[derive(Component, Clone, Copy)]
struct AllFilesParsed;

#[derive(Component, Hash)]
#[component(hash)]
struct BelongsToFile(Entity);

#[derive(Component, Hash)]
#[component(hash)]
struct ImportName(String);

#[derive(Component, Hash)]
#[component(hash)]
struct DefinitionName(String);

#[derive(Component, Hash)]
#[component(hash)]
struct DefinitionKind(String);

#[derive(Component, Hash)]
#[component(hash)]
struct Diagnostic(String);

#[derive(Component, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[component(hash)]
enum Severity {
    Warning,
    Error,
}

fn parse_file(mut commands: Commands, Query((file, text)): Query<(Entity, &FileText)>) {
    println!("parse_file({})", file.raw());

    for line in text.0.lines() {
        let mut parts = line.split_whitespace();
        let Some(kind) = parts.next() else {
            continue;
        };
        let Some(name) = parts.next() else {
            continue;
        };

        match kind {
            "import" => commands.insert((BelongsToFile(file), ImportName(name.to_string()))),
            "type" | "function" | "struct" => commands.insert((
                BelongsToFile(file),
                DefinitionKind(kind.to_string()),
                DefinitionName(name.to_string()),
            )),
            _ => {}
        }
    }
}

fn check_imports(
    _: Query<(Entity, &AllFilesParsed)>,
    imports: View<(Entity, &ImportName, &BelongsToFile)>,
    system_imports: View<&SystemImportDb>,
    files: View<(Entity, &SourceFile)>,
    mut commands: Commands,
) {
    println!("check_imports");
    let system = system_imports.iter().next().unwrap();

    for (import, name, f) in imports.iter() {
        if !system.0.contains(&name.0) {
            let file = files
                .get(f.0)
                .map(|source| source.path.as_str())
                .unwrap_or("<unknown>");
            commands.entity(*import).insert(FilePath(file.to_string()));
            commands.entity(*import).insert(Severity::Warning);
            commands.entity(*import).insert(Diagnostic(format!(
                "unknown import `{}` in file {}",
                name.0, file
            )));
        }
    }
}

fn check_duplicate_definitions(
    Query((_done, _)): Query<(Entity, &AllFilesParsed)>,
    definitions: View<(Entity, &DefinitionName, &DefinitionKind)>,
    mut commands: Commands,
) {
    println!("check_duplicate_definitions");
    let mut seen = HashMap::new();

    for (definition, name, kind) in definitions.iter() {
        if let Some((previous, previous_kind)) =
            seen.insert(name.0.as_str(), (*definition, kind.0.as_str()))
        {
            commands.entity(*definition).insert(Severity::Error);
            commands.entity(*definition).insert(Diagnostic(format!(
                "duplicate definition `{}`; previous {previous_kind} is entity {}",
                name.0,
                previous.raw()
            )));
            commands.entity(previous).insert(Severity::Error);
            commands.entity(previous).insert(Diagnostic(format!(
                "duplicate definition `{}`; duplicate {} is entity {}",
                name.0,
                kind.0,
                definition.raw()
            )));
        }
    }
}

fn main() {
    let mut db = Db::new();
    db.add_system(parse_file.on_complete(insert((AllFilesParsed,))));
    db.add_system(check_imports);
    db.add_system(check_duplicate_definitions);

    db.insert((SystemImportDb::default(),));

    db.insert((
        SourceFile {
            path: "main.porridge".to_string(),
        },
        FilePath("main.porridge".to_string()),
        FileText("import std.io\nimport std.net\nfunction main\ntype UserId".to_string()),
    ));

    db.insert((
        SourceFile {
            path: "lib.porridge".to_string(),
        },
        FilePath("lib.porridge".to_string()),
        FileText("import std.fs\nstruct Widget\nfunction main".to_string()),
    ));

    println!("query diagnostics");
    for (entity, diagnostic) in db.query::<(Entity, &Diagnostic)>().collect() {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\nmain diagnostics at warning or above");
    for (entity, diagnostic) in db
        .query::<(Entity, &Diagnostic, Where<And<Eq<FilePath>, Gte<Severity>>>)>()
        .bind(FilePath("main.porridge".to_string()))
        .bind(Severity::Warning)
        .collect()
    {
        println!("entity {}: {}", entity.raw(), diagnostic.0);
    }

    println!("\nfiles");
    for (_, source) in db.query::<(Entity, &SourceFile)>().collect() {
        println!("{}", source.path);
    }
}
