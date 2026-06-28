use pipeline::{Commands, Component, Db, Entity, Query, SystemExt, insert};

#[derive(Component)]
struct SourceFile {
    path: String,
}

#[derive(Component, Hash)]
#[component(hash)]
struct FileText(String);

#[derive(Component, Hash)]
#[component(hash)]
struct Ast(String);

#[derive(Component)]
struct Diagnostics(Vec<String>);

#[derive(Component, Clone, Copy)]
struct AllFilesParsed;

#[derive(Component, Hash)]
#[component(hash)]
struct DefinitionName(String);

#[derive(Component, Hash)]
#[component(hash)]
struct DefinitionKind(String);

#[derive(Component, Hash, Debug)]
#[component(hash)]
struct CollectedDefinition(String);

fn parse_file(mut commands: Commands, Query((file, text)): Query<(Entity, &FileText)>) {
    println!("parse_file({})", file.raw());
    commands
        .entity(file)
        .insert(Ast(format!("parsed({})", text.0)));

    for definition in parse_definitions(&text.0) {
        commands.insert((
            DefinitionName(definition.name),
            DefinitionKind(definition.kind),
        ));
    }
}

fn collect_definition(
    Query((_complete, _)): Query<(Entity, &AllFilesParsed)>,
    Query((definition, name, kind)): Query<(Entity, &DefinitionName, &DefinitionKind)>,
    mut commands: Commands,
) {
    println!("collect_definition({})", definition.raw());
    commands
        .entity(definition)
        .insert(CollectedDefinition(format!("{} {}", kind.0, name.0)));
}

fn check_file(
    Query((file, source, ast)): Query<(Entity, &SourceFile, &Ast)>,
    mut commands: Commands,
) {
    println!("check_file({})", file.raw());
    let mut diagnostics = Vec::new();
    if ast.0.contains("error") {
        diagnostics.push(format!("{} contains an error", source.path));
    }

    commands.entity(file).insert(Diagnostics(diagnostics));
}

struct ParsedDefinition {
    kind: String,
    name: String,
}

fn parse_definitions(source: &str) -> Vec<ParsedDefinition> {
    source
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let kind = parts.next()?;
            let name = parts.next()?;

            matches!(kind, "type" | "function" | "struct").then(|| ParsedDefinition {
                kind: kind.to_string(),
                name: name.to_string(),
            })
        })
        .collect()
}

fn main() {
    let mut db = Db::new();
    db.add_system(parse_file.on_complete(insert((AllFilesParsed,))));
    db.add_system(collect_definition);
    db.add_system(check_file);

    let main_file = db.insert((
        SourceFile {
            path: "main.porridge".to_string(),
        },
        FileText("function main\ntype UserId".to_string()),
    ));

    let lib_file = db.insert((
        SourceFile {
            path: "lib.porridge".to_string(),
        },
        FileText("struct Widget\nfunction render".to_string()),
    ));

    println!("first run");
    db.query::<(Entity, &CollectedDefinition)>();

    db.entity(lib_file)
        .insert(FileText("struct Widget\nfunction render".to_string()));
    println!("\nsecond run; everything should be memoized based on hash");
    db.query::<(Entity, &CollectedDefinition)>();

    println!("\nchange one file");
    db.entity(lib_file).insert(FileText(
        "struct Widget\nfunction render\nfunction error".to_string(),
    ));
    db.query::<(Entity, &Diagnostics)>();

    println!("\ndefinitions");
    for (_, definition) in db.query::<(Entity, &CollectedDefinition)>() {
        println!("{}", definition.0);
    }

    println!("\ndiagnostics");
    for file in [main_file, lib_file] {
        let path = &db.peek::<SourceFile>(file).unwrap().path;
        let diagnostics = &db.peek::<Diagnostics>(file).unwrap().0;
        println!("{path}: {diagnostics:?}");
    }
}
