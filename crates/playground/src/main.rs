use pipeline::{Commands, Db, Entity, Query};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Pipe {
    Check,
}

struct SourceFile {
    path: String,
}

struct FileText(String);

struct Ast(String);

struct Diagnostics(Vec<String>);

fn parse_file(mut commands: Commands, Query((file, text)): Query<(Entity, &FileText)>) {
    println!("parse_file({})", file.raw());
    commands.insert(file, Ast(format!("parsed({})", text.0)));
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

    commands.insert(file, Diagnostics(diagnostics));
}

fn main() {
    let mut db = Db::<Pipe>::new();
    db.add_system(Pipe::Check, parse_file);
    db.add_system(Pipe::Check, check_file);

    let main_file = db.spawn((
        SourceFile {
            path: "main.porridge".to_string(),
        },
        FileText("fn main() {}".to_string()),
    ));

    let lib_file = db.spawn((
        SourceFile {
            path: "lib.porridge".to_string(),
        },
        FileText("fn lib() {}".to_string()),
    ));

    println!("first run");
    db.run(Pipe::Check);

    println!("\nsecond run; everything should be memoized");
    db.run(Pipe::Check);

    println!("\nchange one file");
    db.insert(lib_file, FileText("fn lib() { error }".to_string()));
    db.run(Pipe::Check);

    println!("\ndiagnostics");
    for file in [main_file, lib_file] {
        let path = &db.get::<SourceFile>(file).unwrap().path;
        let diagnostics = &db.get::<Diagnostics>(file).unwrap().0;
        println!("{path}: {diagnostics:?}");
    }
}
