use std::{collections::HashSet, fmt};

use bowl::{Component, Entity};

pub(crate) mod lexer;
pub(crate) mod parser;

#[derive(Component, Hash, PartialEq, Eq)]
#[component(hash)]
pub(crate) struct FilePath(pub(crate) String);

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct FileText(pub(crate) String);

#[derive(Component)]
pub(crate) struct SystemImportDb(pub(crate) HashSet<String>);

impl Default for SystemImportDb {
    fn default() -> Self {
        let mut imports = HashSet::new();
        imports.insert("std.io".to_string());
        Self(imports)
    }
}

#[derive(Component, Clone, Copy)]
pub(crate) struct AstAvailable;

#[derive(Component)]
pub(crate) struct Ephemeral;

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct BelongsToFile(pub(crate) Entity);

#[derive(Component)]
pub(crate) struct ParsedFile {
    pub(crate) cst: parser::CstData,
}

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct ImportDecl {
    pub(crate) path: String,
    pub(crate) span: Span,
}

#[derive(Debug, Component, Hash)]
#[component(hash)]
pub(crate) enum AstDef {
    Function(FunctionDef),
    Type(TypeDef),
}

impl AstDef {
    pub(crate) fn name(&self) -> &str {
        match self {
            AstDef::Function(def) => &def.name,
            AstDef::Type(def) => &def.name,
        }
    }

    pub(crate) fn kind(&self) -> DefKind {
        match self {
            AstDef::Function(_) => DefKind::Function,
            AstDef::Type(_) => DefKind::Type,
        }
    }

    pub(crate) fn span(&self) -> Span {
        match self {
            AstDef::Function(def) => def.span,
            AstDef::Type(def) => def.span,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DefKind {
    Function,
    Type,
}

impl fmt::Display for DefKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DefKind::Function => f.write_str("function"),
            DefKind::Type => f.write_str("type"),
        }
    }
}

#[derive(Debug, Hash)]
pub(crate) struct FunctionDef {
    pub(crate) name: String,
    pub(crate) span: Span,
}

#[derive(Debug, Hash)]
pub(crate) struct TypeDef {
    pub(crate) name: String,
    pub(crate) span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Span {
    pub(crate) start: usize,
    pub(crate) end: usize,
}

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct Diagnostic(pub(crate) String);

#[derive(Component, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[component(hash)]
pub(crate) enum Severity {
    Warning,
    Error,
}

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverRequest;

#[derive(Debug, Component, Hash)]
#[component(hash)]
pub(crate) struct Position {
    pub(crate) offset: usize,
}

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverInfo(pub(crate) String);
