//! The bowl-level entity schema: the single source of truth for every
//! entity shape derived systems produce.
//!
//! Shapes are *defined* here and *referenced* everywhere else — output
//! declarations select subsets (`Commands<(lang_schema::Diagnostic,)>`,
//! `AstFacts`), strict spawns match against them, facet handles name them,
//! and the commit-time conformance check enforces them by name. Adding a
//! component to an entity kind means editing one field here.

use bowl::{DerivedFrom, Singleton};

use crate::{
    ImportDbStressTouched, StressTouched,
    lang::{
        entities::{
            definition::{AstDef, DefIndex},
            document::{FilePath, FileText, ParsedFile},
            import::{ImportDecl, SystemImportDb},
            namespace::{NamespaceDecl, NamespacePath, QualifiedName},
        },
        facts::{BelongsToFile, Diagnostic, DiagnosticsDemand, Severity},
        service::{
            HoverCandidate, HoverFile, HoverInfo, HoverRank, HoverRequest, HoverWord, Position,
            RequestKey,
        },
    },
};

#[derive(bowl::Schema)]
pub(crate) struct LangSchema {
    // Base inputs: caller-inserted entity kinds. Base writes are not
    // conformance-checked (the dynamic boundary), but the schema must
    // still name them — it closes the component universe that presence
    // bitmaps and registration analyses are laid out over.
    source_file: (FilePath, FileText),
    hover_request: (HoverRequest, FilePath, Position),
    diagnostics_demand: (Singleton<DiagnosticsDemand>, DiagnosticsDemand),
    import_db: (Singleton<SystemImportDb>, SystemImportDb),
    // Lowered syntax facts, one shape per language entity.
    ast_def: (AstDef, BelongsToFile, DerivedFrom, Option<NamespacePath>),
    import: (ImportDecl, BelongsToFile, DerivedFrom),
    namespace: (NamespaceDecl, NamespacePath, BelongsToFile, DerivedFrom),
    // Derived analysis facts.
    diagnostic: (Diagnostic, Severity, DerivedFrom),
    parsed_file: (ParsedFile,),
    qualified_name: (QualifiedName, DerivedFrom),
    def_index: (Singleton<DefIndex>, DefIndex),
    // The hover service: candidates per entity, the answer scaffold on the
    // request entity (optionals only present when the request resolved).
    hover_candidate: (HoverCandidate, RequestKey, DerivedFrom),
    hover_answer: (
        RequestKey,
        HoverRank,
        HoverInfo,
        Option<HoverFile>,
        Option<HoverWord>,
    ),
    // Stress-harness markers (main.rs): the schema describes every shape
    // the *bowl* may see, not just the language's.
    stress_touched: (StressTouched,),
    import_db_stress_touched: (ImportDbStressTouched,),
}
