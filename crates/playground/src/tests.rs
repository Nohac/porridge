//! Integration tests over the real language: the playground is the main
//! integration surface for bowl, so pipeline-shaped guarantees are pinned
//! here against the actual systems rather than synthetic ones.

use bowl::{Bowl, Entity, Query, Singleton};
use futures::executor::block_on;

use crate::lang::{
    self,
    entities::{
        document::{FilePath, FileText},
        import::SystemImportDb,
    },
    facts::{Diagnostic, DiagnosticsDemand},
    service::{HoverInfo, HoverRequest, Position},
};

async fn language_bowl() -> Bowl {
    let db = Bowl::new();
    lang::register_language(&db).await;
    db.insert((
        Singleton::<SystemImportDb>::new(),
        SystemImportDb::default(),
    ))
    .await;
    db
}

/// A hover request batched into the same generation as the source it asks
/// about must answer from that source's AST, not from a mid-derivation
/// snapshot. Ungated, the candidate systems run in the same wave as
/// `generate_ast` and read a view without the new definitions; the
/// `AstAvailable` gate at the pipeline head defers enrichment one
/// generation, after which every downstream read is consistent.
#[test]
fn hover_batched_with_source_answers_from_that_source() {
    block_on(async {
        let db = language_bowl().await;

        db.insert((
            FilePath("test.porridge".to_string()),
            FileText("fn alpha() { return 1; }".to_string()),
        ))
        .await;

        let info = db
            .insert((
                HoverRequest,
                FilePath("test.porridge".to_string()),
                Position { offset: "fn ".len() },
            ))
            .await
            .bind()
            .take::<HoverInfo>()
            .await
            .expect("hover request must be answered");

        assert!(
            info.0.contains("`alpha` is a function definition"),
            "hover answered from a stale snapshot: {}",
            info.0
        );
    });
}

/// Same guarantee for namespaced definitions: the qualified-name candidate
/// depends on the join-derived facts, one derivation step further out.
#[test]
fn hover_batched_with_namespaced_source_sees_qualified_name() {
    block_on(async {
        let db = language_bowl().await;

        db.insert((
            FilePath("core.porridge".to_string()),
            FileText("namespace app.core {\nfn boot() { return 1; }\n}".to_string()),
        ))
        .await;

        let info = db
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
            .expect("hover request must be answered");

        assert!(
            info.0.contains("known as `app.core.boot`"),
            "hover answered from a stale snapshot: {}",
            info.0
        );
    });
}

/// Diagnostics are demand-driven: a hover-only bowl computes none, and
/// inserting the demand fact makes the next settle produce them.
#[test]
fn diagnostics_compute_only_on_demand() {
    block_on(async {
        let db = language_bowl().await;

        db.insert((
            FilePath("demand.porridge".to_string()),
            FileText("import unknown.lib\nfn alpha() { return 1; }".to_string()),
        ))
        .await;

        let info = db
            .insert((
                HoverRequest,
                FilePath("demand.porridge".to_string()),
                Position { offset: "import unknown.lib\nfn ".len() },
            ))
            .await
            .bind()
            .take::<HoverInfo>()
            .await
            .expect("hover works without diagnostics demand");
        assert!(info.0.contains("`alpha`"), "{}", info.0);

        // No demand: the unknown import produced no diagnostic.
        let diagnostics = db.scoop::<Query<(Entity, &Diagnostic)>>().await.len();
        assert_eq!(diagnostics, 0, "undemanded diagnostics must not compute");

        db.insert((Singleton::<DiagnosticsDemand>::new(), DiagnosticsDemand))
            .await;

        let diagnostics = db.scoop::<Query<(Entity, &Diagnostic)>>().await.len();
        assert!(diagnostics > 0, "demanded diagnostics must compute");
    });
}
