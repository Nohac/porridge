//! Integration tests over the real language: the playground is the main
//! integration surface for bowl, so pipeline-shaped guarantees are pinned
//! here against the actual systems rather than synthetic ones.

use std::sync::atomic::{AtomicUsize, Ordering};

use bowl::{Bowl, Component, Entity, Query, Singleton};
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
/// snapshot. The candidate systems read lowered facts ambiently, so they
/// sit one phase after lowering (`Phase::Complete`); everything else in
/// the pipeline is tracked and needs no ordering at all.
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

/// A large component whose change detection runs off an explicit revision
/// counter instead of hashing the payload — the payload is deliberately not
/// `Hash` (an `f64`). Rewriting with an unchanged revision must be a
/// fingerprint hit (no rerun); bumping the revision must invalidate.
#[derive(Component)]
#[component(revision)]
struct Blob {
    revision: u64,
    payload: f64,
}

static BLOB_RUNS: AtomicUsize = AtomicUsize::new(0);

async fn observe_blob(query: Query<(Entity, &Blob)>) {
    let (_entity, _blob) = query.item();
    BLOB_RUNS.fetch_add(1, Ordering::SeqCst);
}

/// dsql-port friction 6 (TODO §1): every big component hand-rolls a
/// revision-counter-as-`Hash` fingerprint. `#[component(revision)]` must
/// stamp the fingerprint from the `revision` field without hashing (or even
/// being able to hash) the payload.
#[test]
fn revision_fingerprints_cut_off_reruns_without_hashing_payloads() {
    block_on(async {
        let db = Bowl::new();
        db.add_system(observe_blob).await;

        let inserted = db
            .insert((Blob {
                revision: 1,
                payload: 1.0,
            },))
            .await;
        db.scoop::<Query<(Entity, &Blob)>>().await;
        assert_eq!(BLOB_RUNS.load(Ordering::SeqCst), 1);

        // Same revision: the rewrite is a fingerprint hit, nothing reruns.
        db.entity(inserted.entity())
            .insert((Blob {
                revision: 1,
                payload: 1.0,
            },))
            .await;
        db.scoop::<Query<(Entity, &Blob)>>().await;
        assert_eq!(
            BLOB_RUNS.load(Ordering::SeqCst),
            1,
            "an unchanged revision must not invalidate"
        );

        // Bumped revision: the fingerprint moves, the observer reruns.
        db.entity(inserted.entity())
            .insert((Blob {
                revision: 2,
                payload: 2.0,
            },))
            .await;
        let blobs = db.scoop::<Query<(Entity, &Blob)>>().await;
        assert_eq!(BLOB_RUNS.load(Ordering::SeqCst), 2, "a bumped revision must invalidate");
        let rows = blobs.collect();
        assert_eq!(rows[0].1.revision, 2);
        assert_eq!(rows[0].1.payload, 2.0);
    });
}
