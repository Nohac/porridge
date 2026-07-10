//! Integration tests over the real language: the playground is the main
//! integration surface for bowl, so pipeline-shaped guarantees are pinned
//! here against the actual systems rather than synthetic ones.

use std::sync::atomic::{AtomicUsize, Ordering};

use bowl::{Bowl, Component, Entity, In, Query, Singleton, Where};
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
    let db = Bowl::builder()
        .plugin(lang::LangPlugin)
        .plugin(
            crate::replication::ReplicationPlugin::new()
                .replicate::<lang::schema::lang_schema::SourceFile>()
                .replicate::<lang::schema::lang_schema::AstDef>(),
        )
        .build();
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

/// The replication plugin dogfoods the plugin surface: its schema
/// fragment joins the bowl universe through `Plugin::shapes`, and its
/// generic tracking system is instantiated with app types at build time
/// without the plugin ever naming them. One replica record per subscribed
/// *shape instance* — component-granular replication could transit
/// illegal partial entities, so the shape is the protocol unit — kept
/// current through `DerivedFrom` by the language plugin's cleanup
/// system.
#[test]
fn replication_plugin_maintains_replica_records() {
    block_on(async {
        let db = language_bowl().await;

        db.insert((
            FilePath("repl.porridge".to_string()),
            FileText("fn one() { return 1; }".to_string()),
        ))
        .await;

        let result = db
            .scoop::<Query<(Entity, &crate::replication::Replica)>>()
            .await;
        let replicas = result.collect();
        // One record for the file text, one for the lowered definition.
        assert_eq!(replicas.len(), 2, "one replica per subscribed row");
        assert!(
            replicas
                .iter()
                .any(|(_, replica)| replica.shape.contains("FileText")),
            "source-file shape subscription must be tracked"
        );
        assert!(
            replicas
                .iter()
                .any(|(_, replica)| replica.shape.contains("AstDef")),
            "definition shape subscription must be tracked"
        );

        // An edit reaps and re-derives; a second definition means a third
        // record.
        let files = db
            .scoop::<Query<(Entity, bowl::Mut<FileText>), Where<bowl::Eq<FilePath>>>>()
            .args(FilePath("repl.porridge".to_string()))
            .await;
        for (_, text) in files.collect() {
            text.with_latest(|text| text.0.push_str("\nfn two() { return 2; }"))
                .await;
        }
        let result = db
            .scoop::<Query<(Entity, &crate::replication::Replica)>>()
            .await;
        let replicas = result.collect();
        // Definition replicas follow: the old def entities were reaped with
        // their replicas, and the two new defs got fresh records.
        assert_eq!(
            replicas
                .iter()
                .filter(|(_, replica)| replica.shape.contains("AstDef"))
                .count(),
            2,
            "definition replicas must follow the derivation"
        );
        // Dogfood finding, pinned: the source-file record is gone. The
        // edit bumped `FileText`, cleanup reaped the record (source
        // revision moved), but head-driven capture (`With<FilePath>`)
        // carries no dep on the rest of the shape, so nothing reran to
        // re-derive it. Shape-granular capture needs shape-granular deps —
        // the staged facet queries (`Entity<H>` rows, spec/declared-
        // outputs.md layer 4). When those land, this asserts 3.
        assert_eq!(
            replicas.len(),
            2,
            "source-file record awaits facet-query capture"
        );
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
        let db = Bowl::builder().system(observe_blob).build();

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

/// The `#[relationship]`/`#[relationship_target]` derive attributes: the
/// edge declares its target, the inverse is fingerprinted by construction,
/// and the whole maintenance + `Where<In<..>>` surface works without a
/// single hand-written trait impl.
#[derive(Component, Hash)]
#[component(hash)]
#[relationship(target = Squad)]
struct SquadMember(Entity);

#[derive(Component)]
#[relationship_target(relationship = SquadMember)]
struct Squad(Vec<Entity>);

#[derive(Component, Hash)]
#[component(hash)]
struct Callsign(&'static str);

async fn roster(
    squads: bowl::Query<(Entity, &Squad)>,
    member: bowl::Query<(Entity, &Callsign), Where<In<Squad>>>,
    mut commands: bowl::Commands<(Rostered,)>,
) {
    let (_squad, _members) = squads.item();
    let (member_entity, _callsign) = member.item();
    commands.entity(member_entity).insert(Rostered);
}

#[derive(Component)]
struct Rostered;

#[test]
fn derived_relationship_attributes_maintain_the_inverse() {
    block_on(async {
        let db = Bowl::builder().system(roster).build();

        let leader = db.insert((Callsign("lead"),)).await;
        let m1 = db
            .insert((Callsign("alpha"), SquadMember(leader.entity())))
            .await;
        let m2 = db
            .insert((Callsign("bravo"), SquadMember(leader.entity())))
            .await;
        // A callsign outside the squad never pairs.
        db.insert((Callsign("stray"),)).await;

        let squads = db.scoop::<Query<(Entity, &Squad)>>().await;
        let rows = squads.collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, leader.entity());
        assert_eq!(rows[0].1.0, vec![m1.entity(), m2.entity()]);

        let rostered = db.scoop::<Query<Entity, bowl::With<Rostered>>>().await;
        assert_eq!(rostered.collect().len(), 2, "only members pair via In");

        // Retracting an edge shrinks the inverse.
        db.entity(m2.entity()).remove::<SquadMember>().await;
        let squads = db.scoop::<Query<(Entity, &Squad)>>().await;
        assert_eq!(squads.collect()[0].1.0, vec![m1.entity()]);
    });
}

/// `#[derive(Schema)]`: named shapes, the companion trait making each
/// shape usable as a `Commands` declaration, and commit-time conformance.
#[derive(Component, Hash)]
#[component(hash)]
struct Report(&'static str);

#[derive(Component, Hash)]
#[component(hash)]
struct Grade(u8);

#[derive(Component, Hash)]
#[component(hash)]
struct Reviewed;

#[derive(bowl::Schema)]
struct ReviewSchema {
    report: (Report, Grade, bowl::DerivedFrom, Option<Reviewed>),
}

async fn write_report(
    query: Query<(Entity, &Position)>,
    // The shape doubles as the output declaration via the companion alias.
    mut commands: bowl::Commands<(review_schema::Report,)>,
) {
    let (entity, _pos) = query.item();
    // Strict spawn: the bundle matches the shape (optional part included
    // here) and the returned handle carries the facet.
    let report: bowl::Entity<review_schema::Report> = commands.insert((
        Report("ok"),
        Grade(5),
        bowl::DerivedFrom::new(entity),
        Reviewed,
    ));
    let _ = report.untyped();
}

#[test]
fn schema_shapes_declare_and_conform() {
    block_on(async {
        let db = Bowl::builder()
            .schema::<ReviewSchema>()
            .system(write_report)
            .build();

        db.insert((Position { offset: 0 },)).await;
        let reports = db.scoop::<Query<(Entity, &Report)>>().await;
        assert_eq!(reports.collect().len(), 1);
    });
}

async fn write_incomplete_report(
    query: Query<(Entity, &Position)>,
    mut commands: bowl::Commands<(Report, bowl::DerivedFrom)>,
) {
    let (entity, _pos) = query.item();
    // Strict spawning makes an incomplete spawn a compile error, so the
    // runtime conformance check guards the door that stays open:
    // incremental writes onto an existing entity that fit a shape but
    // never complete it. Missing the required Grade: the check must name
    // it.
    commands.entity(entity).insert(Report("bad"));
    commands.entity(entity).insert(bowl::DerivedFrom::new(entity));
}

#[test]
#[should_panic(expected = "left required component(s) missing")]
fn incomplete_shapes_panic_with_the_missing_component() {
    block_on(async {
        let db = Bowl::builder()
            .schema::<ReviewSchema>()
            .system(write_incomplete_report)
            .build();

        db.insert((Position { offset: 0 },)).await;
        db.scoop::<Query<(Entity, &Report)>>().await;
    });
}
