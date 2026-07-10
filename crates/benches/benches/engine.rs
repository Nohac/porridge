//! Engine benchmarks targeting the suspected slow paths:
//!
//! - `cold_settle`      full settle of N files through a 3-system pipeline
//!                      (replanning waves + commit path)
//! - `incremental_settle` mutate 1 of N files, re-settle
//!                      (the language-service hot path; full-phase replanning
//!                      cost per small change)
//! - `identical_rerun`  every input changes but every derived output value is
//!                      identical (fingerprint cutoff / derived churn)
//! - `read_scan`        scoop 16 matching rows out of N irrelevant entities
//!                      (dense `0..next_entity` scans)
//! - `where_eq`         path-equality scoop over N files (missing index)
//! - `view_scaling`     per-row system with a View over all rows, no outputs
//!                      (view access amplification + replan-per-no-op-commit)

use std::hint::black_box;
use std::time::Duration;

use benches::{
    Def, PairMark, Parity, ParityNote, Path, Text, W1, W2, W3, bump_all_sources, defs_bowl,
    file_name, file_pipeline_bowl, in_join_bowl, parity_bowl, scan_bowl, settle_files,
    spawn_parity_bowl, touch_file, touch_group, wide_row_bowl,
};
use bowl::{Entity, Eq, Query, Where};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::executor::block_on;

fn cold_settle(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold_settle");
    group.sample_size(10);

    for files in [8, 32, 128] {
        group.bench_with_input(BenchmarkId::from_parameter(files), &files, |b, &files| {
            b.iter_batched(
                || block_on(file_pipeline_bowl(files)),
                |bowl| black_box(block_on(settle_files(&bowl))),
                BatchSize::PerIteration,
            )
        });
    }

    group.finish();
}

fn incremental_settle(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_settle");
    group.sample_size(10);

    for files in [8, 32, 128] {
        group.bench_with_input(BenchmarkId::from_parameter(files), &files, |b, &files| {
            b.iter_batched(
                || {
                    let bowl = block_on(file_pipeline_bowl(files));
                    block_on(settle_files(&bowl));
                    bowl
                },
                |bowl| {
                    block_on(async {
                        touch_file(&bowl, files / 2).await;
                        black_box(settle_files(&bowl).await)
                    })
                },
                BatchSize::PerIteration,
            )
        });
    }

    group.finish();
}

fn identical_rerun(c: &mut Criterion) {
    let mut group = c.benchmark_group("identical_rerun");
    group.sample_size(10);

    for rows in [8, 32, 128] {
        let bowl = block_on(async {
            let bowl = parity_bowl(rows).await;
            bowl.scoop::<Query<(Entity, &Parity)>>().await;
            bowl
        });

        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                block_on(async {
                    bump_all_sources(&bowl).await;
                    black_box(bowl.scoop::<Query<(Entity, &Parity)>>().await.len())
                })
            })
        });
    }

    group.finish();
}

fn spawn_rerun(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawn_rerun");
    group.sample_size(10);

    for rows in [8, 32, 128] {
        let bowl = block_on(async {
            let bowl = spawn_parity_bowl(rows).await;
            bowl.scoop::<Query<(Entity, &ParityNote)>>().await;
            bowl
        });

        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                block_on(async {
                    bump_all_sources(&bowl).await;
                    black_box(bowl.scoop::<Query<(Entity, &ParityNote)>>().await.len())
                })
            })
        });
    }

    group.finish();
}

fn read_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_scan");
    group.sample_size(30);

    for padding in [100, 1_000, 10_000] {
        let bowl = block_on(async {
            let bowl = scan_bowl(padding, 16).await;
            bowl.scoop::<Query<(Entity, &Text)>>().await;
            bowl
        });

        group.bench_with_input(
            BenchmarkId::from_parameter(padding),
            &padding,
            |b, _| {
                b.iter(|| {
                    block_on(async {
                        black_box(bowl.scoop::<Query<(Entity, &Text)>>().await.len())
                    })
                })
            },
        );
    }

    group.finish();
}

/// Multi-part row enumeration, schema bowl (presence-mask matching) vs
/// schema-less (per-part store probing), identical data and query.
fn presence_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("presence_scan");
    group.sample_size(30);

    for rows in [1_000usize, 10_000, 50_000] {
        for (label, schema) in [("probe", false), ("mask", true)] {
            let bowl = block_on(async {
                let bowl = wide_row_bowl(rows, schema).await;
                bowl.scoop::<Query<(Entity, &W1, &W2, &W3)>>().await;
                bowl
            });

            group.bench_with_input(
                BenchmarkId::new(label, rows),
                &rows,
                |b, _| {
                    b.iter(|| {
                        block_on(async {
                            black_box(
                                bowl.scoop::<Query<(Entity, &W1, &W2, &W3)>>().await.len(),
                            )
                        })
                    })
                },
            );
        }
    }

    group.finish();
}

fn where_eq(c: &mut Criterion) {
    let mut group = c.benchmark_group("where_eq");
    group.sample_size(30);

    for files in [100, 1_000] {
        let bowl = block_on(async {
            let bowl = scan_bowl(0, files).await;
            bowl.scoop::<Query<(Entity, &Text)>>().await;
            bowl
        });

        group.bench_with_input(BenchmarkId::from_parameter(files), &files, |b, &files| {
            b.iter(|| {
                block_on(async {
                    black_box(
                        bowl.scoop::<Query<(Entity, &Text), Where<Eq<Path>>>>()
                            .args(Path(file_name(files / 2)))
                            .await
                            .len(),
                    )
                })
            })
        });
    }

    group.finish();
}

fn view_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("view_scaling");
    group.sample_size(10);

    for defs in [8, 32, 64] {
        group.bench_with_input(BenchmarkId::from_parameter(defs), &defs, |b, &defs| {
            b.iter_batched(
                || block_on(defs_bowl(defs)),
                |bowl| {
                    black_box(block_on(async {
                        bowl.scoop::<Query<(Entity, &Def)>>().await.len()
                    }))
                },
                BatchSize::PerIteration,
            )
        });
    }

    group.finish();
}

fn in_join_planning(c: &mut Criterion) {
    let mut group = c.benchmark_group("in_join_planning");
    group.sample_size(10);

    // (groups, members per group): the pair space is groups × members, but
    // naive product planning probes groups × (groups × members) tuples per
    // wave. Incremental: retag one group, settle.
    for (groups, members) in [(8, 32), (16, 64), (32, 128)] {
        let id = format!("{groups}x{members}");
        group.bench_with_input(
            BenchmarkId::from_parameter(id),
            &(groups, members),
            |b, &(groups, members)| {
                b.iter_batched(
                    || {
                        let (bowl, group_entities) = block_on(in_join_bowl(groups, members));
                        block_on(settle_files(&bowl));
                        (bowl, group_entities, 1u64)
                    },
                    |(bowl, group_entities, bump)| {
                        black_box(block_on(async {
                            touch_group(&bowl, group_entities[0], 1000 + bump).await;
                            bowl.scoop::<Query<(Entity, &PairMark)>>().await.len()
                        }))
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    name = engine;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(2))
        .warm_up_time(Duration::from_millis(300));
    targets = cold_settle, incremental_settle, identical_rerun, spawn_rerun, read_scan, presence_scan, where_eq, view_scaling, in_join_planning
);
criterion_main!(engine);
