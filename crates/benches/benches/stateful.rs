//! Generic stateful workloads; sizes are runtime inputs, never Cargo features.
use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};

use benches::{
    fleet_bowl,
    stateful::{churn, purchases},
    touch_slot0,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::{Builder, Runtime};

fn sizes(name: &str, defaults: &[usize], zero_allowed: bool) -> Vec<usize> {
    match env::var(name) {
        Ok(value) => value
            .split(',')
            .map(|part| {
                let size = part
                    .trim()
                    .parse::<usize>()
                    .unwrap_or_else(|_| panic!("invalid {name}: {part}"));
                assert!(zero_allowed || size > 0, "{name} must be positive");
                size
            })
            .collect(),
        Err(env::VarError::NotPresent) => defaults.to_vec(),
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn runtime(workers: usize) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap()
}

fn purchases(c: &mut Criterion) {
    let requests = sizes("PORRIDGE_REQUESTS", &[1, 10, 100, 1000], false);
    let workers = sizes("PORRIDGE_WORKERS", &[1, 4, 8], false);
    let work = sizes("PORRIDGE_WORK", &[0, 10_000], true);
    let contention = sizes("PORRIDGE_PER_OWNER", &[1, 4], false);
    let mut group = c.benchmark_group("purchases");
    group.sample_size(10);

    for workers in workers {
        let runtime = runtime(workers);
        for &requests in &requests {
            group.throughput(Throughput::Elements(requests as u64));
            for &per_owner in &contention {
                for &work in &work {
                    let work = u32::try_from(work).expect("PORRIDGE_WORK must fit u32");
                    let name = format!("workers={workers}/per_owner={per_owner}/work={work}");
                    group.bench_function(BenchmarkId::new(name, requests), |b| {
                        b.iter_custom(|iterations| {
                            let mut elapsed = Duration::ZERO;
                            runtime.block_on(async {
                                for _ in 0..iterations {
                                    // Fixed starting state/history per observation. Adaptive
                                    // Criterion iterations must not silently age the world.
                                    let bowl = purchases::create(
                                        requests.div_ceil(per_owner),
                                        u64::MAX,
                                        u64::MAX,
                                    )
                                    .await;
                                    let start = Instant::now();
                                    let responses =
                                        purchases::batch(&bowl, requests, per_owner, work).await;
                                    elapsed += start.elapsed();
                                    black_box(responses);
                                }
                            });
                            elapsed
                        });
                    });
                }
            }
        }
    }
    group.finish();
}

fn entity_churn(c: &mut Criterion) {
    let live = sizes("PORRIDGE_LIVE", &[64], false);
    let histories = sizes("PORRIDGE_HISTORY", &[0, 10_000, 100_000], true);
    let mut group = c.benchmark_group("entity_churn");
    group.sample_size(10);
    let runtime = runtime(1);

    for live in live {
        group.throughput(Throughput::Elements(live as u64));
        for &history in &histories {
            for facet in [false, true] {
                let path = if facet { "facet" } else { "store" };
                group.bench_function(
                    BenchmarkId::new(format!("{path}/live={live}"), history),
                    |b| {
                        let bowl = runtime.block_on(churn::create(live, history));
                        b.iter(|| {
                            black_box(runtime.block_on(async {
                                if facet {
                                    churn::scan(&bowl).await
                                } else {
                                    churn::scan_store(&bowl).await
                                }
                            }))
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

fn sparse_changes(c: &mut Criterion) {
    let rows = sizes("PORRIDGE_ROWS", &[10, 100, 1000], false);
    let workers = sizes("PORRIDGE_WORKERS", &[1, 4, 8], false);
    let mut group = c.benchmark_group("sparse_changes");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    for workers in workers {
        let runtime = runtime(workers);
        for &rows in &rows {
            // Reuse the existing 32-system fleet fixture: one entity changes,
            // 31 component domains remain idle. No request/entity churn here.
            group.bench_function(BenchmarkId::new(format!("workers={workers}"), rows), |b| {
                let (bowl, target) = runtime.block_on(fleet_bowl(rows));
                let mut revision = 1000_u64;
                b.iter(|| {
                    revision = revision.checked_add(1).expect("fixture revision overflow");
                    runtime.block_on(touch_slot0(&bowl, target, revision));
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, purchases, entity_churn, sparse_changes);
criterion_main!(benches);
