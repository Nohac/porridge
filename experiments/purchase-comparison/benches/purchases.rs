use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use purchase_comparison::{Purchase, bevy, porridge};
use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};

fn number(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|v| v.parse().expect("expected an integer"))
        .unwrap_or(default)
}

fn purchases(c: &mut Criterion) {
    let workers = number("PURCHASE_WORKERS", 1);
    let requests = number("PURCHASE_REQUESTS", 1000);
    let per_owner = number("PURCHASE_PER_OWNER", 1);
    let work = u32::try_from(number("PURCHASE_WORK", 0)).expect("work must fit u32");
    assert!(workers > 0 && requests > 0 && per_owner > 0);
    let backend = env::var("PURCHASE_BACKEND").unwrap_or_else(|_| "both".into());
    assert!(matches!(backend.as_str(), "bevy" | "porridge" | "both"));
    let mut group = c.benchmark_group("purchase_reference");
    group.sample_size(10);
    group.throughput(Throughput::Elements(requests as u64));
    let case = format!("requests={requests}/workers={workers}/per_owner={per_owner}/work={work}");

    if backend != "bevy" {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .build()
            .unwrap();
        group.bench_function(BenchmarkId::new("porridge", &case), |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                runtime.block_on(async {
                    for _ in 0..iterations {
                        let engine =
                            porridge::Engine::new(requests.div_ceil(per_owner), u64::MAX, u64::MAX)
                                .await;
                        let start = Instant::now();
                        for index in (0..requests).rev() {
                            engine
                                .submit(
                                    index / per_owner,
                                    Purchase {
                                        sequence: (index % per_owner + 1) as u64,
                                        price: 1,
                                        work,
                                    },
                                )
                                .await;
                        }
                        let responses = engine.finish().await;
                        elapsed += start.elapsed();
                        black_box(responses);
                    }
                });
                elapsed
            });
        });
    }
    if backend != "porridge" {
        bevy::init_workers(workers);
        group.bench_function(BenchmarkId::new("bevy", &case), |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let mut engine =
                        bevy::Engine::new(requests.div_ceil(per_owner), u64::MAX, u64::MAX);
                    let start = Instant::now();
                    for index in (0..requests).rev() {
                        engine.submit(
                            index / per_owner,
                            Purchase {
                                sequence: (index % per_owner + 1) as u64,
                                price: 1,
                                work,
                            },
                        );
                    }
                    let responses = engine.finish();
                    elapsed += start.elapsed();
                    black_box(responses);
                }
                elapsed
            });
        });
    }
    group.finish();
}

criterion_group!(benches, purchases);
criterion_main!(benches);
