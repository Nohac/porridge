//! Isolate worker dispatch using the production executor, without a Bowl.
// The shared executor also exposes the synchronous engine path, which this
// existing async microbenchmark does not exercise.
#[allow(dead_code)]
#[path = "../../bowl/src/dispatch/executor.rs"]
mod executor;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::{FutureExt, StreamExt, stream::SelectAll};
use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};

fn sizes(name: &str, default: &[usize], zero: bool) -> Vec<usize> {
    env::var(name)
        .map(|value| {
            value
                .split(',')
                .map(|part| {
                    let n = part
                        .trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("invalid {name}"));
                    assert!(zero || n > 0, "{name} must be positive");
                    n
                })
                .collect()
        })
        .unwrap_or_else(|_| default.to_vec())
}

async fn kernel(seed: u64, work: u32, suspend: bool) -> u64 {
    // Keep a modest inline state across suspension; its storage cannot be
    // replaced by a constant because both input and state pass opaque barriers.
    let mut state = black_box([seed; 16]);
    if suspend {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                std::task::Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await;
    }
    let mut value = black_box(&mut state)[seed as usize % 16];
    for _ in 0..work {
        value = black_box(value.wrapping_mul(6364136223846793005).wrapping_add(1));
    }
    black_box(value)
}

#[derive(Clone, Copy)]
enum Variant {
    Typed,
    Boxed,
    SharedTask,
}

impl Variant {
    fn name(self) -> &'static str {
        match self {
            Self::Typed => "typed",
            Self::Boxed => "boxed",
            Self::SharedTask => "shared_task",
        }
    }
}

struct SharedTask;

impl executor::SpawnTask for SharedTask {
    fn spawn<F, T>(runtime: &tokio::runtime::Handle, future: F) -> tokio::task::JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        runtime.spawn(future.boxed())
    }
}

async fn dispatch(
    seeds: &[u64],
    width: usize,
    work: u32,
    suspend: bool,
    variant: Variant,
) -> (usize, u64) {
    let handle = tokio::runtime::Handle::current();
    let mut batches = SelectAll::new();
    for chunk in seeds.chunks(width) {
        let runs = chunk.iter().map(|&seed| kernel(seed, work, suspend));
        let batch = match variant {
            Variant::Typed => executor::spawn(&handle, runs.collect()),
            Variant::Boxed => executor::spawn(&handle, runs.map(FutureExt::boxed).collect()),
            Variant::SharedTask => {
                executor::spawn_with::<SharedTask, _, _>(&handle, runs.collect())
            }
        };
        batches.push(batch);
    }

    let mut count = 0;
    let mut checksum = 0u64;
    while let Some(value) = batches.next().await {
        count += 1;
        checksum = checksum.wrapping_add(value);
    }
    (count, checksum)
}

fn dispatch_benches(c: &mut Criterion) {
    let counts = sizes("DISPATCH_ROWS", &[1, 1000], false);
    let workers = sizes("DISPATCH_WORKERS", &[1, 8], false);
    let widths = sizes("DISPATCH_BATCH", &[16], false);
    let work = sizes("DISPATCH_WORK", &[0, 10000], true);
    let states = sizes("DISPATCH_SUSPEND", &[0, 1], true);
    assert!(
        states.iter().all(|&n| n <= 1),
        "DISPATCH_SUSPEND must be 0 or 1"
    );
    let variant = env::var("DISPATCH_VARIANT").unwrap_or_else(|_| "all".into());
    assert!(matches!(
        variant.as_str(),
        "all" | "typed" | "boxed" | "shared_task"
    ));
    let mut group = c.benchmark_group("dispatch");
    group.sample_size(10);
    for workers in workers {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .build()
            .unwrap();
        for &rows in &counts {
            let seeds: Vec<u64> = (0..rows).map(|row| (row as u64).wrapping_add(17)).collect();
            group.throughput(Throughput::Elements(rows as u64));
            for &width in &widths {
                for &work in &work {
                    let work = u32::try_from(work).expect("DISPATCH_WORK must fit u32");
                    for &suspend in &states {
                        let suspend = suspend == 1;
                        // Correctness checks are outside Criterion timing. Check the
                        // complete result against arithmetic, not only two backends.
                        let expected = seeds
                            .iter()
                            .map(|&seed| {
                                (0..work).fold(seed, |value, _| {
                                    value.wrapping_mul(6364136223846793005).wrapping_add(1)
                                })
                            })
                            .fold(0u64, u64::wrapping_add);
                        for mode in [Variant::Typed, Variant::Boxed, Variant::SharedTask] {
                            let label = mode.name();
                            if variant != "all" && variant != label {
                                continue;
                            }
                            assert_eq!(
                                runtime.block_on(dispatch(&seeds, width, work, suspend, mode)),
                                (rows, expected)
                            );
                            let name = format!(
                                "rows={rows}/workers={workers}/batch={width}/work={work}/suspend={suspend}"
                            );
                            group.bench_function(BenchmarkId::new(label, name), |b| {
                                b.iter_custom(|iterations| {
                                    let mut elapsed = Duration::ZERO;
                                    runtime.block_on(async {
                                        for _ in 0..iterations {
                                            let seeds = black_box(seeds.as_slice());
                                            let start = Instant::now();
                                            let result =
                                                dispatch(seeds, width, work, suspend, mode).await;
                                            elapsed += start.elapsed();
                                            black_box(result);
                                        }
                                    });
                                    elapsed
                                });
                            });
                        }
                    }
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, dispatch_benches);
criterion_main!(benches);
