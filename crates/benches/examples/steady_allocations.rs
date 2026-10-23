//! Allocation traffic on a fixed warmed world. See spec/steady-state-allocations.md.
#[path = "support/allocations.rs"]
mod allocations;

#[global_allocator]
static ALLOCATOR: allocations::CountingAllocator = allocations::CountingAllocator;

use benches::stateful::steady::Purchases;
use bowl::{Bowl, Component, Entity, Query, Schema};
use std::{env, hint::black_box, time::Instant};

#[derive(Component)]
struct Stamp(u64);

#[derive(Schema)]
struct ProbeSchema {
    request: (Stamp,),
}

async fn allocating_probe(query: Query<&Stamp>) {
    let value = query.item().0;
    allocations::kernel(async {
        drop(black_box(vec![value as u8; 64]));
        // Force multiple polls: attribution must not leak across Pending.
        allocations::engine(tokio::task::yield_now()).await;
        drop(black_box(vec![value as u8; 64]));
    })
    .await;
}

fn number(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|value| value.parse().expect(name))
        .unwrap_or(default)
}

fn main() {
    let workers = number("ALLOC_WORKERS", 8);
    let slots = number("ALLOC_SLOTS", 1000);
    let rounds = number("ALLOC_ROUNDS", 10);
    let warmup = number("ALLOC_WARMUP", 3);
    let counting = number("ALLOC_COUNT", 1) != 0;
    let metrics = number("ALLOC_METRICS", 0) != 0;
    let case = env::var("ALLOC_CASE").unwrap_or_else(|_| "purchases".into());
    assert!(workers > 0 && slots > 0 && rounds > 0 && warmup > 0);
    assert!(matches!(case.as_str(), "purchases" | "read" | "probe"));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("porridge-worker")
        .build()
        .unwrap();
    println!(
        "config\tcase={case}\tworkers={workers}\tslots={slots}\twarmup={warmup}\trounds={rounds}\tcounting={counting}"
    );
    if let Some(path) = env::var_os("ALLOC_READY") {
        std::fs::write(path, std::process::id().to_string()).unwrap();
        let mut start = String::new();
        std::io::stdin().read_line(&mut start).unwrap();
        assert_eq!(start.trim(), "go");
    }

    runtime.block_on(async {
        let mut purchases = Purchases::new(if case == "probe" { 0 } else { slots }).await;
        purchases.bowl.set_dispatch_batch_size(16);
        let probe = Bowl::builder()
            .schema::<ProbeSchema>()
            .system(allocating_probe)
            .build();
        probe.set_dispatch_batch_size(16);
        probe.set_commit_limit(bowl::CommitLimit::None);
        let mut ids: Vec<Entity> = Vec::new();
        if case == "probe" {
            for _ in 0..slots {
                ids.push(probe.insert((Stamp(0),)).await.entity());
            }
            probe.scoop::<Query<&Stamp>>().await;
        } else if case == "read" {
            assert_eq!(purchases.round().await, slots);
        }

        for round in 0..warmup.checked_add(rounds).expect("round count overflow") {
            let metrics_before = if metrics {
                bowl::metrics::snapshot()
            } else {
                Vec::new()
            };
            bowl::metrics::set_enabled(metrics && round >= warmup);
            let before = allocations::snapshot();
            let kernel_before = allocations::kernel_snapshot();
            allocations::enable(counting && round >= warmup);
            let start = Instant::now();
            let completed = match case.as_str() {
                "purchases" => purchases.round().await,
                "read" => purchases.read().await,
                "probe" => {
                    for &entity in &ids {
                        probe
                            .entity(entity)
                            .insert((Stamp(round as u64 + 1),))
                            .await;
                    }
                    let result = probe.scoop::<Query<&Stamp>>().await;
                    black_box(
                        result
                            .collect()
                            .into_iter()
                            .map(|stamp| stamp.0)
                            .sum::<u64>(),
                    );
                    result.len()
                }
                _ => unreachable!(),
            };
            let wall_ns = start.elapsed().as_nanos();
            allocations::enable(false);
            bowl::metrics::set_enabled(false);
            let metrics_after = if metrics {
                bowl::metrics::snapshot()
            } else {
                Vec::new()
            };
            let after = allocations::snapshot();
            let kernel_after = allocations::kernel_snapshot();

            // All checks, output, and verification queries are outside measurement.
            assert_eq!(completed, slots);
            if case != "probe" {
                purchases.verify().await;
            }
            if round >= warmup {
                println!("sample\t{}\t{slots}\t{wall_ns}", round - warmup);
                for ((name, before), (_, after)) in metrics_before.into_iter().zip(metrics_after) {
                    println!("metric\t{}\t{name}\t{}", round - warmup, after - before);
                }
                for (((name, before), (_, after)), ((_, kb), (_, ka))) in before
                    .into_iter()
                    .zip(after)
                    .zip(kernel_before.into_iter().zip(kernel_after))
                {
                    let kernel = ka - kb;
                    let engine = after - before - kernel;
                    if counting && name == "alloc_calls" {
                        assert_eq!(kernel, if case == "probe" { 2 * slots as u64 } else { 0 });
                    }
                    println!("allocation\t{}\t{name}\t{engine}\t{kernel}", round - warmup);
                }
            }
        }
    });
}
