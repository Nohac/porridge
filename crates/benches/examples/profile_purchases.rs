//! Fixed fresh-world batches for counters and externally gated perf sampling.
#[path = "support/allocations.rs"]
mod allocations;
#[path = "support/perf_control.rs"]
mod perf_control;
use perf_control::PerfControl;

#[global_allocator]
static ALLOCATOR: allocations::CountingAllocator = allocations::CountingAllocator;

use benches::{fleet_bowl, stateful::purchases, touch_slot0};
use std::{
    env,
    hint::black_box,
    time::Instant,
};

fn number(name: &str, default: usize, positive: bool) -> usize {
    let value = env::var(name)
        .map(|s| s.parse().unwrap_or_else(|_| panic!("invalid {name}")))
        .unwrap_or(default);
    assert!(!positive || value > 0, "{name} must be positive");
    value
}

fn main() -> std::io::Result<()> {
    let mode = env::var("PROFILE_KERNELS").unwrap_or_else(|_| "async".into())
        .parse::<purchases::KernelMode>().expect("PROFILE_KERNELS");
    let workers = number("PROFILE_WORKERS", 8, true);
    let workload = env::var("PROFILE_WORKLOAD").unwrap_or_else(|_| "purchases".into());
    assert!(matches!(
        workload.as_str(),
        "purchases" | "sparse" | "empty"
    ));
    let rows = number("PROFILE_ROWS", 1000, true);
    let diagnostics = number("PROFILE_METRICS", 0, false) != 0;
    let requests = number("PROFILE_REQUESTS", 1000, true);
    let per_owner = number("PROFILE_PER_OWNER", 1, true);
    let work = u32::try_from(number("PROFILE_WORK", 0, false)).expect("PROFILE_WORK must fit u32");
    let rounds = number("PROFILE_ROUNDS", 50, true);
    let warmup = number("PROFILE_WARMUP", 2, false);
    let width = number("PROFILE_DISPATCH_BATCH", 16, true);
    let mut control = PerfControl::open()?;
    println!(
        "config\tworkers={workers}\trequests={requests}\tper_owner={per_owner}\twork={work}\trounds={rounds}\twarmup={warmup}\tbatch={width}\tworkload={workload}\tkernels={mode:?}\tmetrics={diagnostics}\tsampled={}",
        control.is_some()
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("porridge-worker")
        .build()?;
    // The runner pins only this process's threads, before warmup or sampling.
    if let Some(path) = env::var_os("PROFILE_READY") {
        std::fs::write(path, std::process::id().to_string())?;
        let mut start = String::new();
        std::io::stdin().read_line(&mut start)?;
        if start.trim() != "go" {
            return Err(std::io::Error::other("expected runner start signal"));
        }
    }

    runtime.block_on(async {
        for iteration in 0..rounds.checked_add(warmup).expect("round count overflow") {
            let (bowl, sparse_target) = if workload == "sparse" {
                let (bowl, target) = fleet_bowl(rows).await;
                (bowl, Some(target))
            } else {
                (
                    purchases::create_with_mode(requests.div_ceil(per_owner), u64::MAX, u64::MAX, mode).await,
                    None,
                )
            };
            bowl.set_dispatch_batch_size(width);
            let profiles = bowl.profile_all().await;
            // The third existing bucket times hook commits only; ordinary
            // streamed commits must be attributed with the CPU profile.
            let before = bowl::bowl_debug_buckets();
            let mut metric_before = bowl::metrics::snapshot();
            metric_before.extend(allocations::snapshot());
            let measured = iteration >= warmup;
            bowl::metrics::set_enabled(measured && diagnostics);
            allocations::enable(measured && diagnostics);
            if measured && let Some(control) = &mut control {
                control.command("enable")?;
            }

            let start = Instant::now();
            let responses = match workload.as_str() {
                "purchases" => Some(purchases::batch(&bowl, requests, per_owner, work).await),
                "sparse" => {
                    for index in 0..requests {
                        touch_slot0(&bowl, sparse_target.unwrap(), 1000 + index as u64).await;
                    }
                    None
                }
                "empty" => None,
                _ => unreachable!(),
            };
            let elapsed = start.elapsed();

            if measured && let Some(control) = &mut control {
                control.command("disable")?;
            }
            allocations::enable(false);
            bowl::metrics::set_enabled(false);
            let mut metric_after = bowl::metrics::snapshot();
            metric_after.extend(allocations::snapshot());
            let after = bowl::bowl_debug_buckets();
            // Outside the measured/sampled region: catch incomplete work without
            // charging checks or output formatting to the engine's hot path.
            if let Some(responses) = responses {
                assert_eq!(responses.len(), requests);
                assert!(responses.iter().all(|response| response.accepted));
                black_box(responses);
            }
            if measured {
                let round = iteration - warmup;
                println!(
                    "sample\t{round}\t{requests}\t{}\t{}\t{}\t{}\t{}",
                    elapsed.as_nanos(),
                    after.0 - before.0,
                    after.1 - before.1,
                    after.2 - before.2,
                    after.3 - before.3
                );
                for ((name, before), (_, after)) in metric_before.into_iter().zip(metric_after) {
                    println!("metric\t{round}\t{name}\t{}", after - before);
                }
                for (initial, final_) in profiles.into_iter().zip(bowl.profile_all().await) {
                    println!(
                        "system\t{round}\t{}\t{}\t{}\t{}",
                        final_.name,
                        final_.runs - initial.runs,
                        final_.plan_nanos - initial.plan_nanos,
                        final_.run_nanos - initial.run_nanos
                    );
                }
            }
        }
        Ok(())
    })
}
