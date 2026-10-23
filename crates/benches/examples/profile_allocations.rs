//! Heap call-stack attribution for the same warmed fixed-ID purchase workload.
//! Separate executable: DHAT never replaces the production or timing allocator.
use benches::stateful::steady::Purchases;
use std::{env, path::PathBuf};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn number(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|s| s.parse().expect(name))
        .unwrap_or(default)
}

fn main() {
    let workers = number("HEAP_WORKERS", 8);
    let slots = number("HEAP_SLOTS", 1000);
    let warmup = number("HEAP_WARMUP", 8);
    let rounds = number("HEAP_ROUNDS", 1);
    let lifetime = number("HEAP_LIFETIME", 0) != 0;
    let grow_to = number("HEAP_GROW_TO", slots);
    assert!(grow_to >= slots);
    let path = PathBuf::from(env::var_os("HEAP_OUT").expect("set HEAP_OUT to a new profile path"));
    assert!(workers > 0 && slots > 0 && rounds > 0 && warmup > 0);
    assert!(!path.exists(), "refusing to overwrite heap profile");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap();
    if let Some(ready) = env::var_os("HEAP_READY") {
        std::fs::write(ready, std::process::id().to_string()).unwrap();
        let mut signal = String::new();
        std::io::stdin().read_line(&mut signal).unwrap();
        assert_eq!(signal.trim(), "go");
    }
    runtime.block_on(async {
        if lifetime {
            profile_lifetime(slots, grow_to, warmup, rounds, &path).await;
            return;
        }
        let mut fixture = Purchases::new(slots).await;
        fixture.bowl.set_dispatch_batch_size(16);
        for _ in 0..warmup {
            assert_eq!(fixture.round().await, slots);
            fixture.verify().await;
        }
        let before = bowl::metrics::snapshot();
        let profile = dhat::Profiler::builder()
            .file_name(&path)
            .trim_backtraces(Some(32))
            .build();
        bowl::metrics::set_enabled(true);
        let mut completed = 0;
        for _ in 0..rounds {
            completed += fixture.round().await;
        }
        bowl::metrics::set_enabled(false);
        let stats = dhat::HeapStats::get();
        drop(profile);
        let after = bowl::metrics::snapshot();
        assert_eq!(
            completed,
            slots.checked_mul(rounds).expect("operation count overflow")
        );
        fixture.verify().await;
        println!(
            "requests={completed} workers={workers} blocks={} bytes={}",
            stats.total_blocks, stats.total_bytes
        );
        for ((name, before), (_, after)) in before.into_iter().zip(after) {
            println!("metric\t{name}\t{}", after - before);
        }
    });
}

async fn profile_lifetime(slots: usize, grow_to: usize, warmup: usize, rounds: usize, path: &std::path::Path) {
    // Allocate the reporting storage before profiling. Its capacity is fixed,
    // so collecting samples cannot itself look like accumulating engine memory.
    let per_phase = warmup.checked_add(rounds).and_then(|n| n.checked_add(1)).expect("sample count overflow");
    let mut samples = Vec::with_capacity(per_phase.checked_mul(2).expect("sample count overflow"));
    let profile = dhat::Profiler::builder().file_name(path).trim_backtraces(Some(32)).build();
    let mut fixture = Purchases::new(slots).await;
    fixture.bowl.set_dispatch_batch_size(16);
    samples.push(("setup", 0, slots, dhat::HeapStats::get()));
    for (size, warm_label, steady_label) in [
        (slots, "warmup", "steady"),
        (grow_to, "grown-warmup", "grown-steady"),
    ] {
        if steady_label == "grown-steady" {
            if grow_to == slots { break; }
            fixture.grow_to(size).await;
            fixture.verify().await;
            samples.push(("growth", 0, size, dhat::HeapStats::get()));
        }
        for round in 0..warmup.checked_add(rounds).expect("round count overflow") {
            assert_eq!(fixture.round().await, size);
            fixture.verify().await;
            let (label, index) = if round < warmup {
                (warm_label, round)
            } else {
                (steady_label, round - warmup)
            };
            samples.push((label, index, size, dhat::HeapStats::get()));
        }
    }
    drop(profile);
    for (phase, round, size, stats) in samples {
        println!("memory\t{phase}\t{round}\t{size}\t{}\t{}\t{}\t{}",
            stats.curr_blocks, stats.curr_bytes, stats.max_bytes, stats.total_blocks);
    }
}
