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
