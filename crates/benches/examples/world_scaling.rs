//! Runtime-controlled fixed purchase traffic in an evolving persistent world.
use std::{env, time::Instant};
use benches::stateful::{population::{Config, Population}, purchases::KernelMode};

fn number(name: &str, default: usize) -> usize {
    env::var(name).map(|value| value.parse().expect(name)).unwrap_or(default)
}

fn main() {
    let config = Config {
        active: number("WORLD_ACTIVE", 1000), dormant: number("WORLD_DORMANT", 0),
        devices: number("WORLD_DEVICES", 0), updates: number("WORLD_UPDATES", 0),
        extra_types: number("WORLD_TYPES", 0), rows_per_type: number("WORLD_ROWS_PER_TYPE", 0),
        observers: number("WORLD_OBSERVERS", 0) != 0,
        mode: env::var("WORLD_KERNELS").unwrap_or_else(|_| "async".into()).parse::<KernelMode>().expect("WORLD_KERNELS"),
        workers: number("WORLD_WORKERS", 8),
    };
    let warmup = number("WORLD_WARMUP", 5);
    let rounds = number("WORLD_ROUNDS", 10);
    let metrics = number("WORLD_METRICS", 0) != 0;
    assert!(warmup > 0 && rounds > 0);
    println!("config\t{config:?}\twarmup={warmup}\trounds={rounds}\tmetrics={metrics}");
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(config.workers).build().unwrap();
    if let Some(path) = env::var_os("WORLD_READY") {
        std::fs::write(path, std::process::id().to_string()).unwrap();
        let mut start = String::new();
        std::io::stdin().read_line(&mut start).unwrap();
        assert_eq!(start.trim(), "go");
    }
    runtime.block_on(async {
        let setup = Instant::now();
        let mut population = Population::new(config).await;
        println!("setup_ns\t{}", setup.elapsed().as_nanos());
        for _ in 0..warmup { population.round().await; }
        population.verify().await;
        let before = bowl::metrics::snapshot();
        bowl::metrics::set_enabled(metrics);
        let mut samples = Vec::with_capacity(rounds);
        for _ in 0..rounds { samples.push(population.round().await); }
        bowl::metrics::set_enabled(false);
        let after = bowl::metrics::snapshot();
        population.verify().await;
        for (index, sample) in samples.iter().enumerate() {
            println!("sample\t{index}\t{}\t{}\t{}\t{}\t{}", config.active, config.updates, sample.total.as_nanos(), sample.submit.as_nanos(), sample.settle.as_nanos());
        }
        if metrics {
            for ((name, a), (_, b)) in before.into_iter().zip(after) { println!("metric\t{name}\t{}", b-a); }
        }
    });
}
