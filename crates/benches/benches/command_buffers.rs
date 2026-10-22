//! Same commands and queue implementations as the allocation diagnostic.
#[path = "../examples/command_storage/mod.rs"]
mod command_storage;

use command_storage::{Boxed, BumpQueue, Packed, Queue, State, counts, round};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::{hint::black_box, time::Duration};

fn register<Q: Queue>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    count: usize,
    mixed: bool,
    reuse: bool,
) {
    let mut queue = Q::default();
    let mut state = State::default();
    for _ in 0..64 {
        round(&mut queue, &mut state, count, mixed);
    }
    group.bench_function(BenchmarkId::new(name, count), |bench| {
        bench.iter(|| {
            if reuse {
                round(&mut queue, &mut state, count, mixed);
            } else {
                round(&mut Q::default(), &mut state, count, mixed);
            }
            black_box(&state);
        });
    });
}

fn benches(criterion: &mut Criterion) {
    for mixed in [false, true] {
        for reuse in [true, false] {
            let mut group = criterion.benchmark_group(format!(
                "commands/{}/{}/",
                if mixed { "mixed" } else { "scalar" },
                if reuse { "reuse" } else { "fresh" }
            ));
            group.sample_size(20);
            group.warm_up_time(Duration::from_millis(100));
            group.measurement_time(Duration::from_millis(300));
            for count in counts() {
                group.throughput(Throughput::Elements(count as u64));
                register::<Boxed>(&mut group, "boxed", count, mixed, reuse);
                register::<BumpQueue>(&mut group, "bump-scope", count, mixed, reuse);
                register::<Packed>(&mut group, "packed", count, mixed, reuse);
            }
            group.finish();
        }
    }
}

criterion_group!(command_buffers, benches);
criterion_main!(command_buffers);
