//! Allocation counts for retained versus fresh heterogeneous command queues.
#[path = "support/allocations.rs"]
mod allocations;
mod command_storage;
#[cfg(test)]
#[path = "command_storage/tests.rs"]
mod command_storage_tests;

#[global_allocator]
static ALLOCATOR: allocations::CountingAllocator = allocations::CountingAllocator;

use command_storage::{Boxed, BumpQueue, Packed, Queue, State, counts, round};

fn measure<Q: Queue>(name: &str, count: usize, mixed: bool, reuse: bool) {
    let mut queue = Q::default();
    let mut state = State::default();
    // Repeated reset lets a growing bump arena converge to one retained chunk.
    for _ in 0..64 {
        round(&mut queue, &mut state, count, mixed);
    }
    let before = allocations::snapshot();
    allocations::enable(true);
    for _ in 0..100 {
        if reuse {
            round(&mut queue, &mut state, count, mixed);
        } else {
            round(&mut Q::default(), &mut state, count, mixed);
        }
    }
    allocations::enable(false);
    let after = allocations::snapshot();
    std::hint::black_box(state);
    for ((metric, before), (_, after)) in before.into_iter().zip(after) {
        let delta = after - before;
        if reuse && name != "boxed" && metric.ends_with("calls") {
            assert_eq!(
                delta, 0,
                "warmed {name} must reuse allocator capacity: {metric}"
            );
        }
        println!(
            "{name}\t{count}\t{mixed}\t{reuse}\t{metric}\t{delta}\t{}",
            count * 100
        );
    }
}

fn main() {
    println!("queue\tcommands\tmixed\treuse\tmetric\tcount\toperations");
    for count in counts() {
        for mixed in [false, true] {
            for reuse in [true, false] {
                measure::<Boxed>("boxed", count, mixed, reuse);
                measure::<BumpQueue>("bump-scope", count, mixed, reuse);
                measure::<Packed>("packed", count, mixed, reuse);
            }
        }
    }
}
