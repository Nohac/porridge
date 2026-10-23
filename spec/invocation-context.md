# Share one immutable context per system plan

Status: planned small O2/O7 scaling experiment after the completion channel.

FunctionSystem::plan_rows already shares snapshot + prepared stores through one
Arc context. It still clones Bowl and Arc<F> into every row alongside that context.
Those handles are immutable for the entire plan, and all row frames already keep
the shared context alive.

Move Bowl and the callee handle into that same context. Each row then clones one
context handle and borrows its Bowl/callee during fetch/run, instead of cloning
three independent handles. The planned invocation identity, commands, deps,
write rows and guard storage remain individually owned.

Expected mechanism: remove two shared Arc increments/decrements per row and
reduce its captured handles, with no new pool or dependency. This targets
multi-worker reference-count contention and frame size, not allocator-event count.
Do not claim a throughput gain merely from counting removed atomics.

Lifetime check: the context already lasts until its last row completes. Any
remaining row previously kept Bowl and F alive independently; grouping them
preserves that minimum lifetime. Query/custom parameters can still clone whatever
they explicitly retain. No output, memo or external result may borrow a dropped
context.

Run existing engine cancellation, delayed completion, hooks, custom-parameter,
retained-results and playground tests. Benchmark1/4/8/16 workers, record allocation
counts/bytes and timing, and inspect any meaningful memory change. Keep this
separate from reusable future-node storage so its effect can be attributed.
