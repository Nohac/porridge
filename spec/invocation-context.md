# Share one immutable context per system plan

Status: kept after the completion-channel experiment.

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

## Result: keep

All 169 engine tests, workspace build/playground, four optimized smokes and eight
idle/read budgets pass. Stable eight-worker allocator events remain **46,351 per
1,000-request iteration**. Combined requested traffic falls about **683 KB** per
iteration (allocation bytes18,330,527→18,024,251; reallocation-new bytes
12,414,432→12,037,440). This does not claim fewer allocation calls.

At 1/4/8/16 workers, aggregate µs/request medians are respectively
21.13→21.00,22.84→23.15,26.01→25.04,27.04→26.66. Eight/sixteen-worker pairs improve
2/3 times with overlapping ranges; four workers show a small possible cost.
Keep for simpler shared ownership, smaller requested traffic and favorable
multi-worker evidence, with no universal speedup claim.

Retained heap at 1,000 slots is effectively flat. The 2,000-slot candidate holds
about64 KB more (roughly0.6%), a stable pool/memo bucket level; measured peaks are
flat at1,000 and about1%lower at2,000. The clean diagnostic run executes5,000rows
in315batches without stale/already-running rows and observes46.355events/request.
[Results and provenance](benchmarks/2026-10-23-shared-context.json).

Next planning-side experiment: [retired dependency buffers](dependency-buffer-reuse.md).
The future-node storage proposal remains available for the dispatch residual.
