# Direct synchronous kernel experiment

Status: initial direct worker path implemented and validated (2026-10-24).
This resumes only O6 experimentation; the broader optimization campaign remains paused.

```rust
builder.system(update_balances).system(fetch_remote_facts)
```

- Infer synchronous versus asynchronous execution from the function signature
  through internal marker types. Both register with `.system(function)`.
- Share registration metadata, planning, access admission, guards, command reuse,
  stale checks, memoization and publication. Direct worker batches contain
  FnOnce calls and execute a plain loop: no per-row future or future set.
- Keep one runtime task per batch and the same per-row completion channel. Send
  each completed row immediately. An unrelated async sibling can remain pending.
- Preserve reader cancellation by checking receiver closure between calls;
  an executing synchronous call cannot be interrupted. Writers still drain.
  Batch size bounds the chunk; kernels must not block on I/O.
- For the fastest first slice, existing grouped hook/settle and no-runtime
  fallback machinery can adapt sync functions into ready futures. Document this
  limit rather than claim every execution path is future-free.
- Use one purchase implementation with thin async adapters. Select async/sync
  registration at runtime in one benchmark binary; no feature changes. The
  measured interval still includes submission, evaluation and result reading.

Validation: mixed sync/async readiness, ordinary mutation/derived output and memo
behavior, worker panic/cancellation, synchronous multi-owner contention, existing
engine tests and playground. Count completed sync calls separately from async
polls. Delegate measurements after tests/build complete: 1/4/8/16 workers,
1,000 purchases, same grain and warmup, paired runs, work counters and allocations.
Also check the new binary's async mode against the existing frozen control.

The purpose is to isolate future orchestration from the shared planner/driver.
If direct execution barely changes end-to-end performance, prioritize the
remaining driver/planning/publication work. If it improves significantly, use
that result to guide further sync batch optimization. Do not infer CPU shares
from allocator categories or overlapping elapsed timers.

## Result

Both modes are available in the same binary and execute the same purchase bodies.
Every counted 1,000-purchase iteration executes 5,000 kernels: async records
5,000 row polls and no direct calls; sync records 5,000 direct calls and no row
polls. Neither mode had stale publications. Shared admission, captured-dependency
validation, guards, output ownership and publication are retained.

Paired steady medians on the shared workstation:

| Workers | Async µs/purchase | Sync µs/purchase | Async purchases/s | Sync purchases/s |
| --- | ---: | ---: | ---: | ---: |
| 1 | 25.70 | 24.63 | 38,916 | 40,601 |
| 4 | 25.86 | 24.80 | 38,664 | 40,322 |
| 8 | 29.23 | 28.73 | 34,214 | 34,813 |
| 16 | 29.36 | 28.46 | 34,059 | 35,143 |

These are total completed purchases, not kernel invocations. Intervals include
input submission, evaluation and result reading. Process ranges overlap;
eight/sixteen workers improve in two of three paired runs. The small median
changes do not establish a robust throughput win or improved worker scaling.
Do not compare these times directly to a previous round with different machine
load. The new binary's async mode also passed a sanity comparison against the
previous retained binary, with nominal allocation/work counts unchanged.

At eight workers, allocator events fall about 41,352.5→40,597.5 per complete
iteration (about 1.83%). Requested allocation plus reallocation-new traffic falls
about 11.95 MB per iteration, consistent with smaller closures and removed future
storage. Retained heap is unchanged; measured peaks fall about 0.65–1.32%.
Allocation-origin names shift for direct closures, so category disappearance is
not evidence that all those allocations vanished.

An additional **fresh-world** CPU sampling pair puts about 73.59%/75.64% of sampled
cycles on the driver for async/sync. That is a different fixture from the steady
timing table. A higher driver fraction alone does not prove greater driver cost;
total sampled cycles were not clearly lower. Treat this as supporting context,
not an exact CPU partition for warmed purchases.

Conclusion: removing per-kernel futures alone did not reveal a large end-to-end
bottleneck. Shared planning/admission, outer completion polling and publication
remain. Native sync still provides a useful execution mode and much
lower requested byte traffic; keep it without claiming it solves scaling.

## Using and reproducing it

Register plain and async functions directly with `.system(function)`. Internal
marker types select the path statically, so async systems do not instantiate the
direct-call executor. The original measurements above used an explicit
`sync_kernel` wrapper; the registration follow-up removes that wrapper without
changing either executor. Those measurements have not been rerun for this API change.

```text
uv run tools/measure-allocations \
  --binary target/release/examples/steady_allocations \
  --before target/release/examples/steady_allocations \
  --before-kernels async --kernels sync \
  --cases purchases --workers 1,4,8,16 --slots 1000 \
  --rounds 10 --warmup 3 --repetitions 3 --metrics \
  --out target/native-sync-comparison
```

`tools/profile-allocations` and `tools/profile-engine` also accept `--kernels`
for the purchase fixture. Features stay identical for tests, timing and profiling.
The existing async-only dispatch microbenchmark does not test the new sync path.

Current limitations: grouped hooks, Settle and no-runtime fallback adapt sync
functions through existing future machinery. The direct path covers ordinary
worker plans. An executing sync call cannot be interrupted; reader cancellation
is checked between calls, and write batches retain the existing drain behavior.
Batch size bounds call count, not duration. Blocking I/O belongs in async kernels.
The existing completion-channel/outer driver remains asynchronous in both modes.

Validation: 174 engine tests, six fixture tests (including both modes for
contention, refusal and replay), nine playground tests, macro/doctest, two runner
tests, workspace build and playground pass. Four optimized smoke suites and all
eight idle/read allocation budgets pass for both modes. Mixed sync publication
while an async sibling waits, mutation/memo behavior, grouped fallback, worker
panic and between-call cancellation are covered.

[Full measurement report](benchmarks/2026-10-24-native-sync.json).
The broader campaign remains paused; this closes the requested focused test.
