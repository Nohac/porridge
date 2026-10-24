# Per-system profiling overhead experiment

Status: runtime control implemented and measured, 2026-10-24. Default remains ON.
No batch-local counter machinery or structural scheduler change was added.

## Result

Disabling per-system profiling gives modest gains in the synchronous workload,
but mixed asynchronous results. It does not explain the poor worker scaling.
Keep the control for observation/benchmarking and move attention back to shared
driver bookkeeping.

Same-binary ON → OFF medians, microseconds per completed purchase:

| Workers | Async ON | Async OFF | Sync ON | Sync OFF |
| --- | ---: | ---: | ---: | ---: |
| 1 | 20.88 | 20.42 | 22.61 | 22.01 |
| 4 | 22.39 | 22.55 | 22.89 | 22.65 |
| 8 | 24.73 | 25.50 | 26.02 | 25.23 |
| 16 | 25.99 | 24.91 | 27.54 | 25.84 |

Sync eight-worker process medians improve in all three pairs (ON 25.92–26.18,
OFF 23.76–25.72 µs/request); sixteen workers improve in two of three pairs with
overlapping ranges. Eight-worker sync's aggregate gain is about 3%; async has no
consistent improvement. These are three alternating process repetitions on a
shared workstation, not confidence intervals or proof that atomic contention
alone caused the differences.

Every counted round completes 1,000 purchases with 5,000 started kernels,
29,000 revision checks and zero stale publications. Async uses 5,000 kernel polls;
sync uses 5,000 direct calls. Single-worker interleavings can change planning
candidates, already-running skips and allocator traffic without changing this
completed work. Stable multiworker allocation counts are unchanged; disabling
statistics does not remove a collection allocation for every row.

Candidate ON versus the previous frozen ON binary has overlapping timing ranges
and unchanged nominal allocation counts. The new async adapter increases requested
allocation/storage traffic by about 111.6 KB per 1,000-purchase iteration in the
stable comparison; runtime OFF does not materially shrink that same-binary layout.
The control is useful, but is not an allocation optimization.

[Machine-readable results and provenance](benchmarks/2026-10-24-system-profiling.json).
Full results, process TSVs, manifests and frozen binaries are under
`target/system-profiling-toggle-20261024-123919/`.

## API and implementation

```rust
bowl.set_system_profiling(false);
// Evaluate normally; engine results and memoization are unchanged.
bowl.set_system_profiling(true);
let totals = bowl.profile_all().await;
```

Collection is enabled by default to preserve existing behavior. Each evaluation
phase captures the setting. Existing/in-flight work retains its captured policy;
disabling collection does not reset totals. `profile_all` reports the cumulative
collected portion, not an estimate of work done while profiling was disabled.

OFF skips per-system plan clock readings, per-poll/sync-call timers, shared
`SystemStats` updates and per-row statistics Arc clones. When independent metrics
are also OFF, async execution directly awaits the row instead of entering the
instrumented poll wrapper. The same typed worker executor is used.

This control is per Bowl, not a process-global benchmark switch. Independent
`metrics` counters and existing global debug buckets are unaffected. It therefore
isolates per-system instrumentation, not every clock/counter in the engine.
Clock overhead, atomic updates, handle traffic and wrapper differences are
removed together; this pass does not separately attribute their individual costs.

## Measurement and validation

One candidate executable supports both settings and both kernel modes, with
identical Cargo features. All cases use 1,000 fixed slots, five warmup rounds,
ten measured rounds and three alternating processes per setting; driver and
workers are pinned separately. Time-only passes disable allocator and diagnostic
counting. Separate counted passes verify complete work. The async fast path
changes when diagnostics are off, so counted allocations cannot prove its exact
uninstrumented execution cost.

The prior frozen binary was built from uncommitted engine sources matching
`26bcf05`; its original manifest remains labeled `da4e418`. The only checked
source difference is a trailing blank in profiling support, not engine behavior.
Do not relabel that historical manifest as a newly built baseline.

Validation: 175 engine tests, six fixture tests, nine playground tests, macro and
doctest checks, Python runner tests, optimized workspace/all-targets build,
engine benchmark smoke and standalone optimized playground. Pause/resume tests
cover sync/async and cooperative/worker execution; mixed async progress is checked
with profiling both enabled and disabled. No rustfmt pass was run, as requested.

```sh
cargo build --workspace --all-targets --profile bench -j 20
uv run tools/measure-allocations \
  --binary target/release/examples/steady_allocations \
  --before target/release/examples/steady_allocations \
  --kernels sync --before-kernels sync \
  --system-stats off --before-system-stats on \
  --cases purchases --workers 1,4,8,16 --slots 1000 \
  --rounds 10 --warmup 5 --repetitions 3 --metrics \
  --out target/system-stats-comparison
```

Use async for both kernel arguments to repeat that matrix. Direct example runs
use `ALLOC_SYSTEM_STATS=on|off` (default on). No new Cargo feature is involved.

## Next focus

The experiment does not justify expanding into batch-local statistics now.
The next structural candidate remains compact component/invocation bookkeeping:
reduce repeated type-ID hashing and identity copying across reservations,
publication and memo state. Typed-return kernels and the broader scheduler
redesign remain proposals, not implemented parts of this slice.
