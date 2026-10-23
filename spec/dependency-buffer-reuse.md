# Reuse retired memo dependency buffers

Status: simple LIFO prototype measured and deferred; removed from the live engine.

The planner's temporary dependency scratch is already reused. Every admitted row
still copies that scratch into a fresh owned Vec so its observations survive
execution and stale checking. Successful publication then replaces a memo entry,
dropping the previous entry's owned Vec. Both sides run through the existing
single evaluator and state-lock boundary.

Reuse the retired allocation, preserving those distinct lifetimes:

```text
planning: free empty Vec -> copy this row's observed deps -> invocation owns it
execution/publication: invocation -> memo update -> installed memo entry
replacement: previous memo entry -> clear old deps -> planning free list
```

Illustrative ownership flow (internal API names are provisional):

```rust
// Once memo admission succeeds; temporary candidate scratch stays separate.
let deps = storage.dependencies.copy_from(observed_deps);

// After stale checking, write reconciliation and final epoch stamping.
if let Some(previous) = memo.insert(owner, entry) {
    storage.dependencies.recycle(previous.into_dependencies());
}
```

- Keep the free list in driver-owned planning storage, outside snapshots. No new
  Arc, mutex, channel or unsafe code. Dep values are Copy metadata, not user data.
- Never take a buffer from a still-installed memo or executing invocation. The
  prior entry is reusable only after replacement transfers ownership to commit.
- Empty buffers need no allocation. Start with a simple retained-capacity LIFO;
  measure capacity mismatch before adding size classes or search overhead.
- Keep custom/grouped/unpooled planner fallback. Initial coverage is ordinary
  planning and successful memo replacement; stale/cancelled frames and removed
  memo entries may still drop buffers normally. Extend only after attribution.
- Cancellation may lose capacity; it must not publish a partial memo. Existing
  stale checks, presence/absence deps and self-write reconciliation stay unchanged.

Validate changed/optional dependencies, old/new memo isolation, stale completion,
preemption, cancellation and custom parameters with existing engine tests. Add a
focused reuse/lifetime regression where needed. Compare allocation/work counts,
1/4/8/16-worker timing and retained memory through growth. The target is the owned
per-admission dependency allocation, not every remaining planning allocation.

This is separate from future-node storage. Reusing planner-owned buffers may help
feed workers by reducing driver allocation; that benefit requires measurement.

## Pooled completion eligibility

Only a completion produced through pooled planning may return an old memo buffer.
Carry that eligibility in the short-lived SystemRun, not persistent MemoEntry.
Grouped/hook/Settle fallback runs default to false. Otherwise those paths could
return one old buffer per run without ever borrowing one, growing the free list
indefinitely. Test repeated ordinary, grouped and Settle runs explicitly.

## Result: defer the simple LIFO pool

The prototype passes170engine tests (including grouped/Settle non-accumulation),
workspace build/playground, four optimized smokes and eight idle/read budgets.
At1,000requests/eight workers, alloc calls fall45,227→40,227, but reallocation
median rises1,124.5→1,142 as capacity mixes. Combined requested traffic falls
about1.62MB/iteration. The diagnostic profile records42.836events/request with
5,000completed rows,333batches,7already-running candidates and no stale commits.

Retained heap rises **22–23%**: about1.24–1.28MB at1,000slots and2.42–2.47MB at2,000.
Measured peaks rise7.8–8.2%. The twelve-round samples still drift upward within
their range, so they do not establish a flat retained plateau. Capacity mixing is
a plausible cause, not a conclusion proved by those totals.

Eight/sixteen-worker aggregate medians are31.32→31.95 and29.38→30.08µs/request,
with broad overlapping process ranges. One/four workers improve, but there is no
established multi-worker throughput gain to justify the retained-capacity cost.

Defer this implementation. Revisit with capacity attribution/selection or a row
completion design that reuses installed memo storage without retaining a second
large set of buffers. Any lazy recapture of observations must preserve the exact
original dependencies, including custom-parameter/interior-mutation behavior;
re-reading current state is not equivalent to captured observations.

The live engine is restored to the tested shared-context implementation.
[Results/provenance](benchmarks/2026-10-23-dependency-reuse.json) and the
[prototype patch](benchmarks/2026-10-23-dependency-reuse.patch) preserve the experiment
against base ef5bf10. No pool eligibility flags remain in production code.
