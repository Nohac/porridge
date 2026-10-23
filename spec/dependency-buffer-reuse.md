# Reuse retired memo dependency buffers

Status: planned O2/O3 experiment after shared-context measurement.

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
    storage.dependencies.recycle(previous.deps);
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
