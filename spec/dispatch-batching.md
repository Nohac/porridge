# Worker dispatch batching

The scheduler groups independently admitted invocations from one system into
worker tasks. It does not merge their dependency identities, reservations,
outputs, memo entries or commits.

```rust
let bowl = Bowl::builder().schema::<AppSchema>().system(derive).build();
bowl.set_dispatch_batch_size(16); // maximum, not a minimum or a commit group
assert_eq!(bowl.dispatch_batch_size(), 16);
```

The initial maximum is 16. Setting 1 selects individual worker dispatch; zero
is rejected. Each phase captures the setting once. With Tokio, the target size is
`ceil(planned_rows / (workers * 4))`, clamped to `1..=maximum`. The planner's
candidate count can exceed actual admissions; partial batches dispatch immediately
at the end of that system's admission pass. There is no wait to fill a batch.

## Execution and publication

```text
plan system rows
  reserve each conflict-free row individually
  separate reader and writer batches
  submit each batch as one Tokio task
    poll independent row futures cooperatively inside that task
    send each completed row immediately
  driver consumes completion streams
    validate captured dependencies
    publish that row's outputs and memo
    release that row's access reservation
    replan affected work
```

A pending async row does not hold back a ready sibling's result or its downstream
consumers. A single CPU-bound poll cannot be split among threads: rows inside one
batch share a task, while separate tasks can execute on different workers.

Multi-row tasks use a completion channel bounded by the number of admitted rows,
with one sender and at most one result per row. A single-row task skips that
channel. Without an ambient Tokio runtime, the same build polls row futures in one flat
cooperative queue, avoiding nested batch streams; no feature change is needed. Bounds are per batch, not a global
admission or memory budget.

Reader and writer tasks never mix. Preemption drops reader streams (aborting their
tasks), drains writers and publishes their writes, then opens the existing external
mutation boundary. Task abortion is asynchronous. Panics propagate rather than
masquerading as successful stream exhaustion. Batches are not atomic: previously
published sibling results are not rolled back when another invocation panics.

## Validation and scope

Dispatcher tests cover cooperative execution, streamed completion, cancellation
and panic propagation. An end-to-end test places a waiting row and a ready row in
the same 16-row task: a downstream system must see the ready row's published
answer and release the waiting one. Existing workspace contracts cover access
conflicts, stale results, preemption, writer draining and derived ownership.

That integration test also exposed an existing query planning bug:
`Query<&Answer, With<Trigger>>` could watch only `Trigger`, so inserting `Answer`
failed to wake its consumer. Scalar `&T` now declares interest in its own component
store. This is a correctness fix discovered during the batching work, not a
batch-size tuning change.

The implementation retains boxed planned invocations and per-row bookkeeping.
Homogeneous typed batches, fewer allocations and planner/publication profiling are
follow-ups. This is task amortization, not distributed planning or a new scheduling
contract. The maximum of 16 is provisional; quiet-host tuning remains open.

See [the performance plan](stateful-performance-plan.md) for measurements and
remaining work. In the isolated purchase reference, `PURCHASE_DISPATCH_BATCH`
selects the maximum at runtime, and is included in the Porridge benchmark ID.
