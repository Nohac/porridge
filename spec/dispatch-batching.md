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

Ordinary kernels now use typed plans and concrete worker row futures (see below).
Per-row bookkeeping remains; prepared fetching and planner/publication profiling
are follow-ups. This is task amortization, not distributed planning or a new scheduling
contract. The maximum of 16 is provisional; quiet-host tuning remains open.

See [the performance plan](stateful-performance-plan.md) for measurements and
remaining work. In the isolated purchase reference, `PURCHASE_DISPATCH_BATCH`
selects the maximum at runtime, and is included in the Porridge benchmark ID.

## Typed execution plans

```text
heterogeneous system registry
  plan_runs(...) -> Box<dyn RunPlan>          once per system planning pass
    concrete iterator of planned rows
      shared admission: identity, access, reader/writer classification
      concrete row futures -> typed worker batch
        specialized Tokio task
          FuturesUnordered<ConcreteRowFuture>
            kernel future + guards + per-row timing
        erased completion stream -> driver publication
```

`FunctionSystem::plan_rows` owns the callee and snapshot and keeps the row future
concrete. `dispatch::prepare` erases the iterator once at the plan boundary;
`RunPlan::dispatch` returns to generic code that constructs and dispatches row
futures. The dispatcher therefore retains each ordinary kernel's concrete future
type through worker polling. There is no public API or user-system change.

`dispatch/plan.rs` owns shared admission for both typed and erased plans. It uses
the existing reservations and forced-replanning rules; the evaluator still owns
publication and releases reservations only after a row's commit. Reader and writer
batches, adaptive size, pending-sibling completion, cancellation and panic behavior
are unchanged. Poll timing uses safe stack pinning of potentially `!Unpin` futures.

The normal rows of `on_settled` forward to typed plans. `on_start` and `on_complete`
retain their existing grouped, boxed execution paths: their callback boundary is
not silently split into independently publishing rows. Their fallback feeds the
same admission code. The separate settle-hook execution path remains unchanged.

Without Tokio, heterogeneous row futures still need one box each to inhabit the
flat cooperative queue. Previously ordinary rows had both a boxed invocation and
a boxed dispatch wrapper; now that path boxes the combined future once. Worker
batches avoid those two explicit per-row future boxes. Tokio tasks, completion streams,
`FuturesUnordered` nodes, commands, guards and dependency metadata can still
allocate. This is neither allocation-free execution nor elimination of all dynamic
dispatch, and does not imply SIMD execution.

The planner now materializes invocation metadata and returns a lazy iterator of
future construction. Consequently `plan_nanos` excludes that construction, which
runs during admission; old and new planning-counter percentages are not directly
comparable. End-to-end benchmark timing includes both, unchanged. Prepared store
fetching and storage layout changes are deliberately outside this slice.

The retained typed implementation also specializes Tokio task management for every
kernel. That grew the engine benchmark's `.text` from 5,446,824 to 7,030,808 bytes
(29%; GNU `size`'s broader text total grew 33%). A second candidate erases only the
batch future before spawning, sharing Tokio machinery while retaining typed row
polling. Both candidates are measured in alternating fresh processes. Selection
prioritizes speed; code growth is an acceptable tradeoff when it improves runtime.
The fully specialized version is retained. The workload-dependent results do not
establish an execution-speed advantage for either boundary, and smaller code is
not the objective. Keep the alternative as benchmark evidence, not a runtime
policy knob. [Isolated dispatch measurements and full-workload profiles](execution-profiling.md)
now show a benefit from concrete rows in the measured eight-worker tiny-kernel
cases, while driver bookkeeping dominates light complete workloads. The production
worker executor lives in `dispatch/executor.rs`; the microbenchmark includes that
same source. Prepared driver metadata is the proposed next slice.
