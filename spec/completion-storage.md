# Reuse ready-completion storage

Status: planned O3 follow-up after producer measurement.

The streaming driver creates `vec![first]`, drains currently ready reader/writer
completions into it, then consumes the Vec at publication. Both its normal path
and its planner-gated path repeat this allocation. The collection boundary is
intentional: publish the already-finished set before deciding whether to replan.

Keep that behavior, but retain the empty Vec across waves, phases and generations:

```text
start_evaluation (existing state lock): take completion scratch from State
phase: push first; collect ready runs; drain and publish in the same order
next wave/phase: reuse the empty capacity
successful generation (existing state lock): return scratch to State
cancel/panic: ordinary owned Vec/Drain drops discard unpublished results
```

No extra task, pool, channel or state-lock acquisition is needed. The scratch is
owned only by the single-flight evaluator. It must be empty at phase boundaries
and normal return. Cancellation may drop capacity; the next driver can allocate
fresh scratch through the existing recovery path. Do not use a self-referential
buffer or retain live output records in State.

Validate existing cancellation, preemption/writer-drain, stale completion,
non-convergence and phase tests. Run full workspace tests and playground. Compare
complete-iteration allocations and one/eight-worker timing with unchanged kernels,
plus retained memory through growth. Expected benefit follows completion-batch
count, not number of useful requests; report that distinction.

Related follow-ups: running/read-owner/reservation containers still have a
phase lifetime. Consider retaining them together later, especially per-component
reservation map capacity. Do not conflate that separate change with the first
completion-buffer measurement.
