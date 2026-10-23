# Reuse ready-completion storage

Status: O3b measured and deferred; implementation removed.

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

## Result: defer this implementation

At 1,000 requests/eight workers, allocations fall 50,857→50,854 and reallocations
1,124→1,095: only **32 fewer events/iteration (0.062%)**. Combined requested-byte
traffic falls about 1,145,604 bytes/iteration. Retained scratch adds about 229 KB
at 1,000 slots and 459–524 KB at 2,000 (4.1–4.7%); peaks rise about 2%.

Eight-worker process medians are 25.68–26.13→26.01–26.82 µs/request: no demonstrated
throughput gain, possible modest overhead. One-worker evidence is also mixed.
168 engine tests, workspace build/playground, four smokes and eight idle/read
allocation budgets pass. The experiment is semantically sound.

The reviewer offered conditional acceptance for reusable capacity/byte traffic.
The campaign decision is **defer** under the clarified multi-worker throughput
priority: the retained capacity is substantial relative to the small event-count
reduction, and no throughput gain justifies it. Restore the previously tested
driver; retain the measurements and design. Revisit after worker completion
transport/native kernel execution changes, or if final allocation budgets show
completion-drain traffic is a material residual. Do not keep this extra state just
because its allocator count is smaller.

[Measured results and provenance](benchmarks/2026-10-23-completion-reuse.json).
