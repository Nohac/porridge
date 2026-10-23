# Complete-iteration allocation budgets

Status: O0 measurement foundation, 2026-10-23. Lifetime/growth profiling and the
broader semantic audit remain open in the campaign roadmap.

## Boundary

A measured purchase iteration submits fresh revisions to a fixed set of request
entities, runs the normal engine to settlement, and materializes all replies.
The `drive` case submits the same work but calls `Bowl::stir().await`; it does not
materialize results. Verification after the interval checks that all requests
actually applied. The `idle` case measures one already-settled stir, regardless
of world size; its operation count is one, not the number of entities.

`stir` shares ordinary scoop settlement and deferred bound cleanup. It preserves
input epoch rules; it is not a promise to drain inputs that arrive indefinitely
during an active evaluation. It does not bypass memoization or publication.

The runner records both raw per-iteration totals and normalized per-operation
values. `allocation_events = alloc_calls + realloc_calls`. Frees, requested
bytes and user-kernel counts remain separate. Requested bytes are not live heap
usage. Instrumentation snapshots, output and verification stay outside counting.

## Initial budgets

[allocation-budgets.json](benchmarks/allocation-budgets.json) establishes exact
current regression budgets for 1/1,000-slot worlds at 1/8 workers:

| Complete iteration | Alloc | Realloc |
| --- | ---: | ---: |
| Idle stir | 0 | 0 |
| Read one settled result | 4 | 0 |
| Read 1,000 settled results | 4 | 8 |

These budgets pass after eight warmup rounds, over five measured rounds per
configuration. Purchases and drive-only updates still have much larger and
sometimes schedule-dependent costs; they are measured targets, not advertised as
near zero. Do not install a very loose purchase ceiling and call the goal met.

Budgets are opt-in, apply to every candidate iteration (not an average), and fail
if a selected configuration or required metric is missing. They do not restrict
normal benchmark input sizes.

```sh
cargo build --workspace --all-targets --profile bench -j 20
uv run tools/measure-allocations --out target/budget-check --cases idle,read --slots 1,1000 --workers 1,8 --warmup 8 --rounds 5 --repetitions 1 --budgets spec/benchmarks/allocation-budgets.json
uv run tools/measure-allocations --out target/drive-comparison --cases purchases,drive --slots 1,1000 --workers 1,8 --warmup 8 --rounds 5 --repetitions 1 --metrics
```

Counts and timings use separate processes. Drive-only and complete rounds use the
same engine build, features and request fixture. One-worker interleavings can
change the amount of evaluation; subtracting their aggregate counts does not
isolate result materialization. The stable eight-worker/1,000-slot medians in
this pass were 156,815 alloc + 2,124 realloc for complete rounds and 156,811 +
2,116 for drive-only rounds. This supports keeping evaluation as the main target.

## Allocation origins

The profiler now divides the prior “other pipeline” category by the nearest
recognized allocation-origin frame. On the retained planning-storage profile,
the eight-worker breakdown (allocation/reallocation events per request) is:

| Origin | Events |
| --- | ---: |
| Planning, including owned rows | 49.386 |
| Planning join candidates | 22.000 |
| Dispatch/completion | 33.136 |
| Row construction/execution | 21.027 |
| Command application/storage | 11.579 |
| Guard buffers | 6.009 |
| Command producer handles | 5.000 |
| Output/memo updates | 5.000 |
| Publication | 2.589 |
| Structural cloning | 2.040 |
| Unclassified | 1.173 |

This reclassifies existing raw stacks; it is not a new timing measurement or a
claim of exact source-line attribution. Inlining may place several operations
under a caller. DHAT charges reallocations to the original allocation site.
The full source/binary provenance remains in the planning-storage report.

## Correctness finding

The drive-only cleanup test exposed stale published `last_settled` snapshots
after deferred bound cleanup. Refreshing the retained snapshot when cleanup
preserves a settled state fixes this; existing snapshots held by callers remain
readable. Pending evaluation must not be published as settled. This is tracked
in correctness commit `e16164e`, not as an allocation optimization.

Bare `Query<Entity>` enumerates historical IDs, so live-removal checks use a
required component query rather than assuming raw ID enumeration means liveness.

## Remaining O0 work

- Lifetime heap snapshots and explicit grow-then-plateau measurements.
- Broader fixture budgets as more paths become bounded and reusable.
- Audit ambient mutable views and Cow epoch boundaries before optimizing those
  paths. The current purchase fixtures use tracked Query/MutRef, not mutable View
  or external Cow. Equality joins retain the existing fingerprint-only contract;
  collision-safe semantics need their own compatibility decision.

## Validation and provenance

Workspace build/tests (156 engine tests and five fixture tests), example tests,
clippy, playground, all four benchmark smokes, and the Python budget tests pass.
The final idle/read budget matrix passes with the cleanup fix included.
[Measured results and provenance](benchmarks/2026-10-23-iteration-budgets.json)
retain complete-iteration totals; raw runs are in `target/optimization-o0/`.
Formatting is deferred to the final campaign cleanup as requested.
