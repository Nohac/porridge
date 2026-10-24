# Publication and structural copy investigation

Status: measured; instrumentation archived, production path restored, 2026-10-24.
Baseline: `68361ab`.

Dirty-log compaction removed population-sized planning in the sparse monitoring
fixture, improving eight-worker throughput but regressing one worker. Separate
instrumented publication time rose 31.40→55.81ms/round with similar work counts.
Large dormant account stores remain slow. Neither proves copying is the cause.

## Measurement design

Instrument actual payload Clone calls underneath Arc::make_mut, rather than
assuming Arc::strong_count predicts a copy. Use transparent internal wrappers
for entry maps, fingerprint indexes, presence bits and the planning log. Preserve
the current structures, clone behavior, guards, revision/ownership semantics and
allocation layout; no alternative map implementation or commit policy this pass.

When diagnostics are enabled, record clone count, logical elements, a labeled
shallow byte estimate and clone elapsed time for each category. Entry byte counts
exclude BTree node overhead, owned key spill buffers and component heap contents;
index byte estimates exclude allocator/hash-table/BTree overhead. Component
values remain shared. Do not call these estimates measured allocator traffic.
Also attribute clones to publication versus other mutation paths using a scoped
thread-local marker: publication contains no await after taking the state lock.
Counters are process-wide and intended for one isolated bowl per process.

Split publication into dependency validation, command/output replacement,
in-place-write reconciliation, memo refresh and removal cleanup. Separate state
lock wait time from publication time, which already starts after the lock.
Count structural snapshots. Child timings overlap parent timings; never sum them
as disjoint CPU samples. Diagnostic timings are elapsed observations, not proof of
unperturbed CPU cost. Normal metrics-off paired runs check instrumentation cost.

## Validation and experiments

- Confirm the wrapper records only real payload clones: unique mutation, shared
  mutation, and weak-handle dissociation; preserve old snapshots and metadata.
- Workspace tests/build, engine/stateful benchmark smoke and playground.
- Freeze binaries; use the existing paired benchmark agent after tests.
- Normal metrics-off before/after screen: baseline and100k/1k devices,1/8workers.
- Separate diagnostic repeats: baseline,100kstatic,10k/1k,100k/1k,10kfullyactive,
  dormant100k,1/8workers. Hold input counts constant and report copies per round,
  per purchase and where relevant per device update.
- Preserve the single-worker regression as unresolved until evidence explains it.
- Select the next storage/publication experiment from counts and stage timings;
  do not bundle a speculative fix into this instrumentation comparison.

No Cargo feature changes or rustfmt. The broad campaign remains paused; this is
the next authorized focused investigation. No external network sources needed
for measurements of this local implementation.

## Findings

[Full results and provenance](benchmarks/2026-10-24-publication-cow.json) preserve
24 paired metrics-off processes and 36 separate diagnostic processes. Raw logs and
frozen binaries are under `target/publication-cow-20261024-155942/`.

Structural copy amplification is now measured, not inferred from reference
counts. For fixed 1,000 purchases plus 1,000 condition updates among 100,000 devices,
one worker copies about 0.91–1.48 million entry records per round, versus 308,000 at
eight workers. Inline metadata estimates are 73.0–118.7MB versus 24.64MB per round;
these exclude component payloads, node/allocator overhead and owned key spills.

Ranges below are across three diagnostic processes, each normalized over three
measured rounds after five warmup rounds. Times are instrumented elapsed time.

| Case | Workers | Entry-map clones/round | Records copied/round (millions) | Entry clone ms/round | Commands ms/round | Reconcile ms/round |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100k devices / 1k updates | 1 | 27.3–42.0 | 0.912–1.483 | 59.1–85.5 | 68.5–98.0 | 2.1–2.5 |
| 100k devices / 1k updates | 8 | 13.0 | 0.308 | 16.9–19.6 | 22.3–24.4 | 1.9–2.0 |
| 100k dormant accounts | 1 | 23.7–45.0 | 0.919–2.137 | 49.5–120.2 | 5.5–6.0 | 78.3–180.8 |
| 100k dormant accounts | 8 | 10.0–12.3 | 0.308–0.409 | 17.9–25.8 | 5.2–5.6 | 28.3–38.5 |

Entry clone times include input and publication copies. Their publication subset
is nested inside command/reconcile stage timings; do not add these columns.
Device publication cost concentrates in command application/output replacement.
Dormant-account publication concentrates in reconciliation of in-place writes,
including entry-map and fingerprint-index COW. The same request count therefore
has very different costs depending on the population of the stores it writes.

The target's structural snapshot counts are 8.67–12 per round at one worker versus
4 at eight; dormant accounts record 7.33–14 versus 4–5. This supports investigating
snapshot/publication frequency as well as the size of each copied structure.
It does not quantify how many snapshots are avoidable.

Presence bitmap copies were comparatively small (roughly 0.1–0.8ms per round in
these cases), and planning-log payload clones were zero. Initial optimization
priority should be entry/index structures and when new snapshots pin them.
Kernel/revision counts remain correct, with zero stale publications. We have
not proven that COW alone explains the earlier uninstrumented regression: this
instrumentation changes timing and potentially completion grouping.

## Instrumentation disposition

The metrics-off comparison found a 5–11% paired slowdown on the small one-worker
baseline. The large sparse case was roughly flat and eight-worker baseline
results mixed. The probe is therefore preserved as a diagnostic experiment,
not installed in the normal execution path.

[Replayable source and test patch](benchmarks/2026-10-24-publication-cow.patch)
applies to clean `68361ab56d5da4216818e6d9272e5c03c23e3b9e`. It was checked in
reverse against the measured sources and forward against the base index before
restoration. The generic wrapper is repr(transparent), preserves existing COW
policy and records actual payload Clone completions. A regression test covers
unique/shared mutation and weak-handle dissociation; the workspace's existing
guard/revision/ownership tests exercise the wrapped production structures.
All 184 engine tests, seven fixture tests, nine playground tests, macro/doctests,
optimized workspace build and engine/stateful/playground smokes passed on the
instrumented version. Production sources are restored to the committed 183-test
engine; the diagnostic code and evidence remain available for the next experiment.

## Selected next experiment

`run_phase_streaming` currently obtains a fresh structural snapshot before
checking whether any system needs planning. Its quiet branch retains that
snapshot while awaiting and publishing more results. Even without new planned
work, refreshing a snapshot can pin current entry maps and force the next write
to copy them again.

First test live-world planner gating under the state lock before acquiring a
snapshot, retaining a snapshot only for actual planning. The snapshot header clone is
cheap; pinning a current map version can make the following write expensive.
Preserve wake/preemption
handling, dependency checks and streaming progress of ready kernels while other
kernels await I/O. Verify both normal throughput and diagnostic snapshot/copy
counts; the avoidable share remains unmeasured until that comparison.

If avoiding redundant snapshots is insufficient, prototype structural sharing at
a smaller unit than a whole component map/index. Preserve ordered iteration,
lookups, retained snapshots and guarded values; do not replace the component
lifetime model or add arbitrary buffering delays merely to hide copy costs.

Illustrative control flow for the next experiment (not implemented here):

```text
under state lock: does any system in this phase need planning?
  yes -> obtain snapshot -> plan/admit -> await ready work -> publish
  no  -> await ready work or preemption -> publish -> reconsider planning
```

The decision must use the same pure watermark/forced-dirty predicate as today.
A production test should demonstrate that an unrelated pending async kernel does
not force new snapshots or delay ready results. Re-run conflict, preemption and
abandoned-evaluator coverage; queued external writes must retain their existing
generation rules. Compare the fixed population workloads at one/eight workers and a
separate diagnostic build before claiming any copy reduction.

Restoration verification: the 183-test production workspace passed again, the
optimized all-target workspace build passed, and the rebuilt world_scaling binary
is byte-identical to the frozen pre-instrumentation control. No diagnostic
overhead remains in the ordinary build.
