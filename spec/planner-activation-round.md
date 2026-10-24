# Planner activation round

Status: round complete, 2026-10-24. Baseline: `06584d8`.
P1/P3 retained; P2 and P2b archived. The planner is not fully optimized.
The user authorized experimentation and commits between these batches. This
resumes the planner-focused work below, not every item in the broader roadmap.

## Prior-art audit

Rechecked primary material: [Timely activations](https://docs.rs/timely/latest/timely/scheduling/activate/index.html),
[Phreak](https://kie.apache.org/docs/10.0.x/drools/drools/rule-engine/index.html),
and [Bevy executor](https://docs.rs/bevy_ecs/latest/src/bevy_ecs/schedule/executor/multi_threaded.rs.html).
Their applicable ideas are explicit activation, batched incremental propagation,
and precomputed scheduling metadata. Our mapping is an inference; none removes
our need for row-level stale checks, ownership retraction or async readiness.

| Mechanism | Current status |
| --- | --- |
| Presence masks / prepared stores / indexed joins | Implemented |
| Watermark gating / row hints / compact execution IDs | Implemented |
| Preserve hints across busy log compaction | Implemented; measured tradeoffs |
| Skip snapshots for quiet planner waves | P1 retained |
| Compiled component-consumer activation | P2/P2b tested and archived |
| Shared sealed registry / compiled hook and phase metadata | P3 retained |
| Adaptive sparse versus dense candidate discovery | Deferred |
| Shared matching memories / fused worker pipelines | Not implemented; separate semantics/storage design |
| Partition-owned publication / distributed scheduler | Deferred; not needed to remove avoidable local work |

## Batches and acceptance

1. **P1 quiet gating:** check planner interests against the live world under
   state lock before obtaining a snapshot. No snapshot may be created/retained
   solely to await a result when no system needs planning. Preserve ready-result
   visibility, preemption, cancellation and bounded generation input handling.
2. **P2 activation metadata:** compile interests to component slots/consumer lists
   at build time. Explore routing changes once rather than rescanning all systems
   and filtering the same log for every consumer. Preserve explicit reset/retry
   activation, phase boundaries, unbounded/custom fallbacks and unequal layouts.
   Design against actual mutation chokepoints before selecting representation.
3. **P3 candidate density:** examine whether large dirty sets make hint building
   more expensive than a full candidate scan. Any full-scan selection is an
   explicit cost choice, never permission to rerun memo-current rows. Only do
   this after P1/P2 measurements identify a useful workload.

Each batch: targeted semantic/work-count tests, workspace tests and build with
unchanged features and20compile jobs, then delegate paired benchmark work to the
existing benchmark agent while auditing the next batch. Do not run builds/tests
concurrently with timed measurements. Commit retained batches; archive rejected
prototypes with evidence. No rustfmt or new unsafe code unless a measured need
and explicit invariant justify it.

Use1/8workers, fixed purchase traffic, static/sparse/busy device populations,
large dormant stores and a wide idle-system fleet. Separate ordinary timing,
work counters and intrusive COW instrumentation. Preserve frozen controls.
Count candidate work, useful kernels, deferred/stale rows and snapshot behavior;
zero system execution alone does not prove zero planner work.

The outcome is a measured reduction of avoidable work, not a claim that the
planner is fully optimized. Keep unresolved costs and fallback cases explicit.

## P1 result: retained, limited timing benefit

[Paired measurements](benchmarks/2026-10-24-planner-p1.json),84 timing processes
and28 diagnostic processes. Quiet64-observer workload at eight workers improves
in all three pairs (+5.24%,+10.18%,+1.82%). Fully active 10k-device workload at one
worker regresses2–6%; baseline and large sparse results are mixed. No general
throughput gain or measured COW reduction is claimed. Normal work counts remain
correct, with zero stale publication. The184-test engine suite, workspace tests,
optimized build and engine/stateful/playground smokes passed.

Retained because quiet waiting no longer acquires an unused fresh snapshot,
pinned by a test covering ready-result visibility and cancellation recovery.
Timing tradeoffs remain visible. Source changes replace two state acquisitions
with a coherent readiness-and-capture check; no extra snapshot guard crosses a
quiet await/publication.

## P2 selected representation

Compile component types to sparse consumer word masks after plugin registration
seals. Live-world mutation chokepoints coalesce changed types in a non-atomic
bitset under state ownership. At a planning boundary, route each dirty type once
into phase-specific ready bits. System reset/retry paths explicitly activate their
own bit; unresolved/custom interests remain conservative every-wave candidates.
Ready bits select candidates, then existing watermark/memo/access logic decides
actual work. Preserve registration order by iterating bits in ascending ID order.
Snapshots do not copy or own activation state. Settle's separate evaluation path
retains its existing semantics. This first prototype does not replace per-system
row-log hints; measure system-selection cost before merging those mechanisms.

## P2 initial result and refinement

[Initial measurements](benchmarks/2026-10-24-planner-p2.json) show correct bounded
selection (eight-worker baseline16 checks/round;64 idle observers13) and unchanged
useful work, but mixed throughput. The P1 control lacked these new counters, so
no numeric before-check count is invented. [Initial prototype patch](benchmarks/2026-10-24-planner-p2-initial.patch)
applies to `bebb9c8` and is preserved before refinement.

P2b moves dirty-type signalling from an extra TypeId lookup on every log write
to a cached marker on each typed live store. Signal when its watermark advances.
Store/snapshot clones must drop the live marker; otherwise scratch snapshot
mutation could spuriously wake live consumers. The store factory installs markers
once from the compiled consumer index. Atomic markers avoid losing a concurrent
wake; they do not replace any data-access guard or transaction protection.

P3 is refined by source evidence: share the sealed system registry and compile
lists of actual settled hooks and Settle systems. Today default empty hooks still
allocate/execute empty work, while full registry clones duplicate many Arc handles.
Candidate-density heuristics remain deferred rather than bundled into this round.

## P2/P2b disposition: archive, do not retain in production

[Cached-marker comparison](benchmarks/2026-10-24-planner-p2b.json) is directly
against the P1 frozen executable. It does not treat the earlier variant's timings
under different machine load as a causal control. All188 engine tests, workspace
checks, optimized build and smokes passed. Work stayed correct; routed checks
remained bounded with64 idle observers. Eight-worker baseline selected12 systems
per round and 10 dirty-type events; the idle-observer case selected15 with14.33
events (process averages; events coalesce per flush, not once per whole round).

Throughput did not justify keeping the additional machinery. The target sparse
case at eight workers had paired changes of roughly+11%,+1%,−3%; static100k-device
runs regressed in all three pairs, and idle-observer results were flat/adverse.
The initial per-write lookup version was also mixed. Both code variants are
preserved as replayable patches against `bebb9c8`; the [cached variant](benchmarks/2026-10-24-planner-p2-cached.patch)
was checked in reverse before restoring source and forward against the base index.

This rejects these implementations for the measured workloads, not activation
as a general principle. A larger graph with many more consumers could justify a
new comparison. Do not ship a guessed system-count threshold or claim these
models make the serial publisher parallel. Production keeps P1's watermark gates
and incremental row hints. P3 proceeds from that retained baseline.

## P3 implementation boundary

The sealed registry is shared through one Arc; construction is its only mutation
point. Precomputed hook/Settle IDs avoid constructing empty default-hook futures
and avoid taking snapshots or moving memo ownership when those paths are absent.
A registered-phase mask skips empty normal phases before creating reservations
or run queues, while preserving the existing preemption boundary. Actual hook,
Settle and normal kernel semantics remain unchanged. Hook metadata describes
the executable wrapper's existing behavior; this does not change nested wrapper
composition semantics. Existing phase/preemption and real-hook tests remain the
semantic oracle; a new regression checks absent settlement work leaves pending
inputs and the snapshot cache untouched.

## P3 result: retain metadata reuse and absent-work fast paths

[Full paired results](benchmarks/2026-10-24-planner-p3.json) contain 84 world timing
processes, 28 work-counter processes and 12 short Criterion processes. All 185 engine
tests, seven fixture tests, nine playground tests, macro/doctests, optimized build
and engine/stateful/playground smokes passed. Measured Rust sources remained
unchanged during the runs; no feature graph or kernel-body changes.

The short cooperative `planner_gating/512` comparison improved in all three pairs:
P1 process medians 32.21–32.92µs → P3 14.41–14.94µs (about 54–56% lower elapsed time).
`incremental_settle/128` overlaps: 60.02–65.60µs → 57.10–62.43µs. These used 0.2s
warmup/0.5s measurement; group settings produced 20 samples for gating and 10 for
incremental settle. Three alternating processes per variant were CPU-pinned.
The compiler fixtures retain default per-system profiling ON in both binaries;
world fixtures use it OFF. Compare within a workload, not between those modes.

The archived P2b was also compared directly with P1 on these compiler fixtures:
planner gating 28.67–28.94µs → 28.60–28.68µs (roughly flat); incremental settle was
mixed. That separate comparison does not reuse P3's P1 timings under different
load. It adds no clear reason to adopt the activation variants now.

World throughput is mixed, not a general speedup. Static 100k devices at eight
workers regressed in all three pairs (roughly 4–10%); 10k devices/1k updates at one
worker regressed 5–11%; fully active 10k at one worker regressed 2–4%. The target
100k/1k eight-worker case was mixed. Full process ranges and adverse outliers are
preserved. These tradeoffs remain follow-up work, not dismissed as noise.

Diagnostics show precisely what vanished: baseline publication calls 5005→5000;
64 idle observers 5069→5000. Useful kernel/revision counts remain unchanged and
stale publications remain zero. We removed empty default-hook publications, not
required lifecycle cleanup or freshness checks. Snapshot/copy amplification was
not instrumented in this comparison, so no measured COW reduction is claimed.

Retain P3 for the clear sparse-planner improvement and simpler reuse of sealed
metadata. The shared registry, hook lists and phase masks were measured together;
individual causal contributions have not been isolated. The request-heavy
regressions are explicit and must inform the next larger-world optimization.

## What remains

- Production still scans bounded system interests/watermarks and filters per-system
  dirty-row log slices. The tested push-routing designs cost more than they saved
  on the measured workloads; their patches remain available for larger-graph work.
- Sparse/dense hint strategy, shared matching memories and reusable join candidate
  plans remain experiments. A changed tiny gate can legitimately invalidate every
  row, while opaque/custom params and complex products require conservative paths.
- Static masks do not prove dynamic command targets are disjoint. Pipeline fusion,
  partition-owned publication and worker-distributed planning need separate
  lifetime/visibility contracts. The earlier isolated preparation offload also lost.
- Whole-store COW and publication amplification remain a major large-world limit.
  Consider smaller sharing units or safely scoped snapshots; avoid relying on a
  slower planner to incidentally coalesce completions. This round did not change
  storage or quantify which copy reduction would result from a new representation.
- Hook metadata preserves existing executable-wrapper semantics; nested wrapper
  composition is not redesigned here. Formatting remains deferred as requested.

The broader roadmap is not complete. This round tested the applicable local
activation/metadata ideas and records both useful and unsuccessful outcomes.
