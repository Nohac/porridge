# Planner activation round

Status: authorized experimental series, 2026-10-24. Baseline: `06584d8`.
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
| Skip snapshots for quiet planner waves | Selected P1 |
| Compiled component-consumer activation | Selected P2 design/experiment |
| Adaptive sparse versus dense candidate discovery | Selected P3 if evidence supports it |
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
in all three pairs (+5.24%,+10.18%,+1.82%). Fully active10k-device workload at one
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
selection (eight-worker baseline16 checks/round;64idle observers13) and unchanged
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
per round and10 dirty-type events; the idle-observer case selected15 with14.33
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
