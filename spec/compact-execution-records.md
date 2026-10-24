# Compact execution records and reservation slots

Status: implemented, correctness-tested and retained after paired benchmarks (2026-10-24).
This is a scoped scheduling change following the [source investigation](scheduler-investigation.md).
The [system profiling control](system-profiling-overhead.md) from the previous
slice remains intact and is held constant across comparisons.

## Scope

The first boundary is an admitted invocation's lifetime within one evaluator
phase. Memo and derived-output identities still use `(SystemId, entity keys)`.
This is not a persistent invocation interner or a rewrite of world ownership.

```text
logical key -> one duplicate-detection map -> RunId { slot, generation }
                                             |
                        phase record: logical key, accesses, writer flag
                                             |
worker completion carries RunId -> validate ID -> publish -> release record
```

Previously the driver separately maintained a running-key set, a reader-key set,
and an owner-to-access reservation map. Workers also returned the full logical
key. These are replaced by one logical-key index and reusable phase records;
worker completions carry an eight-byte `RunId`.

A record slot is reused with a new generation after completion. Generations never
wrap: an exhausted slot is retired for the remainder of the phase. Reader
cancellation releases reader records while retaining writer reservations through
publication. IDs are phase-local; all queues are drained or dropped with their
phase. An ID is checked before publication, so an invalid completion cannot be
mistaken for a live record. No unsafe code is added.

Full logical keys remain in the duplicate index and record, and in existing
memo/output state. Completion/release no longer hash the key across multiple
separate tracking maps, but the remaining key index still requires hashing.
A later persistent-ID design must address memo/output retirement and is outside
this measured slice.

## Dense reservation components

Reservation counters are seeded from the Bowl's existing schema slot registry.
An admitted access vector is translated once into numeric component indexes;
conflict checks, insertion and release then use those indexes. Components outside
the schema receive phase-local fallback indexes. Read/read, read/write,
whole-store and repeated-access semantics are unchanged.

The original `Access` interface and custom parameter implementations remain
unchanged. This means one TypeId resolution per access still occurs at admission;
this slice does not claim to remove every type hash. Each component's row map
retains capacity during the phase, avoiding repeated map destruction/recreation
when its last reservation finishes. Phase records and counters are dropped at
phase end; capacity follows concurrent work within the phase, not historical
invocations across all settles.

The first variant reduced the access-record size from 40 to 32 bytes on the
measured target. Standard Vec in-place collection then shrank each row's allocation,
adding approximately 5,000 reallocations per 1,000 purchases. That representation
is not retained. Private padding preserves the original allocation width, allowing
the safe standard-library conversion to reuse the buffer. This is a performance
aid, not an unsafe layout assumption; other layouts may use an allocating fallback.

Dense counter initialization scales with schema size, even when few components
are touched. The record/counter capacity and padding must still be assessed by
sparse-workload and peak-heap measurements, not assumed to be free.

## Publication dependency refresh

`Dep::refresh_written` now uses its cached component slot, just as freshness
checks already do. The store verifies the slot's TypeId and falls back when a
layout differs. Missing/undeclared components and custom dependency paths retain
the established behavior. Live revisions still use the existing per-store maps;
there is no raw-pointer or dense component-storage change.

## Validation

178 engine tests, six fixture tests, nine playground tests, macro and doctest
checks pass with the existing feature graph. Added checks cover stale-ID rejection
and bounded slot reuse, failed admissions leaving no reservations, reader-only
cancellation, generation exhaustion, and dependency refresh across cached slots,
different layouts and fallback stores. Existing pairwise conflict-oracle and
mixed reservation-lifecycle tests remain independent of the production admission
path. Async ready-result visibility, preemption and output-retraction tests remain.

No rustfmt pass was run, as requested. Benchmarking compares with the frozen
statistics-toggle candidate, not the older pre-toggle revision. Main purchase
runs use statistics OFF in both binaries, with ON sanity comparisons, separate
counted/time passes, sparse-workload checks and post-settle/peak heap measurements.

Files: `access.rs`, `access/tests.rs`, `dispatch.rs`, `dispatch/plan.rs`,
`bowl.rs::run_phase_streaming`, `query.rs::Dep::refresh_written`, and the shared
registry accessors in `world.rs` / `world/stores.rs`.

## Results

Direct control is the frozen statistics-toggle candidate. All primary cases use
statistics OFF for both binaries and the same Cargo feature graph. Three
alternating process repetitions, 1,000 fixed request slots, five warmup rounds,
ten measured rounds, and separately pinned driver/worker CPUs.

| Workers | Async before µs/request | Async after | Sync before µs/request | Sync after |
| --- | ---: | ---: | ---: | ---: |
| 1 | 23.54 | 22.45 | 22.68 | 22.14 |
| 4 | 24.02 | 21.97 | 24.08 | 21.84 |
| 8 | 27.93 | 25.18 | 26.08 | 23.70 |
| 16 | 28.31 | 24.15 | 27.23 | 24.01 |

Every paired eight/sixteen-worker process improves in both modes, giving about
10–17% higher throughput at those worker counts. This is a repeatable result on
this workstation, not a universal speedup claim. More workers still do not beat
the smaller worker configurations; this reduces serialized work rather than
solving distributed scheduling.

All counted rounds execute 5,000 kernels and 29,000 revision checks per 1,000
purchases, with zero stale publications and identical responses. One/four-worker
planning interleavings can produce different candidate/duplicate counts and
allocation traffic; do not equate lower useful-work counts with an optimization.

The first variant's extra 5,000 shrink reallocations are gone. Stable multiworker
allocation counts fall modestly, while indexing/growth produces about twenty
extra reallocations per iteration. Requested allocation plus realloc-new byte
traffic drops about 2.75 MB/iteration for eight-worker async and 2.29 MB for sync.
The stats-ON eight-worker sanity comparison also improves.

Post-settle retained heap is effectively unchanged: about 5.563 MB at 1,000 slots
and 11.107 MB after growing to 2,000. Measured lifetime peaks decline roughly
0.9–2.4%. These are one matched lifetime process per mode/binary; the tool reports
post-settle live and cumulative peak bytes, not phase-only retained capacity.

A separate sparse schemaless fleet check (64 rows/type, 100 touches per measured
iteration, stats ON for both) improves at one worker and is roughly flat at eight.
It exercises dynamic fallback, not a very large, sparsely used declared schema.
Dense schema initialization remains an explicit unmeasured scaling concern.

[Measurements, process ranges and source/binary provenance](benchmarks/2026-10-24-execution-ids.json).
Raw artifacts: `target/execution-ids-final-20261024-131906/`; the first-variant
shrink-allocation evidence remains in `target/execution-ids-20261024-131243/`.
Optimized engine benchmark smoke and standalone playground also pass.

## Follow-ups

Persistent memo/derived-output IDs, component-to-consumer dirty routing, compact
single-row result containers, and batching publication remain separate changes.
The [post-change profile and worker-planning proposal](worker-planning-experiment.md)
now identify the remaining costs. Do not assume the older CPU percentages remain
exact after this admission/transport change.
