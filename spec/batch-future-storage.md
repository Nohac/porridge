# Reusable future-node storage for known worker batches

Status: O7b inner worker collection kept; outer stream-merger experiment remains unimplemented.

O3a attributes about 19 allocator events/request to dispatch/completion. Channel
transport addresses only part of this. Each worker batch also builds a standard
FuturesUnordered, allocating a task node for every invocation. Batch length is
known before spawning; dynamically growing one node at a time is unnecessary.

The locally available futures-buffered 0.2.13 implementation exposes
FuturesUnorderedBounded with a fixed capacity, pinned slot storage, shared wake
bookkeeping and independently ready results. Evaluate this existing implementation
before writing another unsafe arena. Its advertised benchmarks are motivation,
not evidence about this engine.

## First boundary

- Add/freeze the dependency with an unchanged engine as a feature-matched control.
- Replace only the multi-row worker batch's future collection. Capacity equals
  admitted batch length. Keep single-row execution, cooperative driver fallback,
  outer completion multiplexing and all invocation/publication semantics intact.
- Preserve pending siblings, per-row completion, cancellation and worker panic
  propagation. No wait-for-all collection or detached kernel execution.
- The pinned source forces a yield after a bounded pending-poll loop. Self-waking
  futures can still differ from standard FuturesUnordered: test actual engine
  cancellation/preemption behavior, rather than assume similar APIs imply identical
  scheduling. If that contract cannot be preserved, reject or explicitly design
  the needed adapter before considering a throughput result.

Validate ready/delayed/suspended/skewed batches, cancellation with retained wakers,
non-Unpin futures and full engine tests. Compare complete-iteration allocations,
work counts, multi-worker scaling and memory. Use the same feature graph across
all tests/measurements. Keep source/binary manifests for both implementations.

If the inner collection is useful, inspect outer SelectAll separately: it wraps
streams back into future nodes per yielded result. A reusable stream merger may
address that distinct cost. Do not combine both substitutions in the first
measurement. Native synchronous kernels remain planned; eliminating async node
allocation does not prove their direct batch loops have no additional benefit.

## Result: keep the inner worker collection

The engine and shared executor benchmark use pinned futures-buffered 0.2.13 with
default features disabled. The unchanged executor was built and frozen with the
same dependency/feature graph before switching the worker collection. Existing
dependency versions were preserved; no new unsafe code was added to the engine.

With 1,000 requests and eight workers, alloc calls fall **45,227→40,227** and realloc calls
remain about 1,124. Total allocator events fall **46,351→41,351 (-10.79%) per whole
iteration**, not per request. Requested allocation traffic falls 298,096 bytes per
iteration, with reallocation-new bytes unchanged. Clean diagnostics execute the
same 5,000 rows in 315 batches, no already-running/stale rows, and 41.355 events/request.

Ready/suspended dispatch microbenchmarks generally improve, especially at widths
32 and 64. End-to-end 1/4/8/16-worker timing remains mixed and overlapping: keep for
allocation, requested-byte and peak reduction, with no universal throughput or
linear worker-scaling claim.

Measured peaks fall 2.29–2.91%; retained ranges overlap or sit at stable nearby
pool/memo bucket levels. There is no new long-lived buffer pool. All 169 engine
tests, workspace build/playground, four optimized smokes and eight idle/read
budgets pass. [Results and provenance](benchmarks/2026-10-24-batch-futures.json).
