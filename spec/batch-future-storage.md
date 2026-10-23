# Reusable future-node storage for known worker batches

Status: candidate O7b experiment; decide after completion-channel measurement.
No dependency or engine change has been made for this proposal.

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
