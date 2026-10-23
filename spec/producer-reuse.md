# Reuse uniquely owned command producers

Status: O3a kept after testing and measurement.

## Remaining allocation

Every invocation currently allocates Arc<Mutex<CommandBuffer>>, even when the
command payload arena and guard capacity are reused. Fetching Commands parameters
clones this producer handle; kernels may also retain clones after returning.
The measured purchase pipeline performs five invocations per useful request.

The existing queue storage can also carry a retired, uniquely owned producer.
This avoids a second pool and return channel. It is distinct from skipping
producer creation for parameters that cannot emit commands; that remains a
possible later optimization.

```text
checkout queue -> take retired producer (if any) out of its storage
unique producer -> reset spawn context and put queue into producer
fetch/kernel -> ordinary independently cloned Commands handles
finish -> try exclusive Arc ownership
  unique: drain queue; attach now-empty producer to queue storage
  shared/weak: drain under existing mutex; do not recycle producer
publication/discard -> queue returns through existing pool
```

## Required invariants

- Use Arc::get_mut, not an unchecked strong-count observation. If any strong or
  weak handle survives, retain the existing independent-producer fallback.
- A producer is removed from queue storage before the queue enters its buffer.
  Before attaching it again, its buffer contains a default empty queue. This
  prevents a queue→producer→queue ownership cycle.
- Reset spawn slots, cursor and allocator on checkout. Existing output ownership,
  reserved spawn identity, late-clone behavior and command order remain unchanged.
- Unpooled paths do not allocate retention storage. Panic/cancellation may lose
  reuse opportunities, but must still discard commands and release locks safely.
- Do not share producers between simultaneous invocations or change the contract
  for detached writes after completion. The old handle must remain isolated.

Tests: unique reuse, retained strong/weak handles, old-handle writes during the
next invocation, spawn-context reset, cancellation/unwind, cross-thread return,
and pool teardown without reference cycles. Existing command arena/retraction
and phase tests must pass. No new unsafe code is expected.

Measure producer allocator events, complete-iteration count/bytes/time, and live
heap through growth. Retaining producer allocations will increase the pool's live
heap; report that cost. Include pure read/MutRef kernels and custom/retained-handle
paths. If retained handles are common, reuse may not help; do not count cold-path
or fallback allocations as eliminated.

## Result: keep

At 1,000 requests/eight workers, allocations fall **55,857→50,857**, with 1,124
reallocations unchanged. The producer-handle allocation category disappears from
the diagnostic profile (about five events/request removed); the measured profile
is 51.989 events/request versus the stable count's 51.981. No new command storage
or arena growth is observed after warmup.

Eight-worker process medians improve 29.82–33.92→28.12–30.19 µs/request with some
overlap. An initial one-worker slowdown around 11% did not repeat at that size:
five-process recheck 22.10→22.66 µs/request (+2.53%) has overlapping ranges. A
modest one-worker cost remains possible. No-command gating improves across process
medians; settled reads remain flat. No universal speedup is claimed.

Retained producer capacity adds roughly 193 KB at 1,000 slots and 387–449 KB at
2,000 (about 3.6–4.2%). Plateau variation reflects scheduling/pool buckets.
Measured peaks are about 1.1% higher at 1,000 and flat at 2,000. Keep for warmed
allocation reduction and favorable multi-worker evidence, accepting this bounded
retained-capacity cost.

Validation: 168 engine tests, workspace build and playground, all four optimized
smoke suites and eight idle/read budgets pass. Coverage includes retained strong
and weak handles, reset spawn contexts, cancellation, cross-thread return and
reference-cycle teardown. [Results and provenance](benchmarks/2026-10-23-producer-reuse.json).
