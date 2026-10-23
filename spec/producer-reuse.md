# Reuse uniquely owned command producers

Status: planned O3 experiment after access-list ownership measurement.

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
