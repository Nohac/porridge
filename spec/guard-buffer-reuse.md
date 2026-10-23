# Reuse invocation guard capacity

Status: planned O2c experiment, after ownership metadata measurement.

## Problem and lifetime

Each ordinary invocation creates a GuardStore Vec, fetches read/write guards,
then drops the Vec after its future returns. The current purchase profile assigns
about six allocation events per request to guard buffers (five invocations plus
external result guards). External result guards have a different lifetime and
remain independent in this experiment.

The existing command queue already leases storage from a driver-owned pool and
returns it after publication or discard. Reuse that ownership path rather than
adding another pool, channel, mutex, or oversized inline future field.

```text
plan: lease command storage
run: move empty guard capacity out of that storage
fetch + await: guards exclusively own lock tokens
return: clear guards, releasing every token
finish: move empty capacity into drained output storage
commit or discard: return storage to the existing pool
```

## Implementation boundary

- Store an empty GuardStore alongside the arena and command metadata in pooled
  Storage. Keep the public/custom guard API unchanged.
- Take capacity before moving the queue into Commands. After the system returns,
  clear tokens at the same point where GuardStore was previously dropped.
- Attach empty capacity to the output queue after draining Commands. No extra
  producer mutex acquisition and no retained handles into recycled guards.
- Do not create a storage box for zero-capacity guard buffers. Read-only systems
  may need a storage box on their first run; reuse it thereafter.
- Cancellation and panic drop live guards normally. Losing their capacity on an
  exceptional path is acceptable; recycling a live guard is not.
- Settled hook/grouped fallback and external result guards keep their existing
  behavior initially. No new unsafe code or changed lock semantics.

## Evidence required

Tests must cover capacity reuse across worker return, read/write/custom token
release before reuse, discard/cancellation, panic, and independent overlapping
leases. Existing retained snapshots, stale rejection, preemption and dispatch
cancellation regressions must still pass.

Compare warm complete-iteration allocations, requested bytes, live/peak memory,
and one/eight-worker timing against O2b. New pooled capacity is retained memory;
report that cost. Check no-command and tiny cases too. The expected reduction is
per-invocation guard Vec allocation, not all guard/command allocations. The pool's
return-channel blocks remain an independently measured residual.
