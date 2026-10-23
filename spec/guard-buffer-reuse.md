# Reuse invocation guard capacity

Status: O2c kept with an explicit retained-memory tradeoff.

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

## Following O2 experiment: move access metadata

`FunctionSystem::plan_rows` clones each admitted candidate's access Vec into
PlannedSystemRun for conflict reservation. The future retains the original only
to extract its write rows. We can extract writes once, move the access Vec to the
reservation descriptor, and keep just the write rows in the future. No shared Arc,
new pool or changed reservation lifetime is required.

This moves write-list construction before final conflict admission: rejected
write candidates may build a write list that they never use. They also avoid the
larger access-list clone. Measure independent and contended workloads, not just
the best case. Read-only rows should require no write-list allocation. Preserve
whole-store/custom access behavior and MutRef revision reconciliation. Existing
reservation, stale-publication and preemption tests cover the critical boundary;
add a focused regression only where a behavior is not already exercised.

## Result and decision

At 1,000 requests/eight workers, allocations fall 65,857→60,857 and reallocations
2,124→1,124 per iteration: **6,000 fewer events (8.83%)**. DHAT guard-buffer events
fall about 6.01→0.01 per request; remaining guards are external result storage.
Combined requested allocation/reallocation-byte traffic falls 672,000 bytes per
iteration. This is lower traffic, not lower retained memory.

The pool retains about **432,024 additional bytes at 1,000 slots (+8.75%)** and
about 864,000 at 2,000. Peak requested heap rises about 4.5%. Measured plateaus
remain stable after warmup/growth. Empty capacity lives with existing command
leases; there is no extra return queue or per-component box.

Eight-worker timings are inconclusive: the targeted five-process recheck gives
32.04→32.27 µs/request with overlapping ranges. One-worker initial results improve
22.85→21.85 µs/request. Tiny cases are noisy; ordinary no-command gating is about
2% slower, while gated and external-read cases improve. No universal throughput
win is claimed.

The benchmark reviewer recommended defer/reject unless the retained-memory cost
is acceptable. The campaign decision is **keep**, because the explicit goal is
near-zero warmed engine allocations through reusable storage. The finite retained
capacity is an understood cost, and requested-byte traffic decreases. Revisit
this tradeoff with future shared execution storage; do not treat fewer allocator
calls as proof of higher throughput.

Validation: 166 engine tests, workspace build, playground, clippy (existing
warnings), four optimized benchmark smokes, and eight idle/read budgets pass.
[Measured results and provenance](benchmarks/2026-10-23-guard-reuse.json).
