# Invocation-key storage experiment

## O2a plan

The common invocation identity has one or two entity keys. Its Vec is cloned into
running/reservation sets, queued work, memo updates and derived ownership.
Test an inline two-key SmallVec with heap fallback for wider queries.

- Reuse the already locked smallvec 1.15.2 dependency; enable union permanently
  in bowl so debug/tests/runs/benches use the same feature graph.
- Build/freeze a feature-matched Vec-key baseline before changing the key type.
  The dependency/feature change alone is not an optimization result.
- Measure the descriptor layout. On the current 64-bit host the two-key inline
  buffer should fit in the same three machine words as Vec; do not repeat the
  earlier oversized inline-guard experiment.
- Keep QueryParam/SystemParam Vec-returning extension methods unchanged.
  Invocation identity equality/hash and ordering must still use all keys.
- Test a three-key product through memoization/derived ownership, including
  changes to one provider, and retain the complete workspace contracts.
- Benchmark allocations, small/large purchases, compiler/gated multi-query
  workloads (including the spilled-key case), and memory. Delegate measured
  validation to the gpt-6.1-sol benchmark agent after tests pass.
- Wider-key allocations remain a separate pooling/interning problem; this pass
  must not truncate keys or impose a maximum query width.

No rustfmt during the campaign; final style cleanup remains O11.

## O2a result: keep

The common key list now uses SmallVec<[Entity; 2]>; larger products retain all
keys through heap fallback. Existing query extension methods still return Vec.
The feature-matched Vec control and candidate have identical Cargo/feature
graphs. No unrelated dependency version changed.

At 1,000 requests/eight workers:
- Allocations: **100,857 → 65,857 per iteration** (35,000 fewer; 34.7%).
- Reallocations: 2,124 unchanged.
- Time: **37.96 → 32.88 µs/request**, about 13.4% lower in this run.
  All three paired process medians improve (control 36.46–38.44, candidate
  31.22–34.71). One-worker large cases improve less clearly with overlapping ranges.
- Incremental compiler128 improves across process medians; the three-key spilled
  gated query shows no regression. Other compiler/relationship cases overlap.
- DHAT: 67.981 allocation/reallocation events per request; key scratch growth 0.

### Layout and memory tradeoff

Keys remain 24 bytes and the invocation descriptor 32 bytes on the measured 64-bit
host. Option<SystemInvocation> grows 32→40 bytes because the inline representation
loses the old pointer niche. Stored entries currently carry this optional owner.

Matched lifetime/growth runs retain a stable heap range, but live requested bytes
increase by 55,016 at 1,000 slots and 109,536 at 2,000 (about 1.1%). Peak requested
bytes fall 9,814,592→9,729,008 and 19,589,760→19,417,856 (about 0.87%).
This is an explicit accepted tradeoff, not a zero-memory-cost claim.

Validation: 162 engine tests, including a three-key product through memoization and
derived-output replacement; all optimized smoke suites and idle/read allocation
budgets pass. [Measured results and provenance](benchmarks/2026-10-23-inline-keys.json).

## Next experiment: stored ownership metadata

Source inspection found that ComponentEntry stores both an Origin flag and an
optional owner. The private insertion paths always pair Base with None and
Derived with Some(owner); all origin checks can be expressed through owner
presence. Try removing the redundant stored flag and pending-queue flag so there
is one source of truth. This may recover the optional-owner size penalty without
changing invocation identity or adding unsafe layout tricks.

Test base-over-derived replacement, wrong-owner retraction, deferred DerivedFrom
capture, and snapshot retention. Measure entry layout and live/peak memory against
O2a; do not claim an allocator-count improvement if only footprint changes.

## O2b result: keep stored ownership simplification

Removed the redundant Origin flag from entries and deferred DerivedFrom inserts.
An optional invocation owner now represents the ownership distinction directly.
The entry size falls **80→72 bytes** on the measured 64-bit host. Base replacement,
wrong-owner retraction and retained-snapshot regression coverage passes, along
with all 163 engine tests, workspace build and playground.

Matched eight-worker lifetime runs at 1,000 slots retain
5,042,340–5,042,844→4,939,292–4,939,796 bytes (103,048 fewer, about 2.04%). At 2,000
slots the measured range is 10,064,364→9,858,796–9,859,300. Peaks fall about 2–3%.
Both sizes retain stable plateaus. This recovers the O2a optional-owner footprint
penalty and saves a little more; these are requested heap bytes, not process RSS.

Allocation counts remain **65,857 alloc + 2,124 realloc per 1,000-request
iteration**. Eight-worker time 31.76→32.13 µs/request has overlapping process
ranges, as does the one-worker comparison. Keep for simpler ownership and lower
memory; no throughput or allocation-count win is claimed. All four optimized
smokes and eight idle/read allocation budgets pass.

[Measured results and provenance](benchmarks/2026-10-23-owner-metadata.json).
