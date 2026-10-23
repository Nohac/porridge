# Join candidate storage

## O1a: borrow buckets for dirty-provider hints

Plan: replace the per-lookup fingerprint candidate Vec with an exact-size borrowed
iterator over the snapshot-owned index bucket. Keep the owned candidate API for
pair-row construction until its separate lifetime/iteration change.

This addresses a measured allocation site without changing the equality-join
contract, scheduling, schema, or index layout. The snapshot owns the bucket;
live-world index changes use existing COW behavior. There is no unsafe code.
Missing types/buckets produce an empty iterator and entity order stays sorted.
Exact size preserves destination reservation behavior.

O0 lifetime/growth profiling remains open. This narrow O1 experiment runs ahead
of it because it removes temporary allocation without adding retained pools.
Broader pooling/storage changes still need those memory measurements.

Validation: test a borrowed old bucket across live key changes/removals, ordering,
exact size, and missing types. Run the existing joins, delta-hint, snapshot and
workspace contracts, then compare allocation/timing against the O0 executable.

## O1a result: keep

At 1,000 requests/eight workers, median allocation calls fall from **156,815 to
144,815 per iteration**, or **12 fewer/request (7.65%)**. Reallocations remain
2,124. DHAT's candidate-allocation category falls from 22 to 10 events/request,
matching the counter reduction; the residual is still used by pair construction.
Planning and command arena growth counters remain zero after warmup.

Wall time is effectively flat within process variation: one-worker large batches
25.65 → 25.56 µs/request; eight-worker batches 36.96 → 37.64 µs/request.
Initial one-request/one-worker medians were 62.49 → 67.50 µs; the longer recheck
(20 warmups, 100 rounds, five alternating processes) measured 60.25 → 60.88 µs,
and eight-worker small cases 97.96 → 99.75 µs. No speedup is claimed. The small,
safe change is retained for its demonstrated allocation reduction.

[Results, variation and provenance](benchmarks/2026-10-23-borrowed-hints.json).
The baseline executable is the O0 batch; its original source manifest is retained
in the iteration-budget report. Raw runs are in target/optimization-o1/.

Validation passes: workspace build/tests (157 engine tests), clippy, playground,
all four benchmark smoke suites, and the idle/read allocation budget matrix.
No feature changes or formatting pass were used.

## O1b plan: prepare and reuse pair-row collection

- Prepare a typed candidate collector and static Eq/In binding metadata once
  per parameter/planning pass, not once per provider row.
- Feed borrowed equality buckets directly into it; reuse each bound parameter's
  row Vec while expanding providers. Snapshot-local scalar handles and tuple
  presence masks avoid repeated lookup/metadata construction.
- Keep the existing Vec-returning custom parameter path as a compatibility
  fallback. Preserve provider ordering, empty buckets, compound pruning,
  optional/opaque parts, and snapshot ownership.
- Scope the row buffers and collectors to synchronous planning; no borrowed
  bucket or buffer escapes into worker execution.
- Compare against O1a, including memory behavior and small/compiler cases.
  This does not yet recycle row buffers across planning passes.

## O1b result: keep

Prepared candidate collectors and cached binding descriptions now fill per-pass
pair buffers directly. Scalar queries resolve their store once; tuple queries
prepare their presence mask once. Buffers clear between providers and retain
capacity within the pass. Custom queries/params use the old owned fallback;
opaque parts still run their predicates. Snapshot-specific collectors do not
escape planning into worker futures.

At 1,000 requests/eight workers, allocations fall **144,815 → 100,857 per
iteration** (about 30.4% fewer); reallocations stay 2,124. The DHAT profile reports
102.981 combined allocation/reallocation events per request. Candidate-bucket
allocation sites disappear; remaining planning is 15.429 events/request, mostly
owned admitted-row metadata. Scratch/command growth remains zero after warmup.

Timing remains inconclusive: eight-worker purchases 37.95 → 38.46 µs/request;
one-worker purchases 24.46 → 24.81. Compiler/sparse cases overlap. The large
relationship join improved in two of three runs but has a wide candidate range
(2.960–3.855 ms versus 3.002–3.371 ms). No speedup is claimed.

Live memory matches the O0 eight-worker growth baseline exactly in these runs:
4,987,324–4,987,828 bytes at 1,000 slots and 9,954,828–9,955,332 at 2,000;
peaks 9,814,592 and 19,589,760 bytes. These are finite DHAT observations.

Validation: 160 engine tests, workspace build/tests, clippy and playground pass.
Two test-only needless-lifetime clippy suggestions are deferred to the requested
final cleanup. The benchmark subagent verified frozen source/binary hashes,
all four optimized smoke suites, and three repetitions of the idle/read budget
matrix. Existing timing runs were reused because the hashes matched.

[Results and provenance](benchmarks/2026-10-23-pair-reuse.json).

### Remaining O1 work and next priority

Collectors and row Vecs still allocate once per planning pass; membership
providers still clone their member Vec before filling a reusable destination.
Whole product state lists remain materialized. Static layout reuse across passes,
broader projections/filters and historical facet scans remain candidates.

The profile now places most remaining allocator events in per-row identity,
execution, guards and publication. Move to O2's owned metadata experiments next;
coalesce cross-pass query layout caching with the schema/consumer registry in O5
rather than introduce a competing cache. This is a measured ordering change,
not a claim that every O1 proposal is complete.
