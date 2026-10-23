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
