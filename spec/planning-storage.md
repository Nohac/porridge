# Reusable planning storage

## Plan

- Preserve the current command-arena binary as the comparison baseline.
- Add driver-owned planning scratch alongside the command-buffer pool. Planning
  runs synchronously outside the state lock; scratch is never borrowed by a
  dispatched future. Independent Bowls keep independent storage.
- Reuse invocation lookup keys and bump-allocated dependency/access lists.
  Clear row scratch between candidates; reset the arena after each planning
  pass. Copy admitted metadata into independently owned invocation/memo buffers.
- Add append-to-destination methods with compatibility defaults for custom
  query/system parameters. Ordinary parts, filters and tuples append directly,
  avoiding temporary child Vecs.
- Keep join semantics, dependency ordering, conflict access, and memo ownership
  unchanged. Optimize candidate materialization only where ownership is clear.
- Verify workspace tests, custom/fallback and memo-invalidation regressions,
  playground, benchmark smokes, and warmed allocation/timing comparisons with
  unchanged features. Attribute remaining costs after measuring.

Execution and memo storage remain owned in this batch: pooling them across
suspended invocations is a separate lifetime problem. No shared worker allocator
lock, new unsafe code, or automatic commit is planned.

## Implemented storage boundary

`planning::Storage` travels with the single-flight driver. It owns the command
pool and `Scratch` (a bump arena plus a reusable invocation-key Vec). The state
lock only transfers ownership; planning does not hold it. There is no additional
worker mutex. Cancellation can discard the cache, as with the command pool;
it cannot leave futures referring to scratch.

`plan_prepared_with_scratch` uses a bump scope for the entire pass:

1. Clear key/dependency scratch for each candidate.
2. Append keys and prepared dependencies directly.
3. Compare against the memo. A hit skips access collection and owned copies.
4. Append access metadata for admitted rows.
5. Copy the admitted keys/deps/access into owned row buffers.
6. Reset the scope, including when a custom parameter panics.

Scope reset preserves arena capacity. The admitted row copies may move to a
worker, survive later planning passes, and become memo entries independently.
Existing hook/fallback planning creates local scratch instead of leasing the
driver cache; ordinary streaming invocations use retained storage.

`keys_into`, `deps_into`, and `access_into` accept a generic `Extend<T>`
destination. Built-in scalar parts, entity tuples, query filters, system tuples,
and derived SystemParam bundles append directly. Existing Vec-returning extension
methods remain supported through compatibility defaults; custom implementations
can adopt destination methods incrementally. Prepared dependency methods have
the same fallback. Complex projections retain their existing fallback behavior.

This removes nested temporary Vecs and repeated scratch allocation. It does not
eliminate the owned per-admitted-row buffers, reservation/future clones, memo
copies, or snapshot allocations. Join-candidate materialization is unchanged in
this batch, so comparisons isolate metadata collection and scratch reuse.

## Verification

Regression coverage retains admitted metadata while reusing the scratch arena
for different invocation identities, checks memo hits and optional-component
appearance invalidation, and injects a planning panic after allocating a large
access list. The next pass must succeed with empty scratch usage. Existing
legacy custom-parameter tests verify memo hits still skip access construction.
The full workspace covers joins, conflicts, derived bundles, and the playground.
Final workspace build/tests (155 engine tests), example tests, playground, and
all four benchmark smoke suites pass. Clippy passes with existing repository
warnings; no new warning targets the planning-storage changes. No new unsafe
code or Cargo feature configuration was introduced.

## Follow-ups

- Retain/recycle owned execution metadata after all consumers finish; this needs
  explicit ownership across reservations, futures, and publication.
- Avoid materializing join candidate lists when a borrowed bucket or visitor is
  sufficient. Preserve relationship and compound-filter behavior.
- Reuse memo storage without retaining an entire execution arena indefinitely.
- Extend retained scratch to hook/fallback planning if its measured cost warrants it.

## Results

Three alternating baseline/candidate process repetitions, eight warmup rounds,
ten measured rounds, 1,000 fixed request slots. The baseline is the preceding
command-arena integration, not the older boxed-command engine.

| Workers | Alloc calls/request, before → after | Realloc calls/request, before → after | Wall µs/request, before → after |
| --- | ---: | ---: | ---: |
| 1 | 244.106 → 174.504 | 24.854 → 2.457 | 30.87 → 28.93 |
| 8 | 231.815 → 156.815 | 24.124 → 2.124 | 40.24 → 38.16 |

The stable eight-worker case removes **75 allocations and 22 reallocations per
request**: 32.4% fewer allocation calls, or 37.9% fewer alloc+realloc calls.
Timing improved about 5–6% for these large batches, directionally; these are
busy-workstation measurements. Single-slot and per-process timing results are
retained in the dataset. Settled reads still require four allocations per round.

With metrics enabled, five rounds after eight warmup rounds at 1/1,000 slots and
1/8 workers report **zero planning-arena chunk growth and zero lookup-key buffer
growth**. Command-storage creation/growth also stays zero in all four cases.
This is capacity reuse in a bounded workload, not a zero-allocation engine.

The separate eight-worker DHAT profile records **158.938 allocation/reallocation
calls per request** (profiling changes scheduling):

| Origin | Calls/request |
| --- | ---: |
| Planning, including owned admitted-row metadata | 49.386 |
| Planning join candidates | 22.000 |
| Command producer handles | 5.000 |
| Other pipeline work | 82.552 |

The old access/dependency/key helper allocation sites disappear from this
profile. The admitted-row copies now show under planning itself, and still
allocate. Total planning share falls from about 66% to 45%; this is an allocation
share, not a CPU-time profile. Origin grouping remains conservative with
optimized/inlined code.

## Reproduce

```sh
cargo build --workspace --all-targets --profile bench -j 20
uv run tools/measure-allocations --before target/planning-storage/before-steady_allocations --out target/planning-storage/new-comparison --cases purchases,read --slots 1,1000 --workers 1,8 --warmup 8 --rounds 10 --repetitions 3
uv run tools/measure-allocations --out target/planning-storage/new-diagnostics --cases purchases --slots 1,1000 --workers 1,8 --warmup 8 --rounds 5 --repetitions 1 --metrics
uv run tools/profile-allocations --out target/planning-storage/new-heap-profile
```

The supplied baseline binary must have been preserved before rebuilding.
Counters/profiling run separately from uninstrumented timing. No feature changes.

[Results and binary/source provenance](benchmarks/2026-10-22-planning-storage.json).
Raw reports live under `target/planning-storage/`.
