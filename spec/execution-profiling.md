# Execution profiling and driver preparation

Status: measurement slice complete. The first prepared-driver slice is implemented
in [prepared-driver.md](prepared-driver.md); the proposal below records the original
sequence, including the still-deferred direct dirty routing.
Base: `70ccd1d`. [Machine, commands, hashes and results](benchmarks/2026-10-21-execution-profile.json).

## Conclusion

Retain concrete row futures. In the isolated eight-worker tiny-kernel cases they
completed batches about 2.1–2.8 times faster than boxed rows. This is not a
whole-engine speedup: light purchase workloads still spend 77–82% of sampled
process CPU on the evaluator thread. Prepare the driver's repeated metadata and
lookup work next, using the schema and registered query types.

One-worker results include regressions and substantial variation. The busy local
workstation supports this structural investigation, not precise acceptance of
small percentage improvements. No universal executor speedup is established.

## Isolated worker executor

`crates/bowl/src/dispatch/executor.rs` is the production worker executor, extracted
without changing its policy. `crates/benches/benches/dispatch.rs` includes that
same source and compares three static instantiations:

| Variant | Row futures | Outer Tokio task |
| --- | --- | --- |
| `typed` | Concrete | Concrete |
| `boxed` | One box per row | Concrete |
| `shared_task` | Concrete | Boxed |

Timing includes future construction, allocation, submission, polling, completion
channels and draining. Runtime setup, seed allocation and correctness checks are
outside timing. The synthetic future holds 128 bytes of inline state; optional
suspension self-wakes once and is not actual I/O. Arithmetic passes opaque compiler
barriers. No Bowl planning, component guards, memoization or publication is included.

Median batch microseconds, 1,000 rows, maximum 16 rows/task; two fresh processes
per variant with rotating/reversed order, 20 Criterion samples, 0.1-second warmup
and 0.6-second requested measurement:

| Workers | Arithmetic iterations | Suspends once | Typed | Boxed rows | Boxed task |
| ---: | ---: | --- | ---: | ---: | ---: |
| 8 | 0 | No | 309.38 | 648.85 | 350.81 |
| 8 | 0 | Yes | 283.84 | 804.14 | 268.26 |
| 8 | 10,000 | No | 1,552.74 | 1,599.96 | 1,565.18 |
| 8 | 10,000 | Yes | 1,538.17 | 1,642.23 | 1,549.77 |
| 1 | 0 | No | 538.34 | 326.38 | 295.17 |
| 1 | 0 | Yes | 620.59 | 603.67 | 538.81 |

The one-worker immediate typed estimates were 647/429 us, versus boxed 367/286 us;
that counterexample must remain visible. Eight-worker immediate typed estimates
were 331/288 us versus boxed 613/685 us. Task-only boxing stays near concrete tasks
in the eight-worker cases; specializing all Tokio machinery is not established as
the important win. Heavier work amortizes the dispatch difference.

These are **rows**, not requests. The independent purchase fixture completes five
row invocations per request. Do not turn isolated rows/s into application capacity.
The fixed width matches the production adaptive maximum for the measured large
cases; the microbenchmark does not reproduce production admission decisions.

## Complete purchase workload

`crates/benches/examples/profile_purchases.rs` creates fresh, settled accounts for
each round and drives the existing purchase fixture. Sampling starts immediately
before request insertion/evaluation/response draining and ends before assertions,
formatting and world destruction. Warmup and account setup are excluded.

User-space `cpu-clock` sampling at 499 Hz with DWARF stacks recorded the driver
and inherited workers. Perf control FIFOs gate sampling with acknowledgements.
All four profiles reported zero lost samples. CPU shares below are sampled CPU,
not fractions of wall time and not an Amdahl-law speedup bound.

| Case, 1,000 requests | Workers | Driver CPU | Worker CPU | Unsampled mean batch |
| --- | ---: | ---: | ---: | ---: |
| Independent, light | 1 | 81.75% | 18.25% | 46.49 ms |
| Independent, light | 8 | 77.09% | 22.91% | 49.05 ms |
| Four requests/owner | 8 | 79.33% | 20.67% | 135.83 ms |
| Independent, 10,000 arithmetic iterations | 8 | 49.41% | 50.59% | 39.21 ms |

Unsampled runs used 20 rounds after two warmups; sampled runs used 80 rounds
(30 for contention). The heavier fixture's lower wall time is not evidence that
adding arithmetic improves throughput: scheduling interleavings and machine load
also differ. Compare candidates with matching workload and repeated measurements.

Exclusive leaf-symbol groups for independent/eight-worker/light work:

| Group | Sampled process CPU |
| --- | ---: |
| Allocation/free | 16.99% |
| Hashing | 12.46% |
| Reservation methods | 5.72% |
| Revision lookup | 5.34% |
| Fingerprint lookup | 2.16% |
| Other | 57.17% |

These name-based groups assign each self sample once, cover driver and workers,
and are lower bounds on broader activities. They are not complete stage timings.
TypeId/entity hashing, revision lookups and allocator leaves are concrete targets;
this does not prove every allocation or hash can be removed by schema preparation.

Counter limitations:

- `plan_nanos` excludes lazy future construction during admission.
- Summed row poll time includes fetch/guard/output work and overlaps across workers
  and with driver work. It is not pure kernel CPU or a subtractable wall-time bucket.
- The existing commit counter measures hook commits, not ordinary streamed
  publication. The driver labels it `hook_commit_us`; use CPU stacks for the latter.
- Snapshot, plan and settle counters overlap. Do not sum them into a stage total.
- Contention completes 12,500 row invocations for 1,000 accepted requests; independent
  cases complete 5,000. Row runs include retries/no-ops/cleanup, not just transactions.

## Reproduction and validation

Build all targets together to keep the workspace feature graph consistent with
normal tests and runs (in particular Tokio's transitive features):

```sh
cargo build --workspace --all-targets --profile bench -j 20
cargo bench --workspace --bench dispatch -j 20 -- --test
cargo bench --workspace --bench dispatch -j 20
PROFILE_WORKERS=8 PROFILE_REQUESTS=1000 PROFILE_ROUNDS=20 target/release/examples/profile_purchases
```

Dispatch accepts comma-separated `DISPATCH_ROWS`, `DISPATCH_WORKERS`,
`DISPATCH_BATCH`, `DISPATCH_WORK`, `DISPATCH_SUSPEND` (0/1) and one
`DISPATCH_VARIANT` (`all`, `typed`, `boxed`, `shared_task`). Defaults are
1/1,000 rows, 1/8 workers, width 16, 0/10,000 iterations, both suspension modes.
The profile driver also accepts `PROFILE_PER_OWNER`, `PROFILE_WORK`,
`PROFILE_WARMUP`, and `PROFILE_DISPATCH_BATCH`.

For gated sampling, create two FIFOs, set `PROFILE_PERF_CONTROL` and
`PROFILE_PERF_ACK` to their paths in the child's environment, and launch with:

```sh
perf record -e cpu-clock:u -F 499 --call-graph dwarf,8192 -D -1 --control fifo:CONTROL,ACK -o perf.data -- target/release/examples/profile_purchases
perf report --stdio --no-children --sort comm,symbol
```

Replace CONTROL/ACK with those FIFO paths. Use Python (`uv run python`) for
orchestration; exact measured commands and provenance are in the result JSON.
Raw profiles and local runners remain under ignored `target/execution-profile/`.

Validation: workspace build, all-target builds in debug and bench profiles,
150 workspace unit tests plus one doctest, all three optimized benchmark smoke
targets, the playground and profile-driver smoke passed. Clippy completed with
existing workspace warnings. Perf acknowledgement handling was corrected for
trailing NUL bytes before the recorded measurements.

## Proposed implementation: prepare the driver from the schema

Keep the public model unchanged: register schema and kernels, insert facts, settle.
Prepare known structure once; check current facts at runtime. No new scheduler
concurrency or storage layout rewrite is needed for the first slice.

| Lifetime | Prepared information | Still dynamic |
| --- | --- | --- |
| Bowl construction | Component IDs, query component/access layout, masks | Entity membership and component values |
| Snapshot / planning wave | Resolved stores for each query, including absent stores | Candidate rows, joins and revisions |
| Row admission | Keys, current dependency revisions, access for admitted rows | Memo hit, conflicts and stale-read checks |
| Publication | Existing output/ownership contracts | Revisions, derived cleanup and wakeups |

### 1. Stop constructing access for memo hits

In `system.rs::plan_invocations_hinted`, `Params::access` currently runs before
memo rejection. Move it after that decision. Keep dependency collection and
`always_run` semantics unchanged. This is an independently measurable small
change; its gain depends on the number of clean rows, not total request volume.
Test memo hits plus changed/always-run rows; benchmark no-op and sparse updates.

### 2. Prepare component and store resolution

`world.rs::PresenceIndex` already assigns dense component bit positions. Reuse
that universe as the bowl-local component registry; do not introduce unrelated
IDs or assume IDs match between bowls. Query/system registration can resolve
component types and access modes into a reusable layout.

Today `World::store<T>` hashes TypeId and downcasts, while
`revision_for_entity_dyn` hashes TypeId before a virtual lookup. Prepare a
snapshot-scoped query context resolving participating stores once per wave;
use it for matching, dependency collection and fetching. A prepared value-fetch
path alone would miss the measured driver-side revision cost. Where dynamic
commit checks need stores, stable component slots can remove repeated TypeId
hashing without caching a pointer across snapshots.

Likely files: `world.rs` (registry/store resolution), `query.rs` (prepared query
context), `system.rs` (prepare once and reuse per row), `bowl.rs` (construction
and snapshot integration). Keep typed fetching generic. Resolve absence for
optional/removed stores afresh per snapshot, and retain a fallback for custom
parameters that cannot describe their access. Review trait compatibility before
changing the public `QueryParam`/`SystemParam` extension points.

Start with ordinary tracked queries, including tuples, optional parts and
mutable access. Verify separate bowls, insert/remove, joins, stale commits,
preemption, guarded-cell lifetimes and hook/cooperative paths. No raw cached
pointers, skipped guards or weakened revision checks are part of this proposal.

### 3. Measure before expanding

Run matching independent/contended purchase cases at 1/8 workers, memo-hit and
sparse-update cases, plus compiler regressions. Keep features, fixtures and
profiles constant. Compare full batch latency and CPU stacks; an isolated lookup
win is insufficient. Archive provenance and retain small-case regressions visibly.

### 4. Route changes directly to interested systems

Only after the prepared-query slice is measured, reuse the same registry for a
component-to-system reverse index and candidate queues. Membership changes,
removed outputs, provider-side joins and forced stale/conflict retries must all
route correctly. Compare against full planning as a correctness oracle. This is
a separate slice: static schema knowledge cannot precompute which rows a future
insert, value-dependent join or suspended invocation will affect.

Allocation reuse and publication bookkeeping remain follow-ups guided by the
next profile. SIMD and distributed planning are not prerequisites for this work.
