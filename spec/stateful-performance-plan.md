# Stateful workload performance plan

Status: benchmark foundation added; engine optimizations below are not yet
ported. Baseline engine: `4eb62fc`. All workloads use the ordinary Bowl planner,
access reservations, async systems, publication and retirement paths.

## Goals and boundaries

Measure completed request throughput and how it changes with independent owners,
contention, worker count, work per invocation, live population and retired history.
Keep the existing compiler benchmarks as regression coverage. More CPU activity
is not itself success: small kernels may be fastest with one worker.

No networking, database, packet encoding, game assets or external project
requirements belong in these fixtures. The results measure engine operations,
not end-to-end application capacity. No engine algorithm changes in this batch.

## Next implementation slice

Start with **indexed access reservations** (batch B, step 1). The current
`bowl.rs::conflicts_with_running` compares each candidate access against accesses
of running invocations. The prototype improvement can be evaluated without
changing schemas, request ordering, storage ownership or settlement.

- [ ] Add private `crates/bowl/src/access.rs` for component/entity reservation
  counts, including whole-store access and duplicate declarations.
- [ ] Integrate admission/release in `bowl.rs`; keep reservations through the
  appropriate publication boundary, including stale, cancelled and preempted work.
- [ ] Compare the index against the existing pairwise conflict predicate in tests;
  cover overlapping readers/writers, disjoint entities and removal of reservations.
- [ ] Run workspace tests and both benchmark smoke targets. Measure independent
  and contended purchase batches at 1/10/100/1,000 requests and 1/4/8 workers with
  unchanged features. Include small cases and existing compiler workloads.
- [ ] Record before/after ranges with identical fixtures and fresh processes.
  Evaluate this change separately before batched retirement.

The broader [schema/partition direction](ideas/schema-execution-and-partitions.md)
is exploratory. Dense schema metadata and dirty routing build on batches C–E;
local ownership partitions and network clustering follow only after local costs
and their coordination contracts have been measured.

## Evidence worth carrying forward

Earlier isolated experiments on this same upstream baseline found:

| Change | Local experiment commit | Evidence and disposition |
| --- | --- | --- |
| Indexed access reservations | `64bd2e0` | Removes pairwise running-access scans; worthwhile at large independent populations. Port with equivalence tests. |
| Batched entity retirement | `8e9e55a` | Removes repeated memo/ownership sweeps per drained response. Together with reservations, about 2.5x throughput at 1,000 requests in the original short runs. |
| Adaptive worker dispatch batches | `2c92342` | Around 2.2x throughput for heavy 1,000-request/eight-worker work versus the preceding engine. Preserve streamed row completion. |
| Per-system admission/commit policies | `cab6e1f` | No consistent gain from larger commit groups; restrictive caps increased replanning. Defer as a resource-control API. |
| Automatic component revisions | `282314d` | Useful mutation ergonomics, but repeat measurements reversed initial speedups. Separate API decision, not a proven optimization. |

These commit IDs identify local experimental implementations, not commits already
on main. Their old measurements used uncontrolled machine load and sometimes
aged the bowl throughout a sample. Reproduce here before selecting defaults; do
not multiply the reported speedups or compare different workload boundaries.

The latest profile of that experimental engine still placed roughly 80% of
sampled user CPU on the driver with eight workers. Allocation/free was about 26%,
hashing 12–15%, and presence scans 3–5%. These categories overlap driver attribution.
Planning counters were around one third of wall time with eight workers. This
supports targeting serial bookkeeping; it does not attribute every allocation to
planning or prove a particular new optimization will win.

## Benchmark layout

```text
crates/benches/
  benches/stateful.rs        Criterion entrypoint and runtime configuration
  src/stateful/
    mod.rs                  workload modules
    purchases.rs            schema, validation, guarded apply, response drain
    churn.rs                fixed live population after controlled retirement
    tests.rs                fixture contracts outside benchmark timing
```

Existing `fleet_bowl` / `touch_slot0` supply the sparse-change workload. It is
schema-less today; add a schema-backed counterpart before evaluating bitmap
routing. Keep fixtures grouped by responsibility; do not put unrelated domain
models in the existing general fixture file.

| Workload | Timed operation | What it exposes |
| --- | --- | --- |
| Independent purchases (`per_owner=1`) | Insert N requests, evaluate, copy replies, retire requests | Planner, independent reservations, dispatch, publication and bulk cleanup |
| Contended purchases (`per_owner>1`) | Same; multiple sequenced requests share each owner | Conflicts, stale validation retries, access release and ordered progress |
| Entity churn | Materialize a fixed number of live rows after H retired entities | Typed facet history scan versus live-store tuple matching |
| Sparse changes | Update one entity and settle 32 registered systems | Unrelated-system planning and bookkeeping cost |

Purchases have separate wallet and inventory validators plus one apply system
holding both state guards. Requests are inserted in reverse sequence order;
conflict exclusion alone must not be mistaken for ordering. Rejections consume
the sequence but leave wallet/inventory unchanged. Completed replies are drained;
base account state survives retirement. Completion currently triggers validator
reruns to retract artifacts: keep that lifecycle unchanged for initial comparisons.

Correctness tests cover sequencing under 1/4 workers, refusal without partial
mutation, replay, response retirement, and live results after churn. Assertions
are outside timed loops. These fixture tests are not a claim of general database
transaction semantics or crash durability.

## Measurement controls

- Purchase observations each start with fresh, settled accounts and the same
  empty request history. Setup, runtime construction, final destruction and
  assertions are excluded; insertion through response copying/retirement is timed.
- Entity-churn setup retires a specified number of entities in bounded setup
  batches. Repeated measured reads never change that history. Report scans or
  rows/s, not requests/s. Compare typed facet enumeration against untyped tuple
  matching driven from a live store; only the former scans historical presence rows.
- Sparse changes reuse a fixed population without spawning new entities. Report
  updates/s; the `rows` parameter is per system, with 32 systems plus one target.
- One Tokio multithread runtime per worker configuration, ordinary parallel
  engine feature enabled. Runtime settings change without recompilation. Worker count excludes the caller
  thread driving evaluation.
- Criterion purchase throughput is requests/s (one completed reply per request
  in the valid fixture); contention changes owners, not the number of requests.
  `owners = ceil(requests / per_owner)`. Each observation is a finite batch, not
  a sustained concurrent arrival stream. Criterion intervals are batch-time
  estimates, not per-client latency percentiles.
- No arbitrary input caps. Positive counts, integer representation and normal
  memory/runtime limits still apply. Purchase fixtures disable the default
  commit-count guardrail so it does not masquerade as an input limit.
- Run variants in alternating order across fresh processes and record commit,
  dirty diff, rustc, profile, worker count, inputs and machine load. Compare both
  small and large populations; retain raw Criterion estimates under `target/criterion`.
- Preserve default Cargo features across testing, running and benchmarking. No
  optional profiling feature is needed for these benches. Report per-system
  timing separately if used: aggregate worker poll time can overlap wall time.

```sh
# Complete smoke suite (both original compiler and new stateful workloads).
cargo bench -p benches -- --test

# Purchase matrix: independent and four requests per owner, light/heavy work.
PORRIDGE_REQUESTS=1,10,100,1000 PORRIDGE_WORKERS=1,4,8 \
  PORRIDGE_PER_OWNER=1,4 PORRIDGE_WORK=0,10000 \
  cargo bench -p benches --bench stateful -- purchases

# Fixed live population; vary only retired history.
PORRIDGE_LIVE=64 PORRIDGE_HISTORY=0,10000,100000 \
  cargo bench -p benches --bench stateful -- entity_churn

# One changed entity in a growing, otherwise idle population.
PORRIDGE_ROWS=10,100,1000 PORRIDGE_WORKERS=1,4,8 \
  cargo bench -p benches --bench stateful -- sparse_changes
```

## Ordered implementation batches

### A. Establish measurement and reconciliation

- [x] Preserve the superseded local aggregate-view draft and fast-forward main.
- [x] Remove the disabled external review requirement; retain user commit approval.
- [x] Add independent/contended purchase, history and sparse-change fixtures.
- [ ] Extend the harness with an explicit fixed-round sustained-churn workload:
  equal prehistory and request count for every candidate, not equal timed duration.
- [ ] Add compact provenance/result export and separate per-request latency sampling
  for the sustained workload. Do not turn the timed body into an assertion loop.

### B. Port the demonstrated scan/cleanup improvements independently

1. `crates/bowl/src/access.rs`: index running access by component and entity;
   retain whole-store conflicts, multiple readers and duplicate-access counts.
   Integrate in `bowl.rs`, retaining reservations until publication, including
   preemption and stale-output paths. Test against the original conflict predicate.
2. `bowl.rs`: retire drained entities as a batch, including dropped bound handles;
   process transitive dependent-output frontiers once per batch. Test unrelated
   survivors, linked outputs, repeated drains and subsequent reevaluation.
3. Compare each step on independent and contended requests, plus original compiler
   benches. Separate setup, evaluation and cleanup costs where instrumentation can
   do so accurately. Keep changes that improve the relevant large cases without
   introducing unexplained small-case regressions.

### C. Bound historical and unnecessary planning work

1. `world.rs` / `query.rs`: enumerate live candidates rather than every historical
   presence row. Consider the smallest live store plus mask filtering before a
   larger storage redesign. Measure memory and later COW copying too: faster scans
   alone do not reclaim historical bitmap storage. Never recycle raw IDs without
   stale-handle/generation protection.
2. `system.rs::plan_invocations_hinted`: construct access metadata only after the
   memo check admits a row. Then measure allocations before introducing inline
   vectors or interning component IDs. Keep key/dependency correctness unchanged.
3. Add no-op/memo-hit and schema-backed sparse-change cases, including filters,
   optional components, insertion, removal and joined-provider changes.

### D. Dispatch batching with independently completed rows

Port `dispatch.rs` and the narrow `bowl.rs` integration from the experiment.
Start with an internal/default grain and a runtime override for benchmarking.
Do not batch dependency identities, publication or domain transactions together.
Each completed row must publish without waiting for a pending sibling. Reader
cancellation, writer draining, panic propagation, bounded completion channels and
execution without Tokio need contracts. Compare grain 1/4/16/64 with 1/4/8 workers,
light/heavy work and small populations before choosing a default.

### E. Direct dirty routing after the preceding measurements

Implement schema component IDs and a reverse component-to-system interest index
at registration. Route changes into per-system entity candidate queues instead
of making every system filter the shared write log. Presence transitions handle
membership; value revisions still handle stale reads. Preserve provider-side join
expansion, removed rows/output cleanup, epoch resets and fallback for opaque
params. Deduplicate pending candidates without losing forced stale/conflict retries.

Start with plain tracked queries and schema-backed tests, then joins. Compare
against full planning as a test oracle. Bitmap lookups reduce routing cost; the
cost still includes affected systems and candidate rows. Do not claim O(1) total
scheduling. Avoid adding scheduler concurrency until queue ownership/publication
contracts are established.

## Workloads to add before broader adoption

- [ ] Two-owner exchange: disjoint pairs and overlapping pairs; conserved totals,
  consistent observers, one final guarded update. This is distinct from several
  requests contending for one account and is not covered by that fixture.
- [ ] Room-scoped movement and fanout: membership joins, one input → many derived
  notices, enter/leave changes. Count input requests and delivered notices separately.
- [ ] Mixed arrivals: movement, purchases, exchanges and membership churn, with
  controlled queues and explicit session ordering. The engine remains identical;
  only the input source changes.
- [ ] Suspended async kernels with fast siblings: prove dispatch batching does not
  create a completion barrier or starve downstream work.

These expand coverage; they are not prerequisites for measuring the first two
scan/retirement optimizations. Automatic revisions, policy caps, SIMD kernels and
a drive-only public API remain separate proposals requiring their own evidence.

## Initial baseline and validation (2026-10-21)

Engine `4eb62fc`, rustc 1.99.0, default features, bench profile, 0.2-second
warmup, one-second requested measurement, 10 Criterion samples. Larger batches
required more than the requested second. These are short diagnostic measurements
on an uncontrolled machine, not production capacity or an optimization comparison.
[Estimates and commands](benchmarks/2026-10-21-stateful.json) preserve the intervals.

Independent light purchases, completed requests/s:

| Requests per batch | One worker | Eight workers |
| ---: | ---: | ---: |
| 1 | 23,458 | 22,461 |
| 100 | 26,281 | 22,683 |
| 1,000 | 4,602 | 4,529 |

The large-batch slowdown gives the access-index/retirement work a useful baseline.
It does not establish which individual engine stage accounts for the difference.

Churn, microseconds to materialize the same 64 live rows:

| Retired entities | Live-store tuple query | Typed facet query |
| ---: | ---: | ---: |
| 0 | 15.5 | 22.5 |
| 10,000 | 14.6 | 30.8 |
| 100,000 | 14.8 | 82.6 |

Both query variants fetch the same component fields. The tuple path drives from
live component entries; the typed facet path scans historical presence rows.
The initial untyped-only fixture missed this distinction; the paired benchmark
now directly exercises the relevant path. The zero-history measurements have
wide intervals, so treat the exact ratios cautiously.

Sparse updates grew from roughly 29 to 90 microseconds with one worker when rows
per system increased from 10 to 1,000 (32 systems). The eight-worker samples were
noisy; they do not establish a parallelism advantage for this one-row operation.

Validation completed:

- `cargo build --workspace -j 20`.
- `cargo test --workspace -j 20`: 139 unit tests plus the component doctest passed.
- `cargo bench -p benches -j 20 -- --test`: both targets passed, including the
  1/4/8-worker, 1/10/100/1,000-request matrix. The subsequent paired churn variant
  passed its fixture tests and all six measured cases.
- `cargo run -p playground`: successful integration run.
- `cargo clippy -p benches --all-targets -j 20`: completed with existing warnings;
  the new explicit multi-component apply signature documents its lint exception.
- `git diff --check`: clean.

No engine optimization or new commit was made in this batch. The old local draft
is retained in the named stash `preserve local tracked-view draft before upstream
update 2026-10-21`; untracked changelog and local tooling settings are untouched.
