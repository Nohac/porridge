# Stateful workload performance plan

Status: benchmark foundation, indexed reservations, retirement batching, the
concrete kernel adapter and worker dispatch batching are committed. Typed execution
plans are implemented; isolated execution-speed attribution remains open. Precise performance acceptance remains open. Baseline engine: `4eb62fc`. All workloads use the ordinary Bowl planner,
access reservations, async systems, publication and retirement paths.

## Goals and boundaries

Measure completed request throughput and how it changes with independent owners,
contention, worker count, work per invocation, live population and retired history.
Keep the existing compiler benchmarks as regression coverage. More CPU activity
is not itself success: small kernels may be fastest with one worker.

No networking, database, packet encoding, game assets or external project
requirements belong in these fixtures. The results measure engine operations,
not end-to-end application capacity. Keep fixture changes separate from engine
changes when comparing candidates.

## Current implementation slice

**Typed execution plans** (batch D1), based on commit `c864956`. Ordinary kernels
retain concrete row futures through worker polling. Shared admission lives in
`dispatch/plan.rs`; cooperative execution boxes each row once, and grouped hook
semantics remain on their existing fallback. The contract and boundaries are in
[dispatch-batching.md](dispatch-batching.md#typed-execution-plans).

### Typed execution measurements (2026-10-21)

Two candidates were built: fully specialized Tokio tasks, and a shared-task
candidate that boxes once at the task boundary while keeping rows typed inside.
The retained candidate is **fully specialized**. Code size alone is not a reason
to reject specialization; these end-to-end measurements do not establish a stable
speed advantage for either task boundary.

The initial ten-case matrix reused matching maximum-16 measurements from the
preceding slice. Both new candidates ran freshly in alternating order. Four
contrasting large cases then received fresh alternating runs of all three
executables. Two processes per variant, 0.1-second warmup, requested 0.4-second
measurement and ten Criterion samples; default features and fixtures unchanged.

Fresh recheck, median milliseconds per 1,000-request batch:

| Case | Committed baseline | Specialized (retained) | Shared task |
| --- | ---: | ---: | ---: |
| Independent light, 8 workers | 56.9 | 51.1 | 47.4 |
| Four per owner, 8 workers | 138.4 | 147.7 | 135.6 |
| Heavier arithmetic, 1 worker | 78.6 | 49.8 | 47.8 |
| Heavier arithmetic, 8 workers | 43.0 | 36.7 | 36.8 |

These must not be presented as a universal speedup: the contended case regressed
in the fresh recheck, and candidate ordering reversed between passes in some
cases. For example, the first independent/eight-worker comparison favored
specialized over shared (36.7 vs 55.5 ms), but the fresh check favored shared
(51.1 vs 47.4 ms). The workload includes planning, storage access, guards,
publication and retirement; it cannot attribute the execution portion's speed.
A significantly faster row executor can be masked by those other costs.

The specialized engine benchmark `.text` grows from 5,446,824 to 7,030,808 bytes
(+29%). The shared-task candidate is 6,167,880 bytes (+13%). For the smaller
purchase executable the corresponding sizes are 3,133,656, 3,189,384 and
3,165,496 bytes. These are tradeoff measurements, not acceptance thresholds.

Compiler regression checks: cold settling initially measured 635 → 731 us against
the reused baseline, incremental settling 74 → 78 us. Fresh alternating cold runs
were baseline 698/619 us versus specialized 693/703 us. These overlap but leave a
possible regression unresolved; performance acceptance is not complete. Both
rebuilt optimized executables match the measured specialized binaries byte-for-byte.

The implementation removes two explicit per-row future boxes on the ordinary
worker path. It still allocates future-set nodes and per-invocation metadata and
does not batch memo identities or commits. `plan_nanos` now excludes lazily
constructed futures during admission; compare total latency, not old/new planning
counter percentages.

- [x] Build, 150 workspace unit tests, doctest, playground and optimized smoke.
- [x] Isolated purchase build, three contract tests and optimized smoke.
- [x] Preserve cancellation, pending-sibling publication, conflict/retry and hook
  behavior; exercise a concrete suspended `!Unpin` row future in the dispatch test.
- [ ] Isolate dispatch execution with representative immediate and suspended
  kernels, comparing typed and boxed rows independently of planning/publication.
- [ ] Profile complete workloads to quantify how much time the isolated path
  represents; do not infer an executor speedup from code size or total throughput.
- [ ] Then implement prepared typed-store fetching as a separately measured slice.

[Raw comparisons, variation and provenance](benchmarks/2026-10-21-typed-batches.json)
are retained. Local binaries and scripts live under `target/typed-batches/`.
The shared-task source is retained there for experiments, not exposed as a new
runtime policy knob. Precise acceptance and the contended-case regression remain
open for controlled-host measurements. No new Bevy timing run was needed.

### Dispatch batching measurements (2026-10-21)

Implemented and validated against `f9c6cd7`. Each final comparison below uses one
executable with runtime maxima 1 and 16, two fresh processes per setting in
alternating order, 0.1-second warmup, requested 0.4-second measurement and ten
Criterion samples. Values are medians of two process estimates, in milliseconds
per complete request batch. The runtime setting is a maximum; small plans use
smaller tasks. Maximum 1 is the new dispatcher's individual-task mode, not the
previous executable. Both settings include the scalar-query correctness fix.

| Requests | Workers | Workload | Maximum 1, ms | Maximum 16, ms | Ratio |
| ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 1 | Independent | 0.118 | 0.099 | 1.19x |
| 1 | 8 | Independent | 0.101 | 0.082 | 1.23x |
| 100 | 1 | Independent | 4.431 | 3.545 | 1.25x |
| 100 | 8 | Independent | 3.908 | 4.146 | 0.94x |
| 1,000 | 1 | Independent | 37.471 | 44.157 | 0.85x |
| 1,000 | 8 | Independent | 72.109 | 48.939 | 1.47x |
| 1,000 | 1 | Contended (4/owner) | 216.567 | 118.760 | 1.82x |
| 1,000 | 8 | Contended (4/owner) | 227.716 | 138.853 | 1.64x |
| 1,000 | 1 | Heavier | 65.399 | 83.073 | 0.79x |
| 1,000 | 8 | Heavier | 57.475 | 42.273 | 1.36x |

The most repeatable gains were the three large/eight-worker cases: 1.47x for
independent light requests, 1.64x with four requests per owner, and 1.36x with
10,000 arithmetic iterations per validator. Their final batched throughputs were
about 20.4k, 7.2k and 23.7k completed requests/s respectively. The first implementation
pass also improved those cases (1.45x, 1.63x and 1.41x).

One-worker conclusions are unresolved: the independent and heavier cases reversed
between passes, and contention varied widely. Do not use the table to claim a
universal improvement or a well-tuned default. At one request both settings select
exactly the same grain of 1; the apparent difference is measurement variation.
Maxima 4/64 and four-worker tuning are deferred to a controlled run. The initial
maximum of 16 remains provisional.

The first implementation nested cooperative streams even without Tokio. Fresh
cold-settle comparisons showed before 601–652 us versus after 667–676 us. The
final implementation restores a flat cooperative future queue: fresh cold runs
were before 629–638 us, after 627–642 us; incremental runs were before 73–76 us,
after 73–75 us. Tiny worker cases still varied sharply: one-worker before 46–90 us,
after 91–114 us; eight-worker before 97–103 us, after 52–83 us. Small-case overhead
remains open, not proof of a regression-free universal win.

Validation passes: workspace build, 150 unit tests and the doctest, playground,
both optimized benchmark smoke targets, and Clippy (existing warnings). The
isolated reference build, three purchase contract tests and optimized smoke pass.
No dependency or feature changes were introduced. The initial integration test
stalled on the pre-existing scalar-query interest bug, now fixed and covered.

[Raw final and initial estimates, hashes, commands and validation](benchmarks/2026-10-21-dispatch-batching.json)
are archived. Local logs, preserved baseline executables and scripts are under
`target/dispatch-batching/`; the superseded first pass is retained in its `initial/`
subdirectory. Bevy's earlier reference is reused rather than rerun; this slice does
not establish a new precise Porridge/Bevy ratio.

At the end of this earlier slice, typed system batches and profiling were next.
Typed plans are now implemented above; prepared fetching, live-candidate enumeration
and direct dirty routing remain on the plan. Per-invocation bookkeeping remains.

### Previous slice: batched retirement

**Batched entity retirement** (batch B, step 2), based on commit `6154292`.
Access indexing and the concrete function future stay identical in both measured
variants; the request fixtures and their timing boundaries are unchanged.

- [x] Retire query-drain roots together and reuse the helper for dropped bound
  handles and single-entity takes.
- [x] Sweep memo and derived ownership per transitive frontier, deduplicate roots
  and removed owners, and skip empty cleanup batches.
- [x] Verify transitive cleanup, retained response snapshots, unrelated survivors,
  pending evaluation, duplicate/overlapping roots and repeated drains.
- [x] Complete build, workspace tests, optimized smoke tests and playground checks.
- [x] Compare independent/contended purchases and existing compiler workloads.
  Reuse the previous concrete-future measurements as baseline where available;
  only new cases need fresh baseline runs. Label the between-session comparison
  and preserve raw variation.

Retirement validation: normal workspace build, 145 unit tests plus the doctest,
both benchmark smoke targets and playground pass. Clippy completes with existing
warnings and none in the new cleanup/test code. An initial test compile failure
from unqualified private helper names was corrected before these successful runs.

Short comparison, median milliseconds per 1,000-request batch:

| Case | Before | Batched | Baseline source |
| --- | ---: | ---: | --- |
| Independent, 1 worker | 135.6 | 106.7 | Reused concrete-future runs |
| Independent, 8 workers | 163.6 | 129.1 | Reused concrete-future runs |
| Four per owner, 1 worker | 387.2 | 311.1 | Fresh alternating runs |
| Four per owner, 8 workers | 428.7 | 270.7 | Fresh alternating runs |

The clearest fresh case was contended/eight-worker: before 411–446 ms, after
265–276 ms. Other cases varied more, so these are coarse signals rather than
precise speedups. Cold/incremental compiler cases stayed close. Single-request
results looked worse against old baselines; a fresh check still varied sharply
(one-worker before 63–95 us, after 63–95 us; eight-worker before 47–114 us, after
116–121 us). Small-case performance remains unresolved, not dismissed as noise
and not used to select a special-case threshold.

[Raw estimates and baseline provenance](benchmarks/2026-10-21-batched-retirement.json)
are retained alongside local logs in `target/batched-retirement/`.

The isolated [purchase comparison](../experiments/purchase-comparison/README.md)
adds a Bevy ECS reference using the same observable request/state contract and
explicit engine differences. It does not change main-workspace dependencies.
Initial 1,000-request/one-worker results are roughly 12k requests/s for Porridge
versus 2.1M for the Bevy adapter. Treat this as a workload-specific reference:
Bevy batches ordinary systems, while Porridge maintains per-invocation incremental
bookkeeping. The large gap supports investigating execution granularity and
locality, not attributing it all to one dispatch layer. Exact ratios need a quiet
host; see the scratchpad's per-run results and feature differences.
Batch D is now brought forward to test task amortization first; batch C remains
planned rather than silently dropped.
The broader [schema/partition direction](ideas/schema-execution-and-partitions.md)
remains exploratory; worker ownership and clustering need separate contracts.

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

## Benchmark environment policy

This workstation runs other applications. It remains useful for correctness,
smoke tests, profiling hypotheses and detecting large algorithmic effects.
Repeated large changes across workload sizes can justify pursuing an optimization;
single-run percentages and small changes are not reliable tuning evidence here.
Criterion confidence intervals within a process do not capture all variation
between processes, CPU placement or concurrent application load.

Before choosing small-set crossovers, dispatch grains, hashers or other micro
optimizations, use a quiet dedicated machine. A dedicated-vCPU host is another
option to assess; a shared VPS is not automatically less noisy than a workstation.
No host/provider is selected and no infrastructure has been provisioned.

Controlled comparison checklist:

- [ ] Pin toolchain, dependency lockfile, profile and features; archive exact
  baseline/candidate source and executable hashes.
- [ ] Record CPU model/topology, effective CPU allocation and runtime worker count.
  Include the evaluator driver when allocating cores; distinguish physical cores
  from SMT threads. Record virtualization and CPU contention where observable.
- [ ] Keep competing jobs off the measured CPUs, record frequency/power settings,
  warm up, and alternate candidates in fresh processes across repeated runs.
- [ ] Preserve workload size and history, including setup/timing boundaries.
  Measure throughput and latency, and investigate small-case regressions.
- [ ] Retain raw samples and between-process ranges; repeat on another session
  before selecting a threshold from a small percentage difference.

### Reservation-index experiment status

The index-only implementation passes 142 workspace unit tests plus the component
doctest, both benchmark smoke targets, normal workspace build and the playground.
Clippy completes with existing workspace warnings and none in the new module.
Three new tests compare indexed conflicts/releases against the original rule,
including whole-store access, duplicates, multiple readers and identity reuse.

The local comparison covered 1/10/100/1,000 requests, 1/4/8 workers, independent and
four-per-owner contention, heavier 1,000-request kernels, and five existing engine
cases. Two fresh-process runs per variant alternated before/after order. At 1,000
requests, point-estimate ratios suggest a substantial gain (roughly 1.35–1.75x
across the measured cases), consistent with removing the broad conflict scan.
These are exploratory results, not precise or guaranteed speedups. Tiny-case
process estimates varied widely, and some 10-request cases became slower.

A provisional scan-to-index crossover at 32 active invocations was explored in
source but not retained: the local results do not establish a crossover threshold.
The retained implementation indexes all reservations; small-case index maintenance
cost is an explicit follow-up, not a resolved regression claim. Do not combine
these results with retirement batching or claim universal improvement.

Baseline is commit `b8889c2`; candidate source and executable hashes, per-process
commands and confidence intervals are retained in the local ignored directory
`target/access-reservations/`. This directory also retains the deferred crossover
sketch. No further timed tuning was run after the environment discussion.

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

Implemented: `dispatch.rs`, narrow `bowl.rs` integration and a runtime maximum
for benchmarking. The cooperative path uses a flat queue; the original port did
not. See the current-slice measurements above.
Do not batch dependency identities, publication or domain transactions together.
Each completed row must publish without waiting for a pending sibling. Reader
cancellation, writer draining, panic propagation, bounded completion channels and
execution without Tokio need contracts. Initial measurements cover maxima 1/16 with 1/8 workers, light/heavy work,
contention and small populations. Broader 4/64 and four-worker tuning remains open;
16 is provisional rather than an established optimum.

### D1. Reduce per-invocation type erasure

Audit dispatch boundaries independently of task batching. Registered systems
remain heterogeneous (`Arc<dyn Runnable>`), but `FunctionSystem<F, Marker>` knows
the callee type. Its `SystemParamFunction::run` need not erase that future too.

- [x] Return a concrete `impl Future<Output = ()> + Send` from the internal generic
  function adapter. Preserve borrowed parameter/guard lifetimes and existing APIs.
- [x] Validate ordinary and hook-driven execution, async suspension, cancellation,
  preemption and both cooperative/worker dispatch through existing contracts.
- [ ] Evaluate a concrete enum or equivalent for the dispatch wrapper rather than
  boxing solely to unify spawned and cooperative execution.
- [x] Retain concrete ordinary row futures in per-system plans and worker batches;
  keep per-invocation access, memo and publication semantics. Hook fallbacks and
  the cooperative queue retain the erasure needed at their boundaries.
- [ ] Measure allocations and small-kernel cost on a quiet host before claiming
  a precise speedup. Watch code size and build time from more monomorphization.

The inner function-future box has been removed as a small independent change,
ahead of the larger dispatch-batching work. `SystemParamFunction::run` returns a
concrete future; the generated adapter calls the async function directly. The
outer planned invocation and dispatch wrapper remain boxed. No public API or
suspension/guard-lifetime policy changed. Reservation-index timings above predate
this change and must not be used to claim its performance effect.
Validation: `cargo build --workspace -j 20`, `cargo test --workspace -j 20`
(142 unit tests and the doctest), and `cargo bench -p benches -j 20 -- --test`
all pass. A subsequent short exploratory comparison is recorded below; it is
not a controlled micro-optimization acceptance run.

The outer planned invocation still needs a common representation for the registry.
A stored function pointer is also an indirect call; replacing trait objects with
function pointers alone is not elimination of dynamic dispatch. This work does
not require distributed scheduling or a new synchronous public API first.

#### Exploratory inner-future comparison (2026-10-21)

Eight cases, two fresh-process runs per variant in before/after/after/before
order, using preserved executables with identical default features and benchmark
fixtures. Both variants include indexed reservations; this isolates the inner
future change. No rebuild was needed. Warmup 0.1 seconds, requested measurement
0.4 seconds, 10 samples; larger batches required longer to collect those samples.

| Case | Boxed, median batch time | Concrete future, median batch time |
| --- | ---: | ---: |
| 1 request, 1 worker | 66.1 us | 75.3 us |
| 1 request, 8 workers | 71.9 us | 46.3 us |
| 100 requests, 1 worker | 5.08 ms | 5.63 ms |
| 100 requests, 8 workers | 5.76 ms | 5.31 ms |
| 1,000 requests, 1 worker | 150.1 ms | 135.6 ms |
| 1,000 requests, 8 workers | 178.9 ms | 163.6 ms |
| Cold settle, 128 inputs | 615.3 us | 618.6 us |
| Incremental settle, 128 inputs | 78.2 us | 75.5 us |

These are medians of only two per-process Criterion point estimates. The
1,000-request/one-worker case improved in both runs: boxed 147.8–152.4 ms versus
concrete 133.7–137.6 ms, a promising coarse signal. Eight-worker boxed runs spanned
156.9–201.0 ms, so its median improvement is less convincing. Tiny runs varied
widely (individual one-request estimates ranged about 42–97 us); the small-case
results do not establish either a general gain or a reliable regression.
Compiler cases were close overall. No universal speedup is claimed.

[Hashes, commands and per-process estimates](benchmarks/2026-10-21-unboxed-future.json)
are archived; raw logs and Criterion output are under `target/unboxed-future/`.

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
- [x] Suspended async kernels with fast siblings: prove dispatch batching does not
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
