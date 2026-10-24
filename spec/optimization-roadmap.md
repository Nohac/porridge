# Allocation and execution optimization roadmap

Current source audit and prior-art mapping: [scheduler investigation](scheduler-investigation.md). This is a proposed experiment order, not a restart of the paused campaign.

Status: broader campaign paused; scoped follow-ups recorded through 2026-10-24.
Original baseline: `35247bc`. Earlier reports remain historical evidence; their
unchecked items are not all still unimplemented. Earlier campaign commit authority
does not authorize restarting the entire roadmap. Current changes are being
captured and committed at the user's request before choosing the next slice.

[Typed-return kernels](typed-return-kernels.md) is a new proposal, not an implemented
capability. The scoped [per-system profiling experiment](system-profiling-overhead.md) is
complete: a runtime control is retained, but measured gains do not explain worker
scaling. The selected [compact execution-record slice](compact-execution-records.md) is
implemented and benchmarked: phase-local IDs and dense reservation slots are
retained. Persistent memo/output IDs and wider routing changes remain unimplemented.
After commit `bdcfd7f`, [worker-planning experiments](worker-planning-experiment.md)
tested fine and coarse candidate-preparation chunks. Both failed the initial
throughput screen; the tested prototype is archived and production retains serial
preparation. [Persistent-world scaling](world-scaling-experiment.md) is the next
scoped measurement: fixed purchase traffic alongside larger and busier state.
A separate spawn-bookkeeping control and distributed publication remain unimplemented.

## Objective and completion criteria

Make bounded, warmed execution approach zero global allocations per **complete
iteration**, while improving throughput and latency across compiler and stateful
workloads. Preserve the schema → facts → evaluation authoring model.

An iteration means:

```text
submit a fixed batch to an existing Bowl
  → admit and execute all required work
  → publish and settle
  → consume the requested results
```

A kernel poll, planning wave, request, batch, and settled iteration are different
units. Report all relevant denominators. Dividing a growing allocation count by
more requests is not success.

- Target zero engine allocation/reallocation calls in bounded, warmed core
  execution when kernels themselves do not allocate.
- Target zero, or an explicitly itemized small fixed residual, in the complete
  public-API iteration. Establish numerical budgets per fixture in O0; report any
  residual separately, and do not excuse per-request allocations as fixed cost.
- Growth may allocate. Repeating the new high-water workload must plateau again.
  Retained memory must not grow with iteration count at constant live workload.
- Churn, retained snapshots, arbitrary payload growth, caller-created results,
  and real I/O have separate budgets. A zero-allocation fast path must not break
  them or hide their cost.
- Lower allocations are not enough if CPU cost, tail latency, retained memory, or
  fairness materially worsen. Fully utilizing every worker is not a requirement
  for tiny work that is faster inline.
- This is a finite sequence of measured experiments, not a promise that every
  proposal will win or that one architecture is optimal for every workload.

Baseline from [planning storage](planning-storage.md), 1,000 fixed request slots,
eight workers: **156.815 alloc + 2.124 realloc calls/request**, approximately
**156,815 alloc + 2,124 realloc calls/iteration**, and 38.16 ms/iteration.
Timing is directional on the active workstation.

The corresponding separate DHAT profile attributes about 49.386 calls/request
to remaining planning, 22 to join candidates, 5 to command producer handles,
and 82.552 to other pipeline work. These counts include reallocations and are
not CPU-time percentages. Reprofile after each material shift.

## What is already done

| Area | Current implementation | Remaining work |
| --- | --- | --- |
| Component lookup | Schema slots, presence masks, prepared ordinary stores | Static query layouts, broader prepared projections, access IDs |
| Incremental planning | Watermark gating, write-log hints, forced-row retries, pair-driven joins | Direct consumer routing and per-system candidate queues |
| Publication | Derived-output diffing, ownership index, stable derived spawn identities, batched retirement | Allocation reuse and narrower reconciliation/cleanup |
| Execution | Concrete ordinary kernel futures and typed worker batches | Sync kernels, reusable queues/tasks, hook/cooperative erasure |
| Conflicts | Component/entity reservation index | Reuse containers, tune tiny cases, distribute only with evidence |
| Memory | Guard token Vecs, recycled command arenas, reusable planning scratch | Owned rows/memos, producer handles, runtime queues, snapshots/cells |
| Ambient work | No-view healing disabled | Narrow healing when viewers actually exist |
| Snapshots | Cached snapshot reuse when world key is unchanged | Copy-on-write structures and changed-wave construction costs |
| Waiting | Generation-targeted wakeups and boundary waiters exist | Audit remaining retries, take-pin waits, and waiter allocations |

Source audit matters: older TODO text still calls typed batches, some notification
work, snapshots, relationships, and join improvements wholly pending. Do not
reimplement them from an old description. Presence matching still scans historical
bitmap rows on the typed-facet path. `BoundEntity::take` still yields/retries when
cells are pinned. `World::insert` still allocates a replacement component cell.

## Execution order

```text
O0 measurement/budgets
 → O1 candidates and query plans
 → O2 invocation/guard/memo storage
 → O3 producer and publication storage
 → O4 snapshots and component replacement
 → O5 direct dirty routing
 → O6 synchronous kernels
 → O7 reusable dispatch and batching
 → O8 wait/wakeup and idle costs
 → O9 optional storage/SIMD/revision experiments
 → O10 local distributed scheduling
 → O11 final budget and regression campaign
```

Reorder independent experiments when profiles justify it; record why. O6 may be
prototyped after O2/O3 if execution allocations dominate. O10 is conditional on
measured central-driver saturation, not a prerequisite for the allocation target.
Network clustering is a separate research track.

Each item is complete only when marked **kept**, **rejected with evidence**, or
**deferred with a concrete prerequisite** in the experiment ledger below.

## O0 — Make the target measurable

Files: `crates/benches/src/stateful/steady.rs`, examples/support,
`tools/measure-allocations`, `tools/profile-allocations`, `tools/profile-engine`.

- [x] Add explicit complete-iteration allocation/reallocation/free totals, not
  only normalized per-request values. Keep full input/evaluation/result cost.
- [x] Separate a drive-only measurement from result materialization. Prototype
  a public `stir().await` only if useful: it must use the same real settlement
  path, not a benchmark-only engine path. A blocking SyncBowl wrapper is separate.
- [x] Track high-water retained capacities, peak/live bytes, and growth after a
  deliberately larger batch. Existing requested-byte counters measure traffic,
  not memory residency.
- [x] Extend attribution into the remaining “other pipeline” category. Distinguish
  planning, guards, producer handles, dispatch, publication, snapshots, storage,
  cleanup, waiters, and results. Keep diagnostics off during timing.
- [x] Add repeatable allocation-budget checks for known bounded scenarios.
  Warmup must be finite and recorded; do not warm until a noisy pass happens.
- [x] Refresh operation accounting: candidates, admitted/executed rows, memo
  hits, retries, polls, ready batches, publications, and useful requests.
- [x] Audit known semantic gaps that intersect the work: ambient mutable views,
  external Cow epoch boundaries, and join collision handling. Fix or explicitly
  constrain affected experimental paths before optimizing them.

Exit: reproducible baselines, named fixture budgets, a residual-allocation ledger,
and a timing-independent way to detect regression. No new Cargo feature split.

## O1 — Avoid materializing query and join intermediates

Files: `query.rs`, `prepared.rs`, `system.rs`, `world.rs`, `world/stores.rs`,
and parameter macro generation.

- [ ] Let equality joins visit or borrow snapshot-owned fingerprint buckets,
  rather than allocate a candidate Vec per provider. Apply the equivalent
  destination/visitor approach to relationship members where their API permits.
- [ ] Stream pair/product expansion into the planner or reuse typed row storage;
  avoid building several full intermediate row vectors.
- [ ] Precompute static bound-key descriptors, query masks, component slots and
  access modes once. Resolve snapshot-specific store handles per snapshot.
- [ ] Cache positive/negative filter masks for With/Without; retain fallback
  matching for predicates that cannot be expressed as masks.
- [ ] Use live store candidates plus mask checks for historical typed-facet scans.
  Compare this against live bitmap/index alternatives before changing storage.
- [ ] Extend prepared/destination paths to useful complex parameters and derived
  bundles; avoid optimizing unused variants without evidence.

Tests: empty/broad buckets, changed keys, removed providers/members, optional and
outer joins, relationships, compound filters, custom parameter fallback, schema
layout differences. Maintain deterministic pair identity and existing ordering.
A borrowed index bucket must not escape its snapshot or force a global read lock.

Benchmarks: independent/contended purchases, Eq/In joins, wide fanout, sparse
changes, and equal live population with increasing retired history.

Exit: reduced candidate/planner allocations without increasing work to all pairs;
owned surviving invocation state remains correct. First implementation slice:
borrow/visit equality candidates, then measure before broader join redesign.

## O2 — Reuse owned invocation, guard, and memo storage

Files: `planning.rs`, `system.rs`, `access.rs`, `query/guards.rs`, `world.rs`.

- [ ] Inventory every clone/Vec between planning, reservations, worker frames,
  SystemRun, publication, and memo storage.
- [ ] Prototype owned reusable row records or batch slabs with typed ranges.
  Keep memo data in a separate lifetime class from short-lived execution data.
- [x] Reuse ordinary invocation guard capacity after fetched borrows are dead;
  [kept with measured memory tradeoff](guard-buffer-reuse.md). External/hook paths
  retain independent storage.
- [ ] Reuse output, memo-update and written-row buffers; specialize ordinary
  one-row output without requiring one-element Vecs everywhere.
- [ ] Reduce repeated invocation-key/access copies. Compare shared immutable
  descriptors, compact/interned invocation identities, and pooled owned buffers.
  Count reference operations and lookup costs as well as allocations.
- [ ] Retain phase-level running/read-owner/reservation containers between uses.
  Clear logical contents without discarding capacity.
- [ ] Update memo entries in place where ownership allows; avoid rebuilding a
  full dependency vector for unchanged records.

Ownership sketch:

```text
planning scratch: reset after plan
execution lease: release after reservation + future + publication are done
memo record: retain across settles, recycle only after removal/replacement
snapshot/result guards: release only when the final reader is finished
```

Tests: overlap, delayed completion, cancellation, panic, stale rejection,
preemption, custom guards, and a retained old result during subsequent iterations.
Use generational handles where pooled slots can be reused. No aliasing shortcuts.

Exit: metadata allocations plateau with maximum simultaneous work, not total
requests processed. Avoid keeping an entire batch alive for one long-lived memo.
Previous inline-eight-guard experiment increased future size; do not repeat it
without a new storage/ownership reason.

## O3 — Finish command and publication reuse

Files: `commands.rs`, `commands/arena.rs`, `bowl.rs::commit_system_run`,
`world.rs` ownership/retirement paths.

- [ ] Avoid unconditional producer Arc/Mutex creation for parameters that cannot
  emit commands. Preserve custom-parameter fallback.
- [ ] Extend pool coverage to hooks, remove/despawn inputs, and concurrent staging.
- [ ] Reuse completion-drain, deferred-output, reconciliation and retirement
  scratch. Preserve owner boundaries while draining ready completions.
- [ ] Avoid placeholder-channel allocation when extracting satisfied waiters.
- [ ] Reuse returned-buffer queue storage if profiling identifies channel blocks
  as a residual cost. Prefer simple ownership over a new shared allocator lock.
- [ ] Reduce repeated derived-owner hashing/cloning with stable internal handles;
  retire all references before reuse.
- [ ] Measure registration-time write/consumer metadata for cheaper publication,
  dirty logging, and revision reconciliation.

Producer recycling must not let an old cloned Commands handle write into a new
invocation. Detached writes after invocation completion do not automatically
publish. Keep that contract explicit.

Bump-scope remains the chosen command payload allocator. Revisit packed storage
only if a new profile makes its remaining overhead material; retain alignment,
drop, unwind and cross-thread tests, and use Miri for any new unsafe abstraction.

Exit: bounded command/publication bookkeeping no longer allocates per command
or per completed row, apart from separately identified retained storage changes.

## O4 — Reduce snapshot/COW and replacement-cell costs

Files: `world.rs`, `world/stores.rs`, `bowl.rs::snapshot_locked`,
component-cell and fingerprint-index code.

- [ ] Measure structural COW separately: entry maps, presence bits, fingerprint
  buckets, write logs, prepared contexts and schema slot arrays.
- [ ] Reuse existing snapshot caches and sealed registry storage more effectively;
  audit phase setup and unnecessary registry/context clones.
- [ ] Compare shared/chunked persistent structures or retained double buffers
  against whole-container COW. Include wide sparse schemas.
- [ ] Prototype reuse of a replacement component cell only when the old value is
  unobserved and ownership permits it. Preserve retained-reader behavior.
- [ ] Avoid unnecessary index rebuilding on equal writes; preserve equal-output
  revision cutoffs, ownership and destructor behavior.
- [ ] If maps/cells dominate, evaluate stable slot/chunk stores and recycling
  after the last snapshot/guard releases them.

A snapshot/guard may outlive any number of iterations. “Same entity” does not
authorize overwriting the cell an old reader owns. Pooling must preserve this.
Do not recycle externally visible entity IDs without a generation-safe design.

Exit: fixed-live-world updates stop reallocating broad structural metadata;
retained snapshots and growth remain correct and separately budgeted.

## O5 — Prepare the driver and route changes directly

Files: `declare.rs`, `bowl.rs`, `system.rs`, `world.rs`, presence/write-log code.

- [ ] Build component-slot → interested-system indexes from the sealed registry.
- [ ] Route value writes, membership transitions and removals to reusable
  per-system candidate queues. Deduplicate without losing a later write.
- [ ] Replace repeated per-system filtering of the shared write log where sound.
- [ ] Include provider and member-side join invalidation, untracked ordering
  changes, forced conflict/stale retries, epoch restart and absent stores.
- [ ] Add reusable ready-system bitmaps and precomputed potential conflicts;
  retain row-level conflict checks and revision validation.
- [ ] Narrow ambient healing to eligible current-epoch invocations and changed
  viewed stores. Do not disable healing for active consumers.
- [ ] Optimize no-op/empty phases and hooks based on actual work accounting.

Compare the routed planner to full enumeration in correctness tests. Schema
masks describe possible membership/access, not dynamic values or a static DAG.
Feedback cycles and convergence must keep their existing meaning.

Exit: sparse work scales primarily with affected systems/rows. A bitmap lookup
does not make total scheduling O(1). Keep opaque-parameter full-plan fallbacks.

## O6 — Add genuinely synchronous kernels alongside async

Files: system adapters/builder/macros, `dispatch/plan.rs`, executor and tests.

Supported registration:

```rust
builder.system(update_balances).system(fetch_remote_facts)
```

- [x] Infer non-async registration with shared parameter preparation,
  guards, access admission, revisions, output ownership and publication.
- [x] Execute ordinary synchronous worker rows directly inside a typed batch loop, without
  constructing/polling a future node for each row.
- [ ] Compare tiny inline execution, worker batches, immediate async kernels,
  and genuinely suspending async kernels.
- [x] Preserve mixed sync/async plans and independently available completions.
  Publish incrementally or in bounded ready chunks; no wait-for-all barrier
  behind an unrelated suspended sibling.
- [ ] Keep panic propagation, reader cancellation and writer-drain guarantees.
  Bound synchronous chunk work so fairness/preemption is not starved.
- [ ] Add timing-informed dispatch policy only after measuring a crossover.

Earlier profiles saw one poll per completed immediate invocation; repeated polling
was not the demonstrated bottleneck. This experiment targets future-set nodes,
adapters and completion machinery. Sync kernels can still finish out of order
on different workers. They must not perform blocking I/O on shared runtime workers.
A detached task holding Commands is not a substitute for tracked async completion.

Exit: a real direct sync path, identical observable semantics, mixed-mode tests,
and demonstrated benefit without removing async support.

## O7 — Reuse dispatch infrastructure and tune batching

Files: `dispatch.rs`, `dispatch/executor.rs`, `dispatch/plan.rs`, `access.rs`.

- [ ] Replace boxes used only to unify known executor variants with concrete
  enums where useful. Registry-level heterogeneity can retain type erasure.
- [ ] Reuse batch vectors, completion slots and stream/task bookkeeping.
- [ ] Compare reusable async task-node storage with persistent worker tasks and
  reusable work/completion queues. Keep runtime portability/cooperative fallback.
- [ ] Prototype per-system maximum grain and concurrency policies. Existing
  runtime maximum is global; this is not yet the full policy proposal.
- [ ] Tune grain using admitted work and observed kernel cost, not only candidate
  count. Include contention and skew; no hard-coded workload-specific threshold.
- [ ] Evaluate bounded completion draining/commit batching without conflating
  invocation publication with domain transaction atomicity.
- [ ] Only test minimum-batch waiting with an explicit latency/fairness contract.
  Never wait indefinitely to fill a batch during settlement.

Kernel batches may run rows sequentially within each worker while batches execute
in parallel. Async pending work must not consume a worker or block ready siblings.
Persistent workers need shutdown, panic, cancellation and wakeup protocols.

Exit: queue/task allocations plateau after warmup; small-work latency and heavy
parallel throughput both remain visible. Function pointers alone are still
indirect calls and are not automatically faster than trait objects.

## O8 — Remove waiting churn and fixed idle costs

Files: `bowl.rs` waiters, take/preemption/settlement paths, guarded cells.

- [ ] Audit notification work already landed before replacing anything.
- [ ] Replace remaining take-pin yield/retry loops with lost-wakeup-safe
  notifications or explicit pin-lifetime signaling if measurements justify it.
- [ ] Reuse waiter registrations and avoid repeatedly allocating on retry.
- [ ] Investigate remaining state-lock contention and synchronous drop spins.
  Split intake from world state only if profiling still points there.
- [ ] Make no-op settle/read costs explicit; avoid unnecessary snapshots, hooks,
  registry copies and diagnostics work.
- [ ] Consider reusable caller-owned result destinations/visitors to remove
  materialization allocations while retaining ordinary convenient APIs.

Tests: cancellation at registration, last-reader release races, abandoned driver,
many simultaneous waiters, deferred input, and no-runtime cooperative operation.
No busy-loop “optimization” that raises idle CPU.

Exit: idle workloads sleep, waiter memory follows active callers, and fixed
iteration overhead is measured independently of useful row work.

## O9 — Conditional storage, SIMD, revision and hashing experiments

These are opt-in experiments after the core profile, not required API rewrites.

| Experiment | What to try | Evidence/constraint |
| --- | --- | --- |
| Dense component columns | Stable chunks or column storage for selected schemas | Preserve guards, snapshots, removals and ownership; include sparse workloads |
| Batched query/kernel API | Typed chunks with amortized preparation and access | Separate invocation identity/deps/results where semantics require it |
| SIMD | Span/offset remapping, homogeneous arithmetic, bulk presence tests | Inspect generated assembly; include gather/scatter and branch costs |
| Automatic mutation revisions | Opt-in mutation tracking through MutRef/explicit writes | Distinguish actual mutation from mere declared access; keep equal-value convergence behavior |
| Revision fingerprints | Compare explicit revision, hash and ordinary tracked writes | Do not remove necessary equality cutoff or expect revisions to remove scheduler work |
| Specialized hashing | Dense component IDs, invocation IDs, bounded-key hashers | Benchmark; do not weaken untrusted-key resilience globally |
| Bucket-level dependencies | Finer outer-join/set invalidation | Validate membership/removal and collisions; memory overhead must pay off |
| Local state | Internal scratch first; public Local<T> only for demonstrated use | Explicit per-system/per-invocation lifecycle and parallel safety |
| Once-per-settle work | Explicit sweep marker/policy instead of incidental always_run | Preserve cleanup and phase semantics |

Vectorization depends on the operation and layout, not just “being ECS.”
Do not claim SIMD because a loop has contiguous inputs; check generated code.
Changing layout, revision policy and scheduler simultaneously would obscure results.

## O10 — Distribute scheduling locally, then assess ownership partitions

Prerequisites: O5 routing and O7 execution queues, plus a refreshed profile showing
the centralized driver remains limiting.

- [ ] Prototype atomic ready bitmaps and worker claims with reusable work queues.
  Separate a “ready” bit from actual row admission/reservation and publication.
- [ ] Prevent lost dirty writes while a worker clears/claims a bit: use a tested
  state machine/version, not just fetch-and-clear.
- [ ] Compare worker-local ready queues, affinity and stealing. Affinity is a
  placement preference, not a lock or ordering guarantee.
- [ ] Route by relationship root/explicit logical partition; avoid scattering
  related components randomly.
- [ ] Benchmark hot-group skew, disjoint operations, overlapping multi-owner
  operations, and increasing cross-partition interaction.
- [ ] Define global quiescence over dirty queues, running tasks, pending inputs,
  messages, unpublished completions and lifecycle work.
- [ ] Only then consider partition-owned planning, revisions and publication.
  Moving ready queues alone does not distribute those central costs.

Tests must address generation/input ordering, stale outputs, guards, owner
migration/fencing, and consistent observations. Measure coordination cost for
small workloads; keep a simple local path when it wins.

Network clustering is not part of the near-zero local target. Its follow-up needs
stable identities, serialization/schema compatibility, authoritative ownership,
deduplication, handoff recovery, cross-owner mutation and settlement semantics.
See [the partition/clustering proposal](ideas/schema-execution-and-partitions.md).

## O11 — Establish budgets and finish the campaign

- [ ] Re-run the full allocation/workload matrix at the retained implementation.
- [ ] Tighten stable allocation budgets and add regression checks, including
  complete-iteration totals and growth/plateau behavior.
- [ ] Refresh CPU/hardware profiles; distinguish changed operation counts from
  cheaper individual operations.
- [ ] Repeat small timing claims on a quiet controlled host when available.
- [ ] Update the Bevy comparison on identical fixture contracts; add contended,
  heavier and row-parallel references separately. Do not compare the current
  fixed-ID fixture to an old fresh-world Bevy number as an exact ratio.
- [ ] Publish supported zero/near-zero scenarios, residual allocation sources,
  retained-memory costs and deliberately unsupported cases.
- [ ] Run the final code-style/formatting and cleanup pass (rustfmt is skipped
  during optimization batches per the user’s 2026-10-23 instruction).
- [ ] Mark every experiment kept/rejected/deferred with evidence and clean up
  discarded prototypes. Update this plan and superseded roadmap claims.

Completion means measured budgets are met for the named supported scenarios,
correctness is retained, and remaining exceptions are explicit. A lower
allocation count alone does not complete planning/scheduling optimization.

## Benchmark matrix and measurement discipline

| Workload | Dimensions and purpose |
| --- | --- |
| Fixed-ID purchases | 1/10/100/1,000 slots; independent and contended owners |
| No-op/read/sparse/compiler | Cold vs warm, gated systems, untouched populations, wide schemas |
| Joins | Eq/In, changed provider/member, empty/broad buckets, outer/compound cases |
| Two-owner exchange | Disjoint and overlapping pairs; conservation and atomic apply |
| Movement/fanout | Membership changes, one input → many outputs; count inputs and deliveries |
| Mixed arrivals | Controlled arrivals/backlog, multiple request types and fairness |
| Sustained churn | Equal fixed work/history; retired population varies, live population fixed |
| Retained readers | Held snapshots/results over later updates; delayed recycling |
| Async behavior | Immediate, truly suspended, skewed and cancellation/preemption cases |
| Storage kernels | Span/offset updates and homogeneous arithmetic for actual SIMD tests |

Use 1/4/8/16 workers where available, plus cooperative/no-runtime execution using
the same compiled feature graph. Record driver threads separately from workers.
Sweep grains 1/4/16/64 and larger values where useful; these are experiments,
not user input limits. No artificial workload caps.

For each implementation batch:

After root tests pass, delegate benchmarking to the persistent gpt-6.1-sol
subagent with low reasoning effort (the tool exposes no separate fast switch).
The agent freezes binaries/source manifests and owns benchmark artifacts only;
root retains implementation/commit ownership and plans the next change. Coordinate
TESTS_GREEN / BENCH_RUNNING / BENCH_DONE so compilation never overlaps timed runs.
Do not rerun existing measurements whose exact candidate hashes already match.

1. Preserve the baseline binary/source manifest; change one mechanism.
2. Run meaningful contract tests, then relevant allocation/benchmark smokes.
3. Measure allocations, DHAT attribution, wall time, and hardware counters in
   separate runs. Assertions, formatting, fixture verification and profiler
   output stay outside the measured body.
4. Alternate baseline/candidate order across fresh processes. Keep features,
   toolchain, profile, workload/history and CPU placement fixed. Build with
   `-j 20`; do not compile while timing.
5. Record raw samples, process variation, requests/s, p50/p95/p99 where measured,
   per-iteration totals, useful work/retries and retained-memory growth.
6. Use perf cycles/instructions/cache/branch counters with coverage recorded.
   Sampling and stage elapsed timers are not additive exact per-function cycles.
   Use isolated instruction counting/assembly inspection for narrow hot paths.
7. Keep a change for a demonstrated cost reduction without unexplained important
   regressions. Recheck regressions; do not hide them with arbitrary thresholds.
8. Commit the implementation, tests, measured results and conclusion. Rejected
   experiments retain a short result note; the main path does not accumulate
   unused complexity.

Prioritize multi-worker throughput and scaling. Per the user's 2026-10-23
clarification, a modest single-worker slowdown is acceptable when multi-worker
benefits justify it. Keep single-worker measurements as a regression diagnostic;
they are not an automatic veto or a requirement that every configuration improve.
Record the tradeoff and distinguish measured improvements from overlapping noise.

Fast checks cover representative sizes each commit; the complete matrix runs at
major milestones. A shared VPS is not automatically quieter than this workstation.
Use an existing quiet host if available; provisioning infrastructure is outside
this implementation plan.

## Semantic constraints for every batch

- Preserve input epochs, phases, single-flight settlement and abandoned-driver recovery.
- Preserve per-row stale checks, derived-output replacement and ownership cleanup.
- Preserve hash/revision cutoffs, optional absence observations and relationship maintenance.
- Preserve streaming completion; unrelated slow work must not create a new barrier.
- Preserve conflict admission, guard lifetimes, reader cancellation and writer draining.
- Pool reuse must be safe under growth, cancellation, panic, delayed readers and shutdown.
- No serializer-crate roundtrip tests. Test engine contracts and observable behavior.
- No benchmark-only shortcut that bypasses the production evaluation path.
- Fix encountered correctness bugs in separate reviewable changes; document divergence.

## Source index and other feature ideas

| Repository source | Disposition in this campaign |
| --- | --- |
| [Steady-state allocations](steady-state-allocations.md), [planning storage](planning-storage.md), [command integration](bump-command-integration.md) | O0–O4 baseline and remaining lifetime classes |
| [Prepared driver](prepared-driver.md), [execution profiling](execution-profiling.md), [hardware profiling](hardware-profiling.md) | O1/O5/O6 priorities and measurement controls |
| [Stateful plan](stateful-performance-plan.md), [dispatch batching](dispatch-batching.md) | O2/O7/O11; retain existing batching and reservation improvements |
| [Original performance plan](performance-plan.md), [benchmark reports](bench-reports.md) | Historical evidence; many proposed fixes have landed |
| [TODO](../TODO.md) §§7–14 | Query/routing, mutation, waits, views, local state, scheduling and diagnostics |
| [Joins](joins.md) | O1/O5/O9; broader range, heterogeneous/scoped and named binding features need dedicated semantics/fixtures |
| [Execution cycles](ideas/execution-cycles.md), [streaming evaluation](streaming-evaluation.md), [epochs](epochs.md) | Preserve current streaming model; do not revive superseded barrier/forward-only semantics as a silent optimization |
| [Schema execution and partitions](ideas/schema-execution-and-partitions.md) | O5/O9/O10; network distribution deferred |
| [Replication](ideas/replication.md), [daemon/client](daemon-client.md) | Separate product track; removals/tombstones and subscriptions remain relevant future consumers |
| [Purchase comparison](../experiments/purchase-comparison/README.md) | O11 reference with explicit semantic and dependency differences |

Public SyncBowl, richer non-convergence diagnostics/introspection, macro robustness,
new query operators, daemon APIs, replication and persistence are not prerequisites
for reducing allocation. Address them when needed to validate a selected experiment
or in a separately scoped feature slice. Do not equate a synchronous kernel path
with a blocking public API.

## Experiment ledger

| ID | Status | Result/commit |
| --- | --- | --- |
| Baseline | Complete | `35247bc`; command + planning arena integration |
| O0 | Baseline complete | [Iteration budgets and lifetime growth](iteration-budgets.md); broaden fixtures/audits as new paths change |
| O1 | Measured slices kept | [O1a/O1b](join-candidate-storage.md): 156,815 → 100,857 alloc/iteration; cross-pass layouts deferred to shared O5 registry, remaining row work tracked |
| O2 | In progress | [Inline keys and ownership layout kept](invocation-key-storage.md): 55,857 alloc/iteration at [guard/access slice](guard-buffer-reuse.md); [LIFO dependency pool measured/deferred](dependency-buffer-reuse.md), output/memo work remains |
| O3 | In progress | [Unique producer reuse kept](producer-reuse.md): 50,857 alloc/iteration; [completion retention measured/deferred](completion-storage.md) |
| O4 | Planned | Snapshot/COW/cell reuse |
| O5 | Planned | Schema consumer routing and ready sets |
| O6 | Initial worker path validated | [Same-binary sync/async comparison](synchronous-kernel-experiment.md); small/noisy throughput change, lower byte traffic; fallback and policy work remain |
| O7 | In progress | [Readiness-preserving completion channel kept](worker-completion-channel.md): 45,227 alloc/iteration; [shared context kept](invocation-context.md), [inner batch future storage kept](batch-future-storage.md): 40,227 alloc/iteration |
| O8 | Planned | Wait/wakeup and idle overhead |
| O9 | Conditional | Layout/SIMD/revision experiments after profiling |
| O10 | Conditional | Worker-distributed scheduling after local bottlenecks |
| O11 | Planned | Final budgets, comparisons and documented limits |

Each result entry should link its hypothesis, exact change, baseline/candidate
provenance, tests, allocations/CPU/timing/memory outcomes, and keep/reject decision.

## Round checkpoint — 2026-10-24

Paused at the user's request to wrap up this round. The campaign is not complete.
See [the checkpoint](optimization-round-checkpoint.md) for retained/rejected
changes, current measurements and the next starting point. Formatting/style and
the broader final validation pass remain in O11; rustfmt stayed deferred during
this round as requested.

O6 follow-up (2026-10-24): the user requested and authorized the focused native
sync comparison after the round pause. That slice is implemented and measured;
other campaign work remains paused. The initial API supports mixed kernels, with
the worker fast path and compatibility limits documented above.
