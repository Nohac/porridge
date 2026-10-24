# Preserve incremental work across write-log compaction

Status: retained after validation and paired measurements, 2026-10-24.
Baseline: `2632c93`.

The world-scaling benchmark found 32,000 → 212,000 candidates with the same
1,000 device changes when the device population grew 10,000 → 100,000. Kernel
work stayed at 7,000 invocations. Clearing the write log above 4,096 entries
invalidates every system cursor and routinely forces full-population planning.

## Scoped implementation

Keep the existing global append log and size threshold. At the existing safe
boundary (start of evaluation, under state lock), retire the old log and preserve
its unread relevant entity IDs for each delta-eligible system with a valid cursor.
Merge/deduplicate them into its existing forced-dirty queue, advance its cursor to
the new empty log, and retain its watermark. Consumers whose watermarks are
already current skip the tail scan: fingerprint-equal writes must not create
spurious work. The next plan merges these saved IDs
with new writes using the existing delta and join-provider translation path.

Fresh systems, opaque queries, and explicit cancellation/preemption resets still
use full planning. Startup systems are excluded: they run only at startup or a
restart, where full planning is already required. Retaining their never-consumed
change histories between restarts would otherwise grow with historical entities.

Any existing log handles retain their immutable contents; reclamation never
changes a borrowed log or requires waiting for it. Structural world snapshots
do not carry the write log. No raw pointers, new Cargo features,
new kernel API, or worker ownership changes. This is not per-component routing or
parallel planning. It moves log disposal from invalidating observations to
preserving the work those observations require.

## Validation

- Sparse changes above the log threshold stay sparse across repeated generations.
- Unread changes survive multiple compactions and merge with forced retry rows.
- Fully observed/irrelevant changes do not wake unrelated systems.
- Ordinary derivations, negative/optional membership and pair joins remain correct
  when inputs change/remove across compaction; join hints still expand member IDs.
- Explicit resets still full-plan. Existing preemption and cancellation tests pass.
- Avoid retaining dirty history for Startup consumers that never run again normally.
- Workspace build/tests, optimized benchmark smoke and playground.

After tests, delegate benchmarking to the existing benchmark agent. Compare the
frozen pre-change world_scaling executable with the same workload/features:
baseline, static 100k, 10k/1k, 100k/1k, fully active10k, dormant100k and64idle
observers, workers1/8. Separate timings from counters. Expect sparse candidate
counts to depend on changed rows rather than population; do not assume COW/store
cost disappears. Measure allocation behavior on the existing warmed purchase
runner if throughput warrants retaining the change. No rustfmt during this slice.

## Measured result

[Full paired results, counters, allocation screen and provenance](benchmarks/2026-10-24-dirty-log-compaction.json).
Raw artifacts: `target/dirty-log-compaction-20261024-152508/`. Exact world controls
are the frozen executables from the preceding population study and this build.
There were 84 timing processes and 28 separate diagnostic processes; every
process's final fixture verification passed.

The production engine now retains sparse planning across busy generations without
changing the 4,096-entry log threshold. Five regression tests plus the full
workspace suite pass (183 engine tests, seven fixture tests, nine playground
tests, macro/doctests). The optimized all-target workspace build and engine,
stateful and playground smoke checks pass. No Cargo features or dependencies
were changed; all final checks and comparisons use workspace feature unification.

At eight workers, comparing 10k and 100k devices with 1k updates each round:

| Counter per round | Before 10k | Before 100k | After either size |
| --- | ---: | ---: | ---: |
| Candidate rows | 32,000 | 212,000 | 14,000 |
| Memo hits | 25,000 | 205,000 | 7,000 |
| Dependency records | 80,000 | 260,000 | 62,000 |
| Kernel starts | 7,000 | 7,000 | 7,000 |
| Publication revision checks | 31,000 | 31,000 | 31,000 |
| Planning calls | 16 | 16 | 16 |

Stale publications remain zero. Quiet baseline/background cases retain their
10,000 candidate / 5,000 kernel counts. This establishes the work-count fix;
it does not establish constant latency regardless of population.

The same frozen-binary paired comparison uses three process repetitions per
configuration, five warmup and ten measured rounds, 1,000 purchases per round,
async kernels and one/eight workers. The large sparse case improves on eight
workers in all three pairs: 7.51→10.28k, 7.06→9.81k and 8.00→10.99k purchases/s.
Median throughput rises about 37%; paired changes are +36.96%, +39.05% and +37.39%.
**One worker regresses in all three pairs**:
7.67→7.25k, 6.51→6.21k and7.76→6.24k; medians fall about 19%. Paired declines are 5.44%, 4.65% and 19.58%; the
ratio of the two process medians is not the median paired effect.

Retain this as an incremental-planning and multi-worker scaling improvement,
consistent with the user's stated willingness to accept a single-worker tradeoff.
The one-worker regression remains an explicit follow-up, not dismissed as noise.
Small quiet-workload differences are mixed; fully active10k at eight workers
shows a small consistent regression (~1.8% in medians). Dormant100k accounts
remain highly variable and slow. This is not a universal throughput improvement.

## Remaining work

1. Attribute the single-worker sparse-world regression. Faster planning could
   change completion grouping/publication timing, but that is a hypothesis.
   Compare actual publication structure and COW copying, not just candidate counts.
2. Measure entry/index/presence copying for large written stores; this slice does
   not change storage or snapshot publication.
3. Future schema-based routing could avoid scanning each interested log tail at
   compaction. First measure that cost; this slice deliberately reuses the existing
   dirty queues and join translation rather than adding another planner.
4. The threshold bounds log retention at generation boundaries, not peak burst
   size. Pending per-consumer queues contain deduplicated unread entities and are
   consumed by planning; they are not fixed-size queues. Startup consumers do not
   accumulate histories between restarts.
5. Existing package-only `cargo test -p bowl` relies on workspace activation of
   Tokio's multi-thread runtime in some pre-existing tests. This slice used
   `cargo test --workspace`; standalone test dependency cleanup is separate.

## Allocation screen and publication follow-up

The warmed purchase allocation-only screen found no material event-count
regression. At eight workers, nominal allocations/iteration were 40,106→40,033;
reallocations rose roughly 30–40/iteration. One-worker counts overlap broad
scheduling ranges; no clean allocation win is claimed. This screen's older frozen
control has matching engine, steady-runner and steady-fixture sources, but predates
the purchase builder helper extraction (setup only). That caveat is retained and
its timing is not used for a performance comparison. Main world timings use the
exact prior frozen world executable.

In separate one-worker 100k/1k diagnostics, publication calls stayed at 7,005,
accepted output groups at 7,000, buffered commands at 4,000, revision checks at 31,000
and reconciled writes at 4,000 per round. Planning calls were 29.0→29.33 and dispatch
batches 441→444.33; conflict deferrals/stale outputs remained zero. These counts
show no major extra executed work explaining the one-worker timing regression.
Instrumented publication elapsed time rose 31.40→55.81ms/round while admission
was 3.75→3.62ms. This is a separate diagnostic observation, not exact attribution
of the ordinary run's wall-time change or proof that COW caused it. Profiling
publication and counting copied entries/bytes is the next selected investigation.
