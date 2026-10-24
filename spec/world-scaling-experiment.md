# Persistent world scaling experiment

Status: fixture implemented and measured, 2026-10-24. Baseline engine: `bdcfd7f`.

Question: does a fixed active request population keep its throughput as unrelated
state and concurrent incremental activity grow? A small purchase fixture alone
cannot answer this. No engine changes are planned for this measurement slice.

## Fixture

Reuse the ordinary purchase systems, persistent accounts and request slots. Add
a fleet of devices with identity, condition and load inputs, derived assessment,
and derived maintenance decisions. Inputs change over successive iterations;
all facts and memos persist. This is generic monitoring/workflow data.

- Keep 1,000 purchases per round fixed while growing device count.
- Compare unchanged devices with rotating 1%, 10% and 100% update fractions.
- Grow dormant accounts sharing purchase component stores independently.
- Grow disjoint registered component types and observers independently, including
  an empty-schema control and populated but unchanged stores.
- Compare one and eight workers; use the same binary/features and kernels.

Report purchase throughput, device input updates/s, total input updates/s,
settlement wall time and diagnostic planning/invocation counters separately.
Device derivations are internal work, not additional incoming requests. Setup,
warmup, correctness checks and printing stay outside timed intervals. Do not
compare mixed-workload purchase throughput directly to a purchase-only capacity
claim. Measurements on the shared workstation are directional.

## Verification and scope

Check evolving purchase balances and inventory, derived device assessments and
maintenance decisions after settlement. Include a small multi-round fixture test
with a sparse rotating update set and dormant accounts. Preserve ordinary engine
paths, indexing, memoization, snapshots and publication. No special scheduler or
benchmark-only evaluation shortcut. Large-world first-settle/setup costs are
reported separately, not silently included in warmed request throughput.

## Follow-up hypothesis: independent pipeline affinity

If purchase and monitoring graphs have disjoint reads, writes and ownership,
preparing/executing larger pipeline partitions on preferred workers may reduce
handoffs and improve locality. This differs from splitting one metadata loop
into tasks, gathering results and redispatching kernels. Schema dependencies can
help find partitions, but joins, ambient views, command targets, relationship
maintenance and removals must be included. Affinity alone provides no isolation
or publication guarantee. Measure this fixture before changing scheduling.

## Separate follow-up: continuous ingress and persistent producers

This fixture is closed-loop: submit a bounded batch, wait for settlement, repeat.
It measures service cost under persistent state, not queueing under sustained
arrival rates. A later ingress benchmark should independently control arrival
rate and count completions, backlog and response latency.

Ordinary async kernel commands publish at invocation completion. A never-ending
socket-read kernel therefore cannot publish each buffered batch today, and it
prevents global settlement while retaining invocation state/reservations. The
current compatible shape is a persistent I/O task submitting finite request
facts, with finite derivation kernels. Idle socket futures wake on I/O rather
than busy polling. A future producer/stream API needs explicit per-batch commit,
dependency validation, guard release, cancellation and backpressure semantics;
cloning Commands alone does not provide that lifecycle.

## Running the fixture

Build with the usual feature set:

```sh
cargo build -p benches --example world_scaling --profile bench -j 20
env WORLD_ACTIVE=1000 WORLD_DEVICES=10000 WORLD_UPDATES=1000 WORLD_WORKERS=8 target/release/examples/world_scaling
```

`WORLD_DORMANT` adds inactive accounts to the same stores as active purchases.
`WORLD_TYPES` selects up to the 64 separately instantiated background component
types in this fixture; `WORLD_ROWS_PER_TYPE` populates each, and
`WORLD_OBSERVERS=1` registers one read-only observer per selected type.
`WORLD_KERNELS=async|sync` selects both purchase and monitoring kernel modes
(background observers are synchronous in both). Numeric population counts are
not capped. Schema type count is finite because these are distinct Rust types,
not a run-time safety limit on entities.

`WORLD_WARMUP` and `WORLD_ROUNDS` default to five and ten.
`WORLD_METRICS=1` enables diagnostic counters; use a separate process from timing.
`WORLD_READY` optionally writes the PID and waits for `go` on stdin so a harness
can pin threads before setup. The runner reports total time and its submission
and settlement/reply-materialization portions. Validation runs after warmup and
after measurement, not during samples. Update sets rotate across the population
and both condition fields evolve on revisits. It models independent derivation
chains, not spatial interactions, fanout, persistence or network I/O.

For ingress, prefer explicit immutable `InboundBatch { client, sequence, packets }`
facts initially. Socket readiness wakes a connection task; it drains currently
available packets up to a limit, inserts a batch, and notifies a shared evaluator
to drive `stir()`. Batch insertion is the tracked change. Idle connections create
no processing invocations. A reused per-client inbox/revision could also work,
but needs a protocol preventing overwrites and lost wakeups when arrivals race
with processing/clearing. Registration and memoization alone do not reschedule a
kernel when an external socket changes.

## Results

Follow-up: [dirty-log compaction](dirty-log-compaction.md) removes routine epoch
rollover scans. The measurements below remain the original baseline.

Evidence: [machine-readable results](benchmarks/2026-10-24-world-scaling.json).
Raw logs, runner and frozen executable: `target/world-scaling-20261024-150128/`.
Same executable and features; async purchase/monitoring kernels, 1,000 active
purchases per round, five warmup and ten measured rounds, three interleaved
process repetitions per configuration, one/eight workers. Timing counters were
disabled; separate diagnostic runs measured work. Setup, verification and output
are excluded. Driver/worker affinity was applied before setup.

Median of three process throughputs (each process uses total purchases divided
by total measured round time), thousands of purchases/s:

| Persistent background | One worker | Eight workers |
| --- | ---: | ---: |
| None | 42.3 | 37.1 |
| 10,000 unchanged devices | 39.8 | 38.2 |
| 100,000 unchanged devices | 31.0 | 32.3 |
| 10,000 devices, 100 updates/round | 29.9 | 28.7 |
| 10,000 devices, 1,000 updates/round | 23.6 | 23.7 |
| 100,000 devices, 1,000 updates/round | 7.7 | 7.4 |
| 10,000 devices, all updated/round | 6.9 | 8.6 |
| 100,000 devices, 10,000 updates/round | 4.4 | 4.6 |
| 64 extra empty component types | 42.4 | 38.6 |
| 64 extra types, 1,000 unchanged entities/type | 38.9 | 38.1 |
| Same 64 populated types, with observers | 35.7 | 36.8 |
| 10,000 dormant accounts in purchase stores | 18.1 | 26.7 |

The 100,000-dormant-account case was especially slow and variable: one-worker
processes delivered 1.81–2.38k purchases/s; eight-worker processes 2.78–11.50k/s
(medians 1.91k and 3.80k). Report that spread, not the fastest observation.
Shared-workstation variation remains material throughout; small differences are
not reliable wins. These figures are not a direct timing comparison with a
historical binary under different load.

The fully active 10,000-device case adds ten device inputs per purchase: its
aggregate input throughput is 75–76k/s on one worker and 94–96k/s on eight.
Those are purchase+condition-update bundles, not equal-cost requests, packets or
kernel executions. Additional workers help this larger active workload, but do
not maintain a fixed purchase rate as background work grows.

### Busy rounds lose incremental planning history

At eight workers, 10,000 versus 100,000 devices with the same 1,000 updates/round:

| Counter per round | 10k devices | 100k devices |
| --- | ---: | ---: |
| Kernel starts/polls | 7,000 | 7,000 |
| Candidate rows | 32,000 | 212,000 |
| Memo hits | 25,000 | 205,000 |
| Dependency records | 80,000 | 260,000 |
| Revision checks at publication | 31,000 | 31,000 |
| Dispatch batches | 441 | 441 |
| Planning calls | 16 | 16 |

Both recorded zero stale publications and zero already-running deferrals.
The extra 180,000 candidates are consistent with two additional full-population
scans over the 90,000 extra devices, not extra useful kernel work.

`World::maybe_roll_plan_epoch` clears the write log above 4,096 entries and
increments its epoch (`crates/bowl/src/world.rs`). `Bowl::start_evaluation` calls
it before applying new input. When the system cursor's epoch no longer matches,
`run_phase_streaming` deliberately passes no delta hint and full-plans once
(`crates/bowl/src/bowl.rs`). These busy rounds routinely exceed the budget.
Memoization avoids executing unchanged rows but still pays for observing them.

This is a concrete mechanism to address before repeating isolated worker
preparation offload: keep bounded incremental dirty information across busy
rounds rather than using population scans as routine log reclamation. Tests must
cover cursor compaction, inactive consumers, membership/removal, joins, gates,
preemption and abandoned evaluators. Simply raising the fixed log threshold is
not a durable solution and can increase log/COW retention costs.

### A second scaling cost remains

Adding dormant accounts greatly increases latency with comparatively little
extra candidate work. At eight workers, the diagnostic 100k-dormant case observed
about 10.9k candidates versus 10k baseline, with the same 29k publication revision
checks. This does not explain the large throughput loss by row count alone.

Component entry maps and fingerprint indexes are shared with snapshots and use
`Arc::make_mut` at writes. Presence bitsets also use COW, currently even before
checking whether a bit is already set. Large stores and global metadata may
therefore impose costs beyond dirty-row counts. These are source-backed targets
for profiling, not measured attribution of the entire slowdown. Count copied
entries/bytes and correlate them with publication waves in a separate experiment.

Unchanged disjoint observers started no extra kernels in measured rounds.
Schema width and unrelated unchanged state were much less damaging than large
written stores. Independent pipeline affinity remains a hypothesis, not a
feature or an explanation for all the current scaling loss.

## Validation and limits

Workspace tests passed: 178 engine tests, seven fixture tests, nine playground
tests, macro tests and doctests. The optimized all-target workspace build passed;
engine/stateful benchmark smoke and playground passed. Profiling tool tests passed.
The engine source is unchanged from the retained baseline; only benchmark fixture
composition was factored to reuse the same purchase systems.

The initial five warmup plus ten measured rounds change assessments but leave
the maintenance flag false: this measures stable decision cutoff, not frequent
maintenance transitions. Devices do not interact or join across owners. There
is no open-loop arrival rate, network, database, spatial fanout or fairness policy.
These results identify current scaling limits; they do not establish server
capacity or comparative performance against another engine.
