# Steady-state allocations

Target: zero global allocator calls during bounded, warmed-up engine/runtime
work when the kernels do not allocate. This is **not a current guarantee**.
Growth, entity churn, and retained snapshots are separate workloads; warmup
alone cannot eliminate per-invocation allocations.

## Measurements

The fixed-ID purchase fixture keeps one request slot per account on one world.
Each round revises those slots, runs normal evaluation, and reads outcomes.
A separate settled-read case materializes outcomes without changing inputs.
Assertions, verification queries, formatting, setup, and destruction are outside
measurement. Read operations count returned components; purchase operations
count completed requests. No workload size ceiling is imposed.

The allocating control deliberately creates two 64-byte buffers per kernel,
with a suspension between them. Attribution scopes each poll and explicitly
excludes the Tokio yield call. The initial control caught occasional runtime
allocations inside that call; corrected control runs consistently report
**2 user allocations / 128 bytes per invocation** at one and eight workers.
This is controlled fixture attribution, not general call-stack classification.
Purchase kernels perform no user heap allocation; Commands metadata counts as
engine work. The remainder includes engine, runtime, and host query/input APIs.

Three alternating process repetitions, ten rounds each, 1,000 fixed slots;
allocation counters and wall time measured in separate processes:

| Case | Engine alloc calls/op, before → after | Wall µs/op, before → after |
| --- | ---: | ---: |
| Purchase, 1 worker | 272.23 → 246.00 | 28.30 → 27.97 |
| Purchase, 8 workers | 263.62 → 238.62 | 38.69 → 39.20 |
| Settled read, 1 worker | 1.004 → 0.004 | 0.128 → 0.100 |
| Settled read, 8 workers | 1.004 → 0.004 | 0.125 → 0.099 |

The retained change removes **25 allocation calls per purchase** in the stable
eight-worker case (~9.5%). A 1,000-component settled read falls from **1,004 to
4 allocation calls**, plus the existing overflow Vec reallocations. This is not
zero allocation. Purchase throughput is essentially unchanged in these runs;
the read improvement is clearer. One-worker scheduling interleavings change
planning work, so observed aggregate savings need not be exactly 25 there.
Small single-slot cases are also recorded in the dataset.

These are directional measurements on a busy workstation, not evidence for
small stable timing differences. Driver and worker CPUs were pinned separately,
physical cores first. All runs use the same workspace features and bench profile.
[Machine-readable results and source/binary provenance](benchmarks/2026-10-22-steady-allocations.json)
retain process median ranges; raw TSV/reports are in `target/steady-allocations/`.

## Retained implementation and rejected variant

`query/guards.rs` stores erased lock tokens in one Vec. Each token owns the same
component-cell Arc as its typed guard; unsizing the Arc allocates nothing.
Reader/writer release responsibility transfers exactly once. No component is
moved or cloned. Growing/moving the guard store or dropping it on another worker
preserves the lock and component address until token drop.

This removes `Box<dyn Any>` per ordinary fetched component and the old lifetime
transmute. It retains one guard collection allocation per populated invocation
or result. `GuardStore` is now a hidden support struct instead of a Vec alias;
custom query parameters retain `new()` / `Default` / `push(Box<dyn Any + Send>)`.
Code depending on arbitrary Vec operations needs adapting. Derived SystemParam
implementations and ordinary Query/View APIs are unchanged.

The first experiment inlined eight guards. It saved another five allocation
calls per purchase, but enlarged async future frames: eight-worker requested
allocation bytes rose from **25,443 to 27,581 bytes/request**, with no purchase
speedup. We retained compact Vec storage instead: **25,403 bytes/request**.
These are allocation traffic, not live memory/RSS; realloc bytes are reported
separately in the dataset. Future reuse should address collection allocations
without inflating every suspended invocation.

Lock-lifecycle regression tests cover mixed read/write guards beyond initial Vec
capacity, multiple readers per cell, moved storage, cross-thread drop, custom
guards, cancellation, and unwinding. The fixed-slot fixture also verifies state
and entity identities across repeated revisions.

## Run

```sh
cargo build --workspace --all-targets --profile bench -j 20
uv run tools/measure-allocations --out target/steady-allocations/new-run
```

Defaults: `--cases purchases,read,probe --workers 1,8 --slots 1,1000`,
three warmup rounds, ten measured rounds, three process repetitions. The runner
records allocation/free/realloc counts separately, then reruns with counters off
for timings. It refuses to overwrite an existing result directory.

Preserve the baseline binary before rebuilding, then compare:

```sh
uv run tools/measure-allocations --before target/steady-allocations/before-steady_allocations --out target/steady-allocations/new-comparison --cases purchases,read
```

The original preserved binary predates the control-attribution correction, so
only purchase/read cases are compared against it. Both variants use unchanged
purchase/read fixture logic. A supplied binary needs its original source
manifest; measurement-time workspace hashes describe only current sources.

For direct execution, `target/release/examples/steady_allocations` accepts
`ALLOC_CASE`, `ALLOC_WORKERS`, `ALLOC_SLOTS`, `ALLOC_ROUNDS`, `ALLOC_WARMUP`, and
`ALLOC_COUNT=0` to disable counters. Direct execution does not pin threads.

## Verification

Workspace build/tests, example attribution tests, clippy, playground, and all
three Criterion benchmark smoke suites passed with the ordinary workspace
feature set. Clippy retains existing repository warnings; none target the new
fixture or guard modules.

## Remaining work

- [x] Measure bounded warmed fixed IDs, settled reads, and user allocation control.
- [x] Remove per-component guard boxes; compare compact and inline collections.
- [ ] Reuse query guard collection capacity across invocations.
- [ ] Reuse row keys/dependencies/access metadata and planner scratch capacity.
- [ ] Reuse command buffers; avoid unconditional buffers without a Commands param.
      Preserve cloned-handle and commit ownership semantics.
- [ ] Reuse output/memo bookkeeping and execution queue capacity across waves.
- [ ] Address structural snapshot copying and replacement component-cell allocation
      while preserving outstanding readers and derived-output ownership.
- [ ] Add growth/churn/retained-reader allocation cases and per-path budgets.
- [ ] Enforce zero allocator calls for explicitly supported steady-state cases.

Follow allocation call stacks before choosing the next substantial storage
change. The existing `profile_purchases` continues to measure fresh-world
request retirement; its numbers are not interchangeable with fixed-ID work.
