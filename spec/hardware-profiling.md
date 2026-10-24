# Fixed-work hardware and engine accounting

For the current warmed-world investigation, see [warmed purchase CPU profile](warmed-purchase-profile.md). The measurements below use the earlier fresh-world fixture.

Baseline source: `2821d2f` plus the profiling instrumentation in this slice.
The main tables precede the no-view gate; its follow-up comparison is below.
[Per-run counts, commands, hashes, affinity and overhead measurements](benchmarks/2026-10-22-hardware-profile.json)
are retained. Large perf recordings and unabridged output remain in
`target/hardware-profile/2026-10-22/`.

## Run it

```sh
cargo build --workspace --all-targets --profile bench -j 20
uv run tools/profile-engine --out target/hardware-profile/my-run
```

The runner is a dependency-free Python/uv executable. No Cargo features change
between hardware counts, diagnostics, sampling, tests or normal workspace builds.
Use `--help` for controls. The measured matrix was:

```sh
uv run tools/profile-engine --out target/hardware-profile/2026-10-22 --cases independent-1,independent-8,contended-1,contended-8,compute-8,sparse-1,sparse-8,control --rounds 10 --repetitions 3
```

Each process waits before warmup while the runner assigns affinity. Driver: CPU 0.
Workers: distinct CPUs 2,4,6,8,10,12,14,16 as needed, excluding the driver's SMT
sibling CPU 1. CPU selection and topology are recorded, not assumed portable.
Nothing changes system-wide power settings, affinity, or perf permissions.

This Ryzen AI 9 365 workstation was still running other applications. Affinity
removes migration as a variable but does not isolate caches, memory bandwidth,
frequency or interference. Counts are observations of these runs, not immutable
costs of the algorithm. One worker plus a driver is two execution threads.

## Measurement boundaries

| Pass | Enabled | Purpose |
| --- | --- | --- |
| `counters` | Small `perf stat` event groups; diagnostics off | Whole-workload hardware counts |
| `diagnostics` | Engine counts, elapsed stage timers, allocator API counts | Explain the amount and location of work |
| `sample` | Cycle-event sampling; diagnostics off | Attribute sampled cycles to threads and symbols |

Account creation, initial settling, warmup, validation, formatting and world
teardown are outside hardware gates and elapsed batch timing. Each round starts
from fresh settled state. This is not a sustained-churn benchmark. Enabling and
disabling perf adds a small amount of control work inside hardware intervals;
the `control` case measures an otherwise empty interval, without subtracting it
from results automatically.

Purchases: 1,000 requests per round, including submission, settling, copying
responses and retiring requests. Contended cases submit four requests per owner
in reverse sequence order. Compute adds 10,000 arithmetic iterations to each
validator. Sparse: 100 successive updates per round, with 32 domains of 1,000
entities plus one target; each update materializes the existing 1,001-row result.
Thus a sparse operation includes external query work, not just one kernel call.

Hardware groups: cycles/instructions/branches/branch misses; generic cache events;
L1 data-cache/TLB events; user task-clock/page faults. All **96 counter runs
reported 100% running coverage**. No multiplex scaling was required according to
perf's reported coverage. Cache and TLB aliases retain their exact perf names;
their microarchitectural meaning is CPU-specific. In particular, do not label
`cache-misses` as DRAM accesses or assume a particular cache level.

Kernel scheduler events were unavailable with `perf_event_paranoid=2`.
User-only context-switch zeros are not evidence of zero context switches and
were excluded. No permission change was requested or made.

## Hardware counts per operation

Medians of three fresh-process normalized totals, ten measured rounds each.
The latency column is elapsed batch time divided by operations in the core-event
runs; it is amortized throughput cost, **not per-request response latency**.
Other event groups run in separate processes, so their columns need not describe
one identical scheduling interleaving.

| Workload | Workers | us/op | Instructions/op | Cycles/op | Generic cache misses/op | Branch misses/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Independent purchase | 1 | 26.34 | 312,350 | 140,689 | 1,485 | 898 |
| Independent purchase | 8 | 36.86 | 309,904 | 198,640 | 1,627 | 903 |
| Contended purchase | 1 | 85.02 | 1,061,146 | 447,109 | 5,053 | 2,792 |
| Contended purchase | 8 | 105.14 | 888,803 | 563,240 | 4,931 | 2,428 |
| Compute purchase | 8 | 39.47 | 429,719 | 293,245 | 1,638 | 930 |
| Sparse update + read | 1 | 94.36 | 908,223 | 422,835 | 53,165 | 1,062 |
| Sparse update + read | 8 | 116.32 | 908,016 | 475,530 | 52,922 | 1,171 |

Per-process ranges, raw counts, event coverage, L1/TLB events and task-clock remain
in the JSON. Example: independent/eight-worker core runs were 37.21, 36.86 and
36.16 us/op. Contended/eight-worker runs were 107.03, 97.09 and 105.14 us/op.
Three processes do not establish narrow confidence intervals for all workloads.

The empty gate's median was approximately 1,521 cycles and 1,398 instructions
**per interval**, versus 140–563 million cycles per 1,000-purchase interval.
Its overhead is small for these batches, not necessarily for one tiny operation.

## Amount of engine work

These are diagnostic-pass medians per completed operation; retries can revisit
the same row. They are not counts of unique entities. Diagnostic scheduling can
differ from the hardware runs because counting itself costs work.

| Workload | Workers | Candidate rows | Memo hits | Admitted invocations | Conflict deferrals | Allocator calls | Requested allocation bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Independent | 1 | 10.33 | 5.17 | 5 | 0 | 298.36 | 33,973 |
| Independent | 8 | 10.01 | 5.01 | 5 | 0 | 275.68 | 28,829 |
| Contended | 1 | 55.06 | 27.32 | 12.5 | 11.67 | 1,259.55 | 113,311 |
| Contended | 8 | 35.74 | 17.85 | 12.5 | 4.80 | 853.86 | 78,305 |
| Sparse | 1/8 | 11 | 10 | 1 | 0 | 355 | 191,805 |

`alloc_calls` includes successful Rust allocator `alloc` and `alloc_zeroed` calls;
reallocations are separate (about 24/request for independent/eight-worker and
90/request for contended/eight-worker). Requested bytes are allocation traffic,
not live memory, RSS, or bytes copied. Deallocations can free pre-interval objects.
Native allocations bypassing Rust's global allocator are not counted here.

Independent purchases perform 29 current-revision checks/request; contended ones
perform 72.5. No stale publication was observed. All diagnostic runs verify
admitted/started/completed invocation accounting and purchase retirement outside
the measured region. All purchased requests were accepted.

### Async polling result

Every completed invocation in these workloads was polled **exactly once**:
5 polls/purchase independently, 12.5 under contention, and 1/sparse update.
The extra contended invocations are separate evaluations, not repeated polls of
one suspended future. This does not cover kernels waiting on actual I/O.

A synchronous kernel path could still remove future-set nodes, polling adapters
and some completion machinery. These numbers do not identify repeated polling
as the bottleneck, and `system_poll_ns` includes parameter fetching, guards,
output construction and elapsed preemption time, not just future dispatch.

Synchronous kernels can complete out of order across workers. They cannot suspend
within an invocation. Cloning invocation-local `Commands` into a detached task
is not a replacement for async lifecycle tracking: `Commands::take` drains the
buffer once at completion, so later writes do not automatically publish or retain
the original invocation's dependency checks. Any such API needs an explicit
completion/ownership contract.

## Stage observations

Diagnostic elapsed microseconds per operation:

| Workload | Planning | Admission | Publication | Bound-result cleanup | Ambient healing | Summed worker poll bodies |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Independent, 1 worker | 11.89 | 4.43 | 7.25 | 1.38 | 0.01 | 7.03 |
| Independent, 8 workers | 11.21 | 14.51 | 8.50 | 1.66 | 0.01 | 32.11 |
| Contended, 8 workers | 40.05 | 38.94 | 22.34 | 1.64 | 0.01 | 74.73 |
| Sparse, 8 workers | 2.64 | 2.61 | 2.27 | 0 | 40.94 | 3.28 |

These columns are **not an additive partition of wall time**. Workers overlap the
driver and one another, and elapsed spans include descheduling. Input application,
external materialization, some cleanup paths, snapshots and waits are not all
partitioned here. Existing snapshot/settle counters also overlap. The old hook
commit bucket is still labeled `hook_commit_us`; the new publication timer covers
ordinary streamed publication after acquiring the state lock.

The first-poll-delay counter measures row admission to first poll, including
batch filling and scheduling. Independent/eight-worker mean delay was about
143 us/invocation; delays overlap and are not total request latency.

### Concrete sparse bottleneck

**Every sparse update examines 32,001 memo entries during ambient healing, despite
having no ambient views.** That costs roughly 38–41 diagnostic microseconds/update.
The 11 candidate rows/update are a different metric: one initial full replan in
each fresh 100-update round amortizes to 10 memo hits/update; subsequent work is
narrowed. Removing the no-view healing scan is an independent, justified next step.
The sparse read also materializes 1,001 rows and live updates can copy entry maps;
removing healing alone will not remove all its memory traffic.

## Cycle sampling and overhead

Seven separate cycle profiles sampled every million user cycles. All reported
zero lost samples. Sampled driver cycle shares:

| Case | Driver share | Samples |
| --- | ---: | ---: |
| Independent, 1 worker | 85.87% | 1,522 |
| Independent, 8 workers | 79.00% | 2,157 |
| Contended, 1 worker | 88.67% | 4,988 |
| Contended, 8 workers | 79.64% | 5,702 |
| Compute, 8 workers | 56.51% | 2,948 |
| Sparse, 1 worker | 98.55% | 414 |
| Sparse, 8 workers | 96.49% | 456 |

For independent/eight-worker work, allocation/free leaves account for 21.88% of
samples, hashing 16.04%, reservations 3.29% and revision lookup 4.50%. These are
exclusive symbol-name groups and lower bounds on broader activities. They do
not include caller work merely because it leads to an allocation. Source-level
attribution is sampled, not an exact per-function event census; inlining and
identical-code folding can also make a concrete type name a misleading alias.
Sparse profiles have relatively few samples: use the directly counted memo scans
and timers for the stronger evidence there.

A separate pinned comparison reused the committed executable to check instrumentation
cost. Three fresh processes/version, 20 rounds after five warmups:

| Independent requests | Before instrumentation | New binary, diagnostics off | Diagnostics on |
| --- | ---: | ---: | ---: |
| 1 worker, us/request | 26.50 | 26.32 | 31.84 |
| 8 workers, us/request | 38.50 | 37.70 | 42.45 |

No dormant-instrumentation regression is resolved by these noisy measurements;
that is not proof of zero overhead. Diagnostics add roughly 13–21% in these cases,
which is why hardware counts use diagnostics off. Sampling also perturbs timing.

## Implementation and follow-up

- `crates/bowl/src/metrics.rs`: optional process-global operation counters/timers.
  No resetting while work runs; the harness subtracts quiescent snapshots.
- `system.rs`: candidate/dependency/access/memo accounting and revision checks.
- `dispatch/plan.rs`: admission, conflict and completion/poll accounting.
- `bowl.rs`: streamed publication, bound-result cleanup and ambient healing spans.
- `crates/benches/examples/profile_purchases.rs`: fixed work, perf gating and startup
  handshake; `support/allocations.rs` counts successful allocator API operations.
- `tools/profile-engine`: pinned sequential runs, small PMU groups, coverage,
  raw output and provenance. A nonempty output directory is never overwritten.

Next priorities supported by this pass: skip impossible ambient healing, reduce
per-invocation allocations/metadata construction, then direct dirty routing.
A native synchronous-kernel experiment should retain the same planning/publication
contract and compare against immediate async kernels. Do not remove async support
or treat detached command-buffer writers as safe based on these measurements.

Still open: source-line/assembly investigation of individual hot paths, allocation
call-site counts, deterministic isolated instruction-count benchmarks (for example
Cachegrind), and kernel scheduler tracing where available. This pass gives measured
hardware totals and detailed work accounting; it does not claim exact cycles for
every source line or a complete partition of execution time.

Validation: workspace build; 155 unit tests plus one doctest; all three optimized
benchmark smoke targets; playground; profiling CLI smoke and syntax checks passed.
Clippy completed with existing warnings only. Diagnostic completion, poll and
retirement accounting checks passed in every measured diagnostic process.

## Follow-up: disable unused ambient healing

The sealed system registry now enables ambient healing only if at least one
non-Settle system declares ambient dependencies through `view_freshness`.
Otherwise `heal_stale_ambient` returns before acquiring world state or starting
the diagnostic timer. This covers ordinary tracked-only workloads and registries
whose only viewers run in Settle, which the healing pass already excluded.

A regression test holds the state lock and polls the healing future once: unused
healing must return immediately, while an eligible viewer follows the normal lock
path. Existing same-settle view-chain and cleanup-healing tests remain green.

[Alternating before/after counts and provenance](benchmarks/2026-10-22-ambient-healing-gate.json)
use the same pinned driver/worker placement, fixture, features and bench profile.
Three fresh processes per variant, ten rounds of 100 sparse updates each; core
and cache counters are separate groups, both reporting 100% coverage. Diagnostic
accounting confirms one invocation and one poll per update on both sides.

| Sparse update + read | Before | After |
| --- | ---: | ---: |
| One worker, us/op | 89.19 | 50.34 |
| Eight workers, us/op | 112.47 | 68.90 |
| Eight workers, instructions/op | 908,187 | 367,769 |
| Eight workers, cycles/op | 461,287 | 264,016 |
| Eight workers, generic cache misses/op | 53,485 | 1,392 |
| Healing memo entries examined/op | 32,001 | 0 |

These are fresh paired measurements, not comparisons against the older main-table
latencies. This removes demonstrated unnecessary work; it does not establish a
universal speedup for workloads with views or explain away the remaining external
read and entry-map copying costs.

When views exist, their definitions already provide component/filter dependencies.
The broader routing improvement can index component IDs to viewer systems and
retain only current-settle invocation candidates for those systems. Relevant
watermarks still determine whether a candidate's snapshot is stale. View metadata
identifies potentially affected systems/stores, not every entity a runtime loop
actually reads; ordinary ambient Views retain whole-store semantics. Historical
memos from previous settles must not become tracked dependencies accidentally.

Follow-up validation: 156 unit tests plus the doctest, workspace builds, all three
optimized benchmark smoke targets and playground passed. Clippy completed with
existing warnings only.
