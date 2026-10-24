# Warmed purchase CPU profile — 2026-10-24

The shared evaluator driver, not the planner alone, limits this tiny-kernel
workload. At eight workers the driver accounts for about 77–79% of sampled user
CPU cycles. Publication/command application is the largest identified stage.
These are workload-specific measurements, not a claim about compute-heavy or
I/O-heavy kernels.

## Measurement

Use the existing fixed-ID `Purchases` fixture: 1,000 accounts/request slots,
normal input submission, settlement and outcome materialization. Setup, five
warmup rounds, verification, and printing are outside the sampled interval.
Driver and workers are pinned separately. Production throughput, instrumented
diagnostics, hardware counters, and CPU sampling run in separate processes.
No engine optimization or component revision policy was changed for this study.

The ordinary optimized binary provides throughput and hardware counters.
DWARF sampling recovered too few caller stacks to attribute generic operations
(~83% missing in the initial smoke check). A separate optimized binary built
with `RUSTFLAGS='-C force-frame-pointers=yes'` supplies caller attribution.
The Cargo feature graph is unchanged. Frame pointers can change code generation;
its CPU shares are diagnostic evidence, not exact production wall-time shares.

114,706 samples across eight full recordings: async/sync × one/eight workers × two processes, 100
measured rounds each. The frame-pointer recordings have no empty callchains;
No lost samples were reported; unknown leaf symbols are about 2.4–2.6% at eight workers. Classification uses
actual outer functions, avoiding false attribution from nested generic type
names. Each sample belongs to one stage; mechanism counts are a separate view.

[Machine-readable measurements and preserved source/binary hashes](benchmarks/2026-10-24-warmed-cpu-profile.json).

Raw evidence, frozen executables and classifier:
`target/current-bottleneck-20261024-114657/`.
Production build provenance: `/tmp/porridge-bottleneck-build.jsonl`.
Profiling build provenance: `/tmp/porridge-profile-fp-build.jsonl`.

## Normal-binary throughput

Three process repetitions, medians in microseconds per completed purchase:

| Workers | Async µs/purchase | Sync µs/purchase | Async purchases/s | Sync purchases/s |
| --- | ---: | ---: | ---: | ---: |
| 1 | 20.96 | 20.02 | ~47,700 | ~50,000 |
| 4 | 21.03 | 20.74 | ~47,600 | ~48,200 |
| 8 | 24.63 | 24.23 | ~40,600 | ~41,300 |
| 16 | 24.61 | 24.60 | ~40,600 | ~40,700 |

Rates above are rounded from the reported medians. Workstation activity makes
small differences uncertain; compare paired runs, not older measurements on a
different day/load. More workers do not improve this workload. With full PMU
coverage, eight-worker async consumes about 157.7k user cycles/purchase versus
117.9–118.8k with one worker. Sync shows the same pattern (157.1–161.7k versus
115.6–119.4k). The extra workers cost CPU without increasing throughput.

## Exclusive sampled CPU attribution

Ranges across the two eight-worker recordings; percentages of **all sampled
user CPU cycles**, including driver and worker threads, not elapsed latency:

| Stage | Async | Sync |
| --- | ---: | ---: |
| Publication and command application | 28.8–29.1% | 29.3–29.6% |
| Planning | 22.3% | 20.7% |
| Admission and reservation bookkeeping, including release | 14.2–15.9% | 17.7–18.1% |
| Completion/dispatch/runtime wrappers | 9.5–9.7% | 14.4–14.7% |
| Identified worker row execution | 15.4–15.7% | 9.7–9.9% |
| Identified direct kernel bodies | <0.25% | <0.25% |
| Other/unassigned | 7.9–8.9% | 7.4–7.6% |

Compiler inlining shifts code between executor wrappers and row functions.
The completion/dispatch category is not pure future polling, and the direct
kernel-body category is not a complete accounting of business logic. For
comparisons, combine worker row execution, direct bodies and dispatch/runtime
wrappers: together they account for roughly a quarter of sampled CPU in both
modes. Generic HashMap/allocator costs are attributed through recovered callers;
unrecognized ownership stays unassigned.

Publication includes base-input command application as well as derived outputs.
Publication subcosts occupy the following approximate percentages of **total**
sampled CPU (they are included in publication above, not additional costs):

| Publication work | Share |
| --- | ---: |
| Captured-dependency freshness checks | 5.7–5.8% |
| Derived-output ownership and cleanup | 5.4–5.7% |
| Command application | 5.5–5.6% |
| In-place write reconciliation | 3.8–4.0% |
| Structural copy-on-write cloning | 2.2–2.4% |
| Other publication work | ~6% |

Diagnostics independently count ten candidate rows, five executed kernels and
29 current-revision checks per purchase at eight workers. There are no stale
publications or already-running duplicates in these runs. Diagnostic elapsed
timers overlap other work and are not an additive CPU partition.

## Cross-cutting mechanisms

For eight-worker sync, internal map-key hashing accounts for about 15.5–17.0%
of sampled CPU (TypeId, entity, system and invocation keys), allocator/free
functions 10.4–10.5%, revision lookup 6.6–6.9%, and invocation key cloning
2.5–3.9%. Identified component-payload hashing is only about 2%; another 2.7%
of hashing lacks enough typed context to separate keys from payloads.
These mechanism categories overlap the stage table and must not be added to it.
They are sampled symbol costs, not predictions of the speedup from deleting a
feature. Linker identical-code folding can label generic revision lookups with
the Stamp specialization even though the active workload is purchases.

## What to optimize next

1. Reduce internal key lookup, hashing and invocation identity bookkeeping across
   planning, admission and publication. Prefer existing schema slots and reusable
   invocation metadata where applicable; measure each change independently.
2. Reduce publication work: dependency revision lookups, derived-output ownership
   updates and command application. Preserve stale-dependency checks and output
   retraction semantics.
3. Revisit worker dispatch granularity after shrinking serial coordination.
   Adding workers currently increases work per purchase.

Automatic mutation revisions remain worth testing, but eliminating component
payload hashing cannot remove internal TypeId/entity/invocation hash-table work
or the cost of looking up revisions. The next pass should be a measured change
to the common bookkeeping path, not a planner-only rewrite.

Relevant implementation: `system.rs::plan_prepared_with_scratch`,
`dispatch/plan.rs::dispatch_rows`, `access.rs::Reservations`,
`bowl.rs::commit_system_run`, `world.rs::Store<T>::revision_for_entity`,
`world.rs::reconcile_written`, and `world/stores.rs`.

## Reproduce

```sh
cargo build --workspace --all-targets --profile bench -j 20
uv run tools/measure-allocations --binary target/release/examples/steady_allocations \
  --before target/release/examples/steady_allocations --before-kernels async \
  --kernels sync --cases purchases --workers 1,4,8,16 --slots 1000 \
  --rounds 10 --warmup 5 --repetitions 3 --out target/profile-throughput

CARGO_TARGET_DIR=target/profiling-fp RUSTFLAGS='-C force-frame-pointers=yes' \
  cargo build --workspace --examples --profile bench -j 20
uv run tools/profile-engine --steady --kernels sync --call-graph fp \
  --binary target/profiling-fp/release/examples/steady_allocations \
  --cases independent-1,independent-8 --modes sample \
  --rounds 100 --warmup 5 --out target/profile-cpu-sync
```

Repeat sampling in a new directory for each process and switch `--kernels` for
async. Sampling currently runs once per case per invocation; `--repetitions`
applies to counter/diagnostic passes. Keep frozen binaries so perf resolves the
same symbols used at recording time. `--stack-bytes` controls DWARF dumps when
using the default `--call-graph dwarf`.

Validation: optimized workspace/all-targets build, six optimized fixture tests,
profiling gate smoke, benchmark smoke, standalone optimized playground, and
Python runner tests. A broader optimized playground test run passed eight tests
and exposed an existing mismatch: `incomplete_shapes_panic_with_the_missing_component`
expects a panic from debug-only conformance checks. The earlier debug workspace
test run passed. That unrelated release-test expectation was not changed.
