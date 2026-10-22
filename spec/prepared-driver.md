# Prepared driver lookups

Status: implemented; benchmark comparison recorded below. No public Bowl or
kernel authoring changes. This is the first two steps of the
[execution profiling plan](execution-profiling.md#proposed-implementation-prepare-the-driver-from-the-schema).
Direct component-to-system dirty routing remains a separate follow-up.

## What changed

```text
Bowl construction
  schema component universe
    -> shared TypeId-to-slot registry
       -> presence bits and dense store slots use the same IDs

Planning pass for one system
  query type -> concrete prepared helper
    -> resolve each ordinary component store once
    -> share existing snapshot entry maps (Arc, no component clones)
  enumerate rows using existing masks / candidate / join rules
  for each row
    -> collect revisions through resolved typed stores
    -> memo hit? skip
    -> construct access only for admitted rows

Worker invocation
  prepared helper -> acquire ordinary row guards -> run kernel

Publication
  dependency's cached slot -> current live store -> current revision
  existing stale checks, ownership, reconciliation and output application
```

`system.rs::plan_prepared` no longer builds access metadata for memo hits.
`prepared.rs` composes concrete helpers for scalar reads, mutable scalar queries,
entity tuples, optional reads and system parameter tuples. Dependencies append
into one row vector instead of allocating a vector for each ordinary component.
A single shared context holds the snapshot and prepared helpers, replacing each
row's previous snapshot handle without another per-row Arc operation.

`world/stores.rs` uses the presence universe's existing registry for dense store
slots. Types outside that universe retain a map fallback, including internal
engine components. Component payloads still use the existing entry maps and
guarded cells; this is not an archetype or contiguous-component storage rewrite.

Prepared dependencies carry an optional slot for current-revision checks at
publication. The slot is an accelerator, not dependency identity: equality still
compares type, entity, revision and scope. The slot lookup checks its TypeId and
falls back on a different layout, so a different bowl cannot silently resolve
the same numeric slot to another component type.

## Boundaries and guarantees

- Preparation acquires no component locks. Guards are acquired when a row runs
  and held through the async kernel, exactly as before.
- Prepared entry handles belong to one structural snapshot. Every planning pass
  resolves anew, including an optional store that did not previously exist.
  Later inserts/removals cannot silently replace the prepared row's source.
- Custom parameters use default fallback helpers; adding preparation does not
  require implementing a new associated type. The low-level traits expose an
  optional `prepare` method with concrete `impl Trait` results. No per-row virtual
  dispatch or boxed prepared helper is added.
- Complex projections (`Related`, `Tracked`), ambient views, outer optional query
  parameters and filter dependency collection retain their existing implementations.
  Generated ambient parameter bundles also use the fallback. Their semantics and
  dependency order are preserved.
- Ordinary streamed execution, including cooperative execution, uses prepared
  fetching. Grouped hook execution can prepare dependencies but keeps its existing
  fetch path. External scoop materialization is unchanged.
- Membership matching already prepares presence masks. This slice does not add
  prepared join indexes, cached filter predicates, reverse dirty routing or static
  system ordering. Those depend on runtime facts and need separate evidence.

## Deviations and remaining costs

The proposal's registration-time per-query layout is not fully implemented.
The component universe is fixed at construction, but query-specific typed helpers
are constructed once per planning pass, not cached across snapshots. This avoids
retaining stale store handles and preserves custom parameter compatibility.

A prepared context adds one shared allocation per planned system, and clones
entry-map handles per component. Dense slot arrays are cloned per snapshot and
include unpopulated schema types, so extremely large, sparsely populated schemas
need further memory/snapshot measurements. Fallback lookups retain hashing.
TypeId-based joins, access reservations, write reconciliation, ownership indexes
and memo identities also remain. Current-revision checks avoid TypeId hashing
for prepared ordinary dependencies but still dispatch through the store trait.

Broader preparation should follow measurements, particularly tiny workloads and
wide schemas. The next routing slice should reuse this same registry rather than
introduce a second component identity system.

## Validation

Workspace build and tests use the same default feature graph as the workspace-wide
optimized build. Existing tests cover joins, mutation, optional observations,
preemption, async suspension, hooks and cooperative/worker dispatch. New tests cover:

- Memo hits skip access construction while new and always-run rows retain it.
- Prepared snapshots retain their source values after live replacement/removal.
- Optional stores appearing/disappearing invalidate the recorded observation.
- Prepared mutable reads use the existing cells and commit reconciliation.
- Schema slot lookup handles absent stores, undeclared types and reordered layouts.
- Complex tracked parts retain the original dependency projection.

## Measurements (2026-10-22)

[Commands, source/executable hashes and individual estimates](benchmarks/2026-10-22-prepared-driver.json)
are retained. Local runners, raw Criterion samples and perf data are under
`target/prepared-driver/`. Baseline `f218b69`, admission-only and combined prepared
implementations use identical fixtures, feature selection and bench profile.
Two fresh processes per variant in rotating/reversed order; 0.1-second warmup,
0.5-second requested measurement and 10 samples. These are exploratory measurements
on the active workstation, not precise acceptance thresholds.

Median milliseconds per 1,000-request batch (light kernels):

| Case | Baseline | Admission only | Prepared + admission |
| --- | ---: | ---: | ---: |
| Independent, 1 worker | 52.29 | 34.37 | 33.70 |
| Four requests/owner, 1 worker | 190.09 | 135.86 | 93.27 |
| Independent, 8 workers | 56.52 | 55.15 | 45.72 |
| Four requests/owner, 8 workers | 111.67 | 135.95 | 107.68 |

The strongest separated signal is contended/one-worker: baseline 186–194 ms,
admission 134–137 ms, prepared 91–95 ms. Independent/eight-worker baseline varied
44–69 ms versus prepared 45–46 ms; the medians alone overstate certainty there.
Contention also changes scheduling interleavings, so do not attribute all latency
improvement to a fixed number of saved lookups. Admission alone is not universally
faster: the eight-worker contended case regressed in this pass.

Median microseconds for small/compiler cases:

| Case | Baseline | Admission only | Prepared + admission |
| --- | ---: | ---: | ---: |
| One purchase, 1 worker | 113.34 | 77.51 | 70.24 |
| One purchase, 8 workers | 104.27 | 102.06 | 74.44 |
| Cold settle, 128 inputs | 645.02 | 645.49 | 626.87 |
| Incremental settle, 128 inputs | 76.04 | 74.15 | 73.00 |
| Equal-output rerun, 128 inputs | 243.78 | 243.68 | 239.13 |
| Sparse change, 1 worker | 82.80 | 107.70 | 91.22 |
| Sparse change, 8 workers | 141.71 | 87.90 | 166.63 |

### Sparse regression investigation

The initial sparse/eight-worker slowdown was not dismissed as noise. A longer
three-process recheck (0.2-second warmup, 1-second requested measurement, 20 samples)
measured baseline **83/85/95 us** versus prepared **173/163/172 us**.
CPU profiles of both versions showed similar hot paths: ambient healing scanning
the memo table accounted for roughly one-third of self samples, followed by
entry-map copying, system cloning and allocation. This fixture has no ambient
views; that unnecessary scan is a separate concrete optimization candidate.

To distinguish placement effects, a subsequent paired run pinned only each
benchmark process's driver to CPU 0 and assigned its workers CPUs 1–19, after
runtime creation. No global affinity/governor settings changed. Baseline measured
**97/99/84 us**, prepared **86/86/84 us**. The gap disappeared under matching driver
placement. This indicates a substantial placement/runtime confounder; it does not
prove that preparation improves every sparse workload or explain the exact
hardware cause. Keep both observations. Repeat on a quiet, controlled host before
claiming a stable small-case improvement or regression.

No workload-dependent threshold or special sparse execution path was added to
hide the observation. The retained code is the same in all candidate runs.

### Full purchase CPU profile

For 80 independent 1,000-request batches with eight workers, gated user-space
sampling at 499 Hz recorded 2,500 samples before and 1,929 after preparation.
No loss was reported. Driver share was **78.36% → 75.58%**; worker share was
**21.64% → 24.42%**. Sample count suggests lower total sampled CPU for this fixed
workload, while the driver still dominates. CPU shares are not wall-time fractions.

Exclusive symbol-name groups across driver and workers:

| Group | Baseline | Prepared |
| --- | ---: | ---: |
| Allocation/free | 19.64% | 19.54% |
| Hashing/hash-table methods | 18.00% | 16.28% |
| Revision lookup leaf | 6.28% | 5.55% |

These are lower-bound symbol groups, not exhaustive stage timings. Their matching
rule includes hash-table methods, so they should not be compared directly with
narrower hashing groups in the earlier profiling report. Profiles support the
lookup direction, but allocation, memo/ownership work and publication still need
attention. Reverse dirty routing remains the next planned substantial slice;
the no-view ambient scan can be removed as a smaller independent change first.
