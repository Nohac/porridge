# Benchmark Reports

Per-step results of the optimization work planned in
[performance-plan.md](performance-plan.md). All numbers are criterion medians
from `cargo bench -p benches` on the same machine; "Δ" is criterion's measured
change against the previous step.

Benches:

- `cold_settle/N` — settle N files through a 3-system pipeline from scratch
- `incremental_settle/N` — touch 1 of N settled files, re-settle
- `identical_rerun/N` — all N inputs change, derived output values identical
- `read_scan/N` — scoop 16 matching rows past N irrelevant entities
- `where_eq/N` — path-equality scoop over N files
- `view_scaling/N` — N rows, per-row system with a `View` over all rows, zero outputs

## Baseline (commit `add engine benchmarks`)

| bench | 8 / 100 | 32 / 1000 | 128 / 10000 |
|---|---|---|---|
| cold_settle | 395 µs | 16.4 ms | 867 ms |
| incremental_settle | 59.7 µs | 246 µs | 1.09 ms |
| identical_rerun | 55.3 µs | 1.72 ms | 80.8 ms |
| read_scan | 6.5 µs | 50.2 µs | 516 µs |
| where_eq | 19.5 µs | 218 µs | — |
| view_scaling (8/32/64) | 74.3 µs | 2.68 ms | 19.5 ms |

## Step 1 — store-driven row enumeration (`iterate component stores for query rows`)

Row enumeration now iterates the component store (smallest participating store
for tuple queries, `With<T>` candidate hint for bare `Entity` queries) instead
of scanning `0..next_entity`. Single-part tuple queries skip the match probe
entirely — the store keys *are* the rows.

| bench | 8 / 100 | 32 / 1000 | 128 / 10000 | vs baseline |
|---|---|---|---|---|
| cold_settle | 399 µs | 16.2 ms | 864 ms | flat |
| incremental_settle | 59.8 µs | 244 µs | ~0.94 ms | flat (noisy at 128) |
| identical_rerun | 55.5 µs | 1.69 ms | 82.5 ms | flat |
| read_scan | 5.2 µs | 40.2 µs | 405 µs | **−20 %** |
| where_eq | 18.7 µs | 203 µs | — | −5 % |
| view_scaling (8/32/64) | 62.9 µs | 2.18 ms | 15.5 ms | **−15…−20 %** |

Notes:

- First attempt (without the single-part fast path) *regressed* `view_scaling`
  by ~9 %: collecting store keys plus a redundant `has()` probe on the primary
  part costs more than the dense scan when nearly every entity id is live. The
  fast path turned that into −20 %.
- `read_scan` did not drop to O(matches): the remaining ~400 µs at 10 000
  entities is dominated by the per-scoop structural snapshot clone
  (`World::clone` Arc-bumps every entry). Addressed in step 3.
- Settle benches unmoved, as predicted — they are bound by replanning waves and
  the O(all entries) commit path, not row enumeration (steps 2–3).
