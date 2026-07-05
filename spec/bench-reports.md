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

## Step 2 — shared planning snapshots (`share planning snapshots across invocations`)

Each planning wave now builds one `Arc<Snapshot>` and one `Arc` memo table
shared by every planned invocation. Previously `FunctionSystem::stream_runs`
deep-cloned the entire `World` (all store maps and bookkeeping) **once per
planned invocation**, and wrapper systems deep-cloned the memo table per wave.

| bench | 8 / 100 | 32 / 1000 | 128 / 10000 | vs step 1 |
|---|---|---|---|---|
| cold_settle | 165 µs | 2.52 ms | **39.7 ms** | **−59 % / −84 % / −95 %** |
| incremental_settle | 40.5 µs | 161 µs | 657 µs | **−32 % / −34 % / −40 %** |
| identical_rerun | 32.3 µs | 400 µs | **5.76 ms** | **−42 % / −76 % / −93 %** |
| read_scan | 5.1 µs | 39.8 µs | 397 µs | flat (external scoops still clone once, by design) |
| where_eq | 17.9 µs | 189 µs | — | −7 % |
| view_scaling (8/32/64) | 56.9 µs | 1.79 ms | 12.8 ms | −10…−17 % |

The per-invocation snapshot clone was the single dominant engine cost:
super-quadratic settle scaling mostly collapsed (cold_settle 8→128 now scales
~O(N^2), down from ~O(N^2.7) at 22× the absolute cost).

Discovered the hard way: a first attempt at output diffing (next step) placed
a `derived_owners` index inside `World`, where per-invocation snapshot clones
deep-copied it — cold_settle/128 regressed to 1.75 s (+103 %). That run is
recorded in `tmp/bench/bench-step2a.txt`; the index must stay out of snapshot
clones, and this step had to land first.

## Step 3 — derived output diffing (`diff derived outputs on commit`)

Commits now apply commands *over* the invocation's previous outputs (so equal
fingerprints keep their revisions) and then remove only what the rerun did not
re-emit, driven by a live-world `derived_owners` index (owner → outputs) that
replaces the per-commit `retain` over every entry of every store. The index is
excluded from snapshot clones; settled hooks check ownership through the live
bowl. The memo-currency check and command application also now happen under
one state lock, closing a window where a commit could apply after its deps
went stale.

| bench | 8 / 100 | 32 / 1000 | 128 / 10000 | vs step 2 |
|---|---|---|---|---|
| cold_settle | 165 µs | 2.45 ms | 39.5 ms | flat |
| incremental_settle | 39.0 µs | 158 µs | 657 µs | flat |
| identical_rerun | 30.1 µs | 381 µs | 5.68 ms | −5…−7 % |
| read_scan | 5.2 µs | 39.4 µs | 410 µs | flat |
| where_eq | 17.8 µs | 187 µs | — | flat |
| view_scaling (8/32/64) | 53.4 µs | 1.83 ms | 13.9 ms | flat (noisy) |

Small standalone numbers by design: the runner still replans the phase after
*every* commit, including ones that changed nothing. What this step buys is
that value-identical reruns now produce `needs_followup = false` commits (no
revision bumps, no downstream invalidation) — the signal the next step uses to
skip replan waves.

## Step 4 — fewer replan waves (`skip replan waves for no-op commits`)

The streaming loop now (1) drains every already-finished invocation and
commits the batch before considering a replan, and (2) replans only when a
commit changed the world, a stale run left its row memo-invalid, or a
conflict-deferred row is waiting on freed access rows. Previously every single
commit — including no-ops — triggered a fresh snapshot clone, memo clone, and
full planning pass over all systems.

| bench | 8 / 100 | 32 / 1000 | 128 / 10000 | vs step 3 |
|---|---|---|---|---|
| cold_settle | 44.1 µs | 201 µs | **871 µs** | **−73 % / −92 % / −98 %** |
| incremental_settle | 29.0 µs | 112 µs | 455 µs | **−26 % / −29 % / −31 %** |
| identical_rerun | 12.3 µs | 56.1 µs | **242 µs** | **−59 % / −85 % / −96 %** |
| read_scan | 5.3 µs | 40.1 µs | 973 µs* | flat* |
| where_eq | 18.5 µs | 192 µs | — | flat |
| view_scaling (8/32/64) | 21.1 µs | 418 µs | 3.91 ms | **−61 % / −77 % / −72 %** |

Settle scaling is now roughly linear: cold_settle 8→128 (16× files) costs
20× — down from 2200× at baseline.

\* `read_scan/10000` appeared to jump 410 µs → 973 µs, but re-benchmarking the
two *previous* commits mid-session reproduced ~925–950 µs on those too: the
machine's memory-bound performance drifted between runs (thermal/load), not
the code. Cross-run absolute numbers carry that caveat; the final report
re-runs baseline vs. HEAD back-to-back in one session.

## Step 5 — equality index (`index component fingerprints for eq filters`)

Each `Store<T>` now keeps a fingerprint → entities index behind an `Arc`
(shared with snapshots, copied on first live write after a clone).
`Where<Eq<T>>` resolves candidates through the index when the bound argument
has a fingerprint (`#[component(hash)]`), falling back to a scan otherwise;
`matches` still verifies with `PartialEq`, so hash collisions stay correct.
External `With<T>` filters and `And` chains narrow candidates the same way.

| bench | 8 / 100 | 32 / 1000 | 128 / 10000 | vs step 4 |
|---|---|---|---|---|
| cold_settle | 47.2 µs | 214 µs | 897 µs | +3…+7 % (index maintenance) |
| incremental_settle | 29.2 µs | 107 µs | 472 µs | flat |
| identical_rerun | 13.7 µs | 61.4 µs | 258 µs | +7 % (index maintenance) |
| read_scan | 5.1 µs | 39.1 µs | 968 µs* | flat |
| where_eq | **7.7 µs** | **77.5 µs** | — | **−58 % / −60 %** |
| view_scaling (8/32/64) | 21.1 µs | 439 µs | 3.78 ms | flat |

`where_eq` is now bounded by the per-scoop snapshot clone (2 000 entries →
~77 µs; compare `read_scan/1000` at 39 µs for ~1 000 entries), not the lookup —
the remaining read-side win would come from caching settled snapshots.

Measurement infrastructure note: `view_scaling/64` initially *appeared* to
regress +40 % (3.7 → 5.4 ms) on this change. Bisecting showed neither half of
the change caused it, and pinning `codegen-units = 1` for the bench profile
*inverted* the comparison — the engine's hot access-conflict loops are that
sensitive to codegen-unit partitioning. The workspace now pins
`[profile.bench] codegen-units = 1` so future runs measure the code, not the
layout lottery.

## Final comparison — baseline vs. all five steps

Full-suite runs of the baseline commit (`add engine benchmarks`, pre-engine
work) and HEAD, back-to-back in one session, both with `codegen-units = 1`.

| bench | baseline | HEAD | speedup |
|---|---|---|---|
| cold_settle/8 | 393 µs | 47.1 µs | **8.4×** |
| cold_settle/32 | 16.0 ms | 216 µs | **74×** |
| cold_settle/128 | 834 ms | 882 µs | **945×** |
| incremental_settle/8 | 58.8 µs | 29.0 µs | 2.0× |
| incremental_settle/32 | 238 µs | 109 µs | 2.2× |
| incremental_settle/128 | 1.28 ms | 435 µs | 2.9× |
| identical_rerun/8 | 53.9 µs | 13.2 µs | 4.1× |
| identical_rerun/32 | 1.71 ms | 57.1 µs | **30×** |
| identical_rerun/128 | 78.8 ms | 249 µs | **316×** |
| read_scan/100 | 6.2 µs | 5.1 µs | 1.2× |
| read_scan/1000 | 49.4 µs | 39.6 µs | 1.2× |
| read_scan/10000 | bimodal* | bimodal* | — |
| where_eq/100 | 18.6 µs | 7.8 µs | 2.4× |
| where_eq/1000 | 199 µs | 78.7 µs | 2.5× |
| view_scaling/8 | 70.4 µs | 20.4 µs | 3.5× |
| view_scaling/32 | 2.77 ms | 434 µs | 6.4× |
| view_scaling/64 | 21.3 ms | 3.78 ms | 5.6× |

Settle scaling went from ~O(N^2.7) to roughly linear: 16× more files costs
2122× at baseline, 18.7× at HEAD.

\* `read_scan/10000` measures ~500 µs in some process runs and ~930 µs in
others — *for both baseline and HEAD*, confirmed by interleaved A/B runs. The
bench is dominated by the two per-scoop 10 000-entry snapshot clones and is
hypersensitive to memory layout across process starts. Comparing versions on
it is meaningless; the 100/1000 sizes (stable) show −18…−20 %.

### Impact attribution

| step | main effect |
|---|---|
| 1. store-driven rows | read scans −20 %; prerequisite for index hints |
| 2. shared planning snapshots | **the** dominant win: settle −84…−95 % |
| 3. output diffing + owner index | enabler for 4; fixes stale-commit race; identical-rerun cutoff |
| 4. skip no-op replan waves | second dominant win: settle −73…−98 % on top of 2 |
| 5. fingerprint eq index | `Where<Eq>` scoops −60 % (now snapshot-clone bound) |

### Remaining known work (not yet done)

- ~~Cached settled snapshot for external scoops~~ (round 2)
- ~~Cheaper access-conflict checking for views~~ (round 2)
- ~~Stable derived-entity identity~~ (round 2)
- ~~Memo hygiene for entities removed via `commands.remove`~~ (round 2)
- Dirty-set filtered replans (replan only systems whose component footprint
  intersects committed changes) — less urgent now that no-op waves are gone.
- Optional dense storage (`spec/performance-plan.md` §6).

## Round 2 — read caching, view access, spawn identity, memo hygiene

Four follow-up commits, measured with targeted groups (full-suite numbers
below are the round-2 end state).

1. **`cache settled snapshots for repeated reads`** — `State` caches one
   `Arc<Snapshot>` keyed on `(next_entity, revision, mutations)`; reads of an
   unchanged world share it and `QueryResult` holds the `Arc` instead of a
   structural clone. Destructive `take` drops the cache before unwrapping
   cells. `read_scan` 5 µs–930 µs → **~140 ns flat**; `where_eq` → **~320 ns**;
   `incremental_settle` −13 %.
2. **`declare component-level access for views`** — `Access.entity` became
   `Option<Entity>` (`None` = whole store); a `View` declares one wildcard
   read per component instead of materializing per-row access vectors at plan
   time. `view_scaling` 14.6 µs / 147 µs / **517 µs** (was 21 µs / 439 µs /
   3.78 ms) — and the remaining cost is actual work, not scheduling.
3. **`reuse derived entity ids across reruns`** — spawned outputs reuse their
   entity ids slot-by-slot per invocation, and `DerivedFrom` fingerprints its
   captured anchors. The new `spawn_rerun` bench went 11 ms / 35 ms / 55 ms →
   **17 µs / 82 µs / 346 µs**. This step also uncovered and fixed a real bug in
   the round-1 diffing commit: stale *spawned* outputs were never removed
   (the owner index kept old pairs, so the stale diff skipped them), leaking
   derived entities into query results on every rerun. Same-entity outputs
   masked it in earlier benches; a regression test now covers replacement and
   id stability.
4. **`purge memo entries when entities are removed`** — `commands.remove`
   left memo entries keyed by dead entities forever; the commit path now
   drains removed entities and purges matching memo entries (unit-tested, no
   measurable perf change).

Round-2 end state (same machine, `codegen-units = 1`):

| bench | 8 / 100 | 32 / 1000 | 128 / 10000 |
|---|---|---|---|
| cold_settle | 40.6 µs | 185 µs | 809 µs |
| incremental_settle | 24.3 µs | 86.5 µs | 357 µs |
| identical_rerun | 11.5 µs | 52.0 µs | 230 µs |
| spawn_rerun | 19.6 µs | 84.4 µs | 357 µs |
| read_scan | 147 ns | 149 ns | 148 ns |
| where_eq | 325 ns | 331 ns | — |
| view_scaling (8/32/64) | 16.4 µs | 152 µs | 534 µs |

Against the original baseline: cold settle at 128 files is **~1000× faster**
(834 ms → 809 µs), a repeated read of an unchanged world is **~3 400× faster**
(500 µs → 147 ns) and now O(1) in world size, and derived churn no longer
grows the entity id space.
