# Performance Optimization Plan

Status: benchmarks implemented (`crates/benches`, `cargo bench -p benches`);
engine changes not started.

## Baseline (2026-07-05, criterion medians, release)

| bench | 8 | 32 | 128 | scaling |
|---|---|---|---|---|
| `cold_settle` (N files, 3-system pipeline) | 395 µs | 16.4 ms | **867 ms** | ~O(N^2.7) |
| `incremental_settle` (touch 1 of N files) | 59.7 µs | 246 µs | 1.09 ms | ~O(N) |
| `identical_rerun` (N inputs change, outputs identical) | 55.3 µs | 1.72 ms | **80.8 ms** | ~O(N^2.6) |
| `view_scaling` (N rows, View param, zero outputs) | 74.3 µs (8) | 2.68 ms (32) | 19.5 ms (64) | ~O(N^2.9) |

| bench | 100 | 1 000 | 10 000 |
|---|---|---|---|
| `read_scan` (scoop 16 rows past N irrelevant entities) | 6.5 µs | 50.2 µs | 516 µs (linear in dead ids) |
| `where_eq` (path-equality scoop over N files) | 19.5 µs | 218 µs | — (linear scan) |

What the baseline confirms:

- **Settle cost is super-quadratic in world size** (`cold_settle`,
  `view_scaling`) — replan-per-commit × dense scans × O(all entries) commit
  cleanup compound. `view_scaling` produces *zero outputs* and still pays
  ~20 ms at 64 rows: pure planning/replanning overhead.
- **The fingerprint cutoff does not work for derived outputs**
  (`identical_rerun`): value-identical reruns should settle in ~O(N) after
  reexecution, but cost ~O(N^2.6) because every commit bumps revisions and
  forces a full replan (finding A below).
- **Reads pay for historical churn, not live data** (`read_scan`): a 16-row
  scoop costs ~51 ns per *irrelevant* entity id.
- **A one-file edit pays O(world)** (`incremental_settle`): linear replan +
  scan cost for a constant-size change.

This is based on the short performance report (release playground run ~0.15s)
plus a code review of `crates/bowl`. The report's six issues are all real; the
review found three additional engine-level costs, one of which (dead
fingerprint reuse for derived outputs) likely explains a large share of the
replanning churn the report attributes to streaming commits.

## Findings confirmed from the report

1. **Full phase replanning after every commit** — `Bowl::run_phase_streaming`
   sets `needs_plan = true` after *every* finished invocation, including
   commits that were discarded as stale or made no tracked change. Each wave
   clones the snapshot, clones the whole memo table, and re-plans every system
   in the phase.
2. **Dense entity scans** — every `QueryParam::rows` implementation
   (`&T`, `(Entity, P...)`, `(Mut<T>,)`, bare `Entity`, `CowQueryParam`,
   `external_mut_rows`) enumerates `0..next_entity_raw()` and calls
   `snapshot.has::<T>()` per id (a `HashMap` lookup + `BTreeMap` lookup each).
   Cost grows with historical entity churn, not live data.
3. **Derived output churn** — `commit_system_run` does
   `remove_derived_owned(owner)` then re-applies commands, and
   `Commands::insert` (spawn) allocates a fresh entity id on every rerun.
4. **Views amplify work** — `View`'s `SystemParam::access` materializes the
   access set of *every* view row, per invocation, per planning wave; and
   `View::new` re-runs the dense scan per invocation. Combined with per-`AstDef`
   invocations this is O(N²) planning work before any application-level O(N²).
5. **Memo and snapshot cloning** — per wave: one snapshot clone + one full
   `memo.clone()`. See also new finding B below (it is worse than the report
   assumed).
6. **No query indexes** — `Where<Eq<T>>` filters scan and compare per row.

## Additional findings from code review

- **A. Fingerprint reuse is dead for derived outputs (high impact).**
  `World::insert` reuses the old revision when the new fingerprint matches the
  entry already stored on that entity — but commits run
  `remove_derived_owned(owner)` *before* applying insert commands, so the old
  entry is gone by the time the insert looks for it
  (`bowl.rs::commit_system_run`, `world.rs::insert`). Consequence: a system
  that reruns and emits byte-identical `#[component(hash)]` output still bumps
  the global revision twice (remove + insert), marks the commit as
  `needs_followup`, forces a full replan, and invalidates every downstream
  memo dep on that component. The fingerprint machinery only ever helps base
  inserts and `Cow`/`Mut` mutation today. Fixing the commit order (diff instead
  of remove-then-insert, report item 4) is therefore not a nice-to-have; it
  restores the intended early-cutoff semantics.

- **B. `remove_derived_owned` is O(all component entries) per commit.**
  It iterates *every* store and runs `entries.retain(...)` over every entry —
  even for invocations that own nothing. With E total component entries and C
  commits per settle, commit cost is O(E·C) independent of output sizes. An
  owner → {(TypeId, Entity)} index makes this O(owned outputs).

- **C. Redundant planning and cloning in the system layer.**
  - `FunctionSystem::stream_runs` clones the *entire snapshot once per planned
    invocation* (`system.rs:990`), on top of the per-wave clone. Sharing one
    `Arc<Snapshot>` per wave removes this.
  - `has_work` (used by `on_start`/`on_complete`/`on_settled` wrappers) runs
    `plan_invocations` in full, then `run`/`stream_runs` plans again — wrapped
    systems plan twice per wave.
  - `run_settled_hooks` re-runs full batch planning of every non-cleanup
    system on each settle iteration, and each settle wave clones the systems
    vec and memo again.
  - `entity_revision` (used by `DerivedFrom` capture and
    `cleanup_stale_derived` via `WorldMetaView`) iterates every store per
    entity; cleanup cost scales with #stores × #DerivedFrom rows × sources.
  - `conflicts_with_running` is a nested O(candidate access × total running
    access) loop; View-inflated access vectors make this quadratic-ish.

## Plan

Ordered by (expected payoff ÷ risk). Steps 1–4 need no public API changes.

### 1. Instrumentation first

Add internal counters behind a `stats` cargo feature (or `tracing` targets),
collected on `Bowl` and dumpable after `settle()`:

- evaluation generations, phase planning waves, settle-hook waves
- snapshot clones / memo clones (and their entry counts)
- `stream_runs` calls, invocations planned / memo-skipped / conflict-deferred
- commits applied, commits discarded stale, commits with no revision change
- query rows scanned vs rows matched (per dense scan)
- derived outputs removed / reinserted, revision bumps, max entity id, memo size

Also add a repeatable measurement target: a `--release` playground run (with
`short_sleep` stubs kept empty) timed via `hyperfine`, or a small criterion
bench in `bowl` that models N files × M systems. Everything below must show up
in these numbers before/after.

### 2. Store-driven row enumeration (replaces dense scans)

Stores are `BTreeMap<Entity, _>`, so ordered iteration is already available:

- `&T` / `(Mut<T>,)` / `Cow` rows: iterate `Store::<T>::entries` keys directly.
- `(Entity, A, B, ...)`: iterate the smallest participating store, probe the
  others with `has`. Store sizes are cheap to expose (`entries.len()`).
- `With<T>` / `Without<T>` / `Where` stay as probes on the primary iteration.
- Bare `Entity` rows (e.g. `Query<Entity, With<T>>`) have no primary store;
  add an optional `QueryFilter::candidates(snapshot) -> Option<Vec<Entity>>`
  hint so `With<T>` can supply its store's keys, falling back to the dense
  scan only for genuinely unconstrained `Query<Entity>`.

This also fixes `external_mut_rows` and `View::new` for free since they share
the same `rows()` implementations.

### 3. Fix the commit path (derived-output diffing + owner index)

Two coupled changes in `commit_system_run` / `World`:

- **Diff instead of remove-then-insert.** Collect the (TypeId, Entity) targets
  of the new command buffer first, apply inserts (letting `World::insert` see
  the previous entry so equal fingerprints keep their revision), then remove
  only stale leftovers owned by the invocation. Result: idempotent reruns stop
  bumping revisions, stop triggering `needs_followup`, and stop invalidating
  downstream memos — restoring the cutoff that fingerprints were meant to give
  (finding A).
- **Owner → outputs index.** Maintain `HashMap<SystemInvocation,
  HashSet<(TypeId, Entity)>>` in `World` so removal/diffing touches only the
  invocation's own outputs instead of retaining over every store (finding B).
  `has_derived_owned` and bound-entity cleanup use the same index.
- **Stable derived-entity identity (stretch).** Key spawned derived entities
  by (owner, spawn-index) and reuse the previous entity id on rerun. This stops
  entity-id growth from reruns (shrinking scans and `DerivedFrom` churn). Can
  be a follow-up; the first two changes don't depend on it.

### 4. Smarter streaming replanning

Incremental steps, each measurable on its own:

- **Don't replan on no-op commits.** Only set `needs_plan = true` when the
  commit reported `needs_followup` *or* freed access rows that previously
  caused a conflict deferral (track whether any invocation was deferred).
- **Batch ready completions.** Drain all immediately-ready results from the
  `FuturesUnordered` (poll-now loop) and commit them together before replanning
  once, instead of replan-per-completion.
- **Share, don't clone.** One `Arc<Snapshot>` and one `Arc<MemoTable>` per
  planning wave, passed into `stream_runs`/planned futures (removes the
  per-invocation snapshot clones and per-wave memo clones — finding C).
- **Dirty-set filtered replans.** Record the (TypeId) set touched by commits
  since the last wave; replan only systems whose query/filter component sets
  intersect it. Requires each system to expose its static component footprint —
  derivable from `QueryParam`/`QueryFilter` with a new `component_types()`
  method. Do this last; the first three bullets may already collapse the wave
  count.
- Also: stop double-planning in `has_work` (plan once, reuse the result), and
  merge `commit_system_run`'s two state locks into one.

### 5. Indexes for external equality filters

Add a per-component equality index for hashable components, maintained on
insert/remove in `World`:

```text
HashMap<TypeId, HashMap<u64 /* fingerprint */, SmallVec<Entity>>>
```

`Where<Eq<T>>` (for `T: Component + Hash + PartialEq`) resolves candidates via
the index and verifies with `PartialEq` (hash collisions). This serves
request-style lookups (`FilePath -> Entity`) for both external scoops and,
later, system-side filters. `Gte` and friends keep scanning until a need for
ordered indexes is demonstrated.

### 6. Later: storage classes and scheduling polish

- Optional dense storage (`#[component(storage = dense)]`) with
  `View::iter_dense()`/chunks — only after 2–5, since store iteration changes
  the shape of everything here.
- Cheaper access-conflict checking (hash the running access set instead of the
  nested-loop `conflicts_with_running`; consider component-level pre-filter),
  and consider summarizing `View` access at component granularity instead of
  per-row.
- `entity_revision` caching or a per-entity max-revision map if
  `cleanup_stale_derived` shows up in the counters.
- Memo hygiene: verify memo entries for entities removed via
  `commands.remove(...)` are evicted (only the bound-entity path clears memo
  today); add a counter for memo size to catch unbounded growth in daemon-style
  runs.

## Suggested order

1. Instrumentation + repeatable benchmark (validates everything else).
2. Store-driven row enumeration (§2) — biggest simple win, no semantic change.
3. Commit-path fix (§3: diff + owner index) — restores fingerprint cutoff,
   should cut replanning waves dramatically on its own.
4. Streaming replanning improvements (§4), guided by the wave/commit counters.
5. `Where<Eq<T>>` index (§5).
6. Dense storage / scheduling polish (§6).

Note the interaction: §3 reduces how *often* replanning happens, §4 reduces
what each replan *costs*, and §2 reduces what each plan/scan costs. Measure
after each step — it is plausible that §2+§3 alone get most of the win and §4
can stay minimal (no-op-commit skip + Arc sharing only).
