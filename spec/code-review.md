# Code Review & Architecture Overview

High-level pass over the whole codebase (2026-07-05, after the two
optimization rounds). Each section ends with candidate deep dives. Findings
are ranked ⚠ (correctness/soundness), ● (should fix), ○ (nice to have).

## 1. DRY pass — repeated patterns worth collapsing

The engine is small (~7k lines) and mostly clean, but a few patterns have
copy-drifted:

- ● **Four filter trait families.** `QueryFilter` (system-side),
  `ExternalFilter` (external state-based), `ExternalQueryFilter` (blanket
  adapter), and `FilterExpr` (runtime-arg expressions). `With<T>` is
  implemented in all four; `Without<T>` in three. Adding the index-candidates
  hook meant threading the same method through three of them. A single
  filter trait parameterized over an arg-provider (unit for system filters,
  `&QueryArgs` for external) would halve this surface.
- ● **Three row-enumeration families.** `QueryParam`, `QueryPart`, and
  `CowQueryParam` each reimplement rows/matches/deps/access for `&T` and
  `Mut<T>`. The `rows_hinted` retain-match body now exists in four places
  (`&T`, `(Mut<T>,)`, entity-tuple macro, `cow_rows_hinted`).
- ○ **Wrapper-system boilerplate.** `OnStartSystem` / `OnCompleteSystem` /
  `OnSettledSystem` have near-identical `stream_runs` bodies (owner synth,
  memo Arc clone, boxed run). One generic wrapper with a callback-position
  enum would collapse ~150 lines.
- ○ **`ScoopBuilder::for_each`** is duplicated verbatim for `Query` and
  `Named<Tag, Query>` (only the scope differs). Same for the two `Mut`
  `ExternalScoop` impls.
- ○ **`State` pending-generation get-or-insert** (`if pending.is_none() {
  pending = Some(next) }`) appears five times in `bowl.rs`; wants a
  `State::mark_dirty()` helper (which could also invalidate the snapshot
  cache in one place — see §3).
- ○ `with_component_original` / `with_component_mut` differ by one revision
  check; `component_dep_if_tracked(...).into_iter().collect()` is repeated.

Deep dive candidate: a refactor plan unifying the query/filter trait families
without breaking the public API.

## 2. Formal spec vs implementation

`spec/formal-semantics.md` holds up well overall — the memoization rule,
invocation identity, commit-currency check, phase ordering, settled boundary,
and DerivedFrom relation all match the code. Divergences, all introduced by
the optimization rounds and mostly *refinements* that the spec should adopt:

- ● **Replace-by-invocation is now diff-by-invocation.** Spec §Derived
  Outputs says "remove old Derived(i) outputs, apply new commands". The
  implementation applies commands over the old outputs and removes only
  what was not re-emitted, so equal fingerprints keep revisions. The settled
  fixed point is the same, but the *revision trace* differs (the spec's
  version implies idempotent reruns bump revisions; the code deliberately
  does not). The spec's fingerprint section (§Memoization) actually promises
  the code's new behavior — the two spec sections now contradict each other.
- ● **Replanning cadence.** Spec §Streaming Evaluation says "re-plan from
  the new W_live" after every commit. The code batches completed
  invocations and replans only on world-change / stale run / freed
  conflict. Same fixed point (a no-op commit cannot enable new rows —
  worth stating as a lemma in the spec), but the spec text is stale.
- ● **Entity id allocation.** Spec §Determinism says derived entity id order
  is not part of the deterministic surface. Id *reuse* (round 2) goes
  further: a spawned output's id is now stable across reruns keyed by
  (invocation, spawn ordinal). This is stronger and user-observable —
  should be specified, including the caveat that a system whose spawn
  *order* is data-dependent will see ids swap meaning between slots.
- ○ **Mutation counter.** The spec models one revision counter; the
  implementation now has two (`revision` for tracked deps, `mutations` for
  structural change). Worth one paragraph, since "untracked components
  still wake the runner on insert/remove but not on value change" is a
  semantics statement.
- ○ `access-scheduling.md` describes external guarded reads participating in
  the conflict protocol with internal writes — that is implemented via cell
  locks, but *not* via the planner (external reads are invisible to
  `running_access`). The spec is aspirational here; the gap is exactly the
  soundness hole in §3.

Deep dive candidate: update the spec to the implemented semantics and write
the "no-op commits preserve the fixed point" argument down.

## 3. Ownership model, concurrency soundness, deadlocks

### Systems without `Mut<T>` (read-only)

Sound and simple: rows are enumerated from an immutable structural snapshot,
values are read through per-cell read guards held by the `Query`/`View`
wrapper, commits go through buffered commands under the state lock. Readers
of the same cell overlap freely.

### ⚠ `Query::item(self)` releases read locks while borrows live — FIXED

*Fixed 2026-07-05:* read guards are now owned by the running invocation frame
(created before `SystemParam::fetch`, dropped after the system function
returns), so consuming the query cannot release row locks early and external
`Mut` writes block until the reading system finishes — the protocol
`access-scheduling.md` prescribes. No public API change; `Query` no longer
carries guards (its manual `unsafe Send/Sync` impls are gone too). Residual
notes: the blocking-writer window in the deadlock section below is now wider
by design, making swap-on-write the natural follow-up; and a system that
reads `&T` on a row and calls `with_latest` on the same row inside itself now
self-deadlocks instead of racing (hang beats UB, but worth a doc note).

Original finding, kept for context:

`item(self)` moves the row data out and **drops the guard store**, releasing
the cell read locks, while the returned `&T` references remain usable (their
lifetime is tied to the snapshot, which the runner keeps alive — so the
memory stays valid, but the *lock* was the only thing preventing a writer).
Every example system does `let (e, x) = query.item();`. If an external
`Mut::with_latest` (which does **not** wait for settle — it takes the state
lock directly and can run mid-evaluation) writes the same cell, that is a
data race on `UnsafeCell` contents: UB, not just staleness. The playground's
task storm exercises exactly this shape (hover systems reading `FileText`
concurrently with `mutate_file_by_path`).

Internal system-vs-system conflicts are protected (planner access sets), and
`Cow::for_each` is protected (it settles first, and evaluation cannot start
while it holds the state lock). The hole is *external `Mut` vs running
system after `item()`*.

Fixes, in order of preference:
1. Make `item(self)` keep the guards (return a guarded wrapper that derefs
   to `T`, or deprecate it in favor of `as_item`/destructuring patterns that
   borrow).
2. Make `with_latest`/`with_original` wait for the settled boundary like
   `Cow::for_each` does (changes latency semantics).
3. Declare external `Mut` writes to the planner (the access-scheduling
   spec's stated end goal).

### Concurrent scoops

- **Read-only scoops** are well-behaved: single-flight settle (one runner,
  everyone else waits on generation waiters), shared cached snapshot,
  overlapping read guards. This is the strongest part of the design.
- **Scoops with `Mut<T>`** return inert handles; mutations serialize on the
  state lock; `with_original` gives optimistic-concurrency semantics.
  Between *external* parties this is sound. The gap is only external-vs-
  internal as above.
- **`take()` vs held results:** `Arc::try_unwrap` fails if any old
  `QueryResult` (or the snapshot cache — handled) keeps the cell alive, and
  the failure surfaces as `TakeError("missing component")` — wrong message
  for "still borrowed elsewhere", and a availability landmine in daemons.

### Deadlock likelihood

The cells use a **blocking Condvar** protocol on executor threads. Ranked
scenarios:

1. ● *Writer blocks holding the state lock.* — **FIXED 2026-07-05:**
   `with_latest`/`with_original` now use `try_write` in a retry loop that
   releases the state lock and yields between attempts, so they never wait
   on a cell while holding it.
2. ● *Single-threaded executors.* Largely defused by the same change for
   external writers; system `MutRef` fetches still block on external read
   guards (the intended external-read-blocks-internal-write protocol), so
   holding `collect()`ed rows across an await of a settle remains a
   documented anti-pattern.
3. ○ *`for_each` re-entrancy* is documented ("do not call back into the same
   bowl") but not enforced; violation deadlocks on the state lock.

**System `Mut` redesign (2026-07-05):** system-side mutation is now
`MutRef<'_, T>` — a scheduler-granted in-place `&mut T` (no `Clone`), backed
by an invocation-held write guard. Revision/fingerprint bookkeeping happens
at commit, and the commit absorbs the row's own write into the memo entry,
which fixed a pre-existing convergence bug: `with_latest` inside a system
invalidated the system's *own* commit, silently double-running every
system-side mutation and requiring user-written idempotence to settle
(regression tests now pin single-run semantics for non-idempotent
mutations). `Mut<T>` remains the external scoop type with optimistic
`with_original`/`with_latest` handles. Known documented residual: `MutRef`
writes are live and non-revocable — if a commit is discarded as stale (only
external interference can cause that), the mutation persists while the
commands roll back.

Reduction strategies (deep dive candidate): swap-on-write instead of
in-place write when readers are active (writers never wait — readers keep
the old value, which *is* snapshot semantics); an async-aware cell lock; or
planner-visible external access. A `try_write`-with-diagnostics fallback
would at least convert silent deadlocks into actionable panics.

### Cancellation safety ⚠

`run_evaluation` takes the memo table (`std::mem::take`) and marks
`running_generation` before awaiting user futures. If the *driving caller's
future is dropped* at an await point (timeout, select, client disconnect —
routine in daemons):

- the memo table is lost (full recompute at best), and
- `pending_generation` was consumed while `running_generation` stays `Some`,
  so `completed_generation` never reaches the target: subsequent
  `ensure_evaluated` calls acquire the runner, find no pending generation,
  do nothing, and **spin forever**; waiters are never woken.

The async-single-flight spec lists "what does cancellation mean for a
waiter?" as an open question; for the *runner* it is currently "the bowl is
wedged". This needs a drop-guard that restores memo/generation state (or a
detached-runner design where evaluation is spawned and never cancelled by
waiter cancellation). High priority for anything long-running.

## 4. Code review — error handling and robustness

- ● **User-reachable panics** that should be `Result`s or diagnostics:
  duplicate/missing query args (`.args()` typos panic at runtime), singleton
  re-registration on a different entity, "bundles can contain at most one
  singleton marker", and the `CommitLimit` assert (non-convergence should
  report *which systems* kept committing — TODO §9 agrees).
- ● **`TakeError` conflates** "system never produced it" with "still
  referenced by a live snapshot/result" (see §3).
- ○ Lock-poisoned `expect`s are fine for now but poisoning policy should be
  decided once panics can originate in user systems (a panicking system
  currently unwinds through the runner and poisons nothing critical —
  worth a test).
- ○ "query row referenced a missing component" in fetch is genuinely
  unreachable (row and fetch use the same snapshot) — a debug_assert +
  comment would document why.
- ○ No tracing/metrics at all (planning waves, commits, stale discards,
  memo hit rate). The bench work would have been substantially easier with
  the counters from `performance-plan.md` §1 — still worth adding behind a
  feature.

## 5. Architecture pass

**Good:**
- The conceptual core — *facts in, memoized per-row systems derive more
  facts, callers scoop the settled boundary* — is crisp and the
  implementation genuinely follows it. `Query` (tracked) vs `View`
  (ambient) is a sharp, teachable distinction.
- Single-flight evaluation with waiter piggybacking is the right shape for
  a shared handle, and the runner/state lock split is disciplined.
- Snapshot isolation via structural clone + shared cells gives cheap,
  correct reads without `T: Clone`, and the snapshot cache makes the
  settled-read path O(1).
- Ownership diffing + owner index + spawn-slot reuse give a coherent
  derived-state lifecycle with real early cutoff.
- Spec-driven development and the bench harness are unusual strengths for a
  prototype; the per-step bench methodology caught two real bugs.

**Needs work:**
- ● **Concurrency ≠ parallelism.** All invocations are polled inside the
  single runner task (`FuturesUnordered`). Async I/O overlaps, but CPU-bound
  systems (parsing!) serialize on one core. Futures are already `Send`;
  the missing piece is an executor-agnostic spawn hook. For the compiler
  use case this is likely the next big perf lever.
- ● The trait-family sprawl (§1) makes every cross-cutting feature (hints,
  access, deps) an N-way edit; it will resist contributors.
- ● `entity_revision` = max over all stores is O(#component types) per call
  and makes *any* tracked change on a source entity invalidate DerivedFrom
  relations — coarse false-invalidation (spec acknowledges).
- ○ `View` staleness is a footgun: correct usage requires understanding
  that view contents do not rerun the system. The duplicate-defs checker is
  *only* correct because insert order + id tiebreak happen to work out.
  `TrackedView` (TODO §10) or at least a doc pattern is needed.
- ○ Memo entries for component-removal (not entity removal) still linger;
  bounded but untidy.

**Scaling judgment (large, long-running systems):**
- Read path: excellent now (O(1) settled reads, indexed equality lookups).
- Write path: every settle round pays at least one O(live entries)
  structural snapshot clone per planning wave; fine to ~10⁵ facts, painful
  at 10⁶⁺. Persistent maps (`im`-style) would make snapshots O(1) at the
  cost of slower point ops — a decision worth benchmarking *when* worlds
  get big, not before.
- Long-running: memo/world hygiene is now mostly right, but cancellation
  wedging (§3) and the absence of observability are the real blockers for
  daemon deployment. Global state-mutex serialization of external mutations
  will eventually bottleneck many-writer workloads.

## 6. The Porridge concept

The idea sits in a real gap: **salsa-style incremental memoization with
ECS-style data modeling and an async runtime**. Compared to neighbors:

- vs **salsa** (rust-analyzer): salsa keys memoization by function+interned
  key and has cycle detection and durability tiers; Porridge keys by
  *entity rows*, which makes fan-out ("run per file") and heterogeneous
  facts much more natural, and `scoop`/`take` give it a service boundary
  salsa lacks. Porridge lacks cycle handling and fine-grained
  value-level dependency tracking (View is the escape hatch, with sharp
  edges).
- vs **Bevy ECS**: same data model instincts, but Porridge's memoized
  invocations + settled boundary are something Bevy schedules simply don't
  do; conversely Bevy's archetype storage and parallel executor are years
  ahead.
- The **request-as-entity** pattern (`insert().bind().take()`) is the most
  original piece — modeling RPC as facts that flow through the same
  incremental machinery is genuinely elegant, and the bound-entity cleanup
  semantics show the design has been thought through.

Risks to the concept: entity-id-keyed identity needs an interning story for
compiler-shaped keys (paths, symbols) or memo identity gets fragile;
and per-row memoization has overhead per fact that pure-function memoizers
avoid — the niche is workloads with many medium-grained facts, not
millions of tiny ones.

## 7. Opinions & roadmap

**Distance from production:** this is a good prototype ~2–3 focused months
from "usable in anger for a language server", gated on (in order):

1. Fix the `item()` guard-drop unsoundness and decide the external-mutation
   protocol (§3) — correctness first.
2. Cancellation-safe runner (drop guards or detached evaluation task).
3. Error model: replace user-reachable panics with typed errors +
   non-convergence diagnostics naming systems.
4. Parallel execution via spawn hook.
5. Observability (counters + tracing spans per invocation).
6. Trait-family consolidation so the next ten features are cheap.

**Future ideas beyond the TODO:** subscription scoops (watch a query,
get diffs per settle — pairs naturally with the replication spec);
durability tiers like salsa (mark `SystemImportDb` as rarely-changing to
skip dep checks); interned key components with derive support; a
`#[system]` attribute macro to shrink signatures; snapshot persistence for
warm daemon restarts; and property-based tests driving random
insert/mutate/remove sequences against a naive reference implementation —
the stale-spawn bug this session found is exactly the class such a harness
catches early.

## Suggested deep-dive order

1. Soundness fix design for `item()` + external `Mut` protocol (⚠, small).
2. Cancellation-safe evaluation (⚠, medium).
3. Error-model overhaul (●, medium).
4. Trait consolidation refactor plan (●, large).
5. Parallel executor hook + bench (●, medium).
6. Spec refresh to post-optimization semantics (●, small).
