# Porridge TODO

This is the current implementation roadmap for the async `bowl` runtime and the
toy language playground. Items are ordered roughly from most important to least
important.

## 0. Major Direction Notes

- Move the runner toward streaming evaluation:
  - plan from the current world
  - run planned invocations concurrently
  - commit each completed invocation as soon as its captured deps are still
    current
  - re-plan after each commit
  - treat `on_settled` as the global external boundary
- Keep `on_start` and `on_complete` local to a system's planned work batch.
- Ordering hierarchy for consumers (see `spec/epochs.md`): tracked reads
  (joins) dissolve ordering entirely; phases order within a generation;
  ephemeral markers signal across settles. Markers must not guard reads that
  need present-tense consistency — they are recorded claims that go stale
  when new inputs land in their generation, and marker-gated work is
  invisible to settledness checks while the marker is absent.
- Done: epoch-scoped input batching + preemptive external muts
  (`spec/epochs.md`): settles run against frozen input sets with watermark
  promotion (markers are sound as cross-settle state machines), external
  `Mut` preempts by default — tiered drop, boundary window, Startup-slot
  restart — with `.deferred()`/`.preempting()` modifiers, a per-generation
  preemption budget, and the `.last_settled()` stale-read scoop. Still
  open: `Cow` `for_each` epoch gating, budget configurability.
- Treat `DerivedFrom` as the standard pattern for revision-scoped derived facts
  such as diagnostics, hover results, indexes, and summaries.
- A ~10k-line compiler port (dsql: picante → porridge) validated the model —
  source management, memoization, and orchestration layers dissolved into
  components and systems, and the frontend crate disappeared. Its seven
  friction reports are folded into the sections below (anchor-ordering trap
  §2, external remove + spawn ids + revision fingerprints §1, View footgun
  §10, intra-phase ordering §12, explain facility §14).
- Explore Porridge as a long-running daemon/client runtime.
- Keep a Replicon-like replication/change-stream layer as a future plugin
  track. The engine capabilities it needs are implemented
  (`settled_revision` + `changed_since` cursor reads,
  `bowl.entity(e).insert(..)` targeted inserts, `drain` reads,
  `next_settle` notifications) — see
  `spec/daemon-client.md`, "Engine Support for Out-of-Core Replication and
  Streaming", including the state-sync vs consumed-stream distinction,
  subscriptions-as-facts scoping, and the remaining tombstone gap for
  removal replication.

See:
- `spec/streaming-evaluation.md`
- `spec/lifecycle-and-ephemeral.md`
- `spec/derived-from.md`
- `spec/daemon-client.md`
- `spec/access-scheduling.md`
- `spec/epochs.md`
- `spec/ideas/replication.md`

## 1. Public API Polish Before Larger Migration

- Stabilize naming for the public API:
  - `Bowl`
  - `Query`
  - `View`
  - `Cow`
  - future `Mut`
  - `Where`
  - `DerivedFrom`
  - `cleanup_stale_derived`
  - `on_start`
  - `on_complete`
  - `on_settled`
  - `Phase`
  - `scoop`
- Remove playground/debug prints such as `AstAvailable insert/remove`.
- Add `#[derive(SystemParam)]` param bundles: named structs of system params
  so aggregating systems escape the 8-arity ceiling and signature noise.
  Must support nesting (a bundle member may be another bundle) so recurring
  view clusters get named once. Guidance to document: bundles stay minimal
  and per-system; share via small nested bundles, never a kitchen-sink one —
  over-borrowing widens declared access and creates false scheduler
  conflicts with `MutRef` systems.
- Done: optional query parts — `Option<&T>` in a row tuple matches whether
  or not `T` is present, and *both* transitions invalidate: presence
  records the revision, absence records a `None`-revision dep that goes
  stale the moment the component appears. An optional part never drives
  row enumeration (`store_len = MAX`) and declares read access even when
  absent so writers creating `T` still serialize. Untracked components
  stay untracked (no dep either way). Still open: the outer-join form
  (§7) — the same missing shape on the bound side of a join.
- Done: external component removal — `bowl.entity(e).remove::<T>()`
  mirrors targeted inserts with the same epoch semantics (deferred
  mid-epoch, `.preempting()` to force a boundary); friction 2.
- Done: external despawn — `bowl.entity(e).despawn()` removes the whole
  entity with the same epoch semantics as targeted removal (deferred
  mid-epoch, `.preempting()` to force a boundary), riding the existing
  `remove_entity` machinery (relationship retraction included). The
  runner reconciles at evaluation start: memo entries touching removed
  entities drop, and derived outputs anchored to them cascade
  (transitively) through the `derived_owners` index. Needed for
  hand-rolled request entities and externally-owned lifecycles (dsql
  feedback).
- Done: `Commands::insert` returns the reserved `Entity` so a system can
  link parent/child facts by entity id within one buffer. Ids reserve at
  buffer time against the invocation's previous spawn slots (shared atomic
  allocator for new slots), so idempotent reruns keep entity identity;
  singleton bundles supersede the reservation with the existing singleton
  entity. Friction 3.
- Done: `#[component(revision)]` — the fingerprint is the `revision: u64`
  field verbatim, so large components change-detect without hashing (or
  being able to hash) their payload; mutually exclusive with
  `#[component(hash)]`. Friction 6.
- Rewrite the README for the schema era — it predates declared outputs,
  schemas, strict spawns, facets, the builder, and plugins, so its
  examples (bare `Commands`, `Bowl::new()`, `add_system`) no longer
  compile. Full rewrite, not a patch. Keep it aligned with the final
  mental model:
  - components-only storage
  - immutable snapshots for reads
  - clone-on-write external updates
  - future scheduler-level mutable access
  - systems as memoized per-row functions
  - `View` as ambient/non-invalidating context
  - `on_settled` for readiness gates
  - `Settle` for reaping ephemeral facts (removals apply within the
    settle; inserts defer to the next run)
- Add one realistic integration test for:
  - file changed
  - parse
  - AST generation
  - diagnostics
  - hover/request output
- Document the diagnostics pattern:
  - diagnostics should usually be their own entities
  - use `DerivedFrom::new(entity)` or `DerivedFrom::many([..])` to tie them to
    source facts
  - `cleanup_stale_derived` removes stale diagnostics when any source entity
    changes or is removed
- Audit examples and docs before porting a larger codebase.

Current shortcut:
- The API is usable, but still prototype-shaped.
- The playground is the main documentation.
- Several examples still include tracing/debug prints.

## 2. Stabilize Output Ownership And Invalidation

- Done: the `DerivedFrom` anchor-ordering trap (dsql port, worst friction)
  is fixed — anchor revisions resolve at buffer end (`DerivedFrom` inserts
  defer through `World::pending_derived_from` until the command buffer has
  fully applied), so a derived entity emitted before a same-buffer write
  to its anchor is no longer born stale. A `debug_assert` in the snapshot
  path catches any apply site that forgets to flush.
- Guard the external-writer/anchor trap (dsql feedback: the `OpenBuffer`
  mistake): `DerivedFrom` anchors are deliberately entity-scoped, so an
  external tracked insert onto an anchor-source entity reaps *everything*
  anchored to it — including when the inserted component (an open-buffer
  marker) is semantically unrelated. Remedies, graded: document the
  existing outs (`#[component(untracked)]` markers, own-entity markers à
  la the demand pattern); add a debug warn when an external tracked
  insert lands on an entity that anchors derived facts (cannot be a
  panic — a `FileText` edit is the same operation and *should*
  invalidate); long term, revisit component-scoped anchor granularity.
- Define stronger ownership semantics for derived outputs.
- Decide how `DerivedFrom` and invocation-owned outputs compose:
  - invocation ownership replaces outputs from a rerun
  - `DerivedFrom` removes outputs when source facts change even if the producer
    does not rerun
- Make it clear when rerunning one system invocation should remove:
  - only components it wrote on existing entities
  - derived entities it spawned
  - transitive outputs derived from those entities
- Replace the current coarse invocation-owner cleanup with a model that can
  describe output families explicitly.
- Done: reruns diff outputs instead of remove/reinsert, so equivalent
  hash-stable output keeps its revisions, and spawned outputs reuse their
  entity ids slot by slot.

Current shortcut:
- `Commands::entity(entity).insert(component)` and `Commands::insert(bundle)`
  attach outputs to the current system invocation. On rerun, old outputs for
  that invocation are removed before new commands apply.
- `DerivedFrom` exists and captures source entity revisions on insert.
- `cleanup_stale_derived` exists as a cleanup-phase system.
- This is enough for the playground, but it is not yet a full model for
  replacing complex derived output graphs.

See:
- `spec/derived-from.md`

## 3. Add Lifecycle Hooks And Ephemeral Coordination

- Add lifecycle hook registration:
  - `on_start`
  - `on_complete`
  - `on_settled`
  - possibly `on_idle` / `on_rest`
- Use coarse system phases through `system.run_during(Phase::...)` where a
  full lifecycle hook would be too much API.
- Keep `on_complete` local to actual planned work for a system.
- Use `on_settled` for readiness markers and phase-transition gates.
- Add an `Ephemeral` marker component for generation-scoped coordination facts.
- Add remove commands so cleanup can be expressed as a normal hook.
- Add an `EphemeralPlugin` that removes ephemeral facts at evaluation complete.

Current shortcut:
- `Phase` and `SystemExt::run_during` exist for `Startup`, `Evaluate`,
  `Complete`, and `Settle` (formerly `Cleanup`; renamed when its inserts
  became next-run inputs).
- `Component` has lifecycle hooks for insert, remove, and entity removal.
- `on_start`, `on_complete`, and `on_settled` exist.
- `on_complete` is local to planned/invalid work.
- The playground models `AstAvailable` as an ephemeral singleton readiness
  marker emitted by `generate_ast.on_settled(...)` with a cleanup-phase system.
- Buffered entity/component remove commands exist.
- There is no plugin system or lifecycle hook registry.

See:
- `spec/lifecycle-and-ephemeral.md`
- `spec/ideas/execution-cycles.md`

## 4. Add Singleton Component Support

- Keep the components-only model.
- Add singleton insertion/upsert support backed by an internal index:

```rust
singleton_entities: HashMap<ComponentId, Entity>
```

- Make singleton component revisions follow normal component revision rules.
- For the first pass, use manual singleton marker insertion through the normal
  `insert` path:

```rust
commands.insert((Singleton::<SystemImportDb>::new(), SystemImportDb { .. }));
commands.insert((Singleton::<AstAvailable>::new(), AstAvailable, Ephemeral));
```

- Later, after recursive bundle flattening exists, add `.singleton()` as
  ergonomic sugar.
- Consider a `Single<T, F>` query/system param that validates exactly one
  matching entity.
- Keep insertion enforcement separate from query validation.

Current shortcut:
- `Singleton<T>` marker behavior and the singleton index exist.
- There is no `.singleton()` bundle sugar or `Single<T>` query param.

See:
- `spec/singletons.md`

## 5. Harden BoundEntity Take Semantics

- Continue with the current `insert(...).await.bind().take::<T>().await` model.
- Keep destructive reads as methods on `BoundEntity`, not normal queries.
- Decide whether an explicit `close().await` is useful in addition to consuming
  `take`.
- Improve `TakeError` reporting if missing required output becomes common.
- Add more tests for missing required components and multi-component failure
  behavior.

- Close the internal request/response gap (dsql feedback: the
  insert→derive→take→reap lifecycle is hand-rolled five times, and
  nothing reaps): `BoundEntity` covers *external* callers only — a system
  spawning a request via `Commands` gets no handle and no reaper, so
  internal request entities leak. Design an engine-owned transient-entity
  policy (a `Transient`/TTL marker reaped at the following settle, or
  bound-like handles for command-spawned requests), which also removes
  the temptation to key bulk derivation off request entities.

Current shortcut:
- `BoundEntity::take<T>` is implemented and consuming.
- `Option<T>` works for optional outputs, and tuples take multiple outputs at
  once.
- Dropping a bound handle without `take` queues cleanup for the next bowl
  operation because `Drop` cannot `await`.

## 6. Improve Bound Cleanup

- Make cleanup semantics explicit:
  - when cleanup runs
  - what gets removed
  - which memo entries are invalidated
  - whether cleanup itself should bump revisions or be isolated from normal
    evaluation
- Add more tests for command-spawned request outputs and transitive cleanup in
  the async `bowl` crate.
- Decide whether cleanup of a dropped bound entity should run before or after
  the next evaluation when both cleanup and new inputs are pending.

Current shortcut:
- `take` cleans immediately after extracting requested outputs.
- Drop cleanup is deferred to the next bowl operation.
- Cleanup is scoped by system invocation keys that touched the bound entity.

## 7. Improve Indexed And Filtered Queries

- Design typed filters in the query shape, for example:

```rust
let diagnostics = db.scoop::<Query<
    (Entity, &Diagnostic),
    Where<And<Eq<FilePath>, Gte<Severity>>>,
>>()
.args((FilePath(path), Severity::Warning))
.await;
let rows = diagnostics.collect();
```

- Support at least:
  - `Eq<T>`
  - `And<A, B>`
  - `Or<A, B>`
  - `Not<A>`
  - possibly range predicates such as `Gte<T>`
- Make binds type-safe.
- Add indexes for common equality lookups instead of scanning and filtering
  every entity.
- Ensure indexed filters work for both normal queries and mutable/update-style
  queries later.
- Add additional comparison predicates if useful:
  - `Gt<T>`
  - `Lt<T>`
  - `Lte<T>`
- Done: bound `Where<Eq<T>>` filters in system queries (relational joins) —
  the argument binds to the sibling param providing `&T`, pruning the
  cartesian product to matching pairs with per-pair memoization. Design and
  current shortcuts in `spec/joins.md`; exercised by the playground's
  namespace entity. Still open: scoped views, `Named`-qualified binds,
  index-probe planning.
- Support `And` for system-side filters: plain conjunction (`With` +
  `Without` cannot combine on one query today) and compound join keys
  (`And<Eq<Name>, Eq<Arity>>` for overload resolution). See the operator
  matrix in `spec/joins.md`.
- Done: outer joins — `Option<Query<Q, Where<Eq<K>>>>` runs one invocation
  per matched pair as before, plus exactly one `None` invocation for a
  provider row with zero matches, so "and if nothing matched" branches
  fold into the join (the hover service's `stamp` system is gone;
  `resolve` seeds the fallback itself). The `None` invocation records
  store-scoped watermark deps on the joined stores, so partner churn
  reruns unmatched rows — coarse (any store write invalidates every
  unmatched row) but correct; per-fingerprint-bucket deps are the
  refinement if it ever shows up in profiles. Whole-entity removal sweeps
  do not yet bump store watermarks (component-level removal does).
- Add heterogeneous-key bound joins (dsql resolver blocker): `Eq` today
  requires the *same* component type on both sides, so parent/child
  chains — `ParentKey(node)` on the child, `NodeKey(node)` on the
  parent — cannot join, and every ancestor walk falls back to ambient
  views. That is what pins resolver-shaped systems to `Complete` and
  makes consumers re-derive context at query time (dsql: seven systems
  independently re-running the same `SelectionTree` resolution walk). A
  two-type form (`Where<Eq<ParentKey, NodeKey>>`-ish, fingerprint
  equality across types) turns recursive derivation into a tracked fixed
  point in `Evaluate`: roots resolve from their own facts, each child
  joins its parent's resolved fact, and the streaming replanner iterates
  to convergence — incrementally minimal, since an edit re-derives only
  the chains below it. Contract to pin: both key components must be
  `#[component(hash)]` over identically-shaped data so fingerprints
  compare across types. Validate with a chain-fixpoint regression test
  (depth-N chain, edit mid-chain, assert only the suffix re-derives) and
  check `CommitLimit` headroom on deep chains.
- Done: **pair-driven bound-join planning** (dsql resolver scaling wall).
  Single-key `Where<In<T>>`/`Where<Eq<T>>` params no longer enumerate
  independently: during product construction their rows expand from the
  already-picked provider's pair list (the maintained member list for
  `In`, the fingerprint-index bucket for `Eq`), so planning is O(pairs),
  not O(providers × candidates) — `in_join_planning` bench: −54/−71/−79%
  at 8×32/16×64/32×128, growing with size (spec/bench-reports.md).
  New rule, enforced by panic: the provider param must precede the bound
  param in the signature. Compound (multi-key) joins keep the
  product-and-prune path; `binding_matches` remains as the correctness
  backstop on expanded tuples. dsql can now re-land its fixed-point
  resolver (revert of their revert at eca2d5f).
- Memoize the planner itself (watermark-gated system skipping): every
  wave currently replans every system in the phase — full row
  enumeration, dep computation, memo comparison — even when its driving
  stores are empty or untouched. Give each system a static store-interest
  set (driving row types + tracked dep types + join provider types;
  `view_sets` already exists for the ambient half) and skip planning
  whenever no interested store's watermark moved since the system's last
  plan. Systems over absent component types fall out as the trivial case;
  a fully-memoized steady state (dsql's 30/160/80) makes waves near
  no-ops. Sound because all mutation paths — including whole-entity
  removal and the derived sweeps — now bump watermarks.
- **Presence-bitmap planning (the schema-enabled endgame for this
  section).** A constructor-time schema (`Bowl::of::<S>()`) closes the
  component universe, so bit positions can be assigned once and densely
  at construction: one presence bitmap per entity over the schema's
  components (dsql: ~70 → two `u64` words), maintained at the same world
  chokepoints as watermarks and the fingerprint index, copy-on-write
  against snapshots. Everything that today probes stores becomes a mask
  operation: a query's required set is a precomputed mask
  (`With`/`Without` fold in as positive/negative bits, optional parts
  contribute none, a facet `Entity<H>` is H's required mask), and row
  enumeration is `bits & mask == mask` over a flat array instead of
  per-store BTreeMap probing — killing the dense-scan constant. Shape
  views are derived (`entity_bits & shape_mask`), giving the
  same-phase-completion flag and "N entities are 2 bits short of
  `ast_def`" explain output nearly free. Stage 2: the reverse index
  (mask-transition → interested systems). Commits know exactly what they
  wrote (`written_derived`), so a commit updates bitmaps and a bit
  transition pushes precisely the (system, entity) pairs whose masks
  became (un)satisfied into a dirty queue — planning becomes delta
  application (dirty pairs ∪ watermark-stale pairs), and "a commit of `T`
  wakes exactly the consumers of `T`" becomes a lookup, not a graph
  aspiration. Presence bitmaps solve *matching*; staleness stays
  revision/memo-based — they compose, not compete. Requires: schema at
  construction (not `with_schema`-after), schema grown to cover *base*
  entity shapes too (the universe must contain every queried component;
  on a schema bowl, registering a system that queries an off-schema
  component should refuse loudly), schema-less bowls keep the dense-scan
  path. **Stage 1 done**: `Bowl::of::<S>()` fixes the schema at
  construction and lays out the bit universe; presence bits are
  maintained at all four world chokepoints (insert, targeted removal,
  whole-entity removal, derived sweeps), copy-on-write against
  snapshots; entity-tuple row retention takes the mask path when every
  part is presence-expressible (`presence_scan` bench: −96%…−98% vs
  store probing at 1k–50k rows). Still open (stage 2): the reverse
  index / dirty queues, mask bits for `With`/`Without` filters, the
  off-schema registration refusal, and driving *candidates* from the
  bitmap instead of the smallest store.
- **Done (planner gating stage 1): watermark-gated system skipping.**
  Every system carries a static interest set (`interest_types()`
  unioned over params: query part types, filter types, join keys,
  `Tracked` parts; `View`/`Commands` contribute nothing; dense-scan and
  custom params poison to always-plan) and a planned-mark watermark. A
  wave skips planning any system whose interested stores haven't moved,
  and an all-skip wave skips the whole wave setup (snapshot + memo
  clone). Marks reset on conflict deferral and stale commits. New
  `planner_gating` bench (32 disjoint systems, one touched).
- **Done: the per-wave memo clone is eliminated** (was the dominant
  settle cost at scale — incremental_settle −23…−28%, cold_settle
  −9…−12%, planner_gating −22…−26%; see spec/bench-reports.md). Original
  finding, kept for the record: (`Arc::new(memo.clone())` in `run_phase_streaming`; at 16k
  memo entries — `planner_gating/512` — it is milliseconds per settle
  and buries what gating saves; it is also why gating first *regressed*
  `in_join_planning`: fast empty waves → more waves → more clones,
  fixed by the all-skip wave setup skip). The diagnosis is precise:
  `FunctionSystem` (every plain system) uses the memo *only at plan
  time* (`plan_invocations` inside `stream_runs`); its run futures never
  capture it. Only the three hook wrappers
  (`OnStart`/`OnComplete`/`OnSettled`) clone the Arc into their run
  future, because they re-plan the inner system *inside* it
  (`self.system.run(bowl, &snapshot, &memo)`). Planning is
  deterministic over the captured snapshot+memo, so wrappers can
  pre-plan at stream time (capture the invocation list, not the memo) —
  then `stream_runs` takes plain `&memo` and the per-wave clone
  disappears entirely. Refactor: split the wrapper path into
  plan-at-stream + execute-batch; `run_settled` keeps its by-ref memo.
  Expected to dominate every settle-shaped bench.
- **Playground profile (debug counters, `BOWL_COUNTERS=1`)**: the run is
  37 settles / 34 generations at ~2.8ms per settle in debug — settle
  *count* times per-settle *fixed cost*, not row scale. Dirty queues cut
  the planning slice of that fixed cost; the rest is per-phase snapshot
  clones, state-lock round-trips, `always_run` Settle-phase planning
  (`cleanup_stale_derived` has `WorldMetaView`, so it *runs* — not just
  plans — every `DerivedFrom` row every settle: the playground's explain
  dump shows 96 matched / 0 memoized × 37 settles ≈ 3.5k invocations per
  run doing nothing but `is_current` checks), and the debug-only commit
  checks. Done: `cleanup_stale_derived` is now a *batch sweep* — one
  always-run invocation iterating a whole-store `View` instead of one
  invocation per `DerivedFrom` row (playground: 96 → 1 planned
  invocations per settle, generations 54 → 30). Deliberately user-land
  (public params only), so plugins can build the same sweep shape.
  Follow-ups from the same discussion: (a) an *intentional* once-per-
  settle marker (`Sweep`/`.always()`) instead of `WorldMetaView`'s
  `always_run` side effect being load-bearing; (b) the SIMD track —
  `#[component(dense)]` column storage (`Vec<T>` + entity index,
  whole-column COW against snapshots, per-column revision) and a
  `BatchView<T>` param yielding scheduler-exclusive `&mut [T]`, designed
  against span/offset remapping after edits (the genuinely vectorizable
  compiler workload); engine-side derived-fact cleanup remains the
  fallback if the user-land sweep proves insufficient at dsql scale.
  Fixed-cost thread worth its own pass after stage 2: snapshot reuse
  across phases when the world hasn't changed, a cheaper `is_current`
  path for cleanup, and a true no-op-settle early-out.
- **Done (stage 2, first cut): entity-granular delta planning.** The
  world keeps a settle-scoped write log with per-system cursors;
  delta-eligible systems (exactly one plain tracked query, bounded
  interest) plan only entities written since their last plan
  (`states_hinted`); resets/epoch rolls force one full plan. Joins,
  outer joins, always-run, and custom params keep full planning.
  planner_gating/512 −45.6% (cumulative planner series −58%);
  equivalence pinned by exact-run-count test. Still open: extending
  eligibility to pair-driven joins (provider-side hints), the
  presence-mask reverse index (the original bitmap formulation —
  today's filter is interest-type based, masks would make transition →
  system lookup O(1)), and With/Without mask bits. Original plan, for
  the record:
  1. Registration builds the reverse index: for each system with a
     bounded interest set, its presence mask(s) (schema bowls); store
     `mask → Vec<SystemId>` sorted for lookup.
  2. World keeps a `dirty: Vec<(TypeId, Entity)>` transition log at the
     same four chokepoints as presence bits (only transitions, not value
     writes; value staleness stays watermark/memo-based).
  3. At wave start, drain transitions → for each, compute affected
     systems via the reverse index (bit became set/cleared → masks
     containing that bit) → per-system dirty entity sets.
  4. `plan_invocations` gains a hinted path: instead of enumerating all
     rows, seed candidates = dirty entities ∪ rows whose memo deps are
     watermark-stale (needs a per-store dirty-entity narrowing too, else
     value changes still force full enumeration — the memo table can be
     bucketed by (system, store) to find dep-stale rows cheaply).
  5. Fall back to full enumeration for unbounded-interest systems and on
     the first plan after registration/preemption.
  Correctness invariant: dirty ∪ watermark-stale must cover exactly what
  full enumeration + memo comparison finds runnable; pin with a
  debug-mode cross-check flag that runs both and asserts equality.
- Done (first cut): **parallel runtime.** Planned runs are owned
  `'static + Send` futures; with an ambient tokio runtime the wave loop
  spawns them onto workers (abort-on-drop preserves preemption
  cancellation), else they poll cooperatively as before. 2.1× on the
  `parallel_compute` bench at 32×~14µs rows. Remaining headroom: per-task
  spawn overhead (batch small rows per task), wave/commit serialization
  (commit pipelining — apply finished batches while later runs still
  execute), and a heavier-row bench matrix. Pure racing readers use
  `.last_settled()` to skip the settle queue (dogfooded in the storm;
  commit-bucket lock waits collapsed 108ms → 6.6ms).
- **Done: ambient healing (the A→B→A staleness fix).** Settle-epoch-
  tagged memo entries (epoch = settle *completion* boundary, so
  insert-await generations stay healable); at convergence, before hooks
  and `Phase::Settle`, `View`-carrying invocations committed this epoch
  whose viewed stores moved past their planned revision are healed
  (memo entry removed + row force-replanned; removal is the no-spin
  guarantee for unmatched rows; 128-attempt cap panics with full
  diagnostics). Also fixed: untracked writes now move watermarks
  (ordering-visible, dependency-invisible) — gate markers were invisible
  to planner gating. Still open from codex's review: untracked viewed
  stores in the freshness set rely on the new watermark visibility (now
  covered), and a per-store structural counter remains the cleaner
  long-term shape if untracked revision-bumping ever shows costs.
  dsql follow-through: un-ignore
  `checks::content_roundtrip_edits_rederive_cleanly`, then delete the
  `Session::seen_hashes` full-reload workaround (covers the LSP undo
  path that the workaround missed).
- **Delta widening round (dsql callgrind feedback: plan_invocations was
  77% of the main thread because every real system is multi-Query).**
  Done: multi-driver systems stay delta-eligible — `states_hinted`
  narrows at plan time (hint the one driver whose `row_bound` — O(1)
  store-size lookups, never enumerating — exceeds 1; tiny gates like
  demand markers and singleton configs enumerate fully; a *dirty* tiny
  gate or several large drivers fall back to the full product). Done:
  conflict-deferral and stale-commit replans are row-granular
  (`force_replan(keys)` folds the rows into the next hint) instead of
  resetting the system to a full plan — the suspected source of dsql's
  absolute planning growth on write-heavy waves. New
  `planner_gating/gated` bench models the dsql shape (demand + config +
  row query). Still open, next in line: **hint Eq/In joins** — pairs to
  enumerate = dirty providers' pairs ∪ pairs of dirty members (member →
  provider via the edge component for `In`, via the fingerprint bucket
  for `Eq`); the pair expansion machinery already has both lookups.
  Validate against dsql's edit_cost harness
  (`EDIT_COST_PROJECT=... cargo test -p dsql-project --test edit_cost`).
- **Done: schema authoring feedback (dsql migration)**: (a) all tuple
  ceilings (`declare.rs` traits + `Bundle`) lifted 8 → 12 and the
  `#[derive(Schema)]` macro now rejects wider shapes with an error
  naming the field, its width, and the cap (12-part fixture in the
  playground); (b)–(d) documented in `spec/declared-outputs.md`:
  conformance merges within *one invocation's* command buffer (linking
  components belong in the spawn bundle, two-variant match), the
  group-ambiguity trap (union groups break bare `entity().insert(X)`
  proofs while full spawn bundles keep compiling; route the increment
  through the owning shape alias), and the degenerate one-part inverse
  shape as a write-bundle conformance convention.
- **Congestion roadmap** (from the lock/read telemetry). Done:
  generation-targeted waiter wakeups (waiters register once per wait and
  only satisfied ones wake; the abandoned-run path still broadcasts so a
  waiter can be promoted to driver) and lock-free settled reads
  (`.last_settled()` serves from a read-mostly published slot, never
  touching the state lock; the take path clears it). Measured after
  both: storm waited-time barely moved (~157–202ms) and acquisition
  counts turned wildly variable (2.7k–7.7k/run) — the fingerprint of
  **spin-polling loops**, which are the actual remaining contender:
  `PreemptWaiter` (`Mut` mutators re-locking per yield while polling
  `boundary_reached`) and the `take()` pin-retry loop. Next moves, in
  order: convert preempt-boundary and take-pin waits to notification
  (the `preempt_signal` AtomicWaker machinery half-exists); *then*
  re-measure before the input-intake lock split (deferred — its
  interleaving with singleton resolution, epoch deferral, and preempt
  boundaries makes it a deep refactor, and the data no longer points at
  insert contention as dominant). Far item: interest-scoped settling for
  multi-tenant daemons (schema/presence knows which stores a reader can
  see).
- Add ordered/range predicates as join keys (position-in-span is the
  playground's blocker): with them, the hover candidates become tracked
  joins, move to `Evaluate`, and the finalizer flattens back to a plain
  ambient aggregator behind the `Complete` barrier — retiring the monotone
  pair-fold workaround.
- Follow-up: engine-maintained relationships (Bevy-style inverse components,
  tracked and fingerprinted) to make membership sets a memoizable dependency,
  unlocking `Where<In<T>>` and retiring the hand-rolled set-fingerprint
  pattern. See the companion-design section of `spec/joins.md`. Outer
  joins already collapsed the hover service's bare-request rule into the
  file join (two rules left: request⟕file / request⋈candidates); set-valued
  tracked reads would let arbitration collapse into the same rule too.
  Relations *subsume* heterogeneous-key joins (a parent→child edge is a
  hetero join plus an index plus ordering), so relationships were built
  directly. Done (v1 core): `Component::relationship_edge` +
  `RelationshipTarget` maintain a fingerprinted inverse ordered by entity
  id, written as an ownerless base fact; insert, retarget, component
  removal, whole-entity removal, and the derived-output sweeps all keep
  it current (the sweep routing also closed the store-watermark gap for
  removals); removing a target retracts every source's edge — no despawn
  cascades, lifetime stays `DerivedFrom`. Done: `Where<In<T>>` — the
  identity join over the inverse (one invocation per (set-holder, member)
  pair; membership changes re-pair via the provider's dep on the
  inverse's revision; provider rule shared with `Eq`). Done: the
  `#[relationship(target = T)]`/`#[relationship_target(relationship = E)]`
  derive attributes — the edge's first tuple field is the target entity,
  the inverse is a tuple struct over `Vec<Entity>` with its fingerprint
  generated from the member list (combining with
  `#[component(hash)]`/`revision` is rejected). Done (answering dsql's
  content-invalidation ask): content hashes belong on the *member side*
  of the `In` join — `member_content_changes_rerun_only_their_pair`
  proves a fingerprinted `&SourceHash` in the bound query gives per-pair
  *rerun* invalidation (fingerprint-cut; zero reruns on an equal hash,
  one on a real change) with no engine feature. The test pins execution
  granularity, not planning cost — the counter cannot distinguish
  hinted planning from a full plan that memo-skips; a content-aware inverse (incremental
  domain-separated XOR aggregate, reverse watcher index, `Missing`
  contribution) is only worth building if a consumer reads *just* the
  inverse — no such consumer exists in dsql today. Still open:
  playground adoption to retire the `DefIndex` set-fingerprint pattern,
  and edge-traversal joins (child rows joining their parent's facts
  through the edge — the dsql resolver's ask).

Current shortcut:
- Queries iterate component stores (smallest participating store for tuples).
- External queries support `Where`, `Eq`, `Gte`, `And`, `Or`, `Not`, `With`,
  `Without`, and typed `.args(...)`.
- `Where<Eq<T>>` uses a per-store fingerprint index for `#[component(hash)]`
  components; other predicates scan the candidate store.
- Shared runtime args are keyed by component type. Use `Named<Tag, Query<...>>`
  plus `.args_for::<Tag>(...)` when separate queries in one scoop need different
  args of the same component type.

## 8. Improve Cow Queries And Mutable Access Scheduling

- Keep clone-on-write updates explicit through APIs like:

```rust
db.scoop::<Query<(Entity, Cow<RopeyFile>), Where<Eq<FilePath>>>>()
    .args(FilePath(target))
    .for_each(|(_entity, file)| {
        file.apply_delta(delta);
    })
    .await;
```

- Make mutation work through `&self` so `Bowl` can be shared behind `Arc`.
- Ensure mutations bump component revisions correctly.
- Continue hardening storage-backed scheduler-level `Mut<T>` mutation now that
  live components use guarded cells.
- Avoid deadlocks when many callers mutate/query concurrently.
- Make external guarded reads participate in the same conflict protocol as
  internal reads:
  - external read + internal read can overlap
  - external read blocks internal write for the same row
  - external write blocks internal read/write for the same row
- Continue system-level `Mut<T>` as a planned read/write edge:
  - `&T` declares shared read access
  - `Mut<T>` declares exclusive row-level write access
  - unrelated entity rows can still run concurrently
- Use fingerprints after scoped mutation:
  - hashed components bump revisions only when the fingerprint changes
  - non-hashed components bump revisions after successful mutation
- Consider async external exclusive access with wait-graph cycle detection.

Current shortcut:
- `Cow<T>` external queries exist and run through a synchronous closure while
  the live world is locked.
- `Cow<T>` currently still requires `T: Clone`, although guarded live storage no
  longer mutates through `Arc::make_mut`.
- `Mut<T>` external queries return inert handles with synchronous
  `with_original` / `with_latest`; they do not clone payloads and never wait
  on a cell while holding runner state (try-lock plus yield/retry).
- System queries use `MutRef<'_, T>` as a scheduler-visible write edge that
  yields an in-place `&mut T` for the invocation; the runner serializes
  conflicting rows and reconciles revisions at commit, absorbing a row's own
  write into the invocation's memo entry.

## 9. Add Better Non-Settling And Cycle Diagnostics

- Replace the hard panic with structured errors.
- Detect which systems and component types keep changing.
- Report enough information to debug cycles like:

```text
A reads BOut -> writes AOut
B reads AOut -> writes BOut
```

- Decide how this relates to the dynamic graph execution idea.
- Account for streaming evaluation:
  - report running invocations
  - report stale/discarded completions
  - report systems that repeatedly commit changes
  - report phase gates that repeatedly re-open work

Current shortcut:
- `settle()` uses `DEFAULT_SETTLE_LIMIT = 64`.
- If the bowl does not stabilize within that limit, it panics.
- There is no explanation of which systems caused the non-settling behavior.

## 10. Clarify View Dependency Semantics

- Decide whether `View` should always remain ambient/non-invalidating.
- Consider additional read types if needed:
  - `View<T>`: ambient snapshot read, no memo deps
  - `TrackedView<T>` or similar: ambient read that contributes deps
- Document common patterns for checks that need project-wide context.
- Done (detection): `ExplainReport::stale_views` reports viewed stores
  that moved past an invocation's planned revision — the footgun signature
  is everything-memoized with nonzero stale views (dsql port, friction 4).
  The fingerprinted-index-as-tracked-input pattern (`DefIndex`) remains
  the standard remedy. A separate `TrackedView` read type was considered
  and rejected — an invalidating view contradicts `View`'s deliberate
  ambient semantics and is just a worse query.

Current shortcut:
- `View` never contributes dependencies.
- This is useful for shader-like per-row systems, but ambient changes do not
  rerun a system unless the driving `Query` row changes.
- The duplicate-definition checker relies on this behavior today.

## 11. Add System Local State

- Add a `Local<T>`-style parameter for stable per-system or per-invocation
  state.
- Decide whether local state is keyed by:
  - system id only
  - system invocation keys
  - custom user key
- Use this to prototype smarter aggregate systems such as duplicate checks.

Current shortcut:
- Systems are pure async functions plus command buffers.
- Any persistent state must currently be modeled as components.

## 12. Add Explicit Ordering Only If Needed

- Revisit `run_after` or `depends_on` after output-driven evaluation has been
  pushed further.
- Prefer query/output availability over explicit ordering where possible.
- If ordering returns, make it system-level and cycle-checked.
- Done: registration-time phase analysis (dsql feedback) — with a schema
  registered, `add_system` warns (debug builds) when a new system's
  declared outputs could complete an entity that a same-phase `View`
  matches, checked through the schema shapes so shared vocabulary
  components do not false-positive. Shipped as a warning, not a refusal:
  marker-gated same-phase consumers are legitimate and undetectable
  statically; the commit-time entity-granular flag stays the precise
  enforcement. (Registration ordering is gone: schemas are fixed at
  construction via `Bowl::of::<S>()`.)
- Done: builder-only construction with plugin composition.
  `Bowl::builder().schema::<S>().plugin(P).system(sys).build()` is the
  only construction path — `Bowl::of`/`Bowl::new`/`add_system` are gone,
  the system set is sealed at build (registration analyses and the
  planner can treat the graph as total), and dynamic mid-life
  registration is not supported (conditional subsystems are demand
  markers gating planning, not registration). Plugins
  (`trait Plugin { fn shapes(&self); fn build(&self, &mut Registrar) }`)
  carry their schema fragment and systems as one unit, so fragment/system
  desync is unrepresentable — this subsumed the earlier
  `#[schema(extend)]` idea. Plugins over *app* data export schema-generic
  systems the app instantiates at build. The playground dogfoods this
  with `LangPlugin` plus a dummy replicon-style `ReplicationPlugin`
  (`replication.rs`).
- **Replication is shape-granular** (dogfooded): with an enforced schema,
  component-granular replication could transit illegal partial entities
  on the applying side, so the protocol unit is a *shape instance* — the
  wire analogue of strict spawning; apply lands a whole shape bundle or
  nothing. Subscriptions are shape aliases
  (`.replicate::<lang_schema::SourceFile>()`), `Schema::shapes()` is the
  enumerable manifest, and a daemon/client pair sharing one schema type
  makes the schema the wire contract (see daemon-client porting notes).
  Resolved: capture is now a facet query
  (`Query<(Entity<H>, Tracked<H>)>`) — the anchor matches conforming
  entities, `Tracked<H>` deps the row on every part, and the previously
  pinned assertion flipped (records re-derive after any part of the
  instance changes). Still open from the facet slice: registration-time
  presence-typing validation of sibling parts (`&T` iff required,
  `Option<&T>` iff optional).
- Done: "never produce and ambiently consume in the same phase" is now
  engine-enforced in debug builds (dsql port, friction 5): a commit whose
  derived write is `View`ed by a same-phase system with matched rows
  panics with a fix hint. Marker-gated consumers (no rows in the
  producing generation) and tracked consumers are exempt; the settle phase
  is immune by construction since its inserts defer to the next run. The
  flag immediately caught `check_duplicate_defs` viewing lowering output
  from Evaluate — moved to Complete. The check is *entity-granular*: it
  fires only when the written entity ends up carrying every component one
  of the viewer's `View`s requires, so shared vocabulary components
  (spans, file anchors, price tags) on unrelated entities do not trip it,
  while a write that completes a previously partial row still does.
  Remaining gap: removals are not flagged — a same-phase commit removing
  a viewed component changes the view's row set just as silently.

Current shortcut:
- Systems and invalid rows are polled concurrently, but there is still no
  explicit dependency scheduler.
- There is no `run_after`, `depends_on`, or stage system in `bowl`.
- The playground uses `generate_ast.on_settled(...)` to insert an ephemeral
  singleton `AstAvailable` phase gate.

## 13. Add Async Parallel System Execution

- Keep system functions async.
- Move from full-batch barriers toward streaming evaluation once ownership and
  scheduling semantics are clearer.
- Run independent system invocations concurrently and commit completed
  invocations as soon as their captured deps are still current.
- Add per-system streaming policy hints:
  - `max_concurrency(n)` limits running invocations for that system
  - `min_batch_size(n)` waits for at least `n` runnable rows unless settling
  - `batch_commits(n)` groups completed outputs before commit/replan
- Preserve the current single-flight outer runner:
  - many callers can wait
  - only one evaluation driver runs
  - pending inserts batch into the current/next settlement cycle
- Decide whether parallel execution requires `Send` futures or a local executor
  mode.

Current shortcut:
- Systems and invalid query rows are polled with local `join_all`.
- `Runnable` returns `LocalBoxFuture`, so the current implementation does not
  require system futures to be `Send` and does not spawn work onto a
  multi-threaded executor.
- The current runner still commits a batch barrier after planned work completes.

See:
- `spec/streaming-evaluation.md`

## 14. Add Dependency Graph Introspection

- Done (first pass): `bowl.explain("system_name")` reports registration,
  phase, matched rows (post joins/filters), memoized rows, and
  `stale_views` (viewed stores that moved past the invocation's planned
  revision — the §10 ambient-staleness detection). Friction 7. Still
  open: per-entity explanations, naming *which* filter pruned a row, and
  which dep revisions were compared.
- Add write-amplification visibility to `explain` (dsql feedback, found
  at 58 wall-clock seconds): a `MutRef` target row with N same-generation
  writers costs N serialized commits and their replan cascades — the
  collision is visible in the planned access sets, so `explain` can
  report "component T on this row has N writers" before the fold is
  built, not after it melts.
- Expose enough internal data to inspect:
  - system invocation keys
  - query dependencies
  - component revisions
  - derived output ownership
- Build a debug graph for why a query caused certain systems to run.
- Use this to validate fine-grained invalidation behavior.
- Include long-running daemon observability:
  - what systems are running
  - what each request waits on
  - why a query took time
  - what derived outputs were cleaned up
  - why the bowl did not settle

Current shortcut:
- Memo entries store dependency revisions, but there is no public tracing or
  graph visualization.

## 15. Improve The Toy Language Playground

- Keep expanding the playground as the main integration test.
- Document the "resolution as a derivation" pattern in
  `spec/language-entities.md` (dsql lesson): when a fact is meaningless in
  isolation — its meaning depends on ancestors or surrounding context —
  derive that meaning onto the entity *once*, in a dedicated resolver
  stage, instead of re-resolving it inside every consumer. The smell that
  calls for it: systems sitting in `Complete` not because their answers
  must be late, but because their *inputs* are ambient views of lowered
  facts (dsql had seven systems independently re-running the same
  resolution walk; with a `ResolvedSelection` fact, each collapsed to the
  ~20-line tracked-join shape of the healthy hover systems). One honestly
  cross-cutting resolver is fine — resolution genuinely is cross-cutting;
  seven inlined copies of it are the disease.
- Once heterogeneous-key joins exist (§7), demonstrate recursive
  derivation-to-fixpoint in the playground: derive namespace-qualified
  names through the parent chain as tracked joins instead of the
  lowering-time walk, pinning convergence and per-chain incrementality
  against a real language shape.
- Add a more realistic external `SystemImportDb` that can change over time.
- Expand the language service layer:
  - hover
  - goto definition
  - completions
  - diagnostics by file
- Add more realistic parser/AST tests around the Lelwel-generated CST.
- Add daemon-like request flows:
  - repeated file edits through `Cow<FileText>` or future `Mut<FileText>`
  - hover/goto/completion requests while background analysis is running
  - external `SystemImportDb` updates invalidating import diagnostics through
    `DerivedFrom::many`
- Add demand markers (see "Pattern: demand markers" in
  `spec/language-entities.md`): gate `check_imports`/`check_duplicate_defs`/
  `index_defs` on a `DiagnosticsDemand` fact so hover-only settles skip
  diagnostics entirely; demonstrate demand toggling as LSP debounce.
- Done: hover restructured into the candidate-fact pipeline (the scaling
  remedy for aggregator services): tracked enrichment stamps
  `RequestKey`/`HoverFile`/`HoverWord` plus the fallback answer scaffold on
  the request (file resolution is a `FilePath` join), each entity's own
  `Phase::Complete` system inserts `HoverCandidate { priority, .. }` facts
  from only its own data, and arbitration is a same-phase `RequestKey`
  join — one invocation per (request, candidate) pair, monotonically
  upgrading `HoverRank`/`HoverInfo` (a commutative max-fold, so pair order
  is irrelevant). One phase barrier covers the ambient candidate reads;
  everything else is tracked — dissolved `HoverCtx`, the arbitration
  chain, and the `AstAvailable` gate on hover. Apply the same pipeline to
  future services (goto, completions).
- Migrate `index_defs` off the `AstAvailable` gate marker (to
  `Phase::Complete`, like the hover pipeline): marker-gated work is
  invisible to settledness checks while the marker is absent, which is the
  race that starved hover requests before the phase migration. Markers
  should be reserved for signals that truly need settle scope.

Current shortcut:
- The toy language is intentionally small.
- `SystemImportDb` is a singleton component with hardcoded data.
- Parser and AST extraction are pragmatic prototype code.

## 15. Support Owned Query Results If Needed

- Add query forms that can return owned component values where appropriate.
- Decide how this overlaps with `BoundEntity::take`.
- Avoid cloning by default; borrowed snapshot results should remain the normal
  read path.

Current shortcut:
- `QueryResult::collect()` returns borrowed values tied to the owned snapshot.
- There is no owned query path in `bowl`.

## 16. Improve Macro Robustness

- Keep `#[derive(Component)]` specialized around `bowl`.
- Decide whether the macro should support renamed crates later.
- Consider better parsing through `syn` if attributes become more complex.

Current shortcut:
- The macro emits paths like `::bowl::Component` directly.
- This is fine for the current workspace but will not work automatically if a
  downstream crate renames the dependency.
- The macro uses direct `proc_macro` token walking instead of `syn`.

## 17. Add SyncBowl Wrapper

- Add a thin sync wrapper using `pollster`.
- Keep async `Bowl` as the primary implementation.
- The sync wrapper should only block around async calls:

```rust
sync_bowl.scoop::<Q>()
sync_bowl.insert(bundle)
sync_bowl.add_system(system)
```

Current shortcut:
- There is no `SyncBowl`.
- The playground uses Tokio directly as its async driver.

## 18. Explore Long-Running Daemon Runtime

- Prototype a daemon-style example:
  - initialize one long-lived `Bowl`
  - register systems once
  - apply file watcher events as component mutations
  - accept request entities from a CLI/client layer
  - answer through `BoundEntity::take`
- Make sure the bowl wakes only when inputs, mutations, or requests arrive.
- Decide whether clients can observe:
  - only settled snapshots
  - in-progress streaming commits
  - explicit watch subscriptions
- Add tracing suitable for long-running processes.

Current shortcut:
- The playground is a one-shot binary.
- It demonstrates requests, but not daemon lifetime or event sources.

See:
- `spec/daemon-client.md`

## 19. Explore Replication / Change Streams

- Investigate a Bevy Replicon-like plugin for component fact replication.
- Decide the shape of a changefeed API:
  - per commit
  - per settled snapshot
  - explicit diff query
- Define entity id mapping for remote clients/workers.
- Decide which components are replicated and which are local-only:
  - durable facts likely replicate
  - ephemeral gates likely stay local
  - `DerivedFrom` may replicate only if remote cleanup needs it
- Consider replication as a basis for:
  - daemon/client synchronization
  - remote workers
  - persistent cache replay
  - debugging/replay

See:
- `spec/ideas/replication.md`
