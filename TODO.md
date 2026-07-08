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
- Add optional query parts (`Option<&T>` in a row tuple) so systems stop
  carrying side-`View`s solely to look up a maybe-present component by
  entity (see `index_defs`/`check_duplicate_defs` and their `paths` view).
- Add external component removal: `bowl.entity(e).remove::<T>()` mirroring
  targeted inserts, with the same epoch semantics. The external write API is
  currently insert-only — an LSP `didClose` cannot retract an `OpenBuffer`
  fact (dsql port, friction 2).
- Make `Commands::insert` return the reserved `Entity` so a system can link
  parent/child facts by entity id within one command buffer. Reservation
  must preserve rerun id stability: thread the shared id allocator plus the
  invocation's previous spawn-slot ids into `Commands`, so slot N reuses
  its previous id and only new slots allocate. The dsql port worked around
  the gap with stable `NodeKey`/`ParentKey` join keys — often the better
  design, but the handle unlocks others (friction 3).
- Add `#[component(revision)]`: a first-class fingerprint mode for large
  components (source text, catalog snapshots) where hashing the payload is
  wasteful — an internal counter bumped per mutation stands in for the
  hash. Codifies the global-revision-counter-as-`Hash` trick every big
  component in the dsql port hand-rolled (friction 6).
- Keep the README aligned with the final mental model:
  - components-only storage
  - immutable snapshots for reads
  - clone-on-write external updates
  - future scheduler-level mutable access
  - systems as memoized per-row functions
  - `View` as ambient/non-invalidating context
  - `on_settled` for readiness gates
  - `Cleanup` for ephemeral facts
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

- Fix the `DerivedFrom` anchor-ordering trap (dsql port, worst friction —
  will bite every new user): anchor revisions are captured in
  command-application order, so a derived entity emitted *before* a
  same-buffer write to its anchor (e.g. a diagnostic inserted before
  `ParsedFile` lands on the same file entity) is born stale and silently
  reaped by `cleanup_stale_derived`. Resolve anchor revisions at buffer
  end, after all commands in the batch have applied; at minimum,
  debug-assert when a derived entity is born already-stale.
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
  `Complete`, and `Cleanup`.
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
- Follow-up: engine-maintained relationships (Bevy-style inverse components,
  tracked and fingerprinted) to make membership sets a memoizable dependency,
  unlocking `Where<In<T>>` and retiring the hand-rolled set-fingerprint
  pattern. See the companion-design section of `spec/joins.md`.

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
- Defuse the silent-staleness footgun (dsql port, friction 4): a system
  whose `View`ed data changes while its `Query` deps do not simply stops
  reacting, and nothing warns. Document the fingerprinted-index-as-tracked-
  input pattern (`DefIndex`) as the standard remedy; detection folds into
  the explain facility (§14) as `ExplainReport::stale_views`. A separate
  `TrackedView` read type was considered and rejected — an invalidating
  view contradicts `View`'s deliberate ambient semantics and is just a
  worse query.

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
- Document "never produce and consume in the same phase" as a hard rule:
  intra-phase ordering is undefined, and the dsql port hit it (a
  Cleanup-phase candidate producer raced the Cleanup finalizer; friction
  5). Consider registration-time or debug-time detection when one system's
  output type is a same-phase system's tracked input.

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

- Add a "why didn't X run" explain facility — the single biggest DX ask
  from the dsql port (friction 7): given a system (and optionally an
  entity), report whether any rows matched, whether a join/demand filter
  pruned them, whether the memoized deps were unchanged (and which
  revisions were compared), which phase the system runs in, and whether
  viewed stores changed since its last run (`stale_views`, the §10
  ambient-staleness detection).
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
  remedy for aggregator services): service enrichment stamps
  `HoverFile`/`HoverWord` on the request (file resolution is a `FilePath`
  join), each entity's own system inserts `HoverCandidate { priority, .. }`
  facts from only its own data, and a `Phase::Cleanup` finalizer picks the
  winner. Ordering comes from phases, not gate markers — dissolved
  `HoverCtx`, the arbitration chain, and the `AstAvailable` gate on hover.
  Apply the same pipeline to future services (goto, completions).
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
