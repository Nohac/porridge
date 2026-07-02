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
- Use ephemeral singleton markers emitted from `on_settled` as phase-transition
  gates instead of reintroducing fixed stages.
- Treat `DerivedFrom` as the standard pattern for revision-scoped derived facts
  such as diagnostics, hover results, indexes, and summaries.
- Explore Porridge as a long-running daemon/client runtime.
- Keep a Replicon-like replication/change-stream layer as a future plugin
  track.

See:
- `spec/streaming-evaluation.md`
- `spec/lifecycle-and-ephemeral.md`
- `spec/derived-from.md`
- `spec/daemon-client.md`
- `spec/ideas/replication.md`

## 1. Public API Polish Before Larger Migration

- Stabilize naming for the public API:
  - `Bowl`
  - `Query`
  - `View`
  - `Mut`
  - `Where`
  - `DerivedFrom`
  - `cleanup_stale_derived`
  - `on_start`
  - `on_complete`
  - `on_settled`
  - `Phase`
- Decide whether `db.query::<Q, ()>()` needs ergonomic sugar for the common
  unfiltered case.
- Remove playground/debug prints such as `AstAvailable insert/remove`.
- Write a README that explains the final mental model:
  - components-only storage
  - immutable snapshots for reads
  - mutable external queries as live-world transactions
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
- Avoid unnecessary revision bumps when a system reruns and re-emits equivalent
  hash-stable output.

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
db.query::<(
    Entity,
    &Diagnostic,
    And<Eq<FilePath>, Gte<Severity>>,
)>()
.arg(FilePath(path))
.arg(Severity::Warning)
.collect();
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

Current shortcut:
- Queries scan entity ids from `0..next_entity`.
- External queries support `Where`, `Eq`, `Gte`, `And`, `Or`, `Not`, `With`,
  `Without`, and typed `.arg(...)`.
- There are no indexes; all filters scan.
- Runtime args are keyed by component type, so two args of the same type in one
  filter are ambiguous.

## 8. Improve Mutable Queries And Safe Update APIs

- Support mutation through APIs like:

```rust
db.query::<(Entity, Mut<RopeyFile>), Where<Eq<FilePath>>>()
    .arg(FilePath(target))
    .for_each(|(_entity, file)| {
        file.apply_delta(delta);
    })
    .await;
```

- Make mutation work through `&self` so `Bowl` can be shared behind `Arc`.
- Ensure mutations bump component revisions correctly.
- Define how mutable access interacts with in-flight evaluation.
- Avoid deadlocks when many callers mutate/query concurrently.
- Decide whether clone-on-write `Mut<T: Clone>` is the right long-term storage
  model for large mutable inputs such as ropes.
- Consider a non-cloning mutation mode that fails if the live component is
  shared with an active snapshot.

Current shortcut:
- `Mut<T>` external queries exist and run through a synchronous closure while
  the live world is locked.
- `Mut<T>` currently requires `T: Clone` because live storage uses `Arc<T>` and
  mutates through `Arc::make_mut`.
- There is no mutable system query; systems still write through buffered
  `Commands`.

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
  - repeated file edits through `Mut<FileText>`
  - hover/goto/completion requests while background analysis is running
  - external `SystemImportDb` updates invalidating import diagnostics through
    `DerivedFrom::many`

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
sync_bowl.query::<Q>()
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
