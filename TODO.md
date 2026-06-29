# Porridge TODO

This is the current implementation roadmap for the async `bowl` runtime and the
toy language playground. Items are ordered roughly from most important to least
important.

## 1. Stabilize Output Ownership And Invalidation

- Define stronger ownership semantics for derived outputs.
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
- This is enough for the playground, but it is not yet a full model for
  replacing complex derived output graphs.

## 2. Harden BoundEntity Take Semantics

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

## 3. Improve Bound Cleanup

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

## 4. Add Indexed And Filtered Queries

- Design typed filters in the query shape, for example:

```rust
db.query::<(
    Entity,
    &Diagnostic,
    And<Eq<FilePath>, Gte<Severity>>,
)>()
.bind(FilePath(path))
.bind(Severity::Warning)
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

Current shortcut:
- Queries scan entity ids from `0..next_entity`.
- There is no `Where`, `Eq`, `And`, `Or`, `Not`, or binding API in `bowl` yet.
- The playground does manual filtering inside systems through `View`.

## 5. Add Mutable Queries And Safe Update APIs

- Support mutation through APIs like:

```rust
db.query_mut::<(&mut RopeyFile, With<FilePath>)>()
    .where_eq(FilePath(target))
    .for_each(|file| {
        file.apply_delta(delta);
    });
```

- Make mutation work through `&self` so `Bowl` can be shared behind `Arc`.
- Ensure mutations bump component revisions correctly.
- Define how mutable access interacts with in-flight evaluation.
- Avoid deadlocks when many callers mutate/query concurrently.

Current shortcut:
- All query results are immutable snapshots.
- Base input mutation is modeled as inserting/replacing components, not as
  borrowing `&mut T`.

## 6. Add Better Non-Settling And Cycle Diagnostics

- Replace the hard panic with structured errors.
- Detect which systems and component types keep changing.
- Report enough information to debug cycles like:

```text
A reads BOut -> writes AOut
B reads AOut -> writes BOut
```

- Decide how this relates to the dynamic graph execution idea.

Current shortcut:
- `settle()` uses `DEFAULT_SETTLE_LIMIT = 64`.
- If the bowl does not stabilize within that limit, it panics.
- There is no explanation of which systems caused the non-settling behavior.

## 7. Clarify View Dependency Semantics

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

## 8. Add System Local State

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

## 9. Add Explicit Ordering Only If Needed

- Revisit `run_after` or `depends_on` after output-driven evaluation has been
  pushed further.
- Prefer query/output availability over explicit ordering where possible.
- If ordering returns, make it system-level and cycle-checked.

Current shortcut:
- Systems run serially in registration order.
- There is no `run_after`, `depends_on`, stage system, or `on_complete` in
  `bowl`.
- The playground replaced `on_complete` by having `generate_ast` insert
  `AstAvailable` onto the project entity.

## 10. Add Async Parallel System Execution

- Keep system functions async.
- Run independent system invocations concurrently once ownership and scheduling
  semantics are clearer.
- Preserve the current single-flight outer runner:
  - many callers can wait
  - only one evaluation driver runs
  - pending inserts batch into the next generation
- Decide whether parallel execution requires `Send` futures or a local executor
  mode.

Current shortcut:
- Systems are async, but the runner polls them serially in registration order.
- `Runnable` returns `LocalBoxFuture`, so the current implementation does not
  require system futures to be `Send`.

## 11. Add Dependency Graph Introspection

- Expose enough internal data to inspect:
  - system invocation keys
  - query dependencies
  - component revisions
  - derived output ownership
- Build a debug graph for why a query caused certain systems to run.
- Use this to validate fine-grained invalidation behavior.

Current shortcut:
- Memo entries store dependency revisions, but there is no public tracing or
  graph visualization.

## 12. Improve The Toy Language Playground

- Keep expanding the playground as the main integration test.
- Add a more realistic external `SystemImportDb` that can change over time.
- Expand the language service layer:
  - hover
  - goto definition
  - completions
  - diagnostics by file
- Add more realistic parser/AST tests around the Lelwel-generated CST.

Current shortcut:
- The toy language is intentionally small.
- `SystemImportDb` is a singleton component with hardcoded data.
- Parser and AST extraction are pragmatic prototype code.

## 13. Support Owned Query Results If Needed

- Add query forms that can return owned component values where appropriate.
- Decide how this overlaps with `BoundEntity::take`.
- Avoid cloning by default; borrowed snapshot results should remain the normal
  read path.

Current shortcut:
- `QueryResult::collect()` returns borrowed values tied to the owned snapshot.
- There is no owned query path in `bowl`.

## 14. Improve Macro Robustness

- Keep `#[derive(Component)]` specialized around `bowl`.
- Decide whether the macro should support renamed crates later.
- Consider better parsing through `syn` if attributes become more complex.

Current shortcut:
- The macro emits paths like `::bowl::Component` directly.
- This is fine for the current workspace but will not work automatically if a
  downstream crate renames the dependency.
- The macro uses direct `proc_macro` token walking instead of `syn`.

## 15. Add SyncBowl Wrapper

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
