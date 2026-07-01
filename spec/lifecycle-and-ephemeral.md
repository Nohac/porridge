# Lifecycle Hooks and Ephemeral Facts

This spec describes generation-scoped facts and lifecycle hooks for `bowl`.

The motivating example is `AstAvailable`:

```rust
generate_ast.on_complete(|mut commands| {
    commands.insert((Singleton::<AstAvailable>::new(), AstAvailable, Ephemeral));
})
```

`AstAvailable` is not a durable fact about the project. It means:

```text
during this evaluation pass, AST generation has completed
```

Systems can use that fact to gate project-wide validation. After the evaluation
finishes, the marker should disappear.

## Durable Facts vs Readiness Tokens

Durable facts should remain in the bowl:

```text
FileText
ParsedFile
AstDef
Diagnostic
SystemImportDb
```

Readiness tokens should usually be ephemeral:

```text
AstAvailable
ImportsChecked
DiagnosticsFlushed
```

An ephemeral fact is still an ordinary component. The difference is its
lifetime: it exists only for the current evaluation cycle.

## Completion Semantics

System completion must not mean "the system emitted outputs".

For memoized systems, these are all completions:

```text
system ran at least one invalid invocation
system had matching invocations, but all were memo-clean
```

The default should be match-sensitive:

```text
matched + changed      -> fire on_complete
matched + memo-clean   -> fire on_complete
no matches             -> do not fire on_complete
```

This avoids publishing a readiness token too early when another system later
inserts facts that would make the system match. A system with no matches has
not completed work for any actual row; it has simply had nothing to consider.

For `generate_ast`, a memo-clean pass still means all AST outputs are already
valid. Therefore an `on_complete` hook should be able to insert
`AstAvailable` even when `generate_ast` did not write any new `AstDef`.

The default useful meaning for per-system `on_complete` is:

```text
the system has finished considering at least one matching invocation for this
generation
```

If "no matches" should also count as completion, that should be an explicit
variant rather than the default:

```rust
system.on_complete(callback)
system.on_complete(callback).including_empty()
system.on_always_complete(callback)
```

Exact API names are open, but the semantics should be explicit.

## Lifecycle Hooks

`Ephemeral` cleanup should not be bespoke insert/query logic. The bowl should
support lifecycle hooks that plugins can register.

Possible hook points:

```text
on_evaluation_start
  runs after pending input is applied, before normal systems read the snapshot

on_system_complete(system)
  runs after one registered system has finished its invocations for a generation

on_evaluation_complete
  runs after normal systems have settled, before the caller observes results

on_rest / on_idle
  runs when the bowl has no pending work and is about to return to callers
```

The minimum needed hook is:

```rust
bowl.add_system(cleanup_ephemeral.run_during(Phase::Cleanup));
```

Lifecycle hooks should be expressed using the same command buffer model as
systems:

```rust
async fn cleanup_ephemeral(
    ephemerals: View<'_, Entity, With<Ephemeral>>,
    mut commands: Commands,
) {
    for entity in ephemerals {
        commands.remove(entity);
    }
}
```

This uses normal buffered remove commands.

`run_during` is the coarse system scheduling hook:

```rust
db.add_system(parse_file); // default: Phase::Evaluate
db.add_system(cleanup_ephemeral.run_during(Phase::Cleanup));
```

Current phases:

```text
Startup
  runs once before the first evaluate phase

Evaluate
  default phase for ordinary systems

Complete
  runs after evaluate systems in the same generation

Cleanup
  runs after evaluate/complete have settled
```

Commands are applied between startup/evaluate/complete phases, so later normal
phases can observe facts produced by earlier normal phases in the same
generation. Cleanup runs once at the end of settlement, before outside callers
observe query results.

## Plugins

Plugins should be small objects that register systems, hooks, and possibly
component behavior.

Sketch:

```rust
pub trait BowlPlugin {
    fn build(&self, bowl: &Bowl);
}

bowl.add_plugin(EphemeralPlugin).await;
```

The `EphemeralPlugin` would register:

```text
on_evaluation_complete(cleanup_ephemeral)
```

`Ephemeral` itself stays a normal marker component:

```rust
#[derive(Component)]
pub struct Ephemeral;
```

Helpers are only sugar:

```rust
commands.insert((Singleton::<AstAvailable>::new(), AstAvailable { data }, Ephemeral));
```

`Ephemeral` should initially mean entity lifetime. Cleanup removes entities
marked with `Ephemeral`, including ephemeral singleton entities, and updates any
internal indexes such as singleton caches. Component-scoped ephemeral cleanup can
be added later if a real use case needs it.

## Execution Shape

With snapshot ticks, the intended flow is:

```text
pending input is applied

tick 1 snapshot:
  parse_file
  generate_ast

barrier:
  generate_ast outputs applied
  generate_ast on_complete inserts ephemeral AstAvailable

tick 2 snapshot:
  check_duplicate_defs sees AstAvailable
  check_imports sees AstAvailable if it asks for it

settled:
  on_evaluation_complete removes Ephemeral entities/components

caller observes durable outputs
```

Important rule:

```text
ephemeral cleanup happens after systems that need the token have had a chance
to run, but before the bowl returns to outside callers
```

`on_complete` command buffers therefore need to be injected back into the
evaluation loop. They cannot be final callbacks after all systems have settled:
downstream systems must be able to observe completion markers in a later
snapshot tick within the same overall evaluation.

## Relationship To BoundEntity

`BoundEntity` is still the request/response cleanup primitive:

```rust
db.insert((HoverRequest, FilePath(path), Position { offset }))
    .await
    .bind()
    .take::<HoverInfo>()
    .await
```

`Ephemeral` is not a replacement for `BoundEntity`.

The split is:

```text
BoundEntity
  caller-scoped ownership and destructive take

Ephemeral
  generation-scoped coordination facts
```

Request outputs can still use `BoundEntity` cleanup without being ephemeral.
Readiness markers such as `AstAvailable` should usually be ephemeral.

## Open Questions

- Should `on_complete` fire for zero matching rows by default?
- Should lifecycle hooks be async systems, sync callbacks, or both?
- Should hooks participate in memoization?
- Are hook outputs owned by the hook invocation, by a synthetic system id, or
  by the current evaluation?
- Should `Ephemeral` remove whole entities or only ephemeral components?
- How should ephemeral cleanup affect revisions and memo invalidation?
- Should cleanup run as a final command application in the same generation, or
  create a distinct generation that outside callers never observe?
