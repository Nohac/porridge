# Lifecycle Hooks and Ephemeral Facts

This spec describes generation-scoped facts and lifecycle hooks for `bowl`.

The motivating example is `AstAvailable`:

```rust
generate_ast.on_settled(|mut commands| {
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

Ephemeral readiness tokens should usually be untracked:

```rust
#[derive(Component)]
#[component(untracked)]
struct AstAvailable;
```

That lets systems observe the token during the generation without making token
creation/removal look like durable input churn.

## System Batch Hooks

System batch hooks are local to one system's currently planned work.

`on_start` means:

```text
the system is about to process at least one invalid/planned invocation
```

`on_complete` means:

```text
the system finished processing the invalid/planned invocations it started
```

Neither hook should fire for memo-clean systems or systems with no planned
work. This keeps `on_complete` from acting like a global phase gate.

These hooks are useful for local bookkeeping around real work:

```rust
some_system
    .on_start(|mut commands| commands.insert((WorkStarted, Ephemeral)))
    .on_complete(|mut commands| commands.insert((WorkFinished, Ephemeral)))
```

They are not the right tool for "all ASTs are available" style gates, because
other systems may still be producing facts.

## Settled Hooks

Readiness gates such as `AstAvailable` should usually use `on_settled`, not
`on_complete`:

```rust
generate_ast.on_settled(|mut commands| {
    commands.insert((Singleton::<AstAvailable>::new(), AstAvailable, Ephemeral));
})
```

`on_settled` is colocated with one system, but it runs at the global evaluation
boundary:

```text
normal work runs until no tracked work changes
no runnable invocations remain
no running invocations remain
on_settled hooks run for systems that are memo-clean
if any hook publishes commands:
  continue evaluation
else:
  run cleanup
  return to caller
```

This means a system can publish a readiness token only after the whole bowl has
stopped producing work that could make that system dirty again.

`on_settled` hooks must be idempotent. A hook that writes tracked changes every
time it runs will keep evaluation alive until the commit limit is reached,
unless that limit is disabled. Singleton markers are the intended pattern
because reinserting the same singleton can be made stable, and cleanup removes
the ephemeral marker only after downstream systems have observed it.

## Ephemeral Singleton Phase Gates

An ephemeral singleton emitted from `on_settled` is a phase transition gate:

```text
facts settle
  -> on_settled publishes AstAvailable
  -> systems gated on AstAvailable run
  -> cleanup removes AstAvailable
  -> caller observes durable outputs only
```

This is the preferred replacement for hard-coded stages. Downstream systems can
stay declarative by querying for the gate:

```rust
fn check_project(
    _: Query<Entity, With<AstAvailable>>,
    defs: Query<(Entity, &AstDef)>,
    ...
)
```

## Lifecycle Hooks

`Ephemeral` cleanup should not be bespoke insert/query logic. The bowl should
support lifecycle hooks that plugins can register.

Possible hook points:

```text
on_evaluation_start
  runs after pending input is applied, before normal systems read the snapshot

on_system_complete(system)
  runs after one registered system has finished planned invalid work

on_system_settled(system)
  runs after normal systems have settled and this system is memo-clean

on_rest / on_idle
  runs when the bowl has no pending work and is about to return to callers
```

The minimum needed hook is:

```rust
bowl.add_system(cleanup_ephemeral.run_during(Phase::Settle));
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
db.add_system(cleanup_ephemeral.run_during(Phase::Settle));
```

Current phases:

```text
Startup
  runs once before the first evaluate phase, and again after a
  preemption restart (the retraction slot)

Evaluate
  default phase for ordinary systems

Complete
  runs after evaluate systems in the same generation

Settle
  runs once per settle, after evaluate/complete have converged
```

Commands are applied between startup/evaluate/complete phases, so later normal
phases can observe facts produced by earlier normal phases in the same
generation. Settle uses the same command buffer model, but it is held until
normal phases stop producing tracked changes, and it cannot drive its own
settle forward: removal commands are committed before outside callers observe
query results (stale facts are reaped from the settled view), while insert and
spawn commands are queued as inputs for the start of the next run — a settle
system can seed the next state-machine step, but never re-open the current
settle.

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

`Ephemeral` should initially mean entity lifetime. The settle phase removes entities
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

settled:
  generate_ast on_settled inserts ephemeral AstAvailable

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

`on_settled` command buffers therefore need to be injected back into the
evaluation loop. They cannot be final callbacks after cleanup: downstream
systems must be able to observe phase-transition markers before the bowl
returns to outside callers.

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

- Should lifecycle hooks be async systems, sync callbacks, or both?
- Should hooks participate in memoization?
- Are hook outputs owned by the hook invocation, by a synthetic system id, or
  by the current evaluation?
- Should `Ephemeral` remove whole entities or only ephemeral components?
- How should ephemeral cleanup affect revisions and memo invalidation?
- Should cleanup run as a final command application in the same generation, or
  create a distinct generation that outside callers never observe?
