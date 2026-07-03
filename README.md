# Porridge

Porridge is an ECS-inspired incremental evaluation prototype for compilers,
language tools, CLIs, daemons, and other stateful systems that are not games.

The core idea is simple:

```text
put facts in
register systems that derive more facts
query the facts you want
```

Facts are components on entities. Systems are async functions that read
memoized `Query` rows from immutable snapshots and write buffered `Commands`.
Calling `bowl.scoop::<Query<...>>().await` settles the bowl first, so callers ask for
outputs instead of manually driving pipeline stages.

This is still a prototype. The API is intentionally small and some internals are
still being shaped, but the current runtime is useful enough to explore real
compiler and service patterns.

## Quick Example

```rust
use bowl::{
    Bowl, Commands, Component, DerivedFrom, Entity, Query, Singleton, SystemExt, View,
};

#[derive(Component, Hash, PartialEq, Eq)]
#[component(hash)]
struct SourcePath(String);

#[derive(Component, Hash)]
#[component(hash)]
struct SourceText(String);

#[derive(Component, Hash)]
#[component(hash)]
struct ParsedModule {
    name: String,
}

#[derive(Component, Hash)]
#[component(hash)]
struct Diagnostic(String);

#[derive(Component)]
#[component(untracked)]
struct Ephemeral;

#[derive(Clone, Copy)]
struct ModulesParsed;

impl Component for ModulesParsed {
    fn tracked() -> bool {
        false
    }
}

async fn parse_source(query: Query<(Entity, &SourceText)>, mut commands: Commands) {
    let (source, text) = query.item();

    if text.0.contains("module") {
        commands.entity(source).insert(ParsedModule {
            name: "example".to_string(),
        });
    } else {
        commands.insert((
            DerivedFrom::new(source),
            Diagnostic("expected module declaration".to_string()),
        ));
    }
}

async fn validate_modules(
    _: Query<Entity, bowl::With<ModulesParsed>>,
    module: Query<(Entity, &ParsedModule)>,
    all_modules: View<'_, (Entity, &ParsedModule)>,
    mut commands: Commands,
) {
    let (entity, module) = module.item();

    if all_modules
        .iter()
        .any(|(other, other_module)| other < entity && other_module.name == module.name)
    {
        commands.insert((
            DerivedFrom::new(entity),
            Diagnostic(format!("duplicate module `{}`", module.name)),
        ));
    }
}

async fn cleanup_ephemeral(query: Query<Entity, bowl::With<Ephemeral>>, mut commands: Commands) {
    commands.remove(query.item());
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let bowl = Bowl::new();

    bowl.add_system(parse_source.on_settled(|mut commands: Commands| {
        commands.insert((Singleton::<ModulesParsed>::new(), ModulesParsed, Ephemeral));
    }))
    .await;
    bowl.add_system(validate_modules.run_during(bowl::Phase::Complete))
        .await;
    bowl.add_system(cleanup_ephemeral.run_during(bowl::Phase::Cleanup))
        .await;

    bowl.insert((
        SourcePath("src/main.por".to_string()),
        SourceText("module main".to_string()),
    ))
    .await;

    let diagnostics = bowl.scoop::<Query<(Entity, &Diagnostic)>>().await;
    for (entity, diagnostic) in diagnostics.collect() {
        println!("{}: {}", entity.raw(), diagnostic.0);
    }
}
```

## Mental Model

Porridge has no separate resources. Everything is a component. If a value should
exist once, insert it as a singleton component.

```rust
# use bowl::{Bowl, Component, Singleton};
# use std::collections::HashSet;
# #[derive(Component, Clone)]
# struct ImportDatabase(HashSet<String>);
# async fn example(bowl: Bowl) {
bowl.insert((Singleton::<ImportDatabase>::new(), ImportDatabase(HashSet::new())))
    .await;
# }
```

Entities are bundles of components:

```rust
# use bowl::{Bowl, Component};
# #[derive(Component, Hash, PartialEq, Eq)]
# #[component(hash)]
# struct SourcePath(String);
# #[derive(Component, Hash)]
# #[component(hash)]
# struct SourceText(String);
# async fn example(bowl: Bowl) {
bowl.insert((
    SourcePath("src/lib.por".to_string()),
    SourceText("module lib".to_string()),
))
.await;
# }
```

Systems are functions. A system can take `Query`, `View`, `Commands`, and
`WorldMetaView` parameters in the same style as Bevy system params.

```rust
# use bowl::{Commands, Component, Entity, Query, View};
# #[derive(Component)]
# struct ImportDecl { path: String }
# #[derive(Component, Clone)]
# struct ImportDatabase(std::collections::HashSet<String>);
# #[derive(Component)]
# struct Diagnostic(String);
async fn check_imports(
    import: Query<(Entity, &ImportDecl)>,
    databases: View<'_, (Entity, &ImportDatabase)>,
    mut commands: Commands,
) {
    let (import_entity, import) = import.item();
    let Some((_db_entity, db)) = databases.iter().next() else {
        return;
    };

    if !db.0.contains(&import.path) {
        commands.entity(import_entity).insert(Diagnostic(format!(
            "unknown import `{}`",
            import.path
        )));
    }
}
```

## Core Concepts

### Bowl

`Bowl` is the async database and evaluator.

```rust
# use bowl::Bowl;
let bowl = Bowl::new();
let other_handle = bowl.clone();
```

`Bowl` is internally shared and cheap to clone. Public operations take `&self`,
so callers can store it in application state or clone it into async tasks.

The runtime uses single-flight evaluation: if several callers query while work
is pending, one caller becomes the runner and the others wait for the same
generation. Read-only queries do not start duplicate work.

### Components

Components implement `bowl::Component`.

```rust
use bowl::Component;

#[derive(Component, Hash, PartialEq, Eq)]
#[component(hash)]
struct FilePath(String);

#[derive(Component)]
#[component(untracked)]
struct ScratchMarker;
```

By default, components are tracked. Tracked components participate in revision
based invalidation. If a component uses `#[component(hash)]`, inserting or
mutating an equal fingerprint does not bump its revision.

Use `#[component(untracked)]` for coordination markers that should not
invalidate memoized system rows by themselves.

### Entities And Bundles

`Bowl::insert` creates a new entity unless the bundle contains a `Singleton<T>`
marker. A bundle is currently a tuple of components.

```rust
# use bowl::{Bowl, Component, Singleton};
# #[derive(Component)]
# struct Config;
# #[derive(Component)]
# struct Enabled;
# async fn example(bowl: Bowl) {
let entity = bowl.insert((Enabled,)).await.entity();

let singleton = bowl
    .insert((Singleton::<Config>::new(), Config))
    .await
    .entity();
# }
```

Commands inside systems can insert components on an existing entity, insert a
new derived entity, or remove an entity/component.

```rust
# use bowl::{Commands, Component, Entity};
# #[derive(Component)]
# struct Parsed;
# #[derive(Component)]
# struct Diagnostic(String);
# fn example(mut commands: Commands, entity: Entity) {
commands.entity(entity).insert(Parsed);
commands.insert((Diagnostic("new derived fact".to_string()),));
commands.remove(entity);
# }
```

Command writes are buffered. Systems read an immutable snapshot, then the runner
applies their command buffers after the snapshot tick.

### Query

`Query<T, F = ()>` is the tracked system input. It decides which rows a system
runs for, and it records the component revisions used by that row.

```rust
# use bowl::{Commands, Component, Entity, Query};
# #[derive(Component)]
# struct SourceText(String);
# #[derive(Component)]
# struct Parsed;
async fn parse(query: Query<(Entity, &SourceText)>, mut commands: Commands) {
    let (entity, source) = query.item();
    let _ = source;
    commands.entity(entity).insert(Parsed);
}
```

If a matching entity has not changed since the last run, that system invocation
is skipped.

Multiple `Query` params form a cartesian product. This is useful for shader-like
systems that should run for each combination of matching rows.

```rust
# use bowl::{Component, Query};
# #[derive(Component)]
# struct Rule;
# #[derive(Component)]
# struct File;
async fn apply_rules(rule: Query<&Rule>, file: Query<&File>) {
    let rule = rule.item();
    let file = file.item();
    let _ = (rule, file);
}
```

Use `With<T>` and `Without<T>` when a component should filter a row without
being returned in the item.

```rust
# use bowl::{Component, Entity, Query, With};
# #[derive(Component)]
# struct Ready;
# #[derive(Component)]
# struct Request;
async fn handle_ready_request(_: Query<Entity, With<Ready>>, request: Query<Entity, With<Request>>) {
    let _request_entity = request.item();
}
```

### View

`View<T, F = ()>` is an ambient read of the current snapshot. It does not become
part of the system invocation key and does not invalidate that system by itself.

Use `View` when one row drives the work, but the system needs surrounding facts
to make a decision.

```rust
# use bowl::{Component, Entity, Query, View};
# #[derive(Component)]
# struct Definition { name: String }
async fn find_duplicates(
    current: Query<(Entity, &Definition)>,
    definitions: View<'_, (Entity, &Definition)>,
) {
    let (entity, definition) = current.item();

    let duplicate = definitions
        .iter()
        .find(|(other, other_def)| *other < entity && other_def.name == definition.name);

    let _ = duplicate;
}
```

### External Scoops

External scoops settle the bowl before reading.

```rust
# use bowl::{Bowl, Component, Entity, Query};
# #[derive(Component)]
# struct Diagnostic(String);
# async fn example(bowl: Bowl) {
let result = bowl.scoop::<Query<(Entity, &Diagnostic)>>().await;
let rows = result.collect();
# let _ = rows;
# }
```

Tuple scoops return several independent result sets from the same settled
snapshot. This is not a cartesian product.

```rust
# use bowl::{Bowl, Component, Entity, Query};
# #[derive(Component)]
# struct Diagnostic(String);
# #[derive(Component)]
# struct Definition { name: String }
# async fn example(bowl: Bowl) {
let (diagnostics, definitions) = bowl
    .scoop::<(
        Query<(Entity, &Diagnostic)>,
        Query<(Entity, &Definition)>,
    )>()
    .await;

let _diagnostic_rows = diagnostics.collect();
let _definition_rows = definitions.collect();
# }
```

Use `Where<...>` with typed runtime arguments for filtered reads.

```rust
# use bowl::{Bowl, Component, Entity, Gte, Query, Where};
# #[derive(Component)]
# struct Diagnostic(String);
# #[derive(Component, PartialEq, PartialOrd)]
# enum Severity { Warning, Error }
# async fn example(bowl: Bowl) {
let warnings = bowl
    .scoop::<Query<(Entity, &Diagnostic), Where<Gte<Severity>>>>()
    .arg(Severity::Warning)
    .await;
# let _ = warnings;
# }
```

Filter args are shared by every query in one scoop request. If two queries need
different values of the same arg type, use different wrapper component types for
now.

Available filter building blocks include:

- `With<T>`
- `Without<T>`
- `Where<Eq<T>>`
- `Where<Gte<T>>`
- `Where<And<A, B>>`
- `Where<Or<A, B>>`
- `Where<Not<F>>`

Filters currently scan entities. Typed indexes are a planned optimization.

### Mutable External Queries

Use `Mut<T>` in an external query when the caller needs to mutate live input
state through `&Bowl`.

```rust
# use bowl::{Bowl, Component, Entity, Eq, Mut, Query, Where};
# #[derive(Component, Hash, PartialEq, Eq)]
# #[component(hash)]
# struct SourcePath(String);
# #[derive(Component, Clone, Hash)]
# #[component(hash)]
# struct SourceText(String);
# impl SourceText {
#     fn replace(&mut self, next: impl Into<String>) { self.0 = next.into(); }
# }
# async fn example(bowl: Bowl) {
bowl.scoop::<Query<(Entity, Mut<SourceText>), Where<Eq<SourcePath>>>>()
    .arg(SourcePath("src/main.por".to_string()))
    .for_each(|(_entity, text)| {
        text.replace("module main");
    })
    .await;
# }
```

The closure is synchronous and runs while the live world is locked. Do not call
back into the same `Bowl` from inside the closure.

`Mut<T>` currently requires `T: Clone` because live mutable writes preserve
immutable snapshots with clone-on-write storage. Tracked mutations bump
revisions only when the component fingerprint changes, if the component has a
fingerprint.

### Bound Requests And Take

For request/response style APIs, insert a request entity, bind it, then take the
response component from that same entity.

```rust
# use bowl::{Bowl, Component};
# #[derive(Component, Hash)]
# #[component(hash)]
# struct HoverRequest;
# #[derive(Component, Hash, PartialEq, Eq)]
# #[component(hash)]
# struct SourcePath(String);
# #[derive(Component, Hash)]
# #[component(hash)]
# struct Position { offset: usize }
# #[derive(Component, Hash)]
# #[component(hash)]
# struct HoverInfo(String);
# async fn example(bowl: Bowl) -> Result<(), bowl::TakeError> {
let info = bowl
    .insert((
        HoverRequest,
        SourcePath("src/main.por".to_string()),
        Position { offset: 42 },
    ))
    .await
    .bind()
    .take::<HoverInfo>()
    .await?;

println!("{}", info.0);
# Ok(())
# }
```

`take::<T>()` removes the requested component and closes the bound entity.
Tuples and `Option<T>` are supported:

```rust
# use bowl::{Bowl, Component};
# #[derive(Component, Hash)]
# #[component(hash)]
# struct Info(String);
# #[derive(Component, Hash)]
# #[component(hash)]
# struct Diagnostic(String);
# async fn example(bowl: Bowl) -> Result<(), bowl::TakeError> {
# let request = bowl.insert((Info("ok".to_string()),)).await.bind();
let (info, diagnostic) = request.take::<(Info, Option<Diagnostic>)>().await?;
# let _ = (info, diagnostic);
# Ok(())
# }
```

This pattern keeps temporary request outputs from lingering in the bowl.

### System Phases And Hooks

Systems run during `Phase::Evaluate` by default. Use `run_during` for coarse
ordering:

```rust
# use bowl::{Bowl, Phase, Query, SystemExt};
# async fn cleanup(_: Query<bowl::Entity>) {}
# async fn example(bowl: Bowl) {
bowl.add_system(cleanup.run_during(Phase::Cleanup)).await;
# }
```

Available phases:

- `Startup`: once before the first evaluate phase
- `Evaluate`: default fact production
- `Complete`: checks or request handlers that should run after normal facts
- `Cleanup`: cleanup systems that should not push normal evaluation forward

Hooks colocate coordination behavior with a system:

```rust
# use bowl::{Bowl, Commands, Component, Singleton, SystemExt};
# #[derive(Component)]
# #[component(untracked)]
# struct Ephemeral;
# #[derive(Clone, Copy)]
# struct FactsAvailable;
# impl Component for FactsAvailable { fn tracked() -> bool { false } }
# async fn produce_facts() {}
# async fn example(bowl: Bowl) {
bowl.add_system(produce_facts.on_settled(|mut commands: Commands| {
    commands.insert((Singleton::<FactsAvailable>::new(), FactsAvailable, Ephemeral));
}))
.await;
# }
```

`on_start` runs before a system's planned invalid rows. `on_complete` runs after
that system processed its planned invalid rows. `on_settled` runs after normal
evaluation stops producing tracked changes, before cleanup and before the caller
observes query results.

Keep `on_settled` idempotent. A settled hook that writes tracked changes every
time can keep the bowl alive until the settle limit is reached.

### Derived Outputs

Use `DerivedFrom` for outputs that should disappear when their source facts
change.

```rust
# use bowl::{Commands, Component, DerivedFrom, Entity};
# #[derive(Component)]
# struct Diagnostic(String);
# fn example(mut commands: Commands, file: Entity) {
commands.insert((
    DerivedFrom::new(file),
    Diagnostic("syntax error".to_string()),
));
# }
```

Use `DerivedFrom::many` when an output depends on several entities:

```rust
# use bowl::{Commands, Component, DerivedFrom, Entity};
# #[derive(Component)]
# struct Diagnostic(String);
# fn example(mut commands: Commands, import: Entity, import_database: Entity) {
commands.insert((
    DerivedFrom::many([import, import_database]),
    Diagnostic("unknown import".to_string()),
));
# }
```

Register `cleanup_stale_derived` during cleanup to remove stale derived facts:

```rust
# use bowl::{Bowl, Phase, SystemExt, cleanup_stale_derived};
# async fn example(bowl: Bowl) {
bowl.add_system(cleanup_stale_derived.run_during(Phase::Cleanup))
    .await;
# }
```

The cleanup system compares the current revision of each source entity to the
revision captured when `DerivedFrom` was inserted.

## Patterns

### Parse, Then Publish A Readiness Gate

Some workflows need a marker that says "all currently visible inputs for this
system have been processed". Use an ephemeral singleton emitted from
`on_settled`.

```rust
# use bowl::{Bowl, Commands, Component, Entity, Query, Singleton, SystemExt};
# #[derive(Component)]
# struct SourceText(String);
# #[derive(Component)]
# struct Parsed;
# #[derive(Component)]
# #[component(untracked)]
# struct Ephemeral;
# #[derive(Clone, Copy)]
# struct ParsedAvailable;
# impl Component for ParsedAvailable { fn tracked() -> bool { false } }
# async fn parse_source(query: Query<(Entity, &SourceText)>, mut commands: Commands) {
#     let (entity, _) = query.item();
#     commands.entity(entity).insert(Parsed);
# }
# async fn example(bowl: Bowl) {
bowl.add_system(parse_source.on_settled(|mut commands: Commands| {
    commands.insert((Singleton::<ParsedAvailable>::new(), ParsedAvailable, Ephemeral));
}))
.await;
# }
```

Downstream systems can gate on that marker:

```rust
# use bowl::{Component, Entity, Query, With};
# #[derive(Component)]
# struct Parsed;
# #[derive(Clone, Copy)]
# struct ParsedAvailable;
# impl Component for ParsedAvailable {}
async fn check_project(_: Query<Entity, With<ParsedAvailable>>, parsed: Query<(Entity, &Parsed)>) {
    let _ = parsed.item();
}
```

A cleanup-phase system can remove `Ephemeral` entities after the caller-visible
settled boundary.

### Diagnostics Attached To Current Revisions

Diagnostics should usually be their own entities, derived from the facts that
caused them.

```rust
# use bowl::{Commands, Component, DerivedFrom, Entity};
# #[derive(Component)]
# struct Diagnostic(String);
# #[derive(Component)]
# enum Severity { Warning, Error }
fn emit_unknown_import(
    commands: &mut Commands,
    import: Entity,
    import_database: Entity,
    path: &str,
) {
    commands.insert((
        DerivedFrom::many([import, import_database]),
        Severity::Warning,
        Diagnostic(format!("unknown import `{path}`")),
    ));
}
```

When either the import entity or the import database singleton changes,
`cleanup_stale_derived` removes the diagnostic.

### Request/Response Without Persistent Trash

Use a bound entity for short-lived service requests:

```rust
# use bowl::{Bowl, Commands, Component, Entity, Query, View, With};
# #[derive(Component, Hash)]
# #[component(hash)]
# struct CompletionRequest;
# #[derive(Component, Hash, PartialEq, Eq)]
# #[component(hash)]
# struct SourcePath(String);
# #[derive(Component, Hash)]
# #[component(hash)]
# struct Position { offset: usize }
# #[derive(Component, Hash)]
# #[component(hash)]
# struct CompletionItems(Vec<String>);
# #[derive(Component)]
# struct Definition { name: String }
# async fn completions(
#     request: Query<(Entity, &SourcePath, &Position), With<CompletionRequest>>,
#     definitions: View<'_, (Entity, &Definition)>,
#     mut commands: Commands,
# ) {
#     let (request, _, _) = request.item();
#     let items = definitions.iter().map(|(_, def)| def.name.clone()).collect();
#     commands.entity(request).insert(CompletionItems(items));
# }
# async fn example(bowl: Bowl) -> Result<(), bowl::TakeError> {
let items = bowl
    .insert((
        CompletionRequest,
        SourcePath("src/main.por".to_string()),
        Position { offset: 128 },
    ))
    .await
    .bind()
    .take::<CompletionItems>()
    .await?;
# let _ = items;
# Ok(())
# }
```

The request entity gives the response a unique target, and `take` removes the
request plus remaining outputs scoped to it.

### Mutable Inputs Through Queries

Long-running clients can update input facts without needing `&mut Bowl`.

```rust
# use bowl::{Bowl, Component, Eq, Mut, Query, Where};
# #[derive(Component, Hash, PartialEq, Eq)]
# #[component(hash)]
# struct SourcePath(String);
# #[derive(Component, Clone, Hash)]
# #[component(hash)]
# struct EditableText(String);
# async fn example(bowl: Bowl) {
bowl.scoop::<Query<(Mut<EditableText>,), Where<Eq<SourcePath>>>>()
    .arg(SourcePath("src/main.por".to_string()))
    .for_each(|text| {
        text.0.push_str("\nmodule extra");
    })
    .await;
# }
```

If `EditableText` has `#[component(hash)]`, this only invalidates downstream
systems when the final value hashes differently.

### Ambient Context With View

Use `View` for global or cross-row checks without making every visible fact part
of the driving invocation.

```rust
# use bowl::{Commands, Component, DerivedFrom, Entity, Query, View};
# #[derive(Component)]
# struct Symbol { name: String }
# #[derive(Component)]
# struct Diagnostic(String);
async fn check_duplicates(
    symbol: Query<(Entity, &Symbol)>,
    symbols: View<'_, (Entity, &Symbol)>,
    mut commands: Commands,
) {
    let (entity, symbol) = symbol.item();

    if let Some((previous, _)) = symbols
        .iter()
        .find(|(other, other_symbol)| *other < entity && other_symbol.name == symbol.name)
    {
        commands.insert((
            DerivedFrom::many([entity, previous]),
            Diagnostic(format!("duplicate symbol `{}`", symbol.name)),
        ));
    }
}
```

### Singleton Components As Shared State

Singletons are components on ordinary entities. They are useful for external
truth such as configuration, package indexes, import databases, caches, or
client state.

```rust
# use bowl::{Bowl, Component, Mut, Query, Singleton};
# #[derive(Component, Clone)]
# struct PackageIndex(Vec<String>);
# async fn example(bowl: Bowl) {
bowl.insert((Singleton::<PackageIndex>::new(), PackageIndex(Vec::new())))
    .await;

bowl.scoop::<Query<(Mut<PackageIndex>,)>>()
    .for_each(|index| {
        index.0.push("std.io".to_string());
    })
    .await;
# }
```

## Current Limitations

- This is a prototype runtime, not a stabilized crate API.
- External filters scan rows; equality indexes are not implemented yet.
- Runtime filter args are keyed by component type, so using two `Eq<T>` args of
  the same type in one filter is ambiguous.
- `Mut<T>` requires `T: Clone` because snapshots use clone-on-write storage.
- Systems are async, but the current runner polls local futures rather than
  spawning work across executor worker threads.
- Output ownership is intentionally simple: rerunning a system invocation
  removes previous derived outputs owned by that invocation before applying new
  commands.
- Plugin APIs and ergonomic singleton bundle sugar are still future work.

## Repository Layout

```text
crates/
  bowl/       async runtime and public API
  macros/     Component derive macro
  playground/ experimental prototype crate
spec/         design notes and open runtime ideas
TODO.md       current implementation roadmap
```
