# Porridge

Porridge is an ECS-inspired incremental evaluation prototype for compilers,
language tools, and other stateful systems that are not games.

Callers insert facts. Async systems derive more facts. Queries ask for settled
results. The runtime tracks which component revisions each system invocation
observed, so unchanged rows stay memoized while affected rows run again.

```text
base facts -> snapshots -> memoized systems -> buffered facts -> fixed point
                                                                  |
                                                       settled query results
```

The project is deliberately experimental. Its API and execution model are
still being shaped, but the repository already exercises the model with a toy
language service, demand-driven diagnostics, hover requests, relationships,
joins, schemas, plugins, replication seams, and incremental edits.

The best end-to-end tour is [`crates/playground`](crates/playground). The design
documents in [`spec/`](spec) explain the model in more detail, and
[`TODO.md`](TODO.md) tracks what is implemented versus still exploratory.

## Why this model?

Compiler pipelines tend to accumulate several overlapping mechanisms:

- a source database;
- caches and invalidation keys;
- stage orchestration;
- request-specific work;
- indexes and reverse indexes;
- lifecycle rules for diagnostics and other derived state.

Porridge explores whether those mechanisms can share one vocabulary:

- **entities** identify things;
- **components** are facts about them;
- **systems** derive facts from tracked query rows;
- **views** read ambient context from the same snapshot;
- **commands** publish buffered output;
- **schemas** describe valid entity shapes;
- **settling** drives the graph until callers can observe a fixed point.

This is closer to an incremental database than a traditional staged compiler.
Systems do not call the next stage. They state what facts they need and what
facts they may produce.

## Quick start

The workspace is not currently presented as a published crate. Clone the
repository and use the `bowl` workspace crate while exploring the API.

This complete example defines one schema shape, derives an optional fact on
that shape, and queries the settled result:

```rust
use bowl::{Bowl, Commands, Component, Entity, Query, Schema};

#[derive(Component, Hash)]
#[component(hash)]
struct SourceText(String);

#[derive(Component, Hash)]
#[component(hash)]
struct LineCount(usize);

#[derive(Schema)]
struct QuickSchema {
    source_file: (SourceText, Option<LineCount>),
}

async fn count_lines(
    source: Query<(
        Entity<quick_schema::SourceFile>,
        &SourceText,
    )>,
    mut commands: Commands<(quick_schema::SourceFile,)>,
) {
    let (entity, text) = source.item();
    commands.entity(entity).insert(LineCount(text.0.lines().count()));
}

#[tokio::main]
async fn main() {
    let bowl = Bowl::builder()
        .schema::<QuickSchema>()
        .system(count_lines)
        .build();

    bowl.insert((SourceText("one\ntwo\nthree".to_string()),))
        .await;

    let counts = bowl.scoop::<Query<(Entity, &LineCount)>>().await;
    for (_entity, count) in counts.collect() {
        println!("{}", count.0);
    }
}
```

`scoop` settles pending work before materializing the query. The first read
therefore prints `3`; a later equal `SourceText` fingerprint causes no rerun,
while changed text invalidates only the affected row.

The schema is doing real work here. `source_file` declares that `SourceText` is
required and `LineCount` is optional. The generated
`quick_schema::SourceFile` facet is used both to anchor the query and to declare
which shape the system may update. Through that typed facet, only optional
parts can be added incrementally.

Run the repository itself with:

```sh
cargo build
cargo test --workspace
RUST_LOG=info cargo run -p playground
```

## The programming model

### Components and entities

Every stored fact implements `Component`. Components are tracked by default:
writing one advances the world's revision and invalidates memo entries that
observed its previous revision.

The derive macro supports three important policies:

- `#[component(hash)]` fingerprints the value. An equal fingerprint preserves
  the old revision and cuts off downstream work.
- `#[component(revision)]` uses a `revision: u64` field as the fingerprint.
- `#[component(untracked)]` makes a coordination fact invisible to revision
  invalidation.

Entities are stable IDs carrying bundles of components. There are no
archetypes and no separate resource store. A value that should exist once is a
component inserted with `Singleton<T>`.

External input is queued through `Bowl::insert`. Existing entities can be
updated, stripped of a component, or despawned through `bowl.entity(entity)`.
Inputs arriving during evaluation are assigned to an epoch boundary rather
than mutating the snapshot currently being evaluated.

### Schemas and facets

`#[derive(Schema)]` turns a named-field struct of component tuples into the
data model for a bowl. Each field is an entity shape; `Option<T>` marks a part
that may be added after the required shape exists.

Schemas provide:

- strict compile-time matching for spawned command bundles;
- generated shape aliases such as `lang_schema::Diagnostic`;
- typed `Entity<Shape>` facet handles;
- debug-time conformance checks for incremental writes;
- a closed component universe for presence bitmaps and registration analysis.

A bowl may be schema-less, but schemas are the intended way to model larger
applications. Plugins contribute schema fragments, and the builder unions all
fragments before laying out the world.

### A sealed builder

Construction is builder-only:

```text
Bowl::builder()
    .schema::<AppSchema>()
    .plugin(AppPlugin)
    .system(derive_something)
    .build()
```

The system set and schema universe seal at `build`. Porridge intentionally has
no dynamic system registration. Conditional subsystems are expressed as facts
that gate planning rather than as systems appearing and disappearing at
runtime.

A `Plugin` packages schema shapes and system registration together. This keeps
a reusable subsystem's data model and behavior from drifting apart.

### Tracked `Query`

`Query<T, F>` is a system's tracked input. Every matching row contributes:

- entity keys, which identify the system invocation;
- component revisions, which invalidate the invocation;
- row-level read/write access, which the scheduler uses for conflicts.

One query row normally means one memoized invocation. Multiple query params
form a product, unless a bound join narrows the combinations. A clean row is
planned or skipped without running user code again.

Query parts can include borrowed components, typed or untyped entities,
optional components, and `MutRef<'_, T>` for scheduler-visible in-place system
mutation. Filters such as `With<T>` and `Without<T>` affect matching without
appearing in the item.

### Ambient `View`

`View<T, F>` reads other rows from the invocation's snapshot but contributes no
memo dependency. It is useful when one tracked row drives work that needs broad
context, such as checking one definition against every visible definition.

That distinction is intentional and important: changing only viewed data does
not rerun the system. Use tracked joins, a tracked driving fact, or a phase
boundary when the ambient data must cause or order work. The runtime's settled
view-healing pass protects specific cross-phase cases, but `View` is not a
general dependency declaration.

### Typed, buffered `Commands`

`Commands<S>` declares a system's output set. It has no public wildcard and no
default type parameter.

Commands can:

- add a declared component to an existing entity;
- spawn a complete declared shape;
- remove a component;
- remove an entity.

Writes are buffered while the system reads an immutable structural snapshot.
At commit, the invocation's old derived output is diffed against the new
buffer. Equal fingerprints keep their revisions, and stale output that was not
re-emitted is removed.

`Commands<()>` is a removal-only writer. This is particularly useful for
cleanup systems in `Phase::Settle`.

### Derived facts and ownership

Every system command is owned by the invocation that emitted it. When that
invocation reruns, its output is replaced as a unit even when the output lives
on other entities.

Use `DerivedFrom::new(source)` or `DerivedFrom::many(sources)` for facts whose
lifetime is also tied to source entity revisions. A standard cleanup system
reaps stale derived entities:

```text
cleanup_stale_derived.run_during(Phase::Settle)
```

Diagnostics, indexes, summaries, and service candidates are generally clearer
as their own derived entities rather than components accumulated on the input
entity.

### Filters, indexes, and joins

External and system queries share the same filter vocabulary:

- `With<T>` and `Without<T>` for presence;
- `Where<Eq<T>>` and `Where<Gte<T>>` for runtime values;
- `And`, `Or`, and `Not` for composition;
- `Named<Tag, Query<...>>` when one external scoop needs separate arguments of
  the same type.

Fingerprint stores maintain an index used by `Where<Eq<T>>` candidate lookup.

Inside systems, `Where<Eq<K>>` can bind a query to the unique sibling query
that reads `&K`. `Where<In<R>>` binds member entities through an
engine-maintained relationship inverse. Single-key joins expand from actual
pairs instead of constructing the full product; compound keys retain a
product-and-prune path. See [`spec/joins.md`](spec/joins.md) for the precise
rules and validation constraints.

### Relationships

Relationship edge components can maintain an ordered inverse component on a
target entity. The derive attributes are:

```text
#[relationship(target = TargetComponent)]
#[relationship_target(relationship = EdgeComponent)]
```

Retargeting or removing an edge updates the inverse. `Where<In<Target>>` uses
that inverse as an identity join, which gives graph-like language facts the
same tracked query semantics as ordinary components.

### Phases and hooks

Systems run in `Phase::Evaluate` by default. `SystemExt::run_during` selects a
coarse phase:

1. `Startup` runs at the start of the first generation and after preemption
   restarts.
2. `Evaluate` derives normal facts.
3. `Complete` runs after Evaluate reaches its phase boundary; checks and
   request handlers often live here.
4. `Settle` runs at convergence. Its removals apply before settled reads
   return, while inserts and spawns defer to the next run.

`on_start`, `on_complete`, and `on_settled` attach callbacks to a system's
lifecycle. Ephemeral singleton markers can bridge settle boundaries, but they
represent state, not ordering. Prefer tracked joins and phases for ordering.

### Generations, snapshots, and scheduling

Inputs are batched into generations. Each evaluation wave plans from a shared
structural snapshot and memo table, runs non-conflicting invocations
concurrently, then commits completed buffers. A commit whose captured
dependencies went stale is discarded and replanned.

Snapshots clone maps and shared guarded component cells, not user payloads.
The scheduler admits concurrent row reads and disjoint writes while
serializing conflicting access. User systems are locally polled together by
the active runner; they are not currently dispatched as a general worker-pool
job graph.

`Bowl` itself is cheap to clone. Evaluation is single-flight: one caller drives
pending work while concurrent callers wait on the same generation rather than
starting duplicate evaluation.

`CommitLimit` is the non-convergence guardrail, not the definition of
settlement. The default bounds accepted commits; `CommitLimit::None` supports
experiments that arrange cancellation externally.

## Calling the bowl

### Settled reads

External reads use the same `Query` row language:

```text
bowl.scoop::<Query<(Entity, &Diagnostic)>>().await
bowl.scoop::<Query<(Entity, &Diagnostic), Where<Gte<Severity>>>>()
    .args(Severity::Warning)
    .await
```

A tuple scoop returns independent result sets from one settled snapshot; it is
not a Cartesian product. `.last_settled()` is available when a caller prefers
the most recent completed snapshot over waiting for current work.

### External mutation

There are two scoped mutation paths:

- `Cow<T>` runs a synchronous clone-on-write closure over matching input rows.
- `Mut<T>` returns inert handles whose `with_original` or `with_latest`
  methods acquire live mutable access for a synchronous closure.

Inside systems, use `MutRef<'_, T>` instead. Its write is visible to the
scheduler, and commit bookkeeping absorbs the row's own revision so a system
does not invalidate itself merely by performing its declared mutation.

### Bound request/response

Temporary service requests can use a destructive response path:

```text
bowl.insert((HoverRequest, FilePath(path), Position { offset }))
    .await
    .bind()
    .take::<HoverInfo>()
    .await
```

`BoundEntity::take` waits for settlement, extracts the requested response, and
cleans up the request scope. Tuples and optional response components are
supported.

### Observability

`explain_all` and `profile_all` expose why systems did or did not run and where
work accumulated. The playground uses these alongside process-global debug
counters to make incremental behavior inspectable during experiments.

## The playground

[`crates/playground`](crates/playground) is the primary integration example and
the most useful place to see the pieces composed.

It implements a small language service with:

- parser generation from
  [`syntax.llw`](crates/playground/src/lang/grammar/syntax.llw);
- one [`LangSchema`](crates/playground/src/lang/schema.rs) as the data-model
  source of truth;
- a [`LangPlugin`](crates/playground/src/lang/mod.rs) that installs shapes and
  systems together;
- vertical language entities for documents, imports, definitions, and
  namespaces;
- demand-driven diagnostics;
- request/response hover through candidate facts and tracked arbitration;
- namespace qualification through bound joins and a derived `SystemParam`
  bundle;
- a small replication plugin that captures shape-granular state.

The language entity structure is intentional: grammar code owns syntax,
vertical entity modules own lowering and analysis behavior, and service modules
own request/response facts. See
[`spec/language-entities.md`](spec/language-entities.md).

Run it with tracing:

```sh
RUST_LOG=info cargo run -p playground
```

The executable inserts several files, mutates the import database, requests
diagnostics and hover information, inspects replication records and qualified
names, performs incremental edits, and prints evaluation profiles.

## Current status and limitations

Porridge is a research prototype, not a production-ready database or compiler
framework.

Implemented today:

- sealed builder, schemas, plugins, strict spawns, and typed facets;
- revision tracking with fingerprint cutoffs;
- row-level memoization and conflict-aware async scheduling;
- generation and preemption semantics for external input;
- phases, hooks, singletons, derived ownership, and stale cleanup;
- indexed equality filters, bound joins, relationships, and optional rows;
- guarded snapshots plus scoped external and system mutation;
- bound request/response, settled notifications, explanation, and profiling;
- criterion coverage for major planning and scanning paths.

Important constraints and open directions:

- The public API is still being polished (TODO: **Public API Polish Before
  Larger Migration**).
- `View` remains ambient by design and requires deliberate tracked drivers or
  phase boundaries (TODO: **Clarify View Dependency Semantics**).
- Non-convergence and dependency explanations can become much richer (TODO:
  **Add Better Non-Settling And Cycle Diagnostics** and **Add Dependency Graph
  Introspection**).
- Systems are concurrently polled locally, but broader parallel execution is
  still a design track (TODO: **Add Async Parallel System Execution**).
- Long-running daemon/client operation, persistence boundaries, tombstones,
  and replication streams remain exploratory (TODO: **Explore Long-Running
  Daemon Runtime** and **Explore Replication / Change Streams**).
- There is no dynamic system registration, resource subsystem, archetype
  storage, distributed evaluator, or stable compatibility promise.

Read [`TODO.md`](TODO.md) for the detailed status. Completed and proposed work
currently coexist there, so section titles are a better reference than item
numbers.

## Workspace layout

| Path | Purpose |
| --- | --- |
| [`crates/bowl`](crates/bowl) | Incremental engine: world, storage, queries, systems, scheduling, and public API. |
| [`crates/macros`](crates/macros) | `Component`, `Schema`, relationship, and `SystemParam` proc macros. |
| [`crates/playground`](crates/playground) | Toy language and integration playground. |
| [`crates/benches`](crates/benches) | Criterion fixtures and engine slow-path benchmarks. |
| [`spec`](spec) | Formal model, architecture notes, and active designs. |
| [`TODO.md`](TODO.md) | Detailed implementation roadmap and status notes. |

Engine unit tests are inline, mostly in
[`crates/bowl/src/bowl.rs`](crates/bowl/src/bowl.rs). Playground integration
tests live in
[`crates/playground/src/tests.rs`](crates/playground/src/tests.rs).

## Development commands

```sh
cargo build
cargo test -p bowl
cargo test -p bowl <test_name>
cargo test -p playground
cargo test --workspace
cargo run -p playground
RUST_LOG=info cargo run -p playground
cargo run -p playground --release
cargo bench -p benches
cargo bench -p benches -- --test
```

Grammar changes go through
[`crates/playground/src/lang/grammar/syntax.llw`](crates/playground/src/lang/grammar/syntax.llw).
The playground build script runs `lelwel`; generated parser code is included by
the grammar module.

## Design documents

The specifications are working design documents. Some describe implemented
behavior precisely; others preserve rationale or explore the next step. Check
the code and [`TODO.md`](TODO.md) when implementation status matters.

- [`formal-semantics.md`](spec/formal-semantics.md) — facts, invocations,
  dependency validity, commits, and settled observation.
- [`streaming-evaluation.md`](spec/streaming-evaluation.md) — planning,
  concurrent runs, stale commits, and convergence.
- [`epochs.md`](spec/epochs.md) — input batches, preemption, phase restarts, and
  settled boundaries.
- [`access-scheduling.md`](spec/access-scheduling.md) — row-level access and
  scheduler conflicts.
- [`joins.md`](spec/joins.md) — equality joins, relationship membership joins,
  outer joins, and validation.
- [`language-entities.md`](spec/language-entities.md) — the playground's
  vertical language architecture.
- [`lifecycle-and-ephemeral.md`](spec/lifecycle-and-ephemeral.md) — hooks,
  phases, and coordination markers.
- [`derived-from.md`](spec/derived-from.md) — revision-scoped derived facts and
  cleanup.
- [`bound-entity.md`](spec/bound-entity.md) — destructive request/response
  scopes.
- [`daemon-client.md`](spec/daemon-client.md) — out-of-core state and
  long-running service direction.
- [`performance-plan.md`](spec/performance-plan.md) and
  [`bench-reports.md`](spec/bench-reports.md) — measurement strategy and
  recorded investigations.

If you are evaluating whether the model fits a real compiler or service, start
with the quick example, run the playground with tracing, then read the formal
semantics and streaming evaluation documents alongside the engine source.
