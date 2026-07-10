# Declared Outputs and Schemas

Systems declare *inputs* statically (queries, views, joins) but outputs are
dynamic commands — the engine learns what a system produces only by running
it. This spec adds the missing half: typed output declarations on
`Commands`, component groups, and entity schemas. Together with the
existing input registry they complete the dependency graph, at
`add_system` time.

Motivating workload: dsql (36 systems, 21 pinned to `Complete`, ~70
component types in six natural vocabularies, five demand markers gating
whole subsystems). Every design decision below is checked against it.

## Principles

1. **Declarations never change what converges.** Tracked consumers already
   order correctly through replanning. Declarations are a static-checking
   and scheduling layer over unchanged semantics: a bowl full of
   undeclared systems behaves exactly like today's engine.
2. **A wrong declaration must be impossible or loud.** Compile-time where
   the type system reaches (bundle membership), debug-runtime where it
   cannot (entity shapes, incremental inserts), never trusted silently.
3. **Adoption is per-system.** Bare `Commands` remains legal and means
   "declares everything" (a wildcard edge in the graph). Checks degrade
   conservatively around wildcards.

## Layer 1: typed `Commands<S>` (declared output types)

```rust
type DiagnosticParts = (Diagnostic, Severity, DerivedFrom);   // a group: just a tuple alias

async fn check_imports(
    query: Query<(Entity, &ImportDecl)>,
    mut commands: Commands<(DiagnosticParts, Span)>,
) { ... }
```

- `Commands<S = Anything>`: the default keeps every existing signature
  compiling; `Anything` is the wildcard.
- `S` is a tuple mixing bare component types and group aliases. Groups are
  ordinary tuple aliases — closed, nestable, *enumerable at runtime* by a
  trait walked over the tuple (unlike trait-impl membership, which cannot
  be enumerated). One alias serves both the compile-time checker and the
  registration-time graph.
- `insert`/`spawn`/`entity(..).insert` bound their bundles to `S` via
  type-level list membership (reflexive: every component is its own
  singleton group; the marker-parameter trick disambiguates positions).
  **Emitting an undeclared component does not compile.** Removals are not
  production and stay unbounded.
- Known wart: a component reachable through two declared items makes the
  membership proof ambiguous ("type annotations needed"). Rule: don't
  double-declare; keep groups disjoint.
- Runtime honesty backstop: the commit path already records every derived
  write (`written_derived`); in debug builds, a write outside the writer's
  declared set panics. This exists for the wildcard-into-typed migration
  boundary and for any future dynamic emission path.

Infrastructure components (`DerivedFrom` etc.) are declared like any
other — groups make that painless, and exemptions would weaken the
contract.

## Layer 2: `#[derive(Schema)]` (declared entity shapes)

```rust
#[derive(Schema)]
struct LangSchema {
    diagnostic:  (Diagnostic, Severity, DiagnosticCode, DerivedFrom, Option<Span>),
    hover_answer:(RequestKey, HoverEnriched, Cursor, BelongsToFile, HoverInfo),
    candidate:   (HoverCandidate, RequestKey, DerivedFrom),
}
```

- Named fields = named shapes; `Option<T>` = optional part. The derive
  validates at macro time (duplicates, empty shapes) and generates both
  the trait impls and `const` metadata with type names, so runtime errors
  can say *"entity violates shape `diagnostic`: missing `Severity`"*.
- Shape conformance is **debug-runtime, at commit** — deliberately not
  compile-time: `entity(e).insert(Severity)` is legal or not depending on
  what `e` already carries, which no static check can see, and the probes
  already exist (`written_derived` + `has_dyn`, the entity-granular flag
  machinery).
- Registered via `Bowl::with_schema::<LangSchema>()`; a bowl without a
  schema skips conformance entirely.
- **Shapes are reusable declaration types.** The derive generates a
  companion trait (`LangSchemaShapes`) with one associated type per named
  field, so `LangSchema::Diagnostic` names the shape tuple in type
  position (inherent associated types are unstable; the companion trait is
  the stable spelling). In `Commands<S>` position a shape degrades to a
  *group* of its component types — `Option<T>` declares `T` ("may emit") —
  so the schema is the single source of truth and most hand-written group
  aliases disappear. Stretch, staged later: `insert_as::<S::Diagnostic>`
  bounds a bundle both ways (all required parts, nothing outside the
  shape), moving whole-shape spawns to compile-time conformance and
  leaving debug-runtime checks only for incremental
  `entity(e).insert` completion.

## Layer 3: the graph (what declarations buy)

With inputs (existing registry: queries, `view_sets`, join keys) and
outputs (layer 1) and shapes (layer 2), `add_system` can build the full
system graph. Staged consumers, in order:

1. **Registration-time same-phase refusal**: a new system's `View`
   required-sets checked against same-phase declared producers *through
   the schema's shapes* (a produced shape must be able to match the view
   row — this is what keeps shared vocabulary components like `Span` from
   false-positiving, the lesson from the commit-time flag). Wildcard
   systems stay covered by the existing commit-time flag.
2. **Interest sets for planner gating**: declared outputs + input types
   give exact wake lists; demand-gated subsystems (dsql: five markers
   gating ~20 of 36 systems) skip at zero planning cost.
3. **Stratification**: SCC-collapse the graph, iterate cycles (fixed
   points are legitimate), topo-order between SCCs — killing speculative
   run-discard-replan inside phases, and eventually deriving what phases
   hand-roll today.
4. **Event-driven scheduling**: a commit of `T` wakes exactly the
   consumers of `T` (the streaming-evaluation endgame).

## Implementation order

1. **`Commands<S>` + membership + runtime honesty check** (this pass):
   `Anything` wildcard default, `DeclaredIn`/bundle bounds, group aliases,
   `SystemParam::declared_outputs` registry on `BoxedSystem`, debug panic
   on undeclared derived writes.
2. **`#[derive(Schema)]`**: macro, shape metadata, commit-time conformance
   with named-shape messages, `with_schema` registration.
3. **Registration-time phase refusal** (needs 1 + 2 for precision).
4. **Graph consumers** (gating, stratification, event-driven): separate
   efforts, each with benches.
