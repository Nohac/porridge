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
3. **No silent wildcard.** There is no public undeclared `Commands`: bare
   `Commands` does not compile, every system declares its output set (an
   empty declaration, `Commands<()>`, marks a removal-only writer), and
   hooks declare through their closure's `Commands<S>` parameter — merged
   into the wrapped system's registry entry. A crate-internal wildcard
   (`Anything`) exists only for the runner's raw per-invocation buffer and
   engine tests; a system that genuinely needs breadth declares a wide
   group *explicitly*, so looseness is always spelled, grep-able, and
   visible in the graph.

## Layer 1: typed `Commands<S>` (declared output types)

```rust
type DiagnosticParts = (Diagnostic, Severity, DerivedFrom);   // a group: just a tuple alias

async fn check_imports(
    query: Query<(Entity, &ImportDecl)>,
    mut commands: Commands<(DiagnosticParts, Span)>,
) { ... }
```

- `Commands<S>` has no default: declaring is not optional.
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
  double-declare; keep groups disjoint. This bites harder than it reads:
  a lowering-walk *union* group (several shapes sharing `DerivedFrom`,
  `BelongsToFile`, …) breaks bare `entity(..).insert(X)` proofs for the
  shared components while full spawn bundles keep compiling (`SpawnsAs`
  resolves per shape, membership resolves per component). When an
  increment turns ambiguous, split or narrow the writer's `Commands`
  declaration so the increment has one owning shape
  (`Commands<(my_schema::Thing,)>` locally) — don't thin the shapes
  themselves to dodge the overlap.
- Shape width caps at **12 parts** (the tuple-impl ceiling; the derive
  rejects wider shapes by name with the actual width). Conformance
  merges *within one invocation's command buffer*: a spawn plus
  same-entity stamps from the same invocation check as one bundle — it
  is not a merge across independent system commits, so a linking
  component (`ChildOf`) belongs in the spawn bundle; use two shape
  variants when it is conditional.
- Engine-maintained inverses (`Children`-style `#[relationship_target]`
  components) get **degenerate one-part shapes** in the schema. This is
  a write-bundle conformance convention — it names the inverse for
  universe closure and lets the maintenance writes conform — not a
  claim that the target entity carries nothing else.
- ~~Runtime honesty backstop~~ (retired): with no public wildcard and
  strict spawns, undeclared emission became unrepresentable, so the
  debug-build commit check was removed — the type system carries the
  contract. `written_derived` recording remains (it feeds shape
  conformance and the same-phase flag, which guard what the type system
  cannot see).

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
- Installed at construction (`Bowl::builder().schema::<LangSchema>()` or
  a plugin's fragment); a bowl without a schema skips conformance
  entirely.
- **Shapes are reusable declaration types.** The derive generates a
  companion *module* (snake-cased schema name) with one type alias per
  shape, so `lang_schema::Diagnostic` names the shape tuple in type
  position. (Associated types were tried first: `Schema::Shape` shorthand
  does not resolve on concrete types — E0223 — and a module alias whose
  name matches one of the shape's own components would shadow itself into
  a cycle, so the raw tuples live at the outer scope under hidden names
  and the module re-aliases them. Inherent associated types —
  `impl LangSchema { type Diagnostic = ...; }`, the syntax that would make
  the module unnecessary — are unstable (rust-lang/rust#8995; the
  lazy-normalization blocker landed early 2026), and when they stabilize
  the derive switches its output so `LangSchema::Diagnostic` works
  without touching user code.) In `Commands<S>` position a shape
  degrades to a *group* of its component types — `Option<T>` declares `T`
  ("may emit") — so the schema is the single source of truth and most
  hand-written group aliases disappear. Stretch, staged later:
  `insert_as::<lang_schema::Diagnostic>` bounding a bundle both ways.

## Layer 3: the graph (what declarations buy)

With inputs (existing registry: queries, `view_sets`, join keys) and
outputs (layer 1) and shapes (layer 2), `add_system` can build the full
system graph. Staged consumers, in order:

1. **Registration-time same-phase analysis** (shipped as a *warning*, not
   a refusal): a new system's `View` required-sets checked against
   same-phase declared producers *through the schema's shapes* (a produced
   shape must be able to carry the whole view row — this is what keeps
   shared vocabulary components like `Span` from false-positiving, the
   lesson from the commit-time flag). A warning because marker-gated
   same-phase consumers — whose gate defers them a generation — are
   legitimate and undetectable statically; the commit-time flag remains
   the precise enforcement with its dynamic zero-row exemption. Requires a
   schema; the builder installs it before any system by construction.
2. **Interest sets for planner gating**: declared outputs + input types
   give exact wake lists; demand-gated subsystems (dsql: five markers
   gating ~20 of 36 systems) skip at zero planning cost.
3. **Stratification**: SCC-collapse the graph, iterate cycles (fixed
   points are legitimate), topo-order between SCCs — killing speculative
   run-discard-replan inside phases, and eventually deriving what phases
   hand-roll today.
4. **Event-driven scheduling**: a commit of `T` wakes exactly the
   consumers of `T` (the streaming-evaluation endgame).

## Layer 4: typed entities (facets) — decided design

The end state agreed for the schema arc; supersedes untyped `Entity` in
user-facing APIs (bare `Entity` remains engine-internal and at the dynamic
boundary, where replication/external adapters apply components by runtime
`TypeId`).

- **Facet semantics.** `Entity<H>` means *conforms at least to shape `H`*,
  not "is exactly H": entities are multi-faceted on purpose (a request
  entity carries base request components plus the derived answer shape).
  Two typed handles to the same id may coexist; each bounds what flows
  through *it*. Exclusive table-row semantics were considered and
  rejected — they would forbid ride-along modeling (dsql's resolution
  facts on syntax entities) and force entity-per-shape joins.
- **Acquisition.** Spawns: `commands.insert(bundle)` becomes *strict* —
  the bundle must match one shape declared in `S` (bundle ⊆ shape, all
  required parts present, unions: at least one variant — exactly-one
  stays a runtime check, negative trait bounds do not exist) — and
  returns `Entity<H>` for the matched shape. Loose group declarations
  redeclare at shape granularity. Queries: facet-anchored tuples.
- **Facet-anchored queries with presence-typing.**
  `Query<(Entity<H>, &A, Option<&B>, MutRef<'_, C>)>`: the facet drives
  row matching (H's required set), and the shape's optionality dictates
  each sibling part's form — `&T`/`MutRef<T>` legal iff `T` required in
  `H`, `Option<&T>` iff optional (or a union member), anything outside
  `H` rejected. This makes "bare reference silently narrows matching"
  unrepresentable. Free-form component tuples with untyped `Entity`
  remain as the *vocabulary read* (cross-shape reads like spans and file
  anchors are semantically cross-cutting; reads cannot produce ill-shaped
  data). One facet per row tuple in v1.

  *Coherence constraint found during implementation:* presence-typing on
  plain tuples cannot be a compile-time trait bound — it would need a
  second blanket `QueryParam` impl for tuples containing `Entity<H>`,
  which overlaps the generic tuple impl (rustc's coherence does not use
  negative reasoning). So the tuple form validates at `add_system`
  (loud panic, same precedent as join-provider rules), and *compile-time*
  presence-typing arrives with the derive-generated shape rows
  (`Query<my_schema::DiagnosticRow>`), where the generated nominal type
  carries the correct `Option`-ness by construction. The facet part
  itself contributes row matching but **no** revision deps: siblings'
  reads stay the memo deps, so an unread required part changing value
  does not rerun the row (component-granular incrementality preserved);
  appearance/disappearance is handled by planning re-enumeration.
- **Increments.** `commands.entity(e)` with `e: Entity<H>` allows
  inserting only `H`'s `Option<T>` parts (required parts exist by
  construction) plus union variants; the untyped-handle path keeps
  membership semantics with the runtime conformance backstop. Typed
  handles are snapshot-sound, not transaction-sound: the claim is made
  against the invocation's snapshot, commits reconcile, the debug check
  covers the window.
- **Union slots.** A shape element `OneOf<G>` (G a group alias) declares
  a sum: exactly one member present. The explicit marker exists because
  the macro cannot distinguish an alias from a component at token level;
  it also reads well. In `Commands` position the same alias stays a plain
  may-emit group. Enables typed heterogeneous references
  (`ChildOf(Entity<schema::SyntaxNode>)`).
- **`Bowl<Schema>`.** The schema moves into the bowl's type; `add_system`
  requires each system's declaration to be covered by the schema at
  compile time, retiring the last runtime registry check. Reusable
  plugins become schema-generic with capability bounds.
- **Reads stay component-granular.** Locking reads to whole shapes would
  coarsen memo deps to table granularity (`SELECT *`-only); shape rows
  are derive-generated sugar over per-component deps, never a
  replacement.

Build order: (1) strict spawn returning `Entity<H>` — **done**; (2) facet
handles + optionals-only increments — **done**; (3) facet-anchored
queries — **done** (`Query<(Entity<H>, parts…)>`; the anchor drives row
matching via the presence mask or store probing and contributes no
revision deps; `Tracked<H>` is the opt-in whole-shape dep part, added
when the replication dogfood showed shape-granular consumers need
shape-granular deps; registration-time presence-typing validation of
sibling parts is still open); (4) union slots (`OneOf`); (5)
`Bowl<Schema>` + compile-time declaration coverage; (6) sweep untyped
`Entity` from the public surface; (7) derive-generated shape rows /
union enum readers as sugar.

## Implementation order

1. **`Commands<S>` + membership + runtime honesty check** (this pass):
   `Anything` wildcard default, `DeclaredIn`/bundle bounds, group aliases,
   `SystemParam::declared_outputs` registry on `BoxedSystem`, debug panic
   on undeclared derived writes.
2. **`#[derive(Schema)]`**: macro, shape metadata, commit-time conformance
   with named-shape messages, builder-time schema installation.
3. **Registration-time phase refusal** (needs 1 + 2 for precision).
4. **Graph consumers** (gating, stratification, event-driven): separate
   efforts, each with benches.
