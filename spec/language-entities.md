# Language entities

How the playground organizes a language on top of bowl, and why. The goal is
for the playground to be a good starting point for real DSL and language-tool
development, where the usual failure mode is one concept's implementation
smeared across a parser module, a checker module, a formatter module, and an
LSP module.

## The idea

A **language entity** is a vertical slice of one language concept. One file
co-locates everything the concept owns:

- its fact components (`ImportDecl`, `AstDef`, ...),
- how those facts lower out of the CST,
- the checks that validate them,
- how they present to services (hover, and later goto, completion, ...).

The playground has four: `document` (source files and the parse),
`import` (import declarations and the known-import database), `definition`
(named definitions and the duplicate check), and `namespace` (`namespace
a.b { ... }` declarations and the join-derived qualified names of their
members). Each lives in `crates/playground/src/lang/entities/`.

Cross-cutting facts that every entity shares — `Diagnostic`, `Severity`,
`Span`, `BelongsToFile`, the ephemeral settle markers — live in
`lang/facts.rs`. Syntax stays in `lang/grammar/`, which produces a CST and
knows nothing about semantics.

## The coverage contract

`lang/entity.rs` defines the contract as plain traits — no macros, no
registries:

- `LanguageEntity` — identity plus `register(db)`, where the entity adds its
  derivation and check systems to the bowl.
- `LowerStage` — lower an owned CST rule node into fact components.
- `HoverStage` — contribute hover content for a position, or `None`.

`register_entity::<E>` bounds on every stage, so a new entity does not
compile until it has declared each one. A stage that does not apply is an
explicit no-effect impl (see `Document`'s hover), which keeps the decision
visible instead of silently absent.

This is a deliberately lighter take on the dsql *language atoms* proposal:
the compile-time guarantees come from ordinary trait bounds and two
hand-written exhaustive dispatch points rather than declaration macros and
registry lookups. At playground scale the strictness isn't worth the
machinery; the pattern scales up by adding stage traits, not by adding
infrastructure.

## Dispatch is exhaustive, not registered

Rule ownership routes syntax to entities through a plain `match`:
`lang/entities/mod.rs::lower_rule` matches on the lelwel-generated `Rule`
enum. `generate_ast` is the single generic CST walk; it visits every rule
node once and hands it to the owning entity. Adding a rule to `syntax.llw`
fails to compile until an entity claims it or it is explicitly listed as
structural.

The trade for skipping registries: `LowerCtx` is shared, so an entity that
needs new lowering context may extend it. The edit is compile-checked and
local.

## Services: the candidate-fact pipeline

Service answers are *not* aggregated into one system — an aggregator
accretes one view per contributing entity (and hits the 8-param ceiling).
Instead, requests flow through a phase-ordered pipeline (see
`lang/service/hover.rs`):

1. **Enrichment** (`Phase::Complete`, service-owned): resolve what every
   contributor needs once — the request's file, the word under the cursor —
   and stamp it onto the request entity (`HoverEnriched`, `HoverFile`,
   `HoverWord`). File resolution is a bound join: the request's `FilePath`
   pairs with the file carrying the equal path, so the lookup is a planned
   pair with tracked deps rather than a view scan.
2. **Candidates** (`Phase::Complete`, entity-owned): each entity registers
   its own request-answering system through `HoverStage::register_hover`,
   reading only the enriched request plus its own facts, and inserts
   prioritized `HoverCandidate` facts anchored to the request. In-phase
   streaming plans them as soon as enrichment commits. Param lists stay
   O(own facts) no matter how many entities exist.
3. **Finalization** (`Phase::Cleanup`, service-owned): by settle time every
   Complete wave has run, so every candidate exists; the finalizer picks the
   highest priority and writes the response onto the request. Arbitration is
   data (a priority band), not call order, and the service supplies
   fallbacks.

Phase boundaries carry the whole ordering story. `Phase::Complete` runs
after Evaluate has converged *in every generation*, so the pipeline's
ambient reads (defs, imports, qualified names) are always consistent with
the generation's inputs — including a request batched together with the
source it asks about (pinned by `crates/playground/src/tests.rs`).

An earlier version gated the pipeline on the ephemeral `AstAvailable`
marker instead. That is racy by construction: work gated on a marker is
invisible to every settledness check while the marker is absent, so a
concurrent settle can declare the bowl settled between the marker's cleanup
and its next re-emission, starving requests that arrived in that window —
and a marker is a recorded claim that goes stale when new inputs land in
its generation. `spec/epochs.md` records the full analysis, the ordering
hierarchy (tracked joins > phases > markers), and the epoch/preemption
design that would make markers sound for cross-settle signaling again.
Until then: prefer phase ordering whenever the requirement is "run after
this generation's derivations"; markers remain for genuinely settle-scoped
signals (the `DefIndex` aggregation still uses one).

Priorities live in one place (`service::hover::priority`), so shadowing (a
namespace-qualified answer outranking the plain definition answer) needs no
coordination between entities.

## How the engine replaces atom infrastructure

Porridge dissolves most of what the atoms proposal needed to build:

- **Registry dispatch** → component types. A check driven by
  `Query<(Entity, &ImportDecl)>` *is* the registration; there is nothing to
  look up.
- **Walker + handler wiring** → row enumeration. Systems don't walk anything;
  the engine enumerates matching rows and memoizes per row.
- **Memoization policy** → per-invocation memo deps. Each row's system run
  is cut off by fingerprints; no per-atom cache decisions.
- **Stage ordering** → settle waves. `parse_file` → `generate_ast` → checks
  order themselves through the facts they consume.

What porridge does *not* give you for free is reacting to the **set** of
facts rather than one row: a `View` contributes no memo deps by design. The
definition entity shows the pattern for that:

## Pattern: tracked set fingerprint

`check_duplicate_defs` must rerun when *other* definitions appear or
disappear, but its `View` of them is deliberately non-invalidating. The fix
is a tracked input over the set:

- `index_defs` is gated on the `AstAvailable` marker (re-inserted by an
  `on_settled` hook each wave the AST regenerated, removed in `Cleanup`) and
  aggregates all definitions into a `DefIndex` singleton.
- `DefIndex` is `#[component(hash)]`: idempotent reruns keep its revision,
  so an unchanged set invalidates nothing.
- `check_duplicate_defs` takes `Query<(Entity, &DefIndex)>` alongside its
  driving row (cartesian product of one index row × each definition row).
  When the set changes, every row reruns; otherwise the memo holds.

This composes from existing primitives — gate marker, fingerprint cutoff,
query product — with no engine support, which is the property the playground
is meant to demonstrate.

## Pattern: demand markers

Not everything should compute on every settle: a hover request does not
need diagnostics. Demand-driven evaluation dissolves into facts — a system
gates on a *demand fact*:

```rust
async fn check_imports(
    _: Query<Entity, With<DiagnosticsDemand>>,   // demand gate
    query: Query<(Entity, &ImportDecl)>,
    ...
)
```

No demand fact → the rows are never planned. The LSP adapter owns the
demand lifecycle as ordinary inserts (`didOpen` inserts per-file demand,
`didClose` removes it), and debouncing becomes data: drop the demand while
the user types, re-insert on idle.

Implemented in the playground: `check_imports`, `check_duplicate_defs`,
and `index_defs` gate on the `DiagnosticsDemand` singleton; a hover-only
bowl computes zero diagnostics (pinned by
`diagnostics_compute_only_on_demand` in `crates/playground/src/tests.rs`).
`index_defs` also moved off the `AstAvailable` gate to `Phase::Complete`,
driven by file rows: any text change recomputes the index with settled
defs. Residual: deleting a whole file with no other change leaves a ghost
index until the next change — the clean fix is engine set-deps
(relationships, `spec/joins.md`).

Demand facts are the *safe* kind of marker: unlike ordering gates (which
are claims about derivation state and go stale when inputs move — see
`spec/epochs.md`), a demand fact is a preference, an input in its own
right. Nothing derived can make it lie; only its owner changes it.

Composition points:

- **Scoped demand** — a demand fact carrying a hashed key joins to matching
  facts via `Where<Eq<..>>`: per-file demand with per-pair memoization.
- **Demand-scoped cleanup** — anchor emitted outputs with
  `DerivedFrom::many([source, demand_entity])`; removing the demand reaps
  the outputs automatically.
- **Demand propagation** — demand facts can be derived by systems (a hover
  on a symbol emits resolve-demand for its defining file), reconstructing
  pull-model evaluation as data flow: a request seeds demand for exactly
  its dependency cone.

Trade: cost moves to when it is wanted (re-demanding pays accumulated
invalidation then), the settle barrier stays global (a hover settling
alongside demanded diagnostics waits for both), and the demand vocabulary
is designed by hand per stage rather than derived per-query.

## Pattern: bound join

For *pair*-granular relations the engine now has direct support: a system
query with a bound `Where<Eq<K>>` filter pairs each driving row with the
rows whose `K` equals it (see `spec/joins.md`). The namespace entity uses it
to derive qualified names — one memoized invocation per (namespace, member
definition) pair:

```rust
async fn qualify_members(
    namespaces: Query<(Entity, &NamespaceDecl, &NamespacePath)>,
    members: Query<(Entity, &AstDef), Where<Eq<NamespacePath>>>,
    mut commands: Commands,
) { ... }
```

Rule of thumb: react to *individual related rows* with a bound join; react
to *the set as a whole* with the tracked set fingerprint above.

## Services and the LSP shape

`lang/service/` owns request/response facts. A request is a plain entity
carrying request components (`HoverRequest`, `FilePath`, `Position`); the
service system answers by attaching the response component to the same
entity. External callers use the bound-entity path:

```rust
let info = db
    .insert((HoverRequest, FilePath(path), Position { offset }))
    .await
    .bind()
    .take::<HoverInfo>()
    .await?;
```

An LSP adapter can therefore map protocol requests directly onto bowl
inserts — no separate service layer or request router. Cancellation is safe
on both sides: dropping the `take` future abandons a bound entity that the
engine cleans up, and a cancelled evaluation driver hands off to the next
caller (see `spec/code-review.md` on cancellation recovery).
