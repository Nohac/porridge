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

The playground has three: `document` (source files and the parse),
`import` (import declarations and the known-import database), and
`definition` (named definitions and the duplicate check). Each lives in
`crates/playground/src/lang/entities/`.

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

Two places route work to entities, both plain `match`/call chains:

1. **Rule ownership** — `lang/entities/mod.rs::lower_rule` matches on the
   lelwel-generated `Rule` enum. `generate_ast` is the single generic CST
   walk; it visits every rule node once and hands it to the owning entity.
   Adding a rule to `syntax.llw` fails to compile until an entity claims it
   or it is explicitly listed as structural.
2. **Hover arbitration** — `lang/service/hover.rs` asks each entity in
   order, most specific first (`Import` answers by span, `Definition` by
   word). The first `Some` wins; the service supplies fallbacks.

The trade for skipping registries: the shared contexts (`LowerCtx`,
`HoverCtx`) name concrete entity facts, so an entity that grows new service
behavior may extend a context and the arbitration chain. Both edits are
compile-checked and local.

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
