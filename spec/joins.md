# System Query Joins

This document records the design for bound `Where` filters in system queries:
relational joins between system params without introducing a new concept.

Status: implemented for system-side `Query` params (bound `Where<Eq<T>>`
with registration-time validation and product pruning). Open: scoped views,
`Named`-qualified binds, and the relationships companion below.

Implementation shortcuts (current):

- The product is formed first and then pruned per combination by comparing
  stamped key fingerprints (`SystemParam::binding_matches`), instead of the
  ordered per-driving-row index probe described below. Semantically
  identical; the probe is a planning optimization to revisit if wide joins
  show up in profiles.
- Equality is fingerprint equality only (64-bit); values are not confirmed
  with `PartialEq` the way external `Eq` filters do after index resolution.
  A hash collision would join two unrelated rows.
- `MutRef` parts do not provide join keys, so a join key cannot be mutated
  by an invocation it binds.
- The playground exercises the join end to end: `namespace a.b { ... }`
  declarations pair with their member definitions to derive qualified names
  (`crates/playground/src/lang/entities/namespace.rs`).

## Motivation

Tuple system params form the cartesian product of their state sets. That is
the right default, but many derivations are relational: for each namespace,
the definitions *in that namespace*; for each use site, the definitions
*with the same name*. Today the product either explodes (`namespaces ×
all defs`, each pair filtering in user code) or the system falls back to a
`View` plus the set-fingerprint pattern (`spec/language-entities.md`), which
trades an extra settle wave and a hand-built index for correctness.

The gap is a middle point between `Query` (tracked, identity-forming) and
`View` (ambient, non-invalidating): scoped data with correct invalidation.

## Design

Reuse `Where<Eq<T>>` with the argument bound from a sibling param instead of
from `.args(...)`:

```rust
async fn check_members(
    namespaces: Query<(Entity, &Namespace)>,
    members: Query<(Entity, &AstDef), Where<Eq<Namespace>>>,
    mut commands: Commands,
) {
    let (ns_entity, ns) = namespaces.item();
    let (member, def) = members.item();
    // one invocation per (namespace, member-in-namespace) pair
}
```

`Where<Eq<T>>` keeps one meaning everywhere: rows whose `T` component equals
the argument. Only the argument's source differs — external scoops supply it
via `.args(...)`, systems bind it from the sibling param that provides `&T`.

The product rule is unchanged; the bound filter *prunes* it. The invocation
set is (namespace row) × (member rows whose `Namespace` equals that row's
value). Granularity is per pair: one member changing reruns one pair, not
the whole namespace.

This is a value join on fingerprint equality, so it directly covers name
resolution:

```rust
async fn resolve_name(
    uses: Query<(Entity, &SymbolName, &UseSite)>,
    defs: Query<(Entity, &SymbolName, &AstDef), Where<Eq<SymbolName>>>,
    mut commands: Commands,
) { ... }
```

Per-use-site resolution with per-pair memoization, no symbol table to
maintain.

### Scoped views

The same binding composes with `View` for read-the-set access:

```rust
async fn check_namespace_duplicates(
    namespaces: Query<(Entity, &Namespace)>,
    members: View<'_, (Entity, &AstDef), Where<Eq<Namespace>>>,
    ...
) { ... }
```

Scoped views stay deliberately non-invalidating like all views. Set
reactivity (rerun when membership changes) remains the tracked
set-fingerprint pattern — or, longer term, a maintained relationship
component (see "Companion design" below); a bound view only narrows what is
visible.

## Semantics

### Binding rule

`Eq<T>`'s argument binds to the unique *other* query param whose item
provides `&T`. The filtered query itself is excluded, so self-joins on the
same component type work (see `resolve_name` above). Resolution is validated
at `add_system` time:

- zero candidates → registration error
- multiple candidates → registration error naming the ambiguity; the fix is
  `Named<Tag, Query<...>>` on the intended source and a tag-qualified bind
  on the filter (exact syntax to be settled during implementation)

Registration-time panics with real error messages are preferred over trait
gymnastics; `add_system` already has the full param list in hand.

### Planning

Today every param's rows are enumerated independently and the product is
formed positionally. With bound filters, planning becomes ordered:

1. Resolve unbound params as today (smallest-store enumeration).
2. For each combination, resolve bound params by probing the per-store
   fingerprint index with the concrete key value from the binding row.

Bound params may bind to other bound params, giving chain joins
(namespaces → types in namespace → methods on type). The binding graph must
be acyclic; `add_system` rejects cycles.

A bound probe is O(bucket) via the fingerprint index. Bound `Eq` therefore
*requires* `#[component(hash)]` on the key component — registration error
otherwise. Without the index every wave would degrade to a scan per driving
row, which is the trap this feature exists to avoid.

### Memo deps

A pair invocation's deps are the union of each participating row's deps, and
the key component's revision must be included on both sides:

- member's `Namespace` revision: a member moving to another namespace makes
  the old pair stale (it vanishes at replanning) and creates a new pair.
- namespace's `Namespace` revision: a renamed namespace makes all its pairs
  stale.

Implementation note: filters already have this shape. `With<T>` contributes
the component's revision to row deps (`QueryFilter::deps`, query.rs);
`Without<T>` contributes none because absence has no revision — reappearance
changes row enumeration instead. A bound `Eq<T>` follows the `With` pattern:
its `deps` returns the key component's revision on the row entity.

### Membership changes

No set-level dependency is needed for pair semantics. Planning enumerates
current pairs each wave:

- new member inserted → new pair row → planned and run, like any new entity
- member removed / key changed → pair no longer enumerated; memoized entry
  goes stale via the key-component dep

### Vanished pairs

When a pair stops matching, its derived outputs linger until the invocation
reruns or `DerivedFrom` cleanup removes them. This is the existing semantics
for rows that stop matching `Without<T>` filters — the join introduces no
new lifecycle. Diagnostics and similar facts emitted by join systems should
anchor to both sides with `DerivedFrom::many([left, right])` as usual.

## Operator support

Bound semantics are decided per operator; only operators whose bound
meaning is obvious and cheap get support. The escape hatch is always the
same: filter inside the system against a `View`.

- `Eq<T>` — supported (the equality join above).
- `And` — **supported.** Both payoffs landed: general system-side filter
  composition (`And<With<A>, Without<B>>` on one query) and compound join
  keys — `Where<And<Eq<A>, Eq<B>>>` pairs a row only when *every* key
  matches its provider (`compound_bound_join_requires_every_key_to_match`).
  Bound keys are plural throughout; validation and pruning loop per key.
- Outer form — **supported.** `Option<Query<Q, Where<Eq<K>>>>` runs one
  invocation per matched pair plus exactly one `None` invocation for a
  provider row with zero matches, instead of dropping it from the product.
  The `None` invocation's memo records store-scoped watermark deps on the
  joined stores, so a partner appearing, changing, or disappearing reruns
  the unmatched row (coarse: any write to those stores invalidates every
  unmatched row; per-fingerprint-bucket deps are the refinement). This is
  what lets an inner join absorb its "nothing matched" else-branch system —
  the hover service's `stamp` folded into `resolve`. "Left" outer is the
  only meaningful variant: the provider side owns invocation identity.
- `Gte`/`Gt`/`Lt`/`Lte` — deferred until a concrete need. A bound ordered
  comparison is a theta/band join: fingerprints cannot order, so pruning
  needs value reads and `PartialOrd` at plan time, and the pair set churns
  under small value changes, eroding per-pair memoization.
- `Not<Eq<K>>` — punt. An anti-join pairs nearly the full product; the
  useful absence questions ("defs not in any namespace") are set-shaped and
  belong to the set-fingerprint pattern or future relationships.
- `Or` — punt; cheap to prune but no motivating case.
- `With`/`Without` — nothing to bind; they already work system-side.

External `.args(...)` filters are unaffected: the full expression set
remains available on scoops.

## Companion design: relationships and `Where<In<T>>`

Bound `Eq` covers per-*pair* granularity and leaves per-*set* reactivity
("rerun when membership changes") to the tracked set-fingerprint pattern.
Engine-maintained relationships, in the spirit of Bevy 0.16's
`ChildOf`/`Children`, would close that gap.

### Design

Writing `InNamespace(ns)` on a member makes the engine maintain the inverse
on the target entity:

```rust
Members(BTreeSet<Entity>)   // maintained, tracked, fingerprinted
```

The porridge twist is that the inverse is a **tracked component with a
fingerprint** — hash of the sorted member set. That makes membership itself
a memoizable dependency:

- `Query<(Entity, &Namespace, &Members)>` reruns exactly when membership
  changes; unchanged membership keeps its revision via the fingerprint
  cutoff, so idempotent waves invalidate nothing.
- This retires the hand-rolled gate + aggregator + extra settle wave of the
  set-fingerprint pattern, and removes the opportunity to get it subtly
  wrong (the playground's duplicate-defs checker shipped with exactly that
  staleness bug).

Half the machinery exists: the per-store fingerprint index is already a
reverse map from key to entities. A relationship is that index promoted to a
first-class tracked component.

### `Where<In<T>>`

With a maintained inverse, `In` slots into the same binding rule as `Eq`:

```rust
async fn check_members(
    namespaces: Query<(Entity, &Namespace, &Members)>,
    members: Query<(Entity, &AstDef), Where<In<Members>>>,
    ...
) { ... }
```

`In<Members>` binds to the sibling providing `&Members` and matches rows
whose *entity* is in the set — an identity join, where `Eq` is a value
join. It is simpler to plan than bound `Eq`: the candidate list is the set's
contents (no index probe, no `#[component(hash)]` key requirement), and the
pair's dep on the `Members` revision covers membership changes by
construction. One mechanism, both granularities: `Where<In<Members>>` for
per-pair work, `&Members` in the driving query for per-set work.

### Costs

- A new engine concept: the inverse is written by the commit machinery, not
  by any invocation, so it fits neither `Origin::Base` nor
  `Origin::Derived`. It needs a maintained-origin story — cleanup, and how
  it interacts with derived-output diffing when a member's creator reruns.
- Write amplification is inherent: every real membership change bumps the
  target's inverse and reruns its set-consumers. That is what set semantics
  means, but high-churn targets with many members will feel it.
- Unlike Bevy's `Vec<Entity>` children, the inverse needs canonical ordering
  (a sorted set) so fingerprints are stable across idempotent reruns.

### Boundary

Relationships are *domain structure*; `DerivedFrom` is *provenance*. They
rhyme — both point at entities, both imply cleanup — but they stay separate
concepts. Bevy's despawn cascades correspond to what `cleanup_stale_derived`
already does, not to relationship maintenance.

### Ordering

Bound `Eq` first: it needs no new engine concepts. Relationships second,
as the replacement for the set-fingerprint pattern once bound filters have
proven out the planning changes.

## What this replaces

An earlier sketch introduced a dedicated `Joined<K, P>` param keyed per
driving row, materializing the member set inside one invocation. It required
a new param concept and a new set-hash `Dep` variant so membership changes
could invalidate. The bound-filter design supersedes it:

- per-pair identity makes membership changes ordinary row appearance and
  disappearance — no new dep machinery
- `Query`/`View` with a bound filter covers both the tracked and the ambient
  half of what `Joined` bundled
- the external API keeps its shape; nothing new to learn

If a real need emerges for "the whole joined set as one tracked invocation"
that the set-fingerprint pattern cannot serve, revisit `Joined` then.

## Implementation order

1. Binding resolution + validation in `add_system` (unique provider rule,
   acyclicity, `#[component(hash)]` requirement).
2. System-side `QueryFilter` impl for `Where<Eq<T>>` with key-component deps
   (the `With<T>` pattern) and index-backed `entity_candidates`.
3. Ordered planning: bound-param row resolution through the fingerprint
   index per driving combination.
4. `Named`-qualified binds for ambiguous cases.
5. Scoped views (`View` + bound `Where`), which reuse the same resolution.
