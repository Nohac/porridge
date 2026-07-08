# Singleton Components

Porridge should keep the "components only" model. A singleton is not a separate
resource kind. It is a component value stored on one entity, with an additional
runtime invariant:

```text
for a singleton component type T, there is at most one singleton entity for T
```

This is inspired by Bevy 0.19's resource rewrite, where `Resource` now extends
`Component`, resources live on entities, and an internal marker/cache preserves
resource semantics.

## Why A Marker Alone Is Not Enough

This is expressive:

```rust
commands.insert((Singleton::<AstAvailable>::new(), AstAvailable { data }, Ephemeral));
```

but the singleton marker cannot enforce uniqueness by itself unless the bowl
recognizes it during insertion.

If `Singleton` is just a component, the bowl can still contain:

```text
entity 1: Singleton<AstAvailable>, AstAvailable
entity 2: Singleton<AstAvailable>, AstAvailable
```

A cleanup system could detect duplicates later, but that is not a reliable
singleton invariant:

- duplicates can exist temporarily
- queries may observe duplicates before cleanup
- cleanup requires scanning
- choosing the winner is ambiguous
- memoization can see unstable entity identities

Singletons need runtime support.

## Internal Index

The bowl should maintain an index:

```rust
singleton_entities: HashMap<ComponentId, Entity>
```

The key is the singleton component type, not the `Singleton` marker type.

For example:

```rust
commands.insert((Singleton::<AstAvailable>::new(), AstAvailable { data }, Ephemeral));
```

uses:

```text
key = ComponentId::of::<AstAvailable>()
```

and stores the bundle on that singleton entity:

```text
entity N:
  Singleton<AstAvailable>
  Ephemeral
  AstAvailable
```

## MVP Bundle API

Singletons should not add a new insertion command surface. The operation stays:

```rust
commands.insert(bundle);
```

The singleton behavior comes from bundle metadata.

For the first implementation, use explicit singleton marker insertion:

```rust
commands.insert((Singleton::<AstAvailable>::new(), AstAvailable { data }, Ephemeral));
```

This avoids needing recursive bundle flattening before singleton support exists.

The marker is manual for now:

```rust
Singleton::<AstAvailable>::new()
```

and it means:

```text
route this insertion through the singleton index keyed by AstAvailable
```

## Future Bundle Ergonomics

After `Bundle` supports nested bundle flattening, the component derive macro or
a blanket component extension trait should make this available for every
component:

```rust
commands.insert((AstAvailable { data }.singleton(), Ephemeral));
```

Conceptually:

```rust
AstAvailable { data }.singleton()
```

returns a `SingletonBundle<AstAvailable>` that behaves like:

```rust
(Singleton<AstAvailable>, AstAvailable { data })
```

At that point users should not normally need to spell
`Singleton<AstAvailable>` directly.

To make this compose, bundles should flatten recursively like Bevy bundles:

```text
Component implements Bundle
SingletonBundle<T> implements Bundle
(B1, B2, ..., Bn) implements Bundle where each Bi: Bundle
nested tuples flatten recursively
```

So:

```rust
commands.insert((AstAvailable { data }.singleton(), Ephemeral));
```

flattens to:

```text
Singleton<AstAvailable>
AstAvailable
Ephemeral
```

If the flattened bundle contains exactly one singleton key, the entire bundle is
inserted onto that singleton entity.

If a flattened bundle contains multiple singleton keys, the MVP should reject
it:

```rust
commands.insert((A.singleton(), B.singleton())); // error
```

That bundle is ambiguous because there are two possible singleton target
entities. Sharing one entity across multiple singleton keys can be revisited if
a real use case appears.

## Insert Semantics

Porridge should probably use upsert semantics:

```text
insert(bundle containing Singleton<T>)
  if singleton T already has an entity:
    insert/update bundle on that entity
  else:
    create entity
    insert bundle
    register T -> entity
```

This differs from Bevy's resource duplicate behavior, which keeps the original
resource entity and discards duplicate resource insertions. Upsert is a better
fit for Porridge because completion hooks may re-emit the same ephemeral
singleton every evaluation pass.

## Revision Semantics

Singleton components should use normal component revisions.

The singleton logic only decides which entity owns the component. Once the
entity is known, insertion follows the same rules as any other component:

```text
fingerprint equal -> keep revision
fingerprint changed -> bump revision
no fingerprint -> bump revision on insert/update
create/remove presence -> query set changes
```

Example:

```rust
commands.insert((Singleton::<SystemImportDb>::new(), SystemImportDb { imports }));
```

If `SystemImportDb` is hash-stable and equivalent to the previous value, its
component revision should not bump. Systems that depend on it should remain
memo-clean.

For ephemeral singleton markers:

```rust
commands.insert((Singleton::<AstAvailable>::new(), AstAvailable, Ephemeral));
```

presence matters more than payload value. The settle phase removes the
marker/entity at the end of evaluation, and reinsertion during the next
evaluation should make the token present in that evaluation.

Important distinction:

```text
durable singleton value
  use hash/fingerprint to avoid revision churn

ephemeral singleton readiness token
  use presence within the current evaluation, then remove before outside reads
```

## Querying Singletons

Singleton insertion and singleton querying are related but separate features.

An insertion helper enforces the invariant:

```rust
commands.insert((Singleton::<SystemImportDb>::new(), SystemImportDb { .. }));
```

A query helper validates access:

```rust
async fn check(imports: Single<&SystemImportDb>) {
    // exactly one SystemImportDb must exist
}
```

`Single<T, F>` can be implemented as a query parameter that validates exactly
one match:

```text
zero matches     -> system invocation does not run, or reports validation error
one match        -> pass the item
multiple matches -> validation error
```

This is useful even without singleton insertion, because it can validate any
query shape. But it does not enforce uniqueness at insertion time.

## Lifecycle Behavior

Singleton bookkeeping must stay correct when entities/components are removed:

```text
remove singleton component T
  remove T -> entity from singleton index

despawn singleton entity
  remove all singleton index entries owned by that entity

replace singleton T
  keep the same entity when possible
```

If `Singleton<T>` is modeled as a marker component, removing the
marker should probably remove the singleton component too. This mirrors Bevy's
`IsResource` behavior and prevents an entity from silently retaining a value
that is no longer registered as the singleton.

Open question:

```text
Can one entity be the singleton owner for multiple component types?
```

It is technically possible:

```text
entity N:
  Singleton<SystemImportDb>
  Singleton<ProjectConfig>
  SystemImportDb
  ProjectConfig
```

The first implementation should reject this shape through normal insertion
because the target entity is ambiguous. It can be added later as an explicit API
if needed.

## Open Questions

- Should direct spelling of `Singleton<T>` remain public after `.singleton()`
  exists, or become discouraged convenience-breaking glass?
- Should duplicate raw insertion of `Singleton<T> + T` be rejected, warned, or
  normalized into singleton upsert behavior?
- Should singleton upsert preserve entity identity forever unless explicitly
  removed?
- Should `Single<T>` skip systems on validation failure or return a structured
  scheduler error?
- How should singleton indexes interact with snapshots and derived-output
  ownership?
