# Bound Entity Handles

This spec explores owned entity handles for operations that should not be
available from arbitrary shared queries.

The motivating case is `Take<T>`.

`Take<T>` is a destructive read. It removes a component or output from the bowl,
so it should require proof that the caller has logical ownership of the entity
being consumed.

## Problem

This is too broad:

```rust
db.query::<Take<HoverInfo>>().await
```

A normal database query can match many entities and is available from a shared
database handle. Allowing destructive reads there makes ownership unclear:

- Who is allowed to remove the component?
- What happens if multiple callers race to take the same output?
- How does an ephemeral request clean up only its own outputs?
- How do request queries avoid accidentally consuming another request's result?

## BoundEntity

Introduce a move-only handle:

```rust
pub struct BoundEntity {
    bowl: Arc<Inner>,
    entity: Entity,
}
```

`BoundEntity` represents an entity that is logically bound to the caller. It is
not just an ID; it is a capability.

Possible properties:

```rust
impl BoundEntity {
    pub fn entity(&self) -> Entity;

    pub async fn query<Q>(&self) -> Q::Output;
    pub async fn take<T: Component>(self) -> Option<T>;
}
```

`BoundEntity` should probably be move-only and not `Clone`.

## Creating Bound Entities

Inserting new entities can return a bound handle:

```rust
let request = db.insert_bound((
    Ephemeral,
    HoverRequest,
    FilePath(path),
    Position { offset },
));
```

Request sugar can build on the same primitive:

```rust
let request = db.request((
    HoverRequest,
    FilePath(path),
    Position { offset },
));
```

That request can then query only its own outputs:

```rust
let hover = request.query::<Take<HoverInfo>>().await;
```

or:

```rust
let hover = request.take::<HoverInfo>().await;
```

## Rule

```text
Take<T> is only valid through a BoundEntity-scoped query.
```

Normal database queries can borrow:

```rust
db.query::<(Entity, &HoverInfo)>().await
```

but they cannot destructively take:

```rust
db.query::<Take<HoverInfo>>().await // invalid
```

This keeps destructive operations capability-based.

## Entity vs BoundEntity

```text
Entity
  Copyable identity.
  Can be stored in components.
  Can be returned by queries.
  Does not grant destructive permissions.

BoundEntity
  Owned capability.
  Tied to a bowl.
  Can scope queries to one entity.
  Can take/remove owned outputs.
  Can trigger cleanup on drop.
```

The name `BoundEntity` is intentionally less absolute than `UniqueEntity`.
The handle does not necessarily prove global uniqueness of the entity. It proves
that this entity is bound to the caller for certain operations.

## Ephemeral Cleanup

`BoundEntity` fits ephemeral request cleanup:

```text
caller creates bound ephemeral request
systems produce outputs for that request
caller takes or queries the output
bound handle is consumed or dropped
ephemeral request and owned derived outputs are cleaned up
```

Cleanup should be scoped to the bound entity. It should not remove unrelated
facts that merely happen to match the same query shape.

## Open Questions

- Should ordinary `insert` return `Entity` or `BoundEntity`?
- Should there be both `insert` and `insert_bound`?
- Should `BoundEntity` cleanup happen on `Drop`, explicit `close`, or both?
- Can `BoundEntity` be safely `Send` and `Sync`?
- Should a bound query automatically include `Entity == bound.entity`?
- Can users bind an existing entity, or only newly inserted entities?
- Should `Take<T>` consume the `BoundEntity`, or can multiple takes happen from
  the same bound entity?
