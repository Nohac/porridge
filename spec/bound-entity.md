# Bound Entity Handles

Bound entity handles are the request/response cleanup mechanism for `bowl`.
They replace the earlier `Ephemeral` marker idea.

The core idea:

```rust
let output = db
    .insert((HoverRequest, FilePath(path), Position { offset }))
    .await
    .bind()
    .take::<(HoverInfo, Option<Diagnostic>)>()
    .await?;
```

`Entity` is just a copyable id. `BoundEntity` is a capability tied to a specific
inserted entity.

## Problem

Destructive reads should not be available from arbitrary shared queries:

```rust
db.query::<Take<HoverInfo>>().await
```

That shape is too broad:

- a normal query can match many entities
- concurrent callers could race to consume the same component
- request outputs could be consumed by the wrong caller
- request cleanup would need extra filtering rules

Instead, destructive reads are methods on a bound handle.

## Creating Bound Entities

Durable inserts stay ordinary:

```rust
db.insert((FilePath(path), FileText(text))).await;
```

Request-style inserts opt into a bound lifetime:

```rust
let request = db
    .insert((HoverRequest, FilePath(path), Position { offset }))
    .await
    .bind();
```

The inserted entity can still be inspected as an id:

```rust
let entity = request.entity();
```

but the important part is the handle. It is not cloneable and represents the
right to consume outputs from that exact entity.

## Taking Outputs

`BoundEntity::take<T>(self)` is consuming:

```rust
let hover = request.take::<HoverInfo>().await?;
```

It does all of this:

1. waits for the inserted generation and bowl settlement
2. removes the requested component or component bundle from the bound entity
3. closes the bound entity
4. removes remaining outputs scoped to the bound entity
5. removes the bound entity itself

Tuple bundles take everything at once:

```rust
let (hover, diagnostic) = request
    .take::<(HoverInfo, Option<Diagnostic>)>()
    .await?;
```

Required components fail the take when missing. Optional components use
`Option<T>`:

```rust
take::<HoverInfo>()                  // HoverInfo required
take::<Option<Diagnostic>>()         // Diagnostic optional
take::<(HoverInfo, Option<Diagnostic>)>()
```

The result type is:

```rust
Result<T::Output, TakeError>
```

where `TakeError` identifies the missing required component.

## Cleanup On Drop

`Drop` cannot `await`, so dropping a bound handle without `take` queues cleanup
for the next bowl operation.

```rust
{
    let _request = db.insert((HoverRequest, ...)).await.bind();
}

db.query::<(Entity, &Diagnostic)>().await; // drains deferred cleanup first
```

This gives deterministic cleanup for `take`, and best-effort deferred cleanup
for abandoned request handles.

## Entity vs BoundEntity

```text
Entity
  Copyable identity.
  Can be stored in components.
  Can be returned by queries.
  Does not grant destructive permissions.

BoundEntity
  Move-only capability.
  Tied to one bowl and one inserted entity.
  Can destructively take outputs from that entity.
  Cleans the entity and scoped leftovers on take/drop.
```

The name `BoundEntity` is intentionally less absolute than `UniqueEntity`. The
handle does not prove global uniqueness of the entity. It proves that this
entity is bound to the caller for scoped request cleanup and destructive output
consumption.

## Current Implementation

Implemented:

- `InsertedEntity::bind() -> BoundEntity`
- `BoundEntity::take<T>(self) -> Result<T::Output, TakeError>`
- required component takes
- `Option<T>` optional takes
- tuple take bundles
- cleanup of leftovers scoped to the bound entity
- deferred cleanup when a bound handle is dropped without `take`

Not implemented:

- binding existing arbitrary entities
- non-consuming bound reads
- explicit async `close()`
- query-level `Take<T>`

The current direction is to avoid query-level `Take<T>` entirely unless a later
use case proves it is needed.
