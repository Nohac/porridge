# Access Scheduling

This document records the intended split between clone-on-write updates and
future scheduler-level mutable access.

## Names

```text
Cow<T>
  clone-on-write value access
  may clone T when a snapshot or another holder still shares the payload
  useful for explicit, simple updates

Mut<T>
  planned exclusive access
  declares a write edge before user code runs
  intended for large stateful components where copying is the wrong default
```

The current implementation only has `Cow<T>` for external scoop updates.
`Mut<T>` is intentionally left for the scheduler work.

## Current Cow Semantics

External `Cow<T>` is used through a scoop:

```rust
bowl.scoop::<Query<(Entity, Cow<FileText>), Where<Eq<FilePath>>>>()
    .args(FilePath(path))
    .for_each(|(_entity, text)| {
        text.apply_delta(delta);
    })
    .await;
```

The current closure is synchronous. It runs while the live world is locked and
updates storage through `Arc::make_mut`.

```text
if payload is uniquely held:
  mutate in place
else:
  clone T, then mutate the clone
```

This keeps immutable snapshots valid, but it is not ideal for very large
components.

## Future Mut Semantics

System-level `Mut<T>` should be an access declaration, not a hidden COW value:

```rust
async fn update_file(query: Query<(Entity, Mut<RopeyFile>)>) {
    let (_entity, mut file) = query.item();
    file.apply_delta(delta);
}
```

Before running that invocation, the planner can infer:

```text
reads:
  RopeyFile(entity)

writes:
  RopeyFile(entity)
```

The scheduler can then run non-conflicting rows concurrently while ordering or
blocking conflicting access:

```text
write(T, e) conflicts with read(T, e)
write(T, e) conflicts with write(T, e)
read(T, e) can overlap read(T, e)
access to unrelated entities can overlap
```

This makes `Mut<T>` row-granular. For example, formatting two different files
can run concurrently:

```text
format(file_a) writes RopeyFile(file_a)
format(file_b) writes RopeyFile(file_b)
```

## External Access

With the current single-flight runner, external mutation does not normally race
with running systems because external scoops first drive the bowl to a settled
boundary.

Async external exclusive access is still tricky. If a caller holds an external
reservation and then awaits another bowl operation that needs the same
reservation, it can create a wait cycle:

```text
caller holds write(FileText, file_a)
caller awaits scoop(...)
evaluation needs read/write(FileText, file_a)
```

Possible future approaches:

- keep external `Cow<T>` synchronous and short
- add async external reservations with wait-graph cycle detection
- prioritize external writers and cancel/replan affected systems

## Commands

`Commands` can write dynamically, so the planner cannot infer every write from
parameters alone. The initial access graph can still be useful:

```text
Query<&T>
  visible read edge

Query<Mut<T>>
  visible read/write edge

Commands
  buffered dynamic writes, validated at commit
```

Later APIs may add explicit declarations for command writes if the scheduler
needs stronger static information.

