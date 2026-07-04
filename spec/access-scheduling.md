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
  scoped live access
  external Mut<T> exposes synchronous with_original / with_latest
  system Mut<T> declares a write edge before user code runs
```

The current implementation has external `Cow<T>` and external `Mut<T>`.
System-level `Mut<T>` exists as a scheduler-visible write edge. Storage-backed
system mutation is still incomplete until live component payloads move from
`Arc<T>` snapshots to guarded component cells.

## Target Access Protocol

External access and internal system access should participate in the same
read/write protocol:

```text
read(T, e)  conflicts with write(T, e)
write(T, e) conflicts with read(T, e)
write(T, e) conflicts with write(T, e)
read(T, e)  can overlap read(T, e)
```

The difference is how access is acquired:

```text
external scoop access
  acquired dynamically through scoped guards or scoped closures

internal system access
  declared ahead of execution through system parameters and planned by the
  runner
```

This means external reads count. If an external caller holds a guarded read of
`FileText(file_a)`, systems that also read `FileText(file_a)` can run, but a
system that mutates `FileText(file_a)` must wait until all external and
internal readers release it. Likewise, external writes block internal reads and
writes for the same row.

The long-term storage model should therefore be closer to DashMap than to a
pure immutable snapshot store:

```text
entity component cell
  value lock
  revision metadata
  optional fingerprint metadata
```

Normal immutable snapshots can still exist as an ergonomic read mode, but
guarded reads are the mode that participates in live scheduling.

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

## Current External Mut Semantics

External `Mut<T>` is used through a scoop:

```rust
let rows = bowl
    .scoop::<Query<(Entity, Mut<FileText>), Where<Eq<FilePath>>>>()
    .args(FilePath(path))
    .await
    .collect();

for (_entity, file) in rows {
    file.with_original(|file| {
        file.apply_delta(delta);
    })
    .await;
}
```

The handle itself is inert:

```text
holding Mut<T>
  does not hold a lock
  does not hold live mutable access
```

Live access only exists inside `with_original` or `with_latest`:

```text
with_original / with_latest
  acquire live access
  run synchronous closure
  update revision/fingerprint metadata
  release live access
```

External `Mut<T>` does not clone component payloads. If the live world does not
uniquely own the payload because an immutable snapshot still shares it, the
mutation returns `None`.

This is a prototype shortcut. The target behavior is to wait for existing live
readers to release instead of failing because an immutable snapshot holds an
`Arc<T>`.

The methods encode the conflict policy:

```text
with_original
  mutate only if the component revision still matches the revision observed by
  the scoop that produced the handle

with_latest
  mutate whatever component value is currently attached to the entity
```

Revision bumping happens after the scoped mutation completes:

```text
hashed/fingerprinted component:
  before = fingerprint(value)
  run mutation closure
  after = fingerprint(value)
  bump revision only when before != after

non-fingerprinted component:
  run mutation closure
  bump revision after a successful mutation
```

This keeps no-op edits from invalidating downstream work when the component has
opted into fingerprint tracking. Non-fingerprinted components remain
conservative.

The closure is deliberately synchronous. Normal async code cannot hold the live
access while awaiting another bowl operation. A user can still do explicit
sync-over-async inside the closure, but that is a visible blocking critical
section rather than an accidental `.await` footgun.

## Current System Mut Scheduling

System-level `Mut<T>` is an access declaration, not a hidden COW value:

```rust
async fn update_file(query: Query<(Entity, Mut<RopeyFile>)>) {
    let (_entity, file) = query.item();
    file.with_latest(|file| {
        file.apply_delta(delta);
    }).await;
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

Current caveat: system-side `Mut<T>` uses the same live mutation handle as
external `Mut<T>`, but systems still read from cloned `Arc<T>` snapshots. That
means real mutation from inside systems is not the intended final behavior yet;
the next storage refactor should make `Mut<T>` wait on guarded component cells
instead of failing when snapshots share the old payload.

## Async External Access

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

- keep external `Mut<T>` synchronous access methods short
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
