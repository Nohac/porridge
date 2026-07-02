# Replication Ideas

This note captures early ideas for a Replicon-like layer for Porridge.

The goal is not game networking specifically. It is state replication for
component facts:

```text
local bowl
  entity/component inserts
  component updates
  removals
  singleton changes
  derived cleanup
      |
      v
replication stream
      |
      v
remote bowl / worker / client / cache
```

## Possible Uses

- daemon to CLI/client synchronization
- remote language-service clients
- distributed workers sharing derived cache facts
- local persistent cache replay
- debugging/replay of bowl evaluations

## Component Opt-In

Replication should likely be opt-in:

```rust
#[derive(Component, Replicate)]
struct FileText(String);
```

or configured through a plugin:

```rust
ReplicationPlugin::new()
    .replicate::<FilePath>()
    .replicate::<Diagnostic>()
```

## Entity Identity

Local `Entity` ids are process-local. Replication needs a stable mapping:

```text
local entity -> remote entity
local entity -> stable replicated id
stable id    -> remote entity
```

Singletons may be easier because their identity can be component-type based.

## Revision-Aware Deltas

Component revisions are a natural delta boundary:

```text
send all replicated components changed since generation N
```

For streaming evaluation, deltas may be emitted per committed invocation or
batched until settle depending on the use case.

## Authority

Different facts may have different owners:

```text
client-owned request facts
daemon-owned source facts
worker-owned derived facts
local-only ephemeral gates
```

Replication should not blindly mirror every component in every direction.

## Derived And Ephemeral Facts

Open policy choices:

```text
DerivedFrom
  replicate as normal metadata?
  keep local only?
  use it to clean replicated derived outputs?

Ephemeral
  probably local by default
  maybe replicated only for debugging/tracing
```

For daemon/client use, durable outputs such as `Diagnostic` and `HoverInfo`
matter more than transient gates such as `AstAvailable`.

## Relationship To BoundEntity

Bound request entities can map to RPC-like calls:

```text
client inserts request
daemon processes request
daemon emits response on same bound entity
client takes response
cleanup removes request and scoped outputs
```

The transport needs a request id or entity mapping so the response reaches the
right caller.

## Open Questions

- Should replication observe live commits or settled snapshots?
- Should replication be push, pull, or both?
- How are component schemas/versioning handled?
- How do remote removals interact with `DerivedFrom` cleanup?
- Can replication be implemented entirely as a plugin over public hooks, or
  does bowl need a changefeed API?
- Should replicated components require serialization traits in the `Component`
  derive?
