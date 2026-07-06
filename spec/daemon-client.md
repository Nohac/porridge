# Long-Running Daemon and Client Applications

This spec captures the direction for using Porridge as a long-running async
daemon with CLI/client frontends.

The goal is a fully state-driven application:

```text
external events insert facts
systems derive and update facts
clients insert request entities
bound handles take responses
bowl settles and sleeps until more work arrives
```

## Basic Shape

```text
daemon process
  owns one long-lived Bowl
  registers systems and plugins
  receives filesystem/network/client events
  inserts or mutates components
  settles work
  answers bound requests

client/cli
  connects to daemon
  submits request entity
  waits for response
  prints result
```

Local in-process usage has the same shape:

```rust
let response = bowl
    .insert((CliRequest, CommandLine(args), CurrentDir(cwd)))
    .await
    .bind()
    .take::<CliResponse>()
    .await?;
```

## State-Driven Services

Instead of direct service calls:

```text
handle_hover() calls parser, checker, indexer, cache
```

Porridge should model the application as facts:

```text
FilePath
FileText
ParsedFile
AstDef
Diagnostic
SymbolIndex
HoverRequest
HoverInfo
```

External truth enters as base writes or mutations:

```text
file watcher event -> update FileText
config reload      -> update SystemImportDb singleton
client request     -> insert HoverRequest entity
```

Systems react to current facts and write derived facts.

## Request/Response

Bound entities are the main request primitive:

```rust
let request = bowl
    .insert((HoverRequest, FilePath(path), Position { offset }))
    .await
    .bind();

let hover = request.take::<HoverInfo>().await?;
```

The bound handle gives:

```text
request-scoped output ownership
destructive take
cleanup after take/drop
protection from unrelated callers taking the wrong response
```

## Settled Observation

By default, clients should observe settled results:

```text
insert request
runner streams internal work
on_settled hooks publish phase gates
cleanup removes ephemeral facts
take/query returns durable result
```

This makes daemon behavior deterministic from the client's point of view even
if internal execution is streaming.

## Event Sources

Likely plugins or adapters:

```text
FileWatcherPlugin
  watches disk and updates FileText/FileMissing/etc.

TimerPlugin
  inserts Tick/IntervalElapsed facts

ClientTransportPlugin
  turns socket/IPC requests into bound entities

ShutdownPlugin
  inserts ShutdownRequested and lets systems flush state
```

These should be ordinary producers of base facts where possible.

## Useful Patterns

Singleton components:

```text
SystemImportDb
WorkspaceConfig
OpenFiles
ClientRegistry
```

Ephemeral singleton gates:

```text
AstAvailable
IndexAvailable
DiagnosticsReady
```

Derived cleanup:

```text
Diagnostic derived from [ImportDecl, SystemImportDb]
HoverInfo derived from [HoverRequest, FileText, SymbolIndex]
IndexEntry derived from [AstDef]
```

Mutable external queries:

```rust
bowl.scoop::<Query<(Entity, Cow<FileText>), Where<Eq<FilePath>>>>()
    .args(path)
    .for_each(|(_entity, text)| text.apply_delta(delta))
    .await;
```

## Engine Support for Out-of-Core Replication and Streaming

Replication between a daemon bowl and client bowls stays a plugin — it is
not core (`TODO` §19 keeps a Replicon-like layer as a plugin track). This
section lists what the *engine* must provide so such a plugin is buildable
outside the core. The requirements are generalized from porting experience
with bevy/ecsdk-based isomorphic tools; no single tool's shape is assumed.

### Two distribution semantics

A replication plugin must distinguish two kinds of facts, because their
delivery rules are opposites:

```text
state sync
  idempotent, latest-wins
  source of truth stays on the daemon
  delivered from a revision cursor; a missed update is healed by the next
  clients converge to the same view

streams
  ordered, consumed
  addressed to specific subscribers, never broadcast
  removed from the daemon once delivered (the daemon is a queue, not a copy)
  examples: log lines, progress events, provisioning output
```

Replicating everything to every client — the replicon default — conflates
the two: stream facts get broadcast and retained, and per-client gating has
to be bolted on afterwards. In porridge both the scoping and the semantics
should be *data*:

```text
subscriptions are facts
  Subscription { client, topic }        inserted by the transport adapter
  the plugin's delivery queries join facts to subscriptions
  (Where<Eq<Topic>> / demand markers) — gating is a query, not registry
  configuration

stream facts are consumed
  delivered to the matching subscribers, then removed from the daemon
  anchored with DerivedFrom to the subscription, so unsubscribing reaps
  the undelivered backlog automatically
```

### Already available (the substrate)

```text
revisions + fingerprints   change detection is built into storage; a
                           replication layer reads it instead of bolting
                           on Changed<> machinery
epochs                     consistent boundaries to cut packets at; deltas
                           arriving at a client are ordinary external
                           inputs, batched and frozen per epoch
joins / demand markers     per-client and per-topic scoping as data
bound entities             request/response across the transport
executor-agnostic async    transport adapters are plain tokio tasks
```

### Missing engine capabilities

These are the concrete asks, in rough dependency order:

1. **Revision-cursor reads.** Enumerate `(entity, component)` pairs whose
   revision exceeds a cursor, per store or world-wide, from a settled
   snapshot. This is the delta source for state sync; revisions exist but
   are not exposed to external readers.
2. **External targeted inserts.** Add or update components on an *existing*
   entity from outside a system. Needed by clients applying replicated
   deltas, and independently by long-running task adapters reporting
   completion facts. `insert()` today only creates new entities.
3. **External targeted removal / drain reads.** A destructive external read
   over query results (`take`-by-query): deliver-then-delete is the stream
   contract, and the delete must be atomic with the read so redelivery
   after a crash is at-least-once rather than duplicated-forever. Also
   needed by clients applying replicated removals.
4. **Settle notifications.** An async signal — "settled, revision moved
   past R" — so a push-based publisher wakes exactly when there is
   something to publish instead of polling. This also answers the "watch
   current facts" open question below for live UIs, together with the
   stale-read scoop planned in `spec/epochs.md`.

Cross-process entity identity (daemon entity ↔ client entity mapping) is a
plugin concern: entity ids are stable within a bowl, and the plugin owns
the translation table. Serialization is opt-in per component via ordinary
derives and a plugin-side registry; the engine needs no reflection.

### Sketch: log streaming done right

With 1–4 in place, the failure mode "logs broadcast to every client and
retained forever" is unrepresentable:

```text
daemon
  provisioning task appends LogEntry { seq, line } facts (targeted inserts)
  client subscribes: transport inserts Subscription { client, topic: Logs }
  publisher plugin, woken by settle notification:
    drain-read LogEntry joined to Subscription   (scoped, ordered by seq)
    send to that client's transport
    entries removed by the drain — the daemon keeps no backlog
  unsubscribe removes the Subscription; DerivedFrom cleanup reaps
  undelivered entries
```

## Observability Needs

Daemon usage needs better introspection than a short-lived CLI:

```text
what systems are running now?
what request is waiting on what facts?
what systems last changed a component?
what derived entities were cleaned up?
why did this query take time?
what kept the bowl from settling?
```

Tracing should be treated as a first-class future feature.

## Open Questions

- How should remote clients address entities across process boundaries?
- Should request IDs be components, transport metadata, or bound handle state?
- Should there be a non-settling "watch current facts" API for live UIs?
- How should daemon shutdown wait for or cancel running invocations?
- How should long-running tasks report progress without preventing settle?
- Which common event sources deserve plugins?
