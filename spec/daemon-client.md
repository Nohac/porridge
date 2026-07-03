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
bowl.scoop::<Query<(Entity, Mut<FileText>), Where<Eq<FilePath>>>>()
    .args(path)
    .for_each(|(_entity, text)| text.apply_delta(delta))
    .await;
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
