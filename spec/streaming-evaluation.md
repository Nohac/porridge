# Streaming Evaluation

This spec describes the likely next execution model for `bowl`.

The current implementation plans a batch of system invocations, runs them
concurrently, then applies all command buffers at a barrier. That is simple and
deterministic, but it means one slow async invocation can hold back outputs from
completed invocations.

The proposed model keeps single-flight evaluation, but commits completed
invocations as they finish.

## Core Model

From an initial settled state:

```text
plan runnable invocations from current world
run planned invocations concurrently

as each invocation completes:
  validate its captured deps against the current world
  if still current:
    remove old outputs owned by that invocation
    apply its commands atomically
    update memo
    re-plan from the updated world
  if stale:
    discard its commands

repeat until:
  no runnable invocations
  no running invocations
  no pending commands

run on_settled hooks
if settled hooks write commands:
  continue evaluation
else:
  run cleanup
  notify waiters
```

This turns the runner into a work queue rather than a generation barrier.

## Why Streaming

Batch barriers are fine for many compiler-like passes, but they are a poor fit
for long-running async daemon work:

```text
parse file 1 completes quickly
parse file 2 is waiting on IO

batch mode:
  checks for file 1 wait for file 2

streaming mode:
  parse file 1 commits immediately
  checks for file 1 can start while file 2 is still running
```

Streaming makes lower-latency progress while still letting outside callers wait
for a settled snapshot.

## External Boundary

Streaming is an internal execution strategy. Public query/take APIs should
still observe settled state by default:

```text
internal:
  work can commit continuously

external:
  query waits until settle
  bound take waits until settle
  cleanup has run before results are observed
```

This is where `on_settled` becomes important. It provides the global boundary
that batch mode previously implied.

## Invocation Lifecycle

Each invocation captures:

```text
system id
query keys
dependency revisions
snapshot used for reads
```

When the invocation finishes, the runner checks whether the same dependency
revisions are still current in the live world.

```text
deps match:
  commit outputs

deps changed:
  discard outputs
  let replanning start a fresh invocation if still needed
```

This gives cancellation-by-staleness without requiring the async task to be
actively cancelled.

## Duplicate Work Protection

The runner needs a `running` set:

```text
SystemInvocation -> captured deps
```

Replanning after each commit must not start the same invocation twice while an
older run is still in flight.

If a running invocation's deps become stale, there are two possible policies:

```text
lazy:
  let it finish, then discard

eager:
  mark stale and optionally cancel if cancellation is supported
```

The MVP can use lazy invalidation.

## Command Commit

Command application should be atomic per invocation:

```text
remove old outputs for invocation
apply all commands from invocation
commit memo update
```

Applying one invocation at a time means downstream systems can start as soon as
their facts appear.

## Per-System Work Policies

Streaming evaluation should still support batching, but as per-system policy
knobs rather than one global execution mode.

These policies are scheduling and commit hints. They must not change the core
correctness rules:

```text
systems read immutable snapshots
completed outputs commit only if captured deps are still current
outside callers observe settled state by default
```

### max_concurrency

```rust
parse_file.max_concurrency(10)
```

Meaning:

```text
run at most 10 invocations of this system concurrently
```

The system function still receives one query row per invocation. This only
limits how many invocations the runner starts for that system at once.

This is useful for IO-heavy or CPU-heavy systems where unlimited parallelism is
counterproductive.

### min_batch_size

```rust
parse_file.min_batch_size(10)
```

Meaning:

```text
do not start this system while fewer than 10 runnable rows are available,
unless the bowl is trying to settle
```

The system is still invoked one row at a time. This is not a `QueryBatch` API.
It is a scheduling threshold that lets work accumulate during busy periods.

Quiet periods must not leave work stuck forever:

```text
if no other progress is possible and the bowl needs to settle,
start the partial batch even if it is smaller than min_batch_size
```

### batch_commits

```rust
parse_file.batch_commits(10)
```

Meaning:

```text
collect up to 10 completed outputs from this system before applying them,
or flush the current commit batch when the bowl is trying to settle
```

This reduces commit/replan churn without reintroducing a global barrier.

Validation should happen at flush time:

```text
invocation completes
  store output and captured deps in system commit buffer

flush commit buffer
  validate each output against the current world
  commit still-current outputs
  discard stale outputs
  re-plan after the batch commits
```

Validating at flush time matters because earlier commits in the same settlement
cycle can make a buffered output stale.

### Combined Example

```rust
bowl.add_system(
    parse_file
        .min_batch_size(10)
        .max_concurrency(10)
        .batch_commits(10),
)
.await;
```

Meaning:

```text
during busy periods:
  wait until 10 rows are runnable
  run up to 10 parse_file invocations
  commit completed outputs in batches of 10

during quiet/settling periods:
  run partial input batches
  flush partial commit batches
```

Potential defaults:

```text
max_concurrency: unlimited or executor default
min_batch_size: 1
batch_commits: 1
```

These defaults favor latency. Throughput-oriented systems can opt into larger
batches.

## Hooks

`on_start`:

```text
fires before a system starts processing the currently planned invalid work
does not fire for memo-clean systems
does not fire for systems with no planned work
```

`on_complete`:

```text
fires after the currently planned invalid work for a system has completed
local to that system's work wave
not a global readiness gate
```

`on_settled`:

```text
fires only when the whole bowl has no runnable work and no running work
the only global phase-transition boundary
```

`cleanup`:

```text
runs after settled hooks stop producing more work
cleanup writes are folded into the settled baseline
outside callers should not observe ephemeral cleanup markers
```

## Ephemeral Phase Gates

Ephemeral singleton markers emitted from `on_settled` are proper phase
transition gates:

```rust
generate_ast.on_settled(|mut commands| {
    commands.insert((Singleton::<AstAvailable>::new(), AstAvailable, Ephemeral));
})
```

Meaning:

```text
normal work has settled
publish AstAvailable
systems gated on AstAvailable can run
cleanup removes AstAvailable before outside observation
```

This replaces many hard-coded stages with ordinary facts.

## Single-Flight Still Applies

There should still be only one active evaluator per bowl:

```text
many callers can insert/query
one runner owns planning and commits
waiters subscribe to the active settlement
```

The single-flight runner changes from "run one generation barrier" to "drive
the work queue until settled".

See also `spec/async-single-flight.md`.

## Open Questions

- Should streaming be the only/default mode, or should batch mode remain as an
  explicit option?
- How should stale in-flight invocations be traced?
- Do we need task cancellation, or is lazy discard enough?
- Should `on_complete` fire once per replan wave or coalesce per system until
  global settle?
- How should progress be reported for very long-running daemon jobs?
- How should non-convergence diagnostics present continuously streaming work?
