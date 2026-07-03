# Async Single-Flight Evaluation

This spec describes the async single-flight model for Porridge/Bowl.

The original implementation used generation barriers: plan work, run systems,
apply all commands, repeat. The likely next runtime keeps the single-flight
invariant but changes the internal runner to streaming commits. See
`spec/streaming-evaluation.md`.

The core invariant:

```text
At most one evaluation is active per database.
Concurrent readers share that evaluation.
Inputs are batched into the next evaluation.
All public operations work through a shared database handle.
```

This keeps async callers from accidentally starting overlapping scheduler runs
against the same world.

## Terms

```text
world
  The mutable component store.

snapshot
  An immutable view of the world used by one evaluation.

generation
  A monotonically increasing completed-world version.

evaluation
  A single-flight run that drives pending work until the bowl settles.

pending input
  Base writes submitted by callers while an evaluation is idle or running.

waiter
  A caller waiting for a generation to complete before reading a query.

subscription
  The generation a caller has chosen to wait for.

runner lock
  A single-permit lock that grants the right to execute an evaluation.
```

## Shared Handle API

The database should be usable behind `Rc` or `Arc`.

Public operations must not require `&mut self`:

```rust
impl AsyncBowl {
    pub async fn query<Q>(&self) -> Q::Output;
    pub async fn insert<B>(&self, bundle: B) -> InsertedEntity;
    pub fn entity(&self, entity: Entity) -> EntityRef<'_>;
}
```

The sync wrapper should follow the same shape:

```rust
impl Bowl {
    pub fn query<Q>(&self) -> Q::Output;
    pub fn insert<B>(&self, bundle: B) -> InsertedEntity;
    pub fn entity(&self, entity: Entity) -> EntityRef<'_>;
}
```

This makes these patterns valid:

```rust
let db = Arc::new(AsyncBowl::new());

let a = Arc::clone(&db);
let b = Arc::clone(&db);

let diagnostics = a.scoop::<Query<(Entity, &Diagnostic)>>();
let hover = b
    .insert((HoverRequest, Position { offset }))
    .await
    .bind()
    .take::<HoverInfo>();
```

Internally, the bowl owns its mutable state behind synchronization primitives.
The public API should expose logical mutation through shared references, not
Rust exclusive borrows.

This requirement applies to base inserts, entity mutation, request insertion,
bound cleanup, and query evaluation.

Deadlock avoidance rules:

```text
do not hold the state lock while running systems
do not hold the state lock while awaiting user futures
do not hold the state lock while blocking in the sync wrapper
do not call user code while holding the state lock
use the runner lock only to elect and guard the single evaluator
```

Lock ordering:

```text
short state lock sections may inspect/update generation bookkeeping
runner lock may be held across evaluation
state lock may be reacquired briefly by the runner between phases
state lock must not be held while waiting for runner lock
```

This should keep shared `Arc<AsyncBowl>` usage from deadlocking when multiple
threads concurrently insert, mutate entities, and query.

## Query Behavior

A plain query does not start its own independent evaluation if one is already
running.

```rust
let diagnostics = db.scoop::<Query<(Entity, &Diagnostic)>>().await;
let definitions = db.scoop::<Query<(Entity, &AstDef)>>().await;
```

If both calls happen concurrently, they wait for the same in-flight evaluation.
When that evaluation completes, each caller runs its query against the completed
snapshot.

Rule:

```text
Reads can join the current evaluation.
```

More precisely, readers subscribe to the best known generation:

```text
if an evaluation is running:
  subscribe to the running generation

if no evaluation is running and no pending input exists:
  read the current completed generation
```

Plain reads do not automatically subscribe to a pending generation created by
new input. A plain read asks for the latest completed or currently-running
world, not for future request work it did not submit.

Request-style reads are different: an insert returns the generation that will
include that input, and the following query waits for that generation.

## Input Behavior

A caller can submit input before querying:

```rust
let hover = db
    .insert((HoverRequest, FilePath(path), Position { offset }))
    .await
    .bind()
    .take::<HoverInfo>()
    .await;
```

If an evaluation is already running, the input is not injected into that
evaluation. It is queued for the next generation.

Rule:

```text
Writes schedule the next evaluation.
```

This means a request-style query waits for the generation that includes its
input.

Example:

```text
generation 10 is running

plain query arrives:
  wait for generation 10
  read generation 10

request insert arrives:
  enqueue input for generation 11
  wait for generation 11
  read generation 11
```

Inputs do not each force their own evaluation. If several callers insert while a
generation is running, all of those inputs are batched into the same next
generation:

```text
generation 10 is running

caller A inserts file_a
caller B inserts file_b
caller C inserts hover_request

all three inputs are queued for generation 11

generation 10 completes
generation 11 starts with file_a, file_b, and hover_request
```

Likewise, if the bowl is idle but dirty, readers and request callers subscribe
to their assigned generation rather than creating separate input/execution
pairs.

Rule:

```text
There is at most one pending generation.
All pending inputs are drained into that generation when it starts.
```

## Evaluation Lifecycle

The runtime can be modeled as:

```text
idle, clean:
  queries read current generation immediately
  no systems run

idle, dirty:
  a caller that can acquire the runner lock drains all pending inputs
  pending inputs are applied
  evaluation starts

running:
  plain queries wait for the running generation
  inputs are queued for the single pending next generation

complete:
  commands are applied
  lifecycle completion hooks run
  ephemeral cleanup runs
  generation increments
  waiters are notified
  if pending inputs exist, next generation is dirty
```

Only one task owns evaluation at a time. Other callers wait.

The clean fast path is important:

```text
if completed_generation satisfies the caller's target
and there is no pending input for that caller
then query the current world immediately
```

No system should run just because a caller asked a read-only question against an
already evaluated generation.

Lifecycle hooks must preserve the same single-flight invariant:

```text
only the active evaluator runs hooks
waiters are notified after hook commands and cleanup are applied
outside callers should not observe generation-scoped ephemeral facts
```

See `spec/lifecycle-and-ephemeral.md`.

## Sketch

```rust
struct AsyncBowl {
    state: Mutex<State>,
    runner: Mutex<()>,
    notify: Notify,
}

struct State {
    world: World,
    completed_generation: u64,
    running_generation: Option<u64>,
    next_generation: u64,
    pending_generation: Option<u64>,
    pending_inputs: Vec<BaseCommand>,
}
```

The runner lock is the authority for whether an evaluation is active.

```text
runner.try_lock() succeeds:
  this caller may run an evaluation

runner.try_lock() fails:
  another caller is running an evaluation
```

`running_generation` is informational state used for subscriptions and waiting.
It must match the active runner, but it is not used as the single-executor
authority.

The core operation is:

```rust
async fn ensure_evaluated(&self, target: TargetGeneration) -> Snapshot {
    loop {
        if self.completed_generation().await >= target.generation {
            return self.snapshot().await;
        }

        if let Some(runner) = self.runner.try_lock() {
            self.run_evaluation(runner).await;
        } else {
            self.wait_for_generation(target.generation).await;
        }
    }
}
```

This is only a sketch. The actual implementation should avoid holding the mutex
while systems run.

Subscription assignment is the important part:

```rust
fn subscribe_plain_read(state: &State) -> u64 {
    state
        .running_generation
        .unwrap_or(state.completed_generation)
}

fn submit_input(state: &mut State, input: BaseCommand) -> u64 {
    state.pending_inputs.push(input);

    *state
        .pending_generation
        .get_or_insert(state.next_generation)
}
```

This means many inserts can target the same pending generation.

Generation allocation happens when a runner starts:

```rust
fn start_evaluation(state: &mut State) -> Option<(u64, Vec<BaseCommand>)> {
    let generation = state.pending_generation.take()?;
    let inputs = std::mem::take(&mut state.pending_inputs);

    state.running_generation = Some(generation);
    state.next_generation = generation + 1;

    Some((generation, inputs))
}
```

Inputs submitted while that generation is running use the updated
`next_generation`, so they target the following generation:

```text
completed_generation = 10
pending_generation = 11

runner starts generation 11
next_generation becomes 12

new insert during generation 11:
  pending_generation becomes 12
```

Completion clears the running generation and advances the completed generation:

```rust
fn complete_evaluation(state: &mut State, generation: u64) {
    state.completed_generation = generation;
    state.running_generation = None;
    wake_waiters(state);
}
```

The main loop naturally handles the third case where more work was added while
the caller was waiting:

```rust
loop {
    if target <= completed_generation {
        read;
    } else if let Some(runner) = runner.try_lock() {
        run_evaluation(runner).await;
    } else {
        wait_for_generation(target).await;
    }
}
```

After a waiter wakes, it checks its target again. If new pending work exists and
the target is not complete, the waiter may acquire the runner and process the
next generation.

## Sync Bridge

The sync API should be a wrapper over the async implementation.

```rust
pub struct Bowl {
    inner: AsyncBowl,
}

impl Bowl {
    pub fn scoop<Q>(&self) -> Q::Output {
        pollster::block_on(self.inner.scoop::<Q>())
    }
}
```

The async type remains the real implementation.

Possible public names:

```rust
let db = Bowl::new();       // sync wrapper
let db = AsyncBowl::new();  // async core
```

or:

```rust
let db = Bowl::new_sync();
let db = Bowl::new_async();
```

Separate `Bowl` and `AsyncBowl` types are likely cleaner in Rust because sync
and async methods have different return types.

## System Execution

Async systems should read from immutable snapshots, not the mutable world.

```text
world N
  -> snapshot N
  -> systems read snapshot N and await freely
  -> commands are buffered per invocation
  -> runner applies commands after validating deps
  -> world advances
```

This avoids holding mutable world borrows across `.await`.

In the streaming model, snapshots are still immutable, but the barrier moves
from "all planned invocations" to "one completed invocation":

```text
invocation starts from snapshot N
invocation awaits
invocation completes
runner checks captured deps against live world
if current, command buffer commits immediately
if stale, command buffer is discarded
```

Sync and async systems can coexist:

```rust
db.add_system(parse_file);
db.add_async_system(fetch_remote_imports);
```

A unified `add_system` may be possible later, but separate APIs are acceptable
for the first async implementation.

## Request Queries

Request queries are just input plus query.

```rust
db.insert((HoverRequest, FilePath(path), Position { offset }))
    .await
    .bind()
    .take::<HoverInfo>()
    .await
```

The inserted entity belongs to the generation scheduled by the insert. The
query reads only after that generation completes.

Multiple request queries can share that same scheduled generation. The request
entity is how each query scopes its own output; the generation only controls
when the batch becomes visible.

If the request entity is bound, cleanup happens after `take`. If the bound
handle is dropped before `take`, cleanup is deferred to the next bowl operation.

## Consequences

This model gives:

- no overlapping evaluations per database
- deterministic snapshots for concurrent readers
- natural batching of input writes
- a clean sync bridge with `pollster`
- a foundation for async systems and later parallel execution

It does not decide:

- whether the current implementation keeps barrier mode or moves fully to
  streaming mode
- how non-convergence is reported
- whether systems run serially, parallel, or in ranked waves
- how deferred writes interact with output ownership

Those belong to the execution-cycle and streaming-evaluation models.

## Open Questions

- Should plain `query()` run one generation or settle until clean?
- Should callers be able to request `query_current()` without evaluation?
- Should callers be able to subscribe to in-progress streaming commits, or only
  settled snapshots?
- Should request inserts always force a new generation, even if equivalent
  hashed components already exist?
- What does cancellation mean for a waiter?
- If all waiters cancel, should the in-flight evaluation continue?
- Should `pollster` be an optional feature for the sync wrapper?
- Should `AsyncBowl` require `Send` systems from the start, or support local
  async first?
