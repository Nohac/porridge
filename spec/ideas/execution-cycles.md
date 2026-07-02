# Execution Cycles and Feedback

This note explores how Porridge could execute systems when derived outputs can
trigger other systems, including feedback loops such as `A -> B -> A`.

The goal is to support systems that continuously produce and react to facts
without requiring users to define a full static dependency graph up front, while
also avoiding accidental infinite settle loops.

## Current Concern

If systems can trigger each other immediately, this can loop forever:

```text
A reads input, writes AOut
B reads AOut, writes BOut
A reads BOut, writes AOut
B reads AOut, writes BOut
...
```

Memoization only helps if writes converge to the same revisions/fingerprints. If
each step produces a real change, the engine needs explicit execution semantics
for feedback.

## Snapshot Ticks

The simplest model is epoch-based execution:

```text
tick N:
  all eligible systems read snapshot N
  all commands are buffered

barrier:
  commands are applied
  changed facts become snapshot N+1

tick N+1:
  systems react to snapshot N+1
```

Rule:

```text
A system never observes writes from the same tick it is running in.
```

This is easy to parallelize and reason about. There is no immediate recursive
triggering because all writes become visible in the next tick.

This model also gives generation-scoped coordination facts a natural place to
live. A settled hook can insert an ephemeral marker after normal work has
stabilized, and downstream systems can observe that marker on a later snapshot
tick:

```text
tick N:
  generate_ast reads parsed files

barrier:
  generate_ast outputs are applied

settled:
  generate_ast on_settled inserts ephemeral AstAvailable

tick N+1:
  validation systems gated on AstAvailable run

evaluation complete:
  ephemeral markers are removed before outside callers observe the world
```

See `spec/lifecycle-and-ephemeral.md`.

Possible API:

```rust
db.tick();   // run one snapshot step
db.settle(); // run ticks until no tracked changes, or until a limit is hit
db.watch();  // keep ticking as external inputs arrive
```

If `A` and `B` continually write changed facts for each other, `settle()` does
not terminate. That is a non-converging program, not a scheduler recursion bug.
The runtime should stop after a configured limit and report the systems/facts
that kept changing.

## Forward-Only Transactions

An alternative is a lower-latency transaction model:

```text
transaction:
  rank 0 systems run
  apply commands
  rank 1 systems triggered by rank 0 run
  apply commands
  rank 2 systems triggered by rank 1 run
  ...
```

Rule:

```text
A write can only trigger an invocation that has not already run in the current
transaction.
```

If a write targets an invocation that already ran, the invalidation is deferred
to the next transaction.

Cases:

```text
target has not run yet       -> schedule in this transaction
target already ran earlier   -> defer to next transaction
target ran in the same wave  -> defer to next transaction
```

This prevents immediate `A -> B -> A` recursion inside one transaction while
still allowing `A` to trigger `B` in the same transaction.

Example:

```text
transaction 1:
  rank 0: A runs from external input
  rank 1: B runs from A's output
  B writes A-input
  A already ran, so A is deferred

transaction 2:
  A can react to B's deferred write
```

Sideways writes are also deferred:

```text
transaction 1, rank 0:
  A runs
  B runs

A writes B-input
B writes A-input

Both A and B already ran in rank 0, so both invalidations are deferred.
```

## Dynamic Flow Direction

It is tempting to infer a flow direction from causality:

```text
A changed, causing B to run
therefore A -> B
```

The problem is that a later transaction might start from `B`:

```text
transaction 1: A -> B
transaction 2: B -> A
transaction 3: A -> B
```

If direction is inferred per transaction, the semantics can flip based on which
system happened to be triggered first. That is hard to debug.

A sticky inferred graph could avoid flipping:

```text
first observation: A -> B
later B writes A-input
this violates A -> B, so A is deferred
```

But this raises hard questions:

- What if the first inferred direction was accidental?
- When do inferred edges expire?
- Are edges global per system or per entity/fact?
- How are inferred edges explained to users?
- What happens with longer cycles such as `A -> B -> C -> A`?

Because of that, sticky inferred direction should be treated as a possible
future experiment, not the default model.

## Static Direction

A more predictable way to handle forward-only transactions is to use static
direction:

```text
registration order
explicit run_after / run_before
explicit scheduling dependencies
```

In this model, writes to earlier systems are deferred. Writes to later systems
may be observed in the same transaction.

This is easier to explain, but it moves Porridge closer to a scheduler graph.
That may be useful for some domains, but it should not be required for simple
fact-driven workflows.

## Likely Direction

For compiler and language-service style workloads, snapshot ticks are probably
the best default:

- every system reads a stable snapshot
- commands are buffered
- writes become visible next tick
- parallel execution is straightforward
- feedback is handled by repeated ticks
- non-convergence can be reported explicitly

Forward-only transactions may still be useful as an optional execution mode for
event-driven or lower-latency workflows.

## Streaming Replanning

The newer likely direction is streaming replanning. It keeps the snapshot rule
for each invocation, but removes the full-batch barrier:

```text
plan from current world
run invocations concurrently

when one invocation completes:
  validate captured deps
  commit if still current
  re-plan from updated world
  start newly runnable invocations
```

This gives lower latency without letting systems observe mutable world state
while they are running. Each invocation still reads one immutable snapshot.

The important sets are:

```text
memo
  completed clean invocations and their dependency revisions

running
  invocations currently awaiting

pending inputs
  external writes waiting to enter the world
```

The key safety rule:

```text
an invocation result can commit only if its captured dependencies still match
the live world
```

If a feedback loop continuously changes facts, streaming still does not settle.
The difference is that the non-converging loop can make progress one committed
invocation at a time rather than one whole batch at a time.

See `spec/streaming-evaluation.md`.

The important distinction:

```text
snapshot ticks:
  simpler, more parallel, one-tick reaction latency

forward-only transactions:
  lower latency, more complex, needs deferral rules

streaming replanning:
  lower latency, keeps immutable invocation snapshots, commits completed work
  immediately after dependency validation
```

## Open Questions

- Should `db.query()` run one tick, settle to a fixpoint, or use a configurable
  policy?
- Should `db.settle()` have a required max-tick limit?
- What information should a non-convergence error report?
- Are scheduling dependencies data dependencies, or a separate concept?
- Do we need both snapshot ticks and forward-only transactions?
- Can system output ownership stay clear when writes are deferred?
- How should external inputs enter a running system: as base component updates,
  events, or explicit transactions?
