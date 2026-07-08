# Formal Semantics Sketch

This document gives a more formal way to interpret Porridge/Bowl behavior.
It is not a proof, but it should be precise enough to reason about systems,
inputs, derived state, memoization, and streaming evaluation.

## Objects

A bowl contains a world:

```text
W = (E, C, R, O)
```

Where:

```text
E
  entity ids

C(e, t) = value
  component value of type t attached to entity e

R(e, t) = revision
  tracked revision of component t on entity e

O(e, t) = origin
  either Base or Derived(invocation)
```

The world has a monotonic global revision counter. Tracked writes advance that
counter unless the component supplies a stable fingerprint and the fingerprint
is unchanged.

Untracked components still exist in the world, but do not contribute dependency
revisions.

## Components

A component is a typed fact:

```text
t : Component
v : t
```

Attaching `v` to entity `e` asserts:

```text
C(e, t) = v
```

If `t` is tracked, the assertion also has a revision:

```text
R(e, t) = r
```

If `t` is untracked, queries may observe it, but memo dependency sets do not
include it.

## Base Inputs

External insertion is a base write:

```text
insert(e, bundle) -> W'
```

For every component `(t, v)` in the bundle:

```text
C(e, t) = v
O(e, t) = Base
```

Base inputs are durable. A system rerun does not remove base inputs.

Examples:

```text
FilePath
FileText
SystemImportDb singleton
client state
```

## Derived Outputs

System commands are derived writes:

```text
commands from invocation i -> W'
```

For every component `(t, v)` inserted by invocation `i`:

```text
C(e, t) = v
O(e, t) = Derived(i)
```

Derived writes from `i` replace its previous outputs by diffing:

```text
previous = { (e, t) | O(e, t) = Derived(i) }
apply new commands from i        (writes land over the old entries)
remove previous \ re-emitted     (whatever the rerun did not produce again)
```

Applying over the old entries means a re-emitted component with an unchanged
fingerprint keeps its revision, so an idempotent rerun causes no downstream
invalidation. The observable result is still replace-by-invocation semantics:

```text
same system invocation reruns
  old outputs for that invocation disappear
  new outputs replace them
```

Entities spawned by an invocation are also reused slot by slot in spawn order,
so an idempotent rerun keeps its derived entity ids. A system whose spawn
order is data-dependent will see ids swap meaning between slots; ids are
identity, not order (see Determinism).

## Systems

A system is interpreted as a partial function from a snapshot and one query row
to a command buffer:

```text
s_i : Snapshot x Row -> Commands
```

The function is not called directly by users. The runner enumerates rows from
the current snapshot and creates system invocations.

Systems are allowed to be async. The important semantic rule is:

```text
systems read from an immutable snapshot
systems write only through buffered commands
commands commit later if still current
```

## Query Rows

A query shape `Q` enumerates row states from a snapshot:

```text
rows(Q, S) = [q_1, q_2, ...]
```

Each row has:

```text
item(Q, S, q)
  values passed to user code

keys(Q, q)
  entity ids that identify the invocation

deps(Q, S, q)
  tracked component revisions read by the query row
```

The invocation identity for system `s` and row `q` is:

```text
i = (system_id(s), keys(Q, q))
```

The memo dependency record is:

```text
M(i) = deps(Q, S, q)
```

If a query has multiple `Query` parameters, its rows are the cartesian product
of the parameter row sets. `View` and `Commands` contribute one unit row and do
not multiply by visible facts.

## Query vs View

`Query` is tracked input:

```text
Query
  contributes rows
  contributes invocation keys
  contributes memo deps
```

`View` is ambient context:

```text
View
  reads the same snapshot
  does not contribute invocation keys
  does not contribute memo deps
```

So a system like:

```rust
fn check_duplicate_defs(
    def: Query<(Entity, &AstDef)>,
    all_defs: View<'_, (Entity, &AstDef)>,
)
```

is interpreted as:

```text
run once per AstDef row
rerun when that driving AstDef row changes
inspect all visible AstDef rows at invocation time
do not rerun solely because an unrelated AstDef appeared
```

This is a deliberate tradeoff. If another fact should invalidate the system,
make it part of `Query`, not `View`.

## Memoization

An invocation is runnable when:

```text
M(i) is absent
or
M(i) != deps(Q, S, q)
or
the system parameter set is marked always-run
```

Otherwise the previous result is considered current, and the system invocation
is skipped.

For tracked components:

```text
same revision -> same input for memo purposes
different revision -> changed input
```

For hash-stable components:

```text
insert/mutate equal fingerprint -> revision unchanged
insert/mutate different fingerprint -> revision advances
```

## Streaming Evaluation

The runner maintains:

```text
W_live
  live world

M
  memo table

running
  set of invocation identities currently executing
```

One normal phase is evaluated as:

```text
loop:
  S = clone(W_live)
  plan runnable invocations from S and M
  start any planned invocation not in running

  wait for at least one invocation to finish
  drain every invocation that has already finished
  remove them from running

  for each finished invocation:
    if its captured deps are still current in W_live:
      commit its commands atomically
      update M
    else:
      discard its commands

  re-plan from W_live only when the batch changed the world, a discarded
  run left its row memo-invalid, or a conflict-deferred row was freed
  (a commit that changed nothing cannot enable new rows)

  stop when no runnable invocations exist and running is empty
```

Commit validity is checked by dependency revision equality:

```text
current(M_candidate, W_live) =
  for every dep (entity, component_type, revision):
    W_live.revision(entity, component_type) == revision
```

If this check fails, the output is stale. The runner discards it. Replanning may
start a fresh invocation if the row still exists.

## Atomic Commit

One invocation commit is atomic:

```text
commit(i, commands):
  apply every command in commands over the old Derived(i) outputs
  remove stale Derived(i) outputs the commands did not re-emit
  reconcile rows mutated in place through MutRef access
  publish M(i), absorbing i's own writes into its dep revisions
```

Other systems never observe a partial command buffer. They observe either the
old world or the world after the full invocation commit.

## Phases

Phases impose coarse ordering:

```text
Startup -> Evaluate -> Complete -> on_settled -> Settle
```

Within a normal phase, streaming evaluation runs until the phase is quiescent.
Only then does the runner advance to the next phase.

Normal phases are:

```text
Startup
Evaluate
Complete
```

Settle is terminal for the current external evaluation boundary, and it
cannot drive its own settle forward: its removal commands are applied before
callers observe results (stale facts are reaped from the settled view),
while its insert/spawn commands are queued as inputs for the start of the
next run.

## Settled Boundary

External callers observe settled state:

```text
scoop(...)
take(...)
```

Both wait until:

```text
no pending base inputs
no normal phase can make progress
no normal invocation is running
on_settled hooks have run
cleanup has run
```

So internal work may stream continuously, but public reads still see a coherent
boundary.

## on_settled

An `on_settled` hook is interpreted as:

```text
when normal phases are quiescent,
run this hook for a system that is itself clean/settled
```

If the hook writes commands, those commands may enable more normal work. The
runner continues evaluation rather than returning immediately.

`on_settled` hooks should be idempotent. A hook that writes tracked changes on
every run creates non-convergence.

## Ephemeral Facts

An ephemeral fact is an ordinary component with a conventional lifetime:

```text
exists during one evaluation boundary
removed during Settle
```

Ephemeral facts are useful as phase-transition gates:

```text
normal facts settle
on_settled inserts AstAvailable + Ephemeral
gated systems run
cleanup removes Ephemeral entities
caller observes durable outputs
```

Ephemeral facts should usually be untracked so their creation/removal does not
act like durable input churn.

## DerivedFrom

`DerivedFrom` is a derived fact validity relation:

```text
DerivedFrom([e_1, e_2, ...])
```

At insertion time, it captures entity revisions:

```text
[(e_1, rev_1), (e_2, rev_2), ...]
```

The settle-phase cleanup removes the derived entity when:

```text
any source entity is missing
or
any source entity revision differs from the captured revision
```

This is separate from invocation ownership:

```text
invocation ownership
  replaces outputs when the producer reruns

DerivedFrom
  removes outputs when their source facts become stale
```

## Base State vs Derived State

A useful interpretation is:

```text
base state:
  facts inserted by the outside world
  durable until explicitly changed or removed

derived state:
  facts produced by systems
  owned by the invocation that produced them
  replaceable by rerun
  optionally tied to source revisions with DerivedFrom
```

The bowl is therefore not a pure function from one input to one output. It is an
incremental state machine:

```text
(base facts, durable derived facts, memo table)
  --insert/mutate-->
pending state
  --settle-->
new coherent state
```

## Determinism

Streaming allows nondeterministic completion order:

```text
parse_file(2)
generate_ast(2)
parse_file(1)
generate_ast(1)
```

The intended deterministic surface is not log order or derived entity id order.
The intended deterministic surface is the settled set of logical facts, assuming
systems are deterministic and do not depend on incidental entity allocation
order. Derived entity ids are additionally *stable*: a rerun reuses its spawn
slots, so an id keeps pointing at "the invocation's Nth output" across reruns —
but which logical fact occupies slot N is the system's business, not the
runtime's.

If a system needs stable ordering, it should use stable data in components, such
as paths, spans, names, or explicit sort keys, rather than relying on the order
in which async invocations happen to commit.

## Non-Convergence

Evaluation may fail to settle when systems continuously create new tracked
changes:

```text
A reads B and writes new A'
B reads A and writes new B'
```

A bowl settles when evaluation reaches a fixed point for normal phases:

```text
plan(W, M) = empty
running = empty
```

and settled hooks do not introduce further normal work. The Settle phase runs
after that boundary and cannot advance normal phases: only its removals land
inside the boundary, and its inserts wait for the next run.

This fixed point is the semantic definition. The runtime guardrail is separate:

```text
CommitLimit::Max(n)
  allow at most n accepted non-cleanup commits during one external evaluation
  attempt

CommitLimit::None
  keep driving until the bowl settles or the caller cancels the async operation
```

A commit limit is useful for catching accidental feedback loops while
prototyping, but it is not required for the model. Some applications may
intentionally run in a never-settling mode and rely on external cancellation,
timeouts, or subscription-style APIs.

## Reading The Model

The shortest useful summary:

```text
systems are memoized functions from tracked query rows to buffered commands
queries choose invocation identity and dependencies
views are snapshot context
commands produce derived state owned by the invocation
streaming commits each still-current invocation as it finishes
scoop observes only the settled boundary
```
