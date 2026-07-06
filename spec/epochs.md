# Epochs and Preemption

This document records the design for epoch-scoped input batching and
write-triggered preemption, and the analysis that motivated it: why gate
markers failed as an ordering tool, what they remain good for, and the
hierarchy of ordering mechanisms.

Status: design, not implemented. The current engine drains pending inputs
into every generation, including mid-settle reopened ones.

## Motivation: the two-layer marker failure

The playground's hover pipeline was originally ordered by the ephemeral
`AstAvailable` gate marker: emitted by an `on_settled` hook, consumed by
gated systems in the reopened generation, removed by cleanup, re-emitted
next settle. Under concurrent load this failed in two distinct ways.

### Layer 1: starvation (a scheduling bug)

Marker-gated work is *invisible* while the marker is absent — the gated row
is not unrunnable, it is not enumerable at all. Settle termination requires
the hooks to go quiet, so a post-cleanup window with no marker exists by
design. A request whose generation runs inside that window produces no
plannable work; every settledness check (revision counters, `has_work`,
hook progress) truthfully reports quiescence; any concurrent caller whose
`settle()` lands there concludes "settled" and returns. The request starves
until unrelated activity happens to trigger another hooks pass.

Two partial mitigations exist in the engine today: bound-entity removal
paths only extend `settled_revision` over an actually-settled state, and
`take()` waits for pinned cells. Neither closes the structural hole.

### Layer 2: the lie (an expressiveness limit)

A marker is a *recorded claim* — "the AST was settled when I was written" —
that consumers read as a *live guarantee* — "the AST is settled now". The
reopened generation drains all pending inputs at its start, so a text edit
arriving mid-settle lands in the very generation the marker inhabits;
`parse_file`/`generate_ast` rerun concurrently with the marker-gated
consumers, which read mid-rebuild state. The marker lied — not because it
was written wrongly, but because it went stale the moment new input landed.

This cannot be patched inside the model: retracting the marker on input
arrival is itself a system commit racing the consumers it guards. Marker
truth is checked at *planning* time; the hazard arises from *commits* later
in the same generation. Any gate made of data has that one-generation blind
spot. Ordering within a generation is what phases express: `Phase::Complete`
is a *position in execution*, not a claim in data — a position cannot go
stale.

## Epochs

An **epoch** is one settle run against a frozen input set:

- External inputs arriving while a settle is actively driving are deferred
  to the *next* epoch instead of being drained into the next generation.
- Mid-epoch generations (hook reopenings, derived-work follow-ups) drain no
  external inputs.
- When the settle returns, deferred inputs promote to pending and the next
  epoch begins.

The single-flight runner already provides the boundary — the settle loop
*is* the epoch; today it just does not defend its input set.

### What epochs fix

- **Layer 2 dies by construction.** The marker generation can no longer
  contain fresh edits, so nothing re-dirties the marker's stage within its
  epoch. Markers become honest state machines over frozen inputs.
- **Layer 1 dies too.** A request is present from the start of its own
  epoch, and every epoch runs its full hooks cycle (marker re-emit → gated
  generation → cleanup) before declaring itself settled. The re-emitted
  marker is a new entity, so every waiting request's gated row becomes
  plannable in that cycle regardless of interleaving. There is no window.
- **`settled_revision` accounting becomes sound.** Deferred inputs do not
  contaminate the current epoch's revision, closing the class of
  sync-poisoning races between concurrent settles.

### Caveats

1. **External `Mut` immediacy.** `with_latest` currently mutates the live
   world as soon as the cell lock is free — a mid-settle write by design.
   Under epochs it should block until the epoch boundary: "live" mutation
   that can corrupt in-flight derivations is not a feature. This changes
   external `Mut` latency under load, not its API.
2. **`MutRef` systems writing upstream inputs.** Epochs freeze *external*
   inputs only. A system that mutates the inputs of a stage whose marker
   gates other systems (the playground's `touch_file_text_once` stress
   contrivance does exactly this to `FileText`) re-creates the lie within
   the epoch. This is a design rule, and eventually lintable: a `MutRef<T>`
   write edge upstream of an emitted marker is statically visible in the
   access sets.
3. **Open-ended evaluation.** A bowl running intentionally non-settling
   systems never ends its epoch, so writers would starve. Epochs assume
   settles terminate (`CommitLimit` guards accidents).

### Consequences for ordering patterns

With epochs, markers are sound for cross-settle signaling, so marker-gating
becomes a legitimate alternative to phase-ordering again — the choice is
taste plus latency (phases answer within the request's own generation;
markers cost one reopen cycle per epoch). The ordering hierarchy, strongest
to weakest:

1. **Tracked reads (joins)** — dissolve ordering entirely: a late-landing
   fact invalidates the pair and reruns it. Correct in every interleaving,
   no gate, no phase. Where consumers should head long-term.
2. **Phases** — within-generation ordering for ambient reads. "Runs after
   this generation's Evaluate converged" is exactly and only what a phase
   can say.
3. **Markers** — cross-settle state machines: "stage X has completed at
   least once", warm-up signals, coordination of expensive stages. They
   scale to arbitrary stage DAGs (phases never will — there are only ~3
   slots), but they order *settles*, not *reads*.

## Preemption

Without preemption, epochs mean "complete on stale inputs, then catch up":
a burst of edits waits for the in-flight epoch, whose derivations for the
edited files are wasted effort. Bounded waste, maximal simplicity — right
for a batch compiler, wrong for an interactive LSP.

**Cancellation in this engine is not "start from scratch."** Commits are
streamed and memoized as they land (see `EvaluationGuard`: an abandoned
driver loses only in-flight work). Preempting an epoch keeps every finished
invocation; the restart's replanning memo-skips everything untouched by the
new edit, and the fingerprint cutoff reruns only work genuinely derived
from the stale inputs — exactly the work that became garbage when the edit
landed. Known caveat, shared with driver cancellation: an in-flight
`MutRef` invocation aborted mid-write leaves its partial, non-revocable
write and reruns; such systems must tolerate rerunning over their own
partial writes.

The real cost of preemption is the global settle barrier: under continuous
preemption "settled" never arrives, and scoops/takes wait. Three
composable valves:

1. **Input classes.** Inputs declare their class at insert: *preemptive*
   (file edits — abort in-flight waves, merge, restart the epoch) versus
   *cooperative* (requests — defer to the next epoch, never cancel). Maps
   one-to-one onto LSP semantics: `didChange` preempts, `hover` queues.
   Side effect: requests arriving during an edit burst batch into the epoch
   containing the latest edits — read-your-writes for free.
2. **Preemption budget.** An epoch may be preempted at most K consecutive
   times (or only within its first N milliseconds); past the budget, even
   preemptive inputs queue. Guarantees forward progress: the epoch
   completes and the next one takes the accumulated burst as a single
   batch — what batching would have done anyway, just later.
3. **Stale reads.** The settled-snapshot cache already exists; a scoop
   variant reading the *last settled* state without waiting lets
   latency-tolerant readers opt out of the barrier. Hover wants fresh; a
   status bar does not.

Mechanics: a preemptive write raises a flag; the phase runner checks it
between waves, stops planning, commits whatever has finished, re-queues the
generation, and merges the new inputs — a tamer, deliberate version of the
driver-drop path `EvaluationGuard` already handles.

### Write classes and API

The defaults are not heuristics; they fall out of the model. Preemption is
only valuable when a write invalidates in-flight work:

- **External `Mut`/`Cow` mutate existing tracked facts** — every in-flight
  derivation downstream became garbage the moment the write conceptually
  happened; finishing it is waste. Preemptive by default.
- **`insert` creates new entities** — nothing in flight depends on facts
  that did not exist; no running work is invalidated. Cooperative
  (epoch-deferred) by default.
- **`Commands` and `MutRef` have no knob** — they are the epoch's own
  output and scheduler-granted access inside it, inherently epoch-bound.

One modifier each way:

```rust
// Mutation: preemptive by default — cancel → write → continue.
bowl.scoop::<Query<(Entity, Mut<FileText>), Where<Eq<FilePath>>>>()
    .args(path)
    // .deferred()      // opt-out: apply at the natural epoch boundary
    .await;

// Insert: cooperative by default — joins the next epoch.
bowl.insert((HoverRequest, path, position)).await;
bowl.insert((FilePath(p), FileText(t)))
    .preempting()       // opt-in: force the boundary now
    .await;
```

Today's third mode — `Mut` applying mid-flight into a running evaluation —
is removed; it is the Layer 2 lie source.

Semantic details:

- **Tiered preemption.** On preempt, the runner stops planning and
  immediately *drops* every in-flight invocation that holds only read
  access — safe by construction: buffered `Commands` vanish unapplied,
  guards release on drop, and streaming commits mean finished work is
  already durable (the `EvaluationGuard` driver-drop path proves the
  wholesale case). Invocations holding write access (`MutRef`) are the one
  sharp edge: dropped mid-mutation they leave a half-applied, non-revocable
  write with no revision reconciliation (invisible to memo cutoffs) and a
  rerun that re-applies over the partial state. The runner knows
  write-holders from the planned access sets, so it drains those few to
  completion (their commits stale-discard where the edit invalidated them,
  and even discarded commits reconcile written rows). Hard-dropping writers
  too remains an option — reconcile their rows on drop and lean on the
  documented rule that `MutRef` systems tolerate rerunning over their own
  partial writes — but draining them is cheap because writers are rare and
  local.
- **Preemption retracts settle-scoped claims — via a phase slot, not a new
  primitive.** A marker emitted mid-epoch says "stage X is settled with
  respect to this epoch's inputs"; preemption changes the inputs, so the
  claim must be withdrawn or the restarted epoch runs fresh derivations
  concurrently with gated consumers — the Layer 2 lie reintroduced by the
  preemption itself. The layering rule: the engine owns *positions*,
  userland owns *meanings*. `Phase::Startup` becomes the
  epoch-initialization slot — it already runs before Evaluate in the first
  generation; it additionally runs at the head of a preempt-restarted
  epoch ("this generation begins with possibly-stale residue"). What is
  ephemeral and what to retract stays entirely in userland: the existing
  `Ephemeral` marker and cleanup system, registered for both boundaries —
  `run_during(Phase::Cleanup)` (settle end: re-arm) and
  `run_during(Phase::Startup)` (restart: retract). Atomicity falls out of
  phase sequencing: Startup converges before Evaluate plans, so markers
  are gone before any gated consumer can race the fresh derivations.
  Removal purges gated rows' memo entries (entity-keyed) and re-arms the
  emitting hooks (`!has_derived_owned`). Registration granularity replaces
  type attributes for every edge case: `cleanup_stale_derived` registers
  for Cleanup only — running it at restart would not be *wrong* (same
  fixpoint), but it severs the continuity that makes preemption cheap:
  removal deletes the entries, so re-derived identical facts get fresh
  revisions (no previous entry to fingerprint-match), entity removal
  purges the memo entries keyed on them, and spawn slots reset — the whole
  derived subtree under the edited sources rebuilds and cascades instead
  of diff-replacing in place. The deeper principle: at restart time,
  "stale" is indistinguishable from "about to be diff-replaced by a
  rerunning producer"; orphanhood only becomes decidable at convergence,
  which is why Cleanup's home is the settle boundary — the restarted
  epoch's own settle reaps true orphans a moment later, invisibly
  (mid-epoch state is not externally observable under epochs). A hook's
  durable outputs have no cleanup registered and survive preemption. The "which claims retract" decision is visible in
  the registration function, not hidden in provenance. Discipline cost:
  forgetting the Startup registration lets a marker lie under preemption —
  the same class of pattern discipline as forgetting the Cleanup
  registration today, and a candidate for a `run_during_any` ergonomic
  helper.
- **Cost of dropping.** Read-only invocations whose deps the edit did not
  touch lose partial progress and rerun — noise at micro-task granularity,
  bounded by the preemption budget if systems grow long-running. Dropping
  works because invocations are unspawned futures in the driver's
  `FuturesUnordered`; a future thread-pool executor needs abort handles to
  keep these semantics.
- **Burst coalescing.** The preempt flag is level-triggered: while a
  preemption is pending, further writes of either class join the boundary
  batch. A burst of keystroke deltas costs one cancellation and one
  restart with the whole burst merged.
- **Budget degradation is transparent.** Past the preemption budget,
  `preempting` degrades to `deferred` — same outcome, later boundary,
  never an error.
- **`await` semantics.** A mutation returns once its write is applied at
  the boundary, not after re-settling. Readers keep their contract:
  `scoop`/`take` wait for settled, with the stale-read scoop as the
  opt-out.

Precedent: rust-analyzer/Salsa cancels all in-flight queries on every
write (unwind at query boundaries, memo survives, readers retry on the new
revision) and considers not-cancelling untenable at scale. Porridge's
streaming commits are better positioned still, since partial progress
within a settle is already durable.

## Summary: the two fact lifecycles

The preemption rules reduce to one asymmetry, worth internalizing before
anything else in this document:

> A claim's *presence* is its meaning — retract it the moment it might be
> false. A derived fact's *content* is its meaning — replace it when its
> producer gets there, and let settle collect what no producer wants.

- **Ephemeral facts (claims, markers)** are cleaned at both boundaries —
  `Phase::Cleanup` at settle (re-arm) and `Phase::Startup` on
  preempt-restart (retract). This is correctness, not a trade: a stale
  claim causes wrong reads through the systems gated on it. It is also
  nearly free: markers are few, and their removal only invalidates gate
  rows that must rerun anyway.
- **Stale derived facts (diagnostics, indexes, summaries)** are *not*
  cleaned on restart. They linger through the restarted epoch — bounded,
  never across epochs — where most are diff-replaced in place by their
  rerunning producers, preserving revisions, entity ids, and memo keys
  (the fingerprint cutoff keeps working; that is where preemption's
  cheapness comes from). True orphans are reaped by the epoch's own
  settle-time Cleanup, the earliest point at which orphanhood is
  decidable.

## Implementation order

1. `Phase::Startup` as the epoch-initialization slot: runs on first-ever
   generation (today's behavior) and at the head of a preempt-restarted
   epoch. Userland cleanup systems opt in via a second registration.
2. Epoch input freezing: tag inserts with the epoch-after while a settle is
   actively driving; mid-epoch generations drain nothing external; promote
   deferred inputs when the epoch ends.
3. External `Mut`/`Cow` deferral to the epoch boundary.
4. Preemptive input class + tiered wave-boundary abort in the phase runner,
   restarting through the Startup slot.
5. Preemption budget.
6. Stale-read scoop variant (`last settled` snapshot).
