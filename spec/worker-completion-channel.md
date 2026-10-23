# Batch worker completion channel experiment

Status: O7a readiness-preserving variant kept; initial budgeted variant rejected.

## Why this before native synchronous kernels

The O3a diagnostic profile assigns 19.136 allocator events/request (about 37%)
to dispatch/completion. The existing batch executor sends every completed row
through futures-channel's bounded mpsc. Inspection of the pinned 0.3.32 source
(`src/mpsc/queue.rs`, `Node::new`/`Queue::push`) shows a boxed linked-list node per
pushed message. This is separate from the future-set node per async invocation.

Tokio is already the worker runtime. Its pinned 1.52.3 bounded mpsc stores values
in blocks (32 slots on this target) and can reuse released blocks. Try this
existing implementation before building custom completion storage or changing
kernel execution. The same improvement could serve future native sync batches.
This reorders one focused O7 experiment ahead of O6; remaining O2/O3 work stays
tracked, rather than being declared complete.

## Exact scope

- Permanently enable Tokio's `sync` feature in bowl's existing optional runtime
  dependency. Build/freeze the unchanged futures-channel implementation with this
  feature first, then keep exactly that feature graph for all tests and benches.
- Change only multi-row worker batch completion transport: bounded Tokio mpsc,
  `try_send` from the batch task, `poll_recv` from SpawnedBatch.
- Keep capacity equal to admitted rows, one producer, at most one result per row,
  and explicit panic if Full violates that invariant. Closed means stop producing.
- Preserve single-row dispatch, cooperative no-runtime fallback, task abort on
  reader cancellation, writer draining, and worker panic propagation.
- Preserve independent readiness: never wait for a suspended sibling to complete
  before delivering a finished row. Tokio polling may yield on its cooperative
  budget, so record any changed planning/admission work as well as timing.

## Validation and acceptance

Run workspace tests/playground plus dispatch/engine benchmark smokes. Add a full
batch completion-before-receive regression if existing tests do not cover it;
keep existing delayed-sibling, cancellation and worker-panic tests.

Compare feature-matched complete-iteration alloc/realloc events, requested bytes,
1/8-worker timing and (at a broader milestone)4/16-worker scaling. Multi-worker
scaling is primary; single-worker results diagnose tradeoffs. Check tiny batches,
large immediate batches and genuinely suspended/skewed batches. Include heap
plateaus through population growth and attribution showing which channel costs
remain. No zero-allocation or throughput claim solely from implementation choice.

## First candidate: reject, isolate readiness budgeting

The direct `poll_recv` substitution regressed full-engine eight/sixteen-worker
time by about 40–53%, despite favorable isolated dispatch microbenchmarks.
A three-round/eight-worker diagnostic comparison completed the same 15,000 rows,
but plan calls rose **36→498**, dispatch batches **945→4,269**, and already-running
candidate skips **0→1,897**. More partial ready sets cause repeated planning and
small dispatches. Functional tests/budgets still pass; this is a scheduling-cost
regression, not evidence that final answers are wrong.

Pinned Tokio source shows `chan::recv` checks its cooperative task budget before
reading a queued value. An exhausted budget may therefore return Pending while
completed rows are buffered. Our driver treats that boundary as the end of its
ready set, then publishes/replans. The old futures channel did not consume Tokio's
receive budget. This is an interaction with our driver, not a Tokio defect.

Next variant: add a >128-row regression proving buffered results remain visible
through a single ready drain, then wrap **only receiver polling** in a stack-pinned
`tokio::task::unconstrained(poll_fn(...))`. Keep worker-future polling, JoinHandle
polling, cancellation and the driver's existing preemption/fairness logic intact.
Do not use `try_recv` as a shortcut: the pinned implementation parks the thread
when a producer has reserved but not yet published a slot (`TryPopResult::Busy`).

Use the same frozen sync-enabled control and feature graph. Require passing tests
and measure the whole pipeline again; a faster channel microbenchmark is not an
acceptance criterion by itself. Candidate lifetime profiling was stopped after
the clear regression, so there is no matched-memory conclusion for this rejected
variant. [Results and provenance](benchmarks/2026-10-23-completion-channel-budgeted.json).

## Readiness-preserving variant: keep

The regression fails on the first candidate at exactly 128/130 buffered rows and
passes after constraining only the receive-budget bypass. All 169 engine tests,
workspace build/playground, four optimized smokes and eight idle/read budgets pass.

At 1,000 requests/eight workers, allocations fall **50,857→45,227** with 1,124
reallocations unchanged: events **51,981→46,351 (-10.83%)** per whole iteration.
The targeted three-round diagnostic has 945 dispatch batches in both variants,
15,000 admitted/started rows in both, plan calls36→42 and only25 already-running
skips in the candidate. The first candidate's replan explosion is resolved.

Paired steady timings, µs/request (three processes, shared workstation):

| Workers | Control | Candidate |
| --- | ---: | ---: |
| 1 | 22.54 | 21.48 |
| 4 | 21.60 | 21.48 |
| 8 | 26.61 | 25.69 |
| 16 | 27.74 | 26.43 |

All three eight-worker process pairs improve, with overlapping ranges; two of
three sixteen-worker pairs improve. Isolated ready/suspended width16/32/64
comparisons generally favor the candidate, with noisy outliers. Tiny eight-worker
cases show a small noisy slowdown; no universal speedup or linear scaling claim.

Retained heap is effectively unchanged through 1,000→2,000 slot growth. Measured
transient peaks rise **7.70–8.73%**, and requested allocation bytes rise about
**612,120 per iteration**. Block capacity is32 slots on this target while the
steady fixture uses maximum grain16: fewer allocator events do not imply fewer
allocated bytes. Keep for favorable multi-worker evidence and allocator reduction,
with that cost explicit. Grain tuning remains a separate experiment.

The diagnostic allocator profile has47.967 events/request and357 batches instead
of the stable count's315; retain that work-count distinction. Feature-matched
source/binary manifests and all results are in the
[measurement report](benchmarks/2026-10-23-completion-channel-ready.json).

Next: [share immutable invocation context](invocation-context.md), then consider
[batch future storage](batch-future-storage.md) against remaining profile costs.
Native synchronous kernels and the rest of the campaign remain open.
