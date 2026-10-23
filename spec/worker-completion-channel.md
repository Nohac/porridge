# Batch worker completion channel experiment

Status: planned O7a experiment after completion-scratch measurement.

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
