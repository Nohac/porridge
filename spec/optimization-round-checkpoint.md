# Optimization round checkpoint

2026-10-24. Paused at the user's request; the full O0–O11 campaign remains open.

## Measured endpoint

The stable eight-worker fixture handles 1,000 requests per complete iteration.
Each request causes five kernel invocations in this fixture.

| Metric | Planning-arena baseline | Current retained implementation |
| --- | ---: | ---: |
| Allocations/iteration | 156,815 | 40,227 |
| Reallocations/iteration | 2,124 | 1,124 |
| Total allocator events/iteration | 158,939 | 41,351 |
| Events/request | 158.939 | 41.351 |

Allocation calls are about 74.3% lower than that documented historical baseline.
This is substantial progress, **not near-zero allocation**. Per-change reports
contain matched binaries/features, timings, work counts and memory costs. Do not
infer an exact cumulative speedup from timing samples taken at different times on
this busy workstation. Tiny workloads still do not scale linearly with workers.

## Retained work

- Borrowed/prepared join candidates and reused row enumeration buffers.
- Inline short invocation keys; redundant component origin removed.
- Reused guard capacity and uniquely owned command producers, with retained-handle
  isolation and documented memory costs.
- Moved access metadata into reservations instead of cloning it.
- One immutable plan context shared by row frames.
- Block-based worker completion transport, with receiver-only budget bypass to
  preserve full ready-drain visibility. The 128/130 regression documents why.
- Fixed-capacity future storage for known multi-row worker batches.

The final change passes 169 engine tests, workspace build/playground, four optimized
benchmark smoke suites and eight idle/read allocation budgets. The persistent
benchmark agent uses gpt-6.1-sol at low reasoning effort; no separate fast toggle is
exposed. Benchmarking and compilation were kept separate. Multi-worker throughput
and scaling take priority, with single-worker measurements used diagnostically.

## Measured but not retained

- Cross-generation completion-vector retention: 32 fewer events per 1,000 requests,
  4–5% more retained heap, no demonstrated throughput benefit.
- Simple LIFO dependency-buffer pool: about 5,000 fewer allocations, 22–23% more retained
  heap and no established multi-worker benefit. Its patch/report are archived.
- Initial Tokio completion-channel substitution:the cooperative receive budget
  hid queued completions, causing many more replans/batches and major slowdown.
  The corrected receiver polling is the retained version.

## Next starting points

1. Measure an outer reusable stream merger against SelectAll, which rebuilds a
   future node for each yielded completion. This is separate from the retained
   inner worker collection. Keep streaming, cancellation and ready-drain behavior.
2. Address per-row output/memo bookkeeping without another oversized retained
   free list. Preserve original observations and custom-parameter fallback.
3. Continue schema-driven consumer routing/planning, native synchronous kernels,
   per-system grain policies and the other open roadmap items. These are not done.
4. Finish broad budgets/scaling comparisons, hardware profiling where available,
   and the deferred formatting/style/documentation cleanup before declaring the
   campaign complete.

The scoped target cleanup removed about 85 GiB of obsolete debug artifacts. Current
build/test paths were verified afterwards; release binaries and benchmark evidence
were preserved. Subsequent dependency builds may grow target again. Existing
untracked .claude/ and CHANGELOG.md were left untouched.

[Campaign roadmap](optimization-roadmap.md) ·
[Final batch-future comparison](benchmarks/2026-10-24-batch-futures.json)
