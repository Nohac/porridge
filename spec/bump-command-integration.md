# Bump-scope command integration

Prior work was committed as `4acd2a4` before integration. The user selected
bump-scope after the packed/arena comparison. This replaces per-command boxes
in the production engine, for both external base inputs and derived outputs.

## Ownership and reuse

`commands/arena.rs` owns each arena and its typed dispatch/drop records. Payloads
are allocated with their natural alignment. A record is marked consumed before
its value is moved out, applied, transferred, or dropped. No payload reference
escapes. Apply/transfer guards discard the remaining tail during unwinding,
including queues retained by State after a failed application.

A queue handle contains an optional boxed storage block and return-channel
sender. The storage box is allocated once per live cached buffer, not once per
command. It keeps the queue handle small inside suspended future frames instead
of embedding the arena and metadata Vec there.

The driver owns `Pool` and leases storage while planning ordinary invocations.
Completed, stale, cancelled, and admission-discarded queues clear their payloads
and send their storage back. The driver collects returned storage on checkout.
This adds no shared mutex around the pool's free list; the return channel still
has synchronization and allocation costs. No capacity ceiling is imposed.
Peak arena and metadata capacity remain cached until the Bowl is dropped.

Planning temporarily moves the pool out of State, so it runs without holding the
state lock. External staging during this window uses an unpooled queue. If the
driver is cancelled while returning the pool, its cache may be dropped; the next
planning pass creates a fresh pool. This affects reuse, not command ownership.

Only detached payload storage is recycled. Each invocation still creates its
own `Arc<Mutex<CommandBuffer>>`. Old Commands clones retain the old producer
handle and cannot write into another invocation's recycled storage. Late writes
through those old handles remain unpublished, as before; this slice does not
introduce a new detached-command submission API.

## Publication and input semantics

- External bundles stage in isolated arena queues before transfer into State's
  retained pending-input queue. A custom Bundle that panics cannot publish a
  partially queued bundle or alter earlier pending inputs.
- Mid-epoch input batches retain their arrival tag. A VecDeque promotes eligible
  batches in arrival order without rebuilding a kept-input Vec each time.
- Invocation publication retains owner boundaries, spawn reservation, stale-run
  rejection, DerivedFrom flushing, and derived-output replacement.
- Settle-phase removals apply immediately. Inserts/spawns move into another owned
  arena queue, paired with the invocation owner, and apply at the next generation
  before external base inputs. Recycling the source cannot invalidate them.
- Ordinary Commands/Bowl insertion APIs are unchanged. The hidden Bundle extension
  seam now accepts `BaseCommands` rather than `Vec<Box<dyn BaseCommandOp>>`, and
  BaseCommandOp consumes `self` rather than `Box<Self>`. These support types are
  re-exported with hidden documentation for custom low-level implementations.

## Verification

Workspace build/tests, example tests, clippy, and playground pass. Eight production
queue regressions also pass under Miri with the same workspace feature set:
selective deferral after source reuse, panic cleanup/recycling, producer-clone
isolation, base append order, shutdown with outstanding queues, ZST destruction,
retained input cleanup after panic, and failure during custom bundle staging.
Those tests also exercise over-aligned payloads, growth, and cross-thread return.
Existing epoch/preemption, Settle, spawn ownership and stale-commit tests pass.

## Remaining allocation work

- Hook/grouped fallback paths still use fresh, unpooled arenas.
- External remove/despawn staging also starts unpooled; normal bundle insertion
  uses the pool when it is available.
- Channels can allocate internal blocks; pool return is not advertised as a
  zero-global-allocation operation.
- Producer Arcs, planner/dependency/access storage, future scheduling, snapshots,
  component cells, and result vectors remain. This is not a zero-allocation engine.
- Retained buffers follow peak demand; there is no automatic trimming policy yet.

## Measurement

Use the fixed-ID purchase benchmark with the same workspace features and bench
profile. Preserve the preceding executable, alternate baseline/candidate process
order, and measure counters separately from uninstrumented wall time. Results
below concern the production engine, unlike the isolated command-queue comparison.

Three alternating process repetitions, ten rounds each, counters separate from
timings; 1,000 fixed request slots:

| Workers | Alloc calls/request, before → after | Wall µs/request, before → after |
| --- | ---: | ---: |
| 1 | 255.159 → 243.636 | 27.69 → 26.88 |
| 8 | 238.622 → 231.815 | 37.55 → 37.97 |

The eight-worker reduction is **6.807 net allocations/request (2.85%)**.
This is not the total command-related allocation count: producer handles still
allocate, and returning buffers can allocate channel blocks. Settled-read counts
are unchanged at four allocations per round. There is no demonstrated purchase
throughput improvement.

The first single-slot/eight-worker comparison reported 125.36 → 244.28 µs.
A longer run reported 130.84 → 127.78 µs, but overlapped a filesystem discovery
scan. A second, clean recheck (20 warmup rounds, 100 measured rounds, five
alternating process repetitions, no overlapping builds/profiling from this task)
reported **120.50 → 117.84 µs**. The initial slowdown did not reproduce.
These are directional workstation results, not proof of a small speedup.
Scheduling changes the number of replanning passes, so allocation counts can
also vary between runs.

### Does the arena stop allocating?

Yes, for the warmed bounded workload measured here. Five measured rounds after
eight warmup rounds, at 1/1,000 slots and 1/8 workers, recorded **zero new command
storage blocks, zero arena chunk growth, and zero metadata Vec growth** in all
four configurations. The 1,000-slot/eight-worker case reused storage 30,001 times.
Those counters do not cover producer Arcs or channel allocations, and do not
promise that workloads with larger payloads or more concurrent batches cannot grow.

### Where the remaining allocations originate

A separate DHAT executable profiles the same fixed-ID purchase path after
warmup. One measured round with 1,000 slots/eight workers produced 255.938
allocation/reallocation calls per request:

| Allocation origin | Calls/request | Share |
| --- | ---: | ---: |
| Planning: access lists | 50.000 | 19.5% |
| Planning: other allocations | 34.386 | 13.4% |
| Planning: dependencies | 32.000 | 12.5% |
| Planning: invocation keys | 30.000 | 11.7% |
| Planning: join candidates | 22.000 | 8.6% |
| Command producer handles | 5.000 | 2.0% |
| Other pipeline work | 82.552 | 32.3% |

**Planning accounts for 65.8%**, not the command payload queue. The one-worker
profile shows the same pattern (64.8% planning). The remaining pipeline category
includes execution/guard/result bookkeeping, commits and storage; it is not
fully subdivided here.

These are disjoint stack-pattern groups implemented in
`tools/profile-allocations`. Full raw stacks remain in the local report.
DHAT counts reallocations too and charges them to the block's original
allocation stack. This differs from the earlier **alloc-only** counter, and
profiling changes scheduling. Optimized symbol stacks may merge implementations
or omit inlined frames; these numbers identify work areas, not exact source-line
costs or CPU-time percentages.

The current allocation pattern is visible in `system.rs`:
`plan_prepared` creates per-row keys, dependencies and access lists;
tuple `SystemParam::keys/access` builds a parent Vec by extending temporary
child Vecs. Join expansion also materializes candidate lists. Warmup cannot
reuse those temporary buffers when each call creates and drops them.

Next planned optimization: pass a caller-owned destination through key/access
collection to avoid nested temporary Vecs, then measure again. Reusing retained
row buffers requires respecting in-flight invocations and memo ownership;
do not simply share one scratch buffer across workers. This follow-up is
**not implemented in this slice**.

### Reproduce

```sh
cargo build --workspace --all-targets --profile bench -j 20
uv run tools/measure-allocations --out target/arena-check --cases purchases --slots 1,1000 --workers 1,8 --warmup 8 --rounds 5 --repetitions 1 --metrics
uv run tools/profile-allocations --out target/heap-profile
```

Metrics run only during counted passes; timing passes keep them disabled.
The profiler uses DHAT only in its separate example executable. Production and
timing executables keep their normal allocator. Both tools pin driver/workers,
record provenance, and refuse to overwrite nonempty result directories.

[Retained results and provenance](benchmarks/2026-10-22-bump-command-integration.json).
Full local reports are under `target/bump-command-integration/`.
