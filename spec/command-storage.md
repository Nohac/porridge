# Command storage comparison

Status: the comparison is complete. The user selected bump-scope for production
integration; see [the integration record](bump-command-integration.md). The
comparison below retains its original boxed/arena/packed measurements.

## Implementations

The shared experiment in `crates/benches/examples/command_storage/` compares:

| Variant | Storage | Cleanup |
| --- | --- | --- |
| Boxed | Retained Vec of separately boxed commands | Ordinary Box/Vec drops |
| bump-scope | Owning arena plus retained pointer/consumer records | Consume or drop each payload before resetting arena |
| Packed | Retained byte Vec containing metadata and each payload | Unaligned move into an owned local, then apply/drop |

The packed implementation follows [Bevy's command queue design](https://docs.rs/bevy_ecs/0.19.1/src/bevy_ecs/world/command_queue.rs.html).
It is not a benchmark of Bevy itself: this prototype uses a size + consumer
pointer per record and omits recursive world-command flushing. Bevy encodes
cursor advancement in its consumer function instead of storing that size.

[bump-scope](https://docs.rs/bump-scope/2.4.1/bump_scope/) handles aligned chunk
allocation and bulk reuse. Its BumpBox values normally carry the arena lifetime.
Our owning queue turns them into internal raw pointers, pairs each with a typed
consumer, and releases all values before reset. This supports moving the queue
between workers without self-referential borrowed fields. It still requires
unsafe lifetime/drop bookkeeping, although allocation and alignment are delegated
to the library. Both prototypes consume ownership before calling user code,
so unwinding never consumes a command twice.

## Results

Three pinned-process repetitions; Criterion 20 samples, 100 ms warmup and 300 ms
requested measurement per case. Payload construction and application are timed.
Small commands hold one u64; the mixed case replaces every eighth command
(starting at index 1) with a 64-byte-aligned value carrying 512 payload bytes.
Both mix Add and Multiply operations so order matters. Inputs and state cross
black-box barriers. Command bodies do not allocate.

Median nanoseconds per command, including push + apply:

| Commands/batch, workload | Boxed | bump-scope | Packed |
| --- | ---: | ---: | ---: |
| 1, scalar, reused | 6.26 | 4.17 | 3.28 |
| 2, scalar, reused | 6.10 | 3.17 | 2.40 |
| 32, scalar, reused | 12.18 | 2.52 | 2.17 |
| 1,024, scalar, reused | 13.67 | 2.60 | 2.14 |
| 1,024, mixed, reused | 27.60 | 3.64 | 3.88 |
| 2, scalar, fresh | 10.36 | 9.00 | 22.47 |

Both new variants perform **zero global alloc/realloc/dealloc calls** after
warmup for all measured retained-queue cases. The allocation diagnostic asserts
this separately from the uninstrumented timing run. Boxed storage still allocates
and frees once per nonzero-sized command, despite retaining its outer Vec.

Fresh two-command packed queues incur one allocation and one reallocation; boxed
queues incur three allocations, bump-scope two. Fewer calls alone do not predict
latency: packed growth is expensive here. Capacity recycling must accompany an
engine change; fresh tiny queues can regress.

These are local directional measurements on a busy machine. The tiny ns timings
are not end-to-end purchase throughput and include compiler optimization of this
fixture. Variant registration order is fixed. Per-process median ranges and
allocation counters are preserved in the [result artifact](benchmarks/2026-10-22-command-storage.json). This comparison does
not measure concurrent pool contention, engine mutation, snapshots, or planning.

## Verification

Tests cover noncommutative order, repeated reuse, growth with owned resources,
256-byte-aligned values, zero-sized values with Drop, explicit discard, unwinding,
cloned producer handles and batch transfer between threads. Both safety tests
also pass under Miri with the ordinary workspace features (nightly interpreter
only, no feature changes).

Workspace build/tests, example tests, clippy, playground, and all Criterion
smoke suites are part of this slice's verification. Test helpers live in the
example target, not the harness-free Criterion target.

## Original comparison recommendation

Use the **Bevy-style packed approach as the starting point for small engine
commands**, and retain bump-scope as a measured alternative for larger payloads
or temporary planner allocations. The library works; it is not necessary to
write an allocator to use arena storage. It also does not eliminate the need
for owning command-buffer bookkeeping in this architecture.

The subsequent bump-scope integration addresses these requirements:

1. Replace per-command boxes behind a reusable owned queue abstraction, preserving
   typed enqueue APIs and insertion order. Cover base inputs as well as derived
   commands; changing only one leaves the other allocation path intact.
2. Preserve invocation output boundaries, stale-run discard, spawn ownership,
   DerivedFrom flushing, and Settle's selective deferral. Deferred payloads need
   owned storage until their later application; they cannot borrow a reset arena.
3. Return cleared payload buffers to retained engine storage after publication or
   cancellation. Do not reuse storage still reachable through old Commands clones.
   Reusing payload queues does not itself remove the shared producer Arc allocation.
4. Run existing behavior tests and the fixed-world purchase allocation/timing
   benchmark. Isolated queue speedups are not evidence of whole-engine speedups.

## Reproduce

```sh
uv run tools/measure-command-storage --out target/command-storage/new-run
uv run tools/measure-command-storage --out target/command-storage/single --counts 1
```

The runner builds the whole workspace with the normal features, then pins its
measurement child processes to one allowed CPU. Counts are configurable without
an upper limit. It saves build provenance, raw allocation counters and Criterion
estimates. `--repetitions` and `--cpu` override defaults.

```sh
cargo test --workspace --examples -j 20
cargo +nightly miri test --workspace --example command_buffers command_storage_tests
```

## Integration plan (bump-scope selected)

The user selected bump-scope after the comparison. Integrate an owning arena
queue for base and derived commands, preserving typed insertion and commit
boundaries. Keep selective Settle deferral owned, not borrowed from recycled
storage. A driver-owned pool leases payload queues during ordinary planning and
accepts empty arenas back over a return channel; no new shared pool mutex.
Producer handles are never recycled, only detached payload storage, so old
Commands clones cannot mutate a later invocation's queue. Hook fallback paths
may initially use unpooled arenas; record that limitation if retained.

Verify normal/exceptional destruction, alignment/ZSTs, selective deferral,
watermarked input promotion, stale discard, and pool recycling, including Miri.
Then rerun fixed-world allocation/timing comparisons with preserved binaries.
