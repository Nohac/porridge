# Purchase engine comparison scratchpad

An isolated reference workload for Porridge and Bevy ECS 0.20.0. It has its own
workspace, lockfile and target directory; the main workspace has no Bevy dependency.

## Compared contract

Both adapters accept the same `Purchase` and return the same `Response` types from
the existing purchase fixture. A request validates wallet and inventory, then
updates balance, units and sequence together or rejects without a partial update.
Requests sharing an owner arrive in reverse sequence order. Missing predecessors
remain pending, replays are rejected, and completed request entities are retired.

Porridge directly reuses `crates/benches/src/stateful/purchases.rs`. The Bevy
adapter uses ordinary queries, deferred commands, and a multithreaded schedule:

```text
wallet validation ─────┐
                      ├─ apply deferred commands -> guarded state update
inventory validation ─┘
                         -> collect responses -> despawn completed requests
```

The two adapters share request/inventory/wallet data types. Their small arithmetic
rules are expressed in each engine's systems and checked against independently
expected balances, inventory and responses. Optional synthetic compute uses the
same arithmetic loop. This is an engine workload reference, not a complete server
or a claim that both engines provide identical features.

## Differences to keep visible

| Porridge | Bevy reference |
| --- | --- |
| Per-row async invocations | Two collection-oriented validators and one apply system |
| Incremental dependencies, memoization, output ownership and retraction | Explicit schedule ordering and request cleanup |
| Hashed owner-key joins | Dense owner-to-entity lookup resource |
| Row-level access reservations | System-level component access scheduling |
| Automatic evaluation to a fixed point | Repeat the purchase schedule while requests complete |
| Completion reruns validators to retract their facts | Despawning requests removes their validation components |

The Bevy validators can run concurrently, but their inner loops are sequential;
the apply system is sequential too. There is no manually injected task-per-row
cost and no `par_iter` optimization yet. Additional workers therefore do not imply
linear Bevy scaling. Likewise, this workload does not exercise suspended I/O,
concurrent external observers, preemption, replication or persistence.

## Timing

Each observation starts with fresh, settled/initialized accounts and empty request
history. Timing includes request insertion, evaluation, applying state, copying
responses and retiring requests. Engine construction, account setup, schedule
initialization, runtime creation, final destruction and correctness assertions are
outside timing. `requests/s` means completed request throughput for this valid
fixture, not systems/s or network throughput. Latency is whole-batch time.

Use the same executable for both adapters. The isolated lockfile is independent
of the main benchmark lockfile, so do not compare its absolute timings to old main
workspace runs as if they were identical builds. This comparison includes the
current batched-retirement and concrete-future implementation of Porridge.

`PURCHASE_WORKERS` counts worker threads, excluding the calling/driver thread.
Bevy's compute pool is process-global: **run each worker count in a fresh process**.
The initializer rejects attempts to change its size within one process. Use one
backend per process for measurements and alternate their order across repetitions.

## Commands

```sh
cargo test --manifest-path experiments/purchase-comparison/Cargo.toml \
  --workspace --all-targets --locked -j 20

cargo bench --manifest-path experiments/purchase-comparison/Cargo.toml \
  --workspace --all-targets --locked -j 20 -- --test

PURCHASE_BACKEND=porridge PURCHASE_WORKERS=8 PURCHASE_REQUESTS=1000 \
  cargo bench --manifest-path experiments/purchase-comparison/Cargo.toml \
  --locked --bench purchases -- --warm-up-time 0.2 --measurement-time 1 --noplot

# Same configuration with Bevy, in a separate process.
PURCHASE_BACKEND=bevy PURCHASE_WORKERS=8 PURCHASE_REQUESTS=1000 \
  cargo bench --manifest-path experiments/purchase-comparison/Cargo.toml \
  --locked --bench purchases -- --warm-up-time 0.2 --measurement-time 1 --noplot
```

Runtime settings (no feature changes/recompilation):

| Variable | Default | Meaning |
| --- | --- | --- |
| `PURCHASE_BACKEND` | `both` | `porridge`, `bevy`, or both sequentially for convenience |
| `PURCHASE_WORKERS` | `1` | Worker pool size; one value per process |
| `PURCHASE_REQUESTS` | `1000` | Requests in a batch |
| `PURCHASE_PER_OWNER` | `1` | Requests sharing each owner; try `4` for contention |
| `PURCHASE_WORK` | `0` | Synthetic arithmetic iterations per validation |

Counts must be positive; work can be zero and must fit `u32`. No artificial input
limits. The local workstation can reveal a large gap, but small percentage targets
need the controlled-host policy in `spec/stateful-performance-plan.md`.

## Next comparisons

- [x] Record light independent work at 1/10/100/1,000 requests and 1/8 workers.
- [ ] Add contended and CPU-heavier cases after establishing the initial reference.
- [ ] Consider a Bevy row-parallel variant separately, preserving the same rules.
- [ ] Repeat on a quiet host before treating an exact ratio as an optimization goal.

## Initial reference (2026-10-21)

Completed requests/s, derived from the median of two per-process batch-time
estimates. Same executable, fresh processes, alternating engine order. Warmup
0.1 seconds, requested measurement 0.4 seconds, 10 Criterion samples. This is an
active workstation; exact ratios are provisional.

| Requests/batch | Workers | Porridge req/s | Bevy req/s | Bevy/Porridge |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 13,057 | 48,236 | 3.7x |
| 1 | 8 | 14,618 | 45,771 | 3.1x |
| 10 | 1 | 29,111 | 367,220 | 12.6x |
| 10 | 8 | 25,424 | 297,308 | 11.7x |
| 100 | 1 | 20,604 | 1,599,319 | 77.6x |
| 100 | 8 | 19,249 | 1,477,707 | 76.8x |
| 1,000 | 1 | 12,126 | 2,119,869 | 174.8x |
| 1,000 | 8 | 7,275 | 2,659,713 | 365.6x |

At 1,000 requests/one worker, individual runs were 11.9–12.4k requests/s for
Porridge and 1.96–2.30M for Bevy. With eight workers, Porridge was 7.1–7.5k and
Bevy 2.20–3.36M. The large workload gap is clear even though the exact ratio is
not stable. It motivates amortizing invocation scheduling and bookkeeping over
batches and examining storage/query locality. It does not isolate a single cause
or rank the engines across workloads and features.

This is a reference to approach for this particular observable contract. Bevy
executes three collection-oriented systems and explicit cleanup; Porridge also
maintains incremental dependencies, memoization and derived-output lifetimes.
The initial comparison includes no contended or heavy-compute timing cases.

[Raw per-run estimates, commands, versions and hashes](results/2026-10-21.json)
are archived. Logs/individual Criterion samples are under
`target/purchase-reference/` at the repository root. The isolated lockfile uses
Bevy ECS 0.20.0, Tokio 1.54.0 and Criterion 0.5.1; both backends share that build.
Main-workspace dependency files are unchanged.

Validation: isolated normal build, all-target tests, optimized benchmark smoke
and Clippy pass (dependency warnings remain). Contract tests include a separate
1,000-request check confirming every response and expected final state; there
are no correctness assertions in timed iterations. The first compile used an
older Bevy executor API and was corrected to 0.20's `MultiThreadedExecutor`.
