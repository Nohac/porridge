# Typed-return kernels

Status: proposal captured 2026-10-24; not implemented or selected as the next
optimization. See [scheduler investigation](scheduler-investigation.md) and
[warmed purchase profile](warmed-purchase-profile.md).

## Intent

Support kernels with read-only engine inputs and typed returned outputs, alongside
existing synchronous and asynchronous command-based kernels. Preserve ordinary
`.system(function)` registration; infer the adapter from the signature instead of
requiring another public wrapper.

Execution and output capability are separate axes:

| Initial form | Engine access | Output | Execution |
| --- | --- | --- | --- |
| Async with commands | Existing supported parameters | Buffered commands / existing in-place mutation | Independent futures can suspend and progress |
| Sync with commands | Existing supported parameters | Buffered commands / existing in-place mutation | Direct calls inside worker batches |
| Sync with typed return | Read-only allowed inputs | Explicit typed result | Direct calls with specialized result storage |

The first experiment may support only synchronous returns. That does not make
async typed returns conceptually invalid; avoid an architecture that couples
output representation permanently to sync versus async execution.

## Meaning of side-effect free

The enforceable contract is **read-only engine access during evaluation**:

- No `Commands` or `MutRef`, direct Bowl/world writer, or equivalent write capability.
- Initially admit only known read-only parameter types. Unknown custom parameters
  must not silently enter the restricted path.
- Tracked inputs are suitable for deterministic derivation. Ambient `View` inputs
  require retaining their existing invalidation/visibility semantics; merely
  returning a value does not make an ambient read tracked.
- Rust does not generally prove that arbitrary function bodies avoid global
  mutation, I/O, clocks, randomness or interior mutability. Stronger purity and
  deterministic-retry guarantees remain an author contract unless the API
  introduces further restrictions.

Do not advertise arbitrary Rust functions as compiler-proven mathematically pure.

## Return contracts

These names illustrate possible contracts, not implemented API types:

```text
fn derive(read_only_inputs) -> Derived<Debit>
fn replace(read_only_inputs) -> Replace<Wallet>
```

| Contract | Ownership and publication |
| --- | --- |
| Derived output | Invocation owns stable output slots. Rerun replaces outputs, equal-value cutoff remains, omitted output retracts prior output. |
| Targeted state replacement | Result identifies an existing target and expected input state/dependencies. Publication validates and replaces it; target is not accidentally adopted as a disposable derived artifact. |

Start by specifying derived outputs. Targeted replacements need an explicit
conflict and ownership contract before implementation. Two computations can read
the same old wallet and return competing replacements: return syntax alone does
not serialize them or make both valid. Stale-result handling and any retry must
respect the declared purity contract.

Output types contribute schema/output declarations automatically where possible.
Preserve strict shape conformance and an explicit target when it cannot be inferred
unambiguously. Do not infer a target from the first entity in an arbitrary join.
Define tuple/optional/multiple output and error semantics deliberately: `None`
meaning retract differs from retaining an old result, and error is not an implicit
request to perform a partial publication.

## Runtime shape

```text
prepare tracked read inputs
  -> run plain function
  -> store typed result in owned batch/result storage
  -> validate dependencies and target
  -> publish through a specialized typed path
  -> update revisions, output ownership and downstream activation
```

Commands already defer writes. Returning values only buys a faster path if it
actually avoids generic command infrastructure. A proposed specialized path can
skip command-producer setup, enqueue operations, generic command dispatch and
arbitrary spawn bookkeeping when the output layout is known. Do not lower every
returned value into the old command buffer and claim those savings.

Result types can provide monomorphized publication functions with erasure at the
batch boundary, rather than a separate allocated command per value. Typed arrays
or reusable output storage are options; changing storage is not required to test
the minimal return path. Dependency checks, publication ownership, retractions,
revisions and quiescence still apply.

## Ordering and parallelism

A synchronous batch runs calls sequentially. Different batches on different
workers can finish out of order; no global finish-order guarantee is implied.
Keep prompt publication of ready results even when an unrelated async sibling
is suspended. Running pure synchronous work locally for very small batches is a
separate execution-policy experiment.

Returned data is not automatically visible engine state. Dependent kernels must
observe validated publication. Fusing producers and consumers through private
intermediate results is a further optimization requiring a proven dependency,
visibility and retraction contract. Do not silently fuse arbitrary functions
because their input/output component types overlap.

Read-only execution makes speculative discard easier than in-place mutation, but
it does not remove world lifetime protection or externally held query guards.
Unsafe typed result storage would require explicit initialization/drop and
reclamation invariants; it is not needed to establish the basic API.

## Minimal experiment and evidence

- [ ] Specify derived-output slots, target mapping and omitted/error outcomes.
- [ ] Add signature-inferred sync returns with restricted read-only parameters.
- [ ] Execute a return value without creating a command producer/queue for that row.
- [ ] Validate replacement/retraction, equal-output convergence, stale results,
  cancellation/panic drops, and mixed async/return-kernel progress.
- [ ] Compare the same derivation as commands versus typed returns in one binary
  and feature graph, at 1/4/8/16 workers; retain identical output lifecycle semantics.
- [ ] Measure allocations, cycles/request, publication cost, retained storage and
  throughput. Keep fixtures with cheap and expensive outputs and overlapping reads.

The five-kernel purchase path includes output cleanup. An experiment that omits
that lifecycle work is a different workload, not a valid return-path speedup.
This idea complements compact invocation IDs and consumer routing; it does not
by itself eliminate their bookkeeping.
