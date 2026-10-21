# Schema-guided execution, affinity and clustering

Status: exploratory direction, not an implemented API or a commitment to replace
local Bowl semantics. Immediate work remains in the
[stateful performance plan](../stateful-performance-plan.md).

## Objective

Minimize overhead per completed operation, including tiny kernels. Exploit known
schema and system declarations before adding scheduler complexity. Measure
throughput, latency and memory; saturating every worker is not itself success.
Small workloads can be cheaper to execute directly. GPU-style batching and
locality are useful inspirations, not promises of universal parallel speedup.

```text
construction:
  schemas + system declarations
    -> dense component IDs, query/access plans, consumer indexes

execution:
  publish changes -> route affected work -> ready batches -> kernels
                          ^                                 |
                          +---------- publication ----------+
```

## What can be prepared ahead of time?

| Prepare at construction | Resolve at runtime |
| --- | --- |
| Dense component IDs and indexed metadata | Actual entities and component presence |
| Required/optional/filter masks where representable | Value revisions and memo validity |
| Component-to-consumer reverse indexes | Rows/pairs affected by a change |
| Potential read/write conflicts between kernels | Entity overlap and actual access reservations |
| Known join providers and candidate strategies | Join membership and key values |
| Inline type size/alignment, with suitable metadata | Heap payload sizes, cardinality and work cost |

The schema alone does not describe arbitrary computation, hidden I/O or captured
external state. System declarations contribute access information. Feedback
cycles remain valid incremental computations; a static interest graph is not
necessarily a DAG or a complete execution order. Preserve explicit phases,
tracked dependencies, stale-output rejection and output retraction.

Possible later compute specialization: synchronous kernels process batches inside
one task; async kernels can suspend and complete independently. Share dependency,
access and publication semantics. Do not require a batch to finish before exposing
an independently completed row. Packed component storage and SIMD need separate
measurement of gathering/scattering and locality; the current stores are not
contiguous component columns.

## Distributed scheduling within one process

Distinguish three things:

```text
logical affinity key -> ownership partition -> preferred worker
                                             -> ready execution batches
```

- Affinity expresses locality. It is not a thread ID, a lock or an ordering rule.
- A partition owns some authoritative state and its bookkeeping. An initial
  queue-partitioning experiment may still share existing storage.
- A worker executes runnable batches. Moving execution is different from moving
  ownership; stolen work may access remote caches or still need owner publication.

Eventually, partitions could own indexes, pending work, revisions and publication
as well as data. Merely distributing ready queues leaves central planning and
publication bottlenecks intact. Partition-local revisions would also require an
explicit replacement/mapping for existing global revision cursors.

Prefer placement by a relationship root or explicit affinity key over spreading
individual components randomly. Keep an entity's frequently co-accessed state
together. Consider more logical partitions than workers to allow rebalancing;
measure metadata and coordination overhead before choosing granularity.

```text
workspace A -> partition 7 -> worker 2
  documents, requests, diagnostics

room instance B -> partition 9 -> worker 4
  participants, inventories, pending exchanges
```

Room identity must distinguish separate instances. An exchange permitted only
within a room is usually local, but affinity does not validate that rule.
Disjoint invocations in the same group may still run concurrently if reservations
and publication support it. Cross-group joins and multi-owner mutations need a
specified coordination path, not implicit unsynchronized access.

### Ownership changes

Illustrative handoff constraints, not a selected protocol:

```text
request move -> fence old ownership -> finish/cancel/admit pending work by policy
             -> transfer state and required bookkeeping -> install new owner
             -> redirect subsequent requests
```

Define handling for in-flight kernels, guards, memo/derived output ownership,
request order, outstanding responses and external handles. At most one owner may
publish authoritative mutations. Migration should not silently change entity
identity. Some workloads may be better served by moving request execution while
leaving persistent state in its original partition; compare both designs.

### First local partition experiment

After the measured local optimizations:

- [ ] Partition ready-work queues while preserving existing storage semantics;
  measure how much driver cost remains in planning and publication.
- [ ] Compare independent account operations and two-account operations.
- [ ] Vary cross-partition interaction rate, partition size, worker count, skew,
  and tiny/heavy kernel work. Include one hot group among mostly idle groups.
- [ ] Compare preferred-worker execution with work stealing. Verify reservations,
  cancellation, fairness and consistent observations.
- [ ] Define quiescence including queued messages, running work and unpublished
  completions. An empty local queue is insufficient for a settled result.
- [ ] Only then decide whether independent ownership/publication partitions are
  justified. Do not add network concepts to the ordinary local hot path yet.

## Possible network clustering

```text
local:    affinity -> partition -> worker
cluster:  affinity -> partition -> authoritative node -> worker
```

Starting hypothesis: one authoritative node per partition, ordinary local
execution within each node, explicit cross-partition messages. Keep remote reads
and joins explicit rather than making every component access a potential RPC.

| Contract | Needed before a clustering prototype can claim it |
| --- | --- |
| Transferable facts | Opt-in serialization, stable IDs and schema/version compatibility |
| Executable kernels | Compatible registered implementations; arbitrary Rust closures are not network messages |
| Authoritative ownership | Directory/routing plus ownership fencing; failure must not create two writers |
| Retried requests | Request IDs, deduplication and defined response retention; timeout is not proof of non-execution |
| Migration/failure | State transfer, handoff recovery and an explicit durability/recovery policy |
| Cross-owner mutation | Chosen coordination and visibility guarantees; local guards are insufficient |
| Settlement | Define the queried scope and account for in-flight messages and failures |

Partition-local settlement is a candidate default for a future clustered API.
Broader settled results need explicit coordination. This is not a change to the
current whole-Bowl settlement contract. Durable recovery and replication are
additional mechanisms; networking alone provides neither. See
[replication ideas](replication.md) for fact transfer and removal tracking.

- [ ] Specify identities, serialization, compatibility and failure semantics.
- [ ] Prototype two nodes with explicit routed requests and partition-local reads.
- [ ] Test duplicate delivery, delayed replies, disconnection during handoff,
  stale owners and restart recovery before claiming transparent migration.
- [ ] Decide whether a cluster coordinator belongs above Bowl or inside a new
  runtime layer. No transport or consensus protocol is selected here.

## What to preserve now

Keep schema metadata, ready-work routing, access reservations, publication and
settlement responsibilities explicit. Avoid exposing OS thread IDs as entity
identity or affinity. Introduce cross-partition ownership APIs only with a tested
prototype; local optimizations need not pay distributed-system overhead in advance.
