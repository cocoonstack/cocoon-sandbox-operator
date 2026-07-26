# Scaling design — decentralized sandbox scheduling on Kubernetes semantics

Modal's ["1M concurrent sandboxes"](https://modal.com/blog/scaling-to-1-million-concurrent-sandboxes-in-seconds)
post argues Kubernetes cannot reach that scale: scheduling is `O(n×p)` and
serialized, every Pod causes multiple etcd writes, etcd is not shardable within a
keyspace, and kubelet heartbeats impose an `O(nodes)` write floor. Their answer is
to **leave Kubernetes entirely** — a fleet of stateless schedulers over in-memory
worker state published to a Redis stream, direct scheduler→worker RPC, and *no
datastore on the sandbox-creation critical path*.

Their diagnosis is correct. Their conclusion is not the only option. What Modal
actually removed is a **centralized transaction path**, not API semantics — and
Kubernetes already separates those two things in its own design: kubelet **static
Pods** (the node acts first, the apiserver records after), the **coordination.k8s.io
Lease** (a dedicated tiny object for heartbeats instead of full Node writes), and
the **metrics.k8s.io aggregation layer** (a virtual resource served by
scatter-gathering live node state, with *zero* etcd storage). Our thesis:

> **Keep Kubernetes as the record-of-intent and policy plane; push the
> transaction plane down to the node — behind CRDs, RBAC, and watch, so
> `kubectl get sandboxes` never stops working.**

We stage this as four layers. **L0 is shipped.** L1 is implemented in this repo.
L2/L3 have full designs, Go interface skeletons, and migration paths here;
their complete implementations are follow-up work.

```mermaid
flowchart LR
    subgraph L0["L0 — API hygiene (shipped)"]
        L0a["cache-fed reads<br/>diff-before-write<br/>LIST off etcd"]
    end
    subgraph L1["L1 — ownership transfer (this repo)"]
        L1a["claim = single PATCH<br/>O(nodes) pool status<br/>per-pool sharded operator"]
    end
    subgraph L2["L2 — node-local claim gateway"]
        L2a["DaemonSet → sandboxd<br/>sub-ms delivery<br/>async Bound record"]
    end
    subgraph L3["L3 — aggregated apiserver"]
        L3a["scatter-gather node inventory<br/>etcd stores intent only<br/>O(sandboxes)→O(pools+nodes)"]
    end
    L0 --> L1 --> L2 --> L3
```

### L0 — API hygiene (shipped)

The prerequisite, delivered in the `vk-cocoon` provider (2026-07-17): every
periodic read is served from a node-scoped informer cache, every write is
diffed first, and no control-loop `LIST` hits etcd (list at `ResourceVersion=0`,
or a field-selected node-local lister). This is the qualifier — without it any
scale test wedges the apiserver first. Measured on an idle virtual-kubelet node
afterward: **0.2 req/s, zero LIST** (lease renew + node-status patch only). The
root cause it fixed: Kubernetes APF prices `LIST` seats by the **total object
count** of the resource, so at 2500 pods even a tiny per-node list goes
max-width and saturates a dedicated priority level — client QPS caps cannot fix
a seat-seconds problem, only removing the lists can.

### L1 — claim is ownership transfer, not scheduling (implemented)

A warm claim is not a create. The Pod is already scheduled, bound, image-pulled,
and booted; a `SandboxClaim` only needs to **transfer ownership** of one
pre-warmed `Sandbox` — the exact semantics Kubernetes already ships for
`PersistentVolumeClaim → PersistentVolume` binding (`Phase: Bound`). Nothing on
the claim path needs the scheduler, kubelet bind, or image pull.

**Mechanisms**

1. **Claim fast-path — one select, one PATCH.** Pick one `warm ∧ unclaimed`
   Sandbox via a label index and adopt it with a single optimistic PATCH guarded
   by its `resourceVersion`. On conflict (two claims raced the same Sandbox), the
   loser simply tries the next candidate — no requeue, no exponential backoff, no
   adoption-cache-lag requeue. This collapses the claim to Modal's "two network
   hops and one cheap CPU op," expressed entirely in Kubernetes objects.
2. **Pool status is `O(nodes)`, not `O(sandboxes)`.** `readyReplicas` is
   maintained incrementally from informer add/update/delete events and
   metadata-only reads, so replenishment reconciliation never re-lists the full
   pool's Sandbox specs. A 2500-sandbox pool costs a counter update per event,
   not a 2500-object list per reconcile.
3. **Per-pool sharded operator.** Each `SandboxWarmPool` is an independent
   workqueue shard; operator replicas take a per-shard `coordination.k8s.io`
   Lease and scale horizontally. This is the Kubernetes-native spelling of
   Modal's "fleet of scheduling servers" — no shared scheduler serialization.

**Kubernetes-semantics mapping**

| Modal mechanism | L1 in pure Kubernetes |
|---|---|
| stateless scheduler fleet | per-pool sharded operator + Lease |
| worker accepts/rejects placement | optimistic PATCH with `resourceVersion` precondition |
| no datastore on create path | claim = ownership PATCH of a pre-warmed object (like PVC→PV `Bound`) |
| async result write | Sandbox status/conditions written after the fast-path returns |

**Failure modes**

| Scenario | Behavior | Breaks k8s semantics? |
|---|---|---|
| Two claims race one warm Sandbox | `resourceVersion` PATCH conflict; loser adopts the next candidate | No — standard optimistic concurrency |
| Warm pool exhausted | Claim stays `Pending` until replenish (unchanged) | No |
| Operator shard dies mid-claim | Lease expiry → another replica resumes; claim is idempotent | No |
| Stale informer picks an already-claimed Sandbox | PATCH precondition fails → next candidate | No |

**Acceptance:** claim p50 stays near-constant from a 100-pool to a 2000+-pool
(today it degrades 33 ms → 516 ms); the pod-exclusivity invariant (one Sandbox,
at most one claim) holds under concurrent claims.

### L2 — node-local claim gateway (designed; `ClaimGateway` skeleton)

L1 still round-trips the apiserver. L2 takes the claim off the central path
entirely for the runtimes that have a node-local warm pool (`sandboxd`), while
keeping the `SandboxClaim` object as the durable record.

**Mechanism.** A `ClaimGateway` DaemonSet on each virtual-kubelet node fronts
`sandboxd`. A claim request reaches the node gateway directly; `sandboxd` hands
over an already-running microVM in **0.2–0.7 ms** and returns connection info
immediately. The `SandboxClaim` is marked `Bound` **asynchronously** — the record
follows the action, exactly as kubelet static Pods record to the apiserver after
the container is already running.

**Authorization stays central (correctly).** The gateway runs a
`SubjectAccessReview` + `ResourceQuota` check before delivery. Policy is the part
of Kubernetes that *should* stay centralized; only the ownership-transfer
transaction moves to the node.

```go
// ClaimGateway is the node-local fast path for warm-pool claims.
// A claim is served by the node that already holds a warm microVM; the
// SandboxClaim object is reconciled to Bound asynchronously afterward.
type ClaimGateway interface {
    // Claim transfers ownership of a node-local warm sandbox to the caller,
    // returning connection info. It performs the SubjectAccessReview +
    // quota check inline; it does NOT block on writing the SandboxClaim.
    Claim(ctx context.Context, req ClaimRequest) (Assignment, error)
    // Release returns a sandbox to the node-local pool (or tears it down).
    Release(ctx context.Context, assignment Assignment) error
}
```

**Failure modes**

| Scenario | Behavior | Breaks k8s semantics? |
|---|---|---|
| Gateway crashes after delivery, before recording `Bound` | Orphan binding → audit-only orphan GC + adopt reconciles the record (the VM is never destroyed on pod-level state — see the delete-authorization contract) | No — eventual consistency |
| Node has no warm VM | Falls back to the L1 Kubernetes path (create a new Sandbox) | No |
| Quota exceeded | Gateway rejects inline before delivery | No |

**Acceptance:** claim p50 sub-millisecond on the sandboxd tier; orphan-binding
rate converges to 0 via GC; `kubectl get sandboxclaims` still shows every claim.

### L3 — aggregated apiserver: etcd stores intent, not sandboxes (designed; `SandboxStore` skeleton)

A million `Sandbox` objects in etcd is a dead end (churn alone blows the ~8 GB
keyspace). The Kubernetes-native fix is the aggregation layer: serve
`sandboxes.agents.x-k8s.io` from an **aggregated apiserver** (an `APIService`)
that **scatter-gathers** live node inventory on read. etcd stores only *intent* —
one `SandboxWarmPool` spec expressing a million desired replicas, plus one
`inventory` object per node (`O(nodes)`). Object count drops from
`O(sandboxes)` to `O(pools + nodes)`.

Each virtual-kubelet node already knows its own VMs (L0 made that cache the
source of truth), so the aggregated server assembles a `SandboxList` by fanning
out to node inventories — the exact pattern `metrics.k8s.io` uses to serve
`PodMetrics` with zero etcd storage. `kubectl get sandboxes`, RBAC, field
selectors, and `watch` (implemented as a merge of per-node inventory streams)
all keep working; users never see that storage decentralized.

```go
// SandboxStore backs the aggregated apiserver for sandboxes.agents.x-k8s.io.
// It holds NO per-sandbox etcd objects: List/Get/Watch scatter-gather live
// node inventories, and Create/Delete translate to intent (warm-pool desired
// replicas) plus a node-local RPC.
type SandboxStore interface {
    List(ctx context.Context, opts ListOptions) (*SandboxList, error)   // fan-out to node inventories
    Get(ctx context.Context, ns, name string) (*Sandbox, error)         // route to owning node
    Watch(ctx context.Context, opts ListOptions) (watch.Interface, error) // merge per-node streams
}

// NodeInventory is the one O(nodes) etcd object per node: the durable
// summary of that node's live sandboxes, server-side-applied on a slow
// cadence. The per-sandbox truth lives in the node, not etcd.
type NodeInventory struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Node    string           `json:"node"`
    Entries []InventoryEntry `json:"entries"` // {name, phase, claimRef, addr}
}
```

**Failure modes**

| Scenario | Behavior | Breaks k8s semantics? |
|---|---|---|
| Node partitioned from aggregated server | Its sandboxes briefly absent from `List` (eventual consistency, same as an informer lag) | No |
| A node `inventory` object lost | Rebuilt from the node's own live state on next publish | No |
| Aggregated server restart | Stateless; rebuilds from node fan-out | No |
| Client needs strong read-after-write | Route `Get` to the owning node (authoritative), not the summary | No |

**Acceptance:** 1M sandbox *intent* costs `O(nodes)` etcd objects; `kubectl get
sandboxes` returns the fanned-out list; per-sandbox `Get` is authoritative.

### L3 routing: why node choice is sampled, not maximized

The operator schedules off `NodeInventory`, which each node republishes on a
5–30 s cadence. Picking the node with the highest advertised warm count would
therefore send *every* claim in a refresh window to whichever node looked best
in that one snapshot — draining it while its peers stay idle. The repo's own
100-burst run showed the signature: 98/100 warm, and both misses were
cold-provision on a single over-scheduled node.

Node choice is instead power-of-two-choices: sample two candidates that
advertise warm capacity for the requested pool and take the warmer one. That
keeps the bias toward warm capacity while spreading a burst across the fleet,
and a stale pick still costs at most one gossip redirect inside sandboxd, so
correctness is unchanged.

The watch path makes the same trade in the other direction. Re-deriving the
fleet view is `O(nodes × sandboxes)` — 40 ms at 200 nodes and 50k sandboxes —
so a watch with nothing to report stretches its poll up to 8× the base interval
and snaps back to it on the first observed change.

### L3 follow-up: resolve a sandbox without the summary (TODO)

The risk table above already names the fix — *route `Get` to the owning node
(authoritative), not the summary* — but today nothing does. Every single-sandbox
path resolves through the synthesized read view instead: `lifecycleREST.Create`
(the `pause` / `resume` / `snapshot` / `fork` subresources) calls
`SandboxStore.Get`, and the e2b surface's `lookup` calls `SandboxStore.List` and
linear-scans it for one claim id. Both inherit the summary's two properties, and
neither is acceptable at the scale L3 targets.

**It is stale for one publish interval.** A sandbox is live on its node the
moment `Claim` returns, but it does not appear in the read view until that node
republishes its `NodeInventory` (default 30 s). Measured against a 20-node
fleet: `p50` 29.0 s on the e2b surface, 28.6 s through the aggregated API. A
lifecycle verb issued inside that window answers `404`. Callers work around it
by polling until visible — which is what `examples/lifecycle` does — so
"create, then immediately pause" costs half a minute of polling.

**It is `O(total sandboxes)` per call.** `List` scatter-gathers every
`NodeInventory` and materializes *every* sandbox into a `Sandbox` object before
the caller filters for one. A materialized `Sandbox` measures 1392 B of Go heap
(`ObjectMeta` + synthesized labels/annotations + a `Condition`), so one `pause`
allocates ~139 MB at 100 k sandboxes and ~1.4 GB at 1 M — transient garbage, to
find a single record. This is the binding constraint, not the staleness.

Both fall out of the same omission: `Claim` already returns the node and the
claim id — the e2b create response even hands the node back to the client as
`clientID` — and a lifecycle verb needs nothing else. The plan keeps that
routing information instead of re-deriving it:

- **A. Claim-time index.** Record `sandboxID → (node, claimID)` when the claim
  is made, consulted before the read view. Bound it with an LRU so memory is a
  fixed budget rather than a function of load: measured 206 B/entry, so a
  200 k-entry cap is ~31 MB. Steady-state occupancy is `claim rate × TTL`, far
  below the cap — 1 M sandboxes averaging a 5-minute life is ~117 k entries.
- **B. Authoritative fan-out on a miss.** A different replica, or an evicted
  entry, falls back to asking the nodes directly — the authoritative route the
  risk table already prescribes. Bounded by node count, off the read path for
  anything older than one publish interval.

Neither touches etcd. **Publishing inventory on change was considered and
rejected:** `NodeInventory` carries one 105 B entry per live sandbox
(measured), so a node holding 2500 of them is a 263 KB object. Re-applying that
on a 2 s debounce costs 52.6 MB/s of large-object server-side-apply traffic
across 400 nodes at 1 M sandboxes, against 3.5 MB/s for the current 30 s
cadence — and it would still leave the `O(total sandboxes)` allocation in place,
because `lookup` would go on listing everything.

**Acceptance:** a lifecycle verb succeeds on a sandbox claimed milliseconds ago;
resolving one sandbox allocates `O(1)`, not `O(total sandboxes)`; index memory
is capped independently of fleet size.

### How this differs from Modal

Modal buys throughput by leaving Kubernetes: a proprietary SDK and a proprietary
control plane. Every layer here keeps `kubectl` / CRDs / RBAC / the ecosystem
intact. The one-line framing:

> **Modal proved 1M needs a decentralized transaction plane. We show the
> decentralized transaction plane can hide behind Kubernetes semantics.**

| | Modal | sandbox-operator |
|---|---|---|
| Scheduling | stateless fleet, in-memory worker state | per-pool sharded operator + Lease (L1) |
| Create critical path | direct scheduler→worker RPC, no datastore | ownership PATCH (L1) → node-local gateway (L2) |
| State of record | Redis stream (async) | Kubernetes objects; node inventory in etcd is `O(nodes)` (L3) |
| Sandbox storage | proprietary | aggregated apiserver, etcd stores intent only (L3) |
| Client interface | proprietary SDK | any Kubernetes client — unchanged |
| Scale ceiling | no practical limit | decoupled from etcd object count at L3 |

### Measured performance

Every acceptance claim above is backed by a reproducible benchmark committed under
`test/` — the evidence is regenerated by the harness, never hand-written. Numbers
are labelled by substrate: **algorithmic complexity** is proven on a fake apiserver
(so it isolates the scaling term, not machine speed), while **absolute latency on
real microVMs** is measured on a single `vk-cocoon` node (384 vCPU / 1.5 TB bare metal).

| Layer | Acceptance claim | Measured | Substrate / harness |
|---|---|---|---|
| **L1** | claim p50 stays near-constant as the pool grows | fast-path p50 **0.644 ms → 0.646 ms** from N=100 to N=2000 (**1.003×**); a full-`LIST` selection over the same fixtures degrades **15.7×** (1.3 → 20 ms) | fake apiserver + real reconciler — `test/scalebench` |
| **L1** | warm claim on real microVMs | claim→Bound p50 **129 ms**, p95 926 ms, p99 935 ms; 100/100 warm hits, 0 failures. Pool fills 100 microVMs in 62 s (boot p50 47 s) | the microVM node, 100 concurrent claims — `test/poolbench` |
| **L2** | sub-millisecond node-local claim | gateway overhead p50 **0.039 ms**, p95 0.053 ms (sandboxd delivery itself is 0.2–0.7 ms by contract); 200/200 orphan bindings reconciled, **0** VM destroys | httptest sandboxd + fake recorder — `test/l2bench` |
| **L3** | etcd stores intent only, `kubectl` unchanged | **3000** sandboxes served through client-go List/Get/Watch from **8** etcd objects (3 nodes + 5 pools) — **0** per-sandbox objects, 3 server-side-apply writes | in-process aggregated apiserver — `test/l3bench` |
| **e2e** | admission→claim→release→cleanup, zero leak | 100 real microVMs: four-way cross-check 100/100/100/100, 100/100 claims bound, **0 leaked**, production workloads on the same node unaffected | the microVM node, full stack — `test/e2ebench` |
| **sandboxd tier (deployed)** | hot-pool warm claim via k8s, apiserver flat under load | 100 `Sandbox` (`runtime: sandboxd`) create→Ready **p50 < 1 s** (warm), 98/100, submitted in 2.9 s; **100 %** routed to the sandboxd plane; apiserver LIST 37 ms/7 ms, **0 APF rejections, in-queue 0**; cocoon microVMs untouched | 26-node fleet, `vk-sandbox` + sandboxd — `test/run100` |

Two honest caveats. The sub-millisecond L1/L2 figures measure algorithmic cost and
gateway overhead on fake substrates; real end-to-end latency additionally pays the
apiserver round-trip, sandboxd delivery (0.2–0.7 ms), and informer convergence. And
the real-microVM claim p95 (926 ms, ~7× the p50) is single-node
optimistic-concurrency contention under 100 simultaneous claims — exactly the tail L1's per-pool operator
sharding is designed to spread across shards and nodes.

