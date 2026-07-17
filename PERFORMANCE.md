# Performance

All numbers below are **measured on a live cluster through the Kubernetes API**
(no mocks, no direct data-plane calls) using the reproducible drivers in
[`test/`](test/). Sandboxes are **real Cloud-Hypervisor/KVM microVMs** provisioned
through the `vk-cocoon` runtime.

## Test environment

| | |
|---|---|
| Cluster | 27 virtual-kubelet (`vk-cocoon`) nodes, 384 cores / 1.5 TiB each; managed Kubernetes v1.26 |
| Operator | `cocoon-sandbox-operator`, `--sandbox-concurrent-workers=16`, `--sandbox-warm-pool-concurrent-workers=8`, `--kube-api-qps=200` |
| Sandbox | `agents.x-k8s.io/v1beta1` Sandbox, `runtime: vk-cocoon`, Ubuntu microVM (2 vCPU / 8 GiB, hugepage-backed on demand) |
| Driver | `test/poolbench` — controller-runtime client; watch-driven claim timing |

## Headline: warm-pool claim latency (real microVM)

A `SandboxWarmPool` keeps microVMs pre-booted; a `SandboxClaim` adopts one. The
claim is **control-plane only** — the microVM is already running — so latency is
a Kubernetes round-trip, independent of the microVM boot cost.

| pool size | p50 | p95 | warm hits |
|---|---|---|---|
| 10 | **35 ms** | 40 ms | 100% |
| 200 | **33 ms** | 39 ms | 100% |

**~33 ms p50, and flat as the pool grows.** This is below e2b's published ~150 ms
sandbox start, on a real microVM.

### Comparison to e2b

e2b's headline "~150 ms" is a snapshot-resume start
([e2b.dev](https://e2b.dev), vendor-published — verify before quoting). Matching
tiers rather than headlines:

| tier | cocoon-sandbox-operator (this repo) | cocoonstack/sandbox `sandboxd` | e2b (published) |
|---|---|---|---|
| **warm claim** (pre-booted) | **33 ms** p50 (measured, k8s control plane) | **0.2–0.7 ms** (node-local VM ownership transfer) | — |
| **clone / snapshot resume** | n/a (uses cold boot) | 45–75 ms | ~150 ms |
| **cold boot** (kernel start) | 26–32 s (full OCI microVM boot) | 200–350 ms | — |
| substrate | real CH/KVM microVM | real FC/CH microVM | Firecracker microVM |
| control plane | **Kubernetes CRDs, any k8s SDK** | node-local daemon + Go/Python SDK | proprietary hosted SDK |
| self-hosted | yes (Apache 2.0) | yes | no |

Two honest caveats:

1. **Cold boot is slow** (full OCI microVM boot, 26–32 s) — the warm pool exists
   precisely to keep that off the request path. cocoon's snapshot-**clone** tier
   (45–75 ms) is faster but lives in the `sandboxd` stack, not this operator.
2. **The 33 ms claim degrades under extreme fan-out** (see below). The number
   that beats e2b *at any scale* is the `sandboxd` sub-millisecond claim, because
   it never touches a centralized control plane.

### Claim latency vs. concurrency and scale

| pool | concurrency | p50 | p95 | note |
|---|---|---|---|---|
| 200 | 1 | 33 ms | 39 ms | serial — the comparable single-start number |
| 200 | 5 | 53 ms | 183 ms | mild contention |
| 200 | 20 | 316 ms | 454 ms | 20 simultaneous claims + their replenishment |
| ~2300 | 1 | 516 ms | 554 ms | apiserver LIST + operator informer cache of 2500 objects |

The operator's warm claim beats e2b up to ~1000-sandbox pools. Beyond ~2000
concurrent sandboxes the **centralized Kubernetes control plane** (apiserver list
throughput + the operator's informer cache) becomes the bottleneck. This is
inherent to a k8s-native design; the `sandboxd` node-local pool does not degrade.

## Scale: thousands of concurrent microVMs

A single `SandboxWarmPool` scaled to 2500:

- **readyReplicas = 2303 / 2500** concurrent real microVMs (operator status;
  cross-checked via per-node `cocoon vm list`).
- **CR creation ~36/s**, **microVM boot ~27/s** at scale.
- **0 operator restarts**; production microVMs on the same cluster **unaffected**
  (their hugepage pool held steady the entire run).
- Clean scale-to-0 with **0 stuck finalizers**.

Reaching this scale required a `topologySpreadConstraint` (hostname) in the Pod
template: virtual-kubelet nodes under-report utilization, so the default
scheduler packs one node while leaving others idle; the spread constraint
rebalances (an idle node went from 0 to ~127 microVMs). At very high per-node
density the cocoon-net DHCP path is the next limiter (a small `WaitingForIP`
tail).

## Data plane

k8s Pod `exec` is **not** available to `vk-cocoon` microVMs on a managed cluster
(the control plane cannot reach virtual-node kubelets over the microVM network);
the microVM data plane is `cocoon vm exec` / silkd (in-VM agent), validated in
test evidence. The portable standard-kubelet backend uses ordinary Pod exec.

## Reproduce

```bash
# warm-pool claim latency (real microVM), pinned to a labeled node pool
go run -tags poolbench ./test/poolbench \
  -runtime vk-cocoon -image <cocoon-oci-image> \
  -node-selector <pool-label>=<v> -snapshot-policy never \
  -pool 200 -claims 40 -claim-conc 1

# core + extensions E2E (12 scenarios) against a real cluster
KUBECONFIG=<vke> go test -tags e2e ./test/e2e/ -run TestE2E -v
```
