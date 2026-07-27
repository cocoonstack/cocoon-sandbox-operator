# sandbox-operator

A Kubernetes operator for **fast, warm-poolable agent sandboxes backed by real
microVMs**. It implements the [`kubernetes-sigs/agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox)
API in full and is driven **entirely through the standard Kubernetes API** — no
proprietary SDK. A pre-warmed sandbox is acquired in **~33 ms at p50**, below
e2b's published ~150 ms sandbox start, and each sandbox is a genuine
Cloud-Hypervisor/KVM microVM, not a shared-kernel container (see
[PERFORMANCE.md](PERFORMANCE.md)).

**Documentation: [cocoonstack.github.io/sandbox-operator](https://cocoonstack.github.io/sandbox-operator/)**
(source in [`docs/`](docs/)).

You create an `agents.x-k8s.io` `Sandbox` with any Kubernetes client; the
operator schedules it, and with the `vk-cocoon` runtime the backing Pod is
materialized as a [Cocoon](https://github.com/cocoonstack/cocoon) microVM on a
virtual-kubelet node. A portable **standard-kubelet** backend (ordinary Pods, any
conformant cluster) is also available for environments without the microVM
substrate.

## Why

| | |
|---|---|
| ✅ **Standards-compliant** | Implements the complete `agents.x-k8s.io` `Sandbox` (v1alpha1 + v1beta1) API and the `extensions.agents.x-k8s.io` `SandboxTemplate` / `SandboxWarmPool` / `SandboxClaim` API from [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox), including conversion webhooks, lifecycle, status/conditions, PVCs, Services, and NetworkPolicy. |
| ✅ **Pure Kubernetes SDK** | Create and manage sandboxes with any Kubernetes client — [`client-go`](https://github.com/kubernetes/client-go), [`controller-runtime`](https://github.com/kubernetes-sigs/controller-runtime), `kubectl`, or the client library of any language. The control plane is 100% Kubernetes CRDs. |
| ✅ **Real microVM isolation** | With `vk-cocoon`, each sandbox is a hardware-isolated Cloud-Hypervisor/KVM guest via [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) + [cocoon](https://github.com/cocoonstack/cocoon) — not a shared-kernel container. Warm claims stay Kubernetes-native. |
| ✅ **Faster than hosted microVM services** | Pre-warmed claim p50 **~33 ms** (real microVM) vs e2b's published ~150 ms. Validated to a warm pool of **thousands of concurrent microVMs**. Full numbers and methodology in [PERFORMANCE.md](PERFORMANCE.md). |

## Architecture

```mermaid
flowchart TB
    subgraph client["Any Kubernetes client (client-go / controller-runtime / kubectl)"]
        A["create Sandbox<br/>(agents.x-k8s.io/v1beta1)"]
        B["create SandboxClaim<br/>(→ SandboxWarmPool)"]
    end

    A -->|Kubernetes API| APISERVER
    B -->|Kubernetes API| APISERVER
    APISERVER["kube-apiserver + agent-sandbox CRDs"]

    subgraph op["sandbox-operator"]
        SB["Sandbox controller"]
        WP["SandboxWarmPool controller"]
        CL["SandboxClaim controller"]
        CW["conversion webhook<br/>(v1alpha1 ↔ v1beta1)"]
    end

    APISERVER <--> op

    WP -->|pre-provisions| POOL["Warm pool:<br/>N Ready microVMs"]
    CL -->|"adopt (~33 ms, control-plane only)"| POOL
    SB -->|"creates Pod + mutates for runtime"| POD

    POOL --> POD

    subgraph backends[" "]
        POD{{"backing Pod"}}
        POD -->|"runtime: vk-cocoon"| VK["virtual-kubelet node → Cocoon microVM<br/>(Cloud-Hypervisor / KVM)"]
        POD -->|"runtime: standard (default)"| STD["ordinary Pod on a<br/>standard kubelet node"]
    end

    VK --> DP["Data plane: cocoon vm exec / silkd (in-VM agent)"]
    STD --> DP2["Data plane: Pod exec / Service FQDN"]
```

- **Cold path:** a `Sandbox` (or `SandboxClaim` with no warm pool) creates a Pod
  on demand; with `vk-cocoon` the Pod boots a microVM.
- **Warm path:** a `SandboxWarmPool` keeps N microVMs Ready; a `SandboxClaim`
  *adopts* one instantly (control-plane only — the microVM is already booted),
  then the warm pool replenishes in the background. This is the ~33 ms path.
- **Deeper tier:** the [`cocoonstack/sandbox`](https://github.com/cocoonstack/sandbox)
  runtime this builds on has a node-local `sandboxd` warm pool whose claims are
  **0.2–0.7 ms** (VM ownership transfer) via its own Go/Python SDK — use it when
  you want sub-millisecond claims outside the Kubernetes control plane.

## API coverage

| API | Implemented semantics |
|---|---|
| `agents.x-k8s.io/v1beta1` **Sandbox** | Pod, PVC, optional headless Service, status/conditions, suspend/resume, expiry & shutdown policy, resource adoption, metadata propagation, dual-stack status, v1alpha1 conversion |
| `extensions.agents.x-k8s.io/v1beta1` **SandboxTemplate** | reusable blueprints, managed/unmanaged NetworkPolicy, secure defaults, env & PVC injection policy, conversion |
| `extensions.agents.x-k8s.io/v1beta1` **SandboxWarmPool** | desired/ready capacity, `scale` subresource, template-drift & update strategies, warm replenishment, conversion |
| `extensions.agents.x-k8s.io/v1beta1` **SandboxClaim** | atomic warm adoption, cold fallback, lifecycle & finished TTL, foreground deletion, metadata/env/PVC injection, conversion |

v1beta1 is the storage version; v1alpha1 remains served via conversion webhooks.
Upstream provenance and the pinned revision are in [UPSTREAM.md](UPSTREAM.md).

## Use it with the Kubernetes SDK

Typed (controller-runtime), or `unstructured` / dynamic client if you don't want
to vendor the types:

```go
import (
    sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
    "sigs.k8s.io/controller-runtime/pkg/client"
)

sb := &sandboxv1beta1.Sandbox{
    ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
    Spec: sandboxv1beta1.SandboxSpec{
        SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
            PodTemplate: sandboxv1beta1.PodTemplate{
                // Opt into a real microVM; omit for the portable standard-kubelet backend.
                ObjectMeta: sandboxv1beta1.PodMetadata{
                    Annotations: map[string]string{"sandbox.cocoonstack.io/runtime": "vk-cocoon"},
                },
                Spec: corev1.PodSpec{
                    Containers: []corev1.Container{{Name: "agent", Image: "ghcr.io/cocoonstack/cocoon/ubuntu:24.04"}},
                },
            },
        },
    },
}
_ = c.Create(ctx, sb) // standard client-go / controller-runtime client
```

Or plain YAML:

```yaml
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata: { name: demo, namespace: default }
spec:
  podTemplate:
    metadata:
      annotations: { sandbox.cocoonstack.io/runtime: vk-cocoon }   # real microVM
    spec:
      containers:
        - { name: agent, image: ghcr.io/cocoonstack/cocoon/ubuntu:24.04 }
```

For low-latency acquisition, define a `SandboxTemplate` + `SandboxWarmPool` and
create `SandboxClaim`s — see [examples/](examples/).

### Lifecycle: pause, resume, fork, snapshot

A delivered sandbox also takes four action verbs, served as subresources so the
standard `agents.x-k8s.io` schema stays untouched: `pause`, `resume`, `fork`,
`snapshot`. They are not uniformly fast — `resume` and `fork` are node-local,
while `pause`/`snapshot` cost time proportional to guest memory. Verb table,
a runnable walk-through over both surfaces, and the two consistency behaviors
callers must handle are in [docs/lifecycle.md](docs/lifecycle.md).

## Use it with the e2b SDK

The aggregated apiserver can also serve an e2b-compatible REST surface, so an
**unmodified e2b SDK** claims from these same warm pools — point `E2B_API_URL`
at it:

```bash
sandbox-apiserver --enable-e2b-api --e2b-api-key-file=/etc/e2b/keys
```

```js
export E2B_API_URL=https://your-apiserver:8080
const sandbox = await Sandbox.create('registry.example.com/rt:24.04')
```

It is a translation layer, not a second control plane: an e2b create is the same
node-local claim, and the sandbox stays visible to `kubectl get sandboxes`. Flags,
endpoint mapping, and limits in [docs/e2b-compat.md](docs/e2b-compat.md).

## Install

Each release publishes multi-arch (amd64/arm64) images to GHCR:
`ghcr.io/cocoonstack/sandbox-operator` (controller) and
`ghcr.io/cocoonstack/sandbox-apiserver` (aggregated apiserver).

Helm:

```bash
helm upgrade --install sandbox-operator ./helm \
  --namespace sandbox-system --create-namespace \
  --set image.tag=<version>
```

Kustomize (replace the `ko://` image reference):

```bash
kustomize build k8s | sed 's#ko://.*/sandbox-operator#ghcr.io/cocoonstack/sandbox-operator:<version>#' | kubectl apply -f -
```

The default (standard-kubelet) backend needs no special nodes. The `vk-cocoon`
backend requires [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) virtual
nodes. See [docs/migration-from-mindos.md](docs/migration-from-mindos.md) for a
safe, no-double-write rollout alongside an existing installation.

## Runtime backends

Standard kubelet is the default. To place a sandbox on the `vk-cocoon` microVM
backend, set the Pod-template annotation
`sandbox.cocoonstack.io/runtime: vk-cocoon`; the adapter adds only the missing
scheduling fields (`node.kubernetes.io/instance-type=virtual-node`, the provider
toleration, and the `cocoonset.cocoonstack.io/*` boot annotations) and rejects
(never overwrites) conflicting explicit values. See
[docs/runtime-backends.md](docs/runtime-backends.md).

To pin sandboxes to specific nodes (for example, microVM hosts with fast local
NVMe/xfs storage), label those nodes and set a `nodeSelector` in the Pod template
or `SandboxTemplate`; the operator never mutates user-supplied scheduling.

## Scaling design

L0 API hygiene → L1 claim-as-ownership-transfer → L2 node-local claim gateway →
L3 aggregated apiserver, and why etcd stays off the claim path:
**[docs/scaling-design.md](docs/scaling-design.md)**.

## Performance

All numbers are measured on live clusters through the Kubernetes API — real
Cloud-Hypervisor/KVM microVMs, reproducible drivers in [`test/`](test/).

| | |
|---|---|
| Warm claim (pool 200, real microVM) | **p50 33 ms** · p95 39 ms |
| `create` → `Ready` on the sandboxd tier | **p50 < 1 s** (warm claim) |
| Fleet fill, one `kubectl patch` | **50 000** microVMs in **10–15 s** across 20 nodes |
| Effective supply rate | **3 300–5 000** microVMs/s |
| Memory, mmap CoW recovery | **99 MB** net per microVM |
| etcd during the 50 k run | **~2 writes/s** — sandboxes are synthesized, never stored |

Method, per-tier comparisons, the memory ledger, the scaling law, and the
honest counter-examples are in **[PERFORMANCE.md](PERFORMANCE.md)**.

## Development

Go 1.26+.

```bash
make all         # fmt-check vet test build
make test-race
make generate    # CRDs, RBAC, deepcopy (idempotent)
```

See [docs/](docs/) for API, configuration, and runtime details, including
[snapshot placement](docs/snapshot-placement.md) — where a checkpoint lives,
how a branch reaches it from another node, and what that costs.

## Community

- Contributions: [CONTRIBUTING.md](CONTRIBUTING.md)
- Governance: [GOVERNANCE.md](GOVERNANCE.md) · [MAINTAINERS.md](MAINTAINERS.md)
- Security reports: [SECURITY.md](SECURITY.md)
- Direction: [ROADMAP.md](ROADMAP.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## License

AGPL-3.0 — see [LICENSE](LICENSE).

The `agents.x-k8s.io` APIs, controllers, and conversion webhooks are imported
from [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)
under Apache-2.0 and keep their original headers, as that license requires;
see [UPSTREAM.md](UPSTREAM.md).
