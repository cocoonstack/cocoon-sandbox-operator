# Runtime backends

## Decision

The rollout default is standard kubelet. vk-cocoon is an explicit per-Sandbox
opt-in until the target cluster is ready and the integration test suite has
passed.

vk-cocoon provides the intended Kubernetes-facing contract for Cocoon: Pods
schedule to virtual nodes and are materialized as MicroVMs, while Pod status,
exec, projected configuration, networking, recovery, and VM identity remain
visible through Kubernetes. It is retained behind explicit selection for the
upcoming cluster validation.

The current `containerd-shim-cocoon-v2` prototype is not yet a replacement for
that contract. It needs conformance work for complete multi-container Pod,
volume/mount, streaming I/O and signals, pause/resume persistence, and restart
recovery semantics before standard kubelet plus the Cocoon shim can replace
vk-cocoon as the Cocoon hot-MicroVM implementation.

## Selection rules

Runtime selection is deterministic, in this order:

1. `sandbox.cocoonstack.io/runtime: vk-cocoon|standard` on the Pod template.
2. Any explicit `spec.runtimeClassName` selects the standard kubelet path.
3. The operator's `--default-runtime` value, which defaults to `standard`.

An explicit `vk-cocoon` annotation conflicts with `runtimeClassName` and is
rejected. A conflicting virtual-node selector is also rejected. User-provided
annotations and tolerations are otherwise preserved.

## vk-cocoon contract

For a vk-cocoon Pod, the operator supplies these defaults:

| Key | Value or source |
|---|---|
| node selector `node.kubernetes.io/instance-type` | `virtual-node` |
| toleration `virtual-kubelet.io/provider` | `Exists`, `NoSchedule` |
| `sandbox.cocoonstack.io/runtime` | `vk-cocoon` |
| `cocoonset.cocoonstack.io/mode` | `run` |
| `cocoonset.cocoonstack.io/managed` | `true` |
| `cocoonset.cocoonstack.io/image` | first container image |
| `cocoonset.cocoonstack.io/os` | Pod OS, default `linux` |
| `vm.cocoonstack.io/name` | stable namespace/name-derived DNS label |

The image must exist in the Cocoon image library on eligible vk-cocoon nodes.

## Standard kubelet contract

The standard path leaves the generated Pod runtime and scheduling fields
untouched. It can use the cluster default runtime or an explicit RuntimeClass
such as Kata Containers or gVisor. This path implements the upstream
agent-sandbox Kubernetes semantics, but Cocoon-specific hot-MicroVM behavior is
available only when the selected standard-kubelet runtime handler implements it.
