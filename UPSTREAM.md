# Upstream provenance

The `agents.x-k8s.io` and `extensions.agents.x-k8s.io` APIs, controllers,
conversion webhooks, lifecycle handling, metrics, and generated CRDs were
imported from `kubernetes-sigs/agent-sandbox` at:

- repository: `https://github.com/kubernetes-sigs/agent-sandbox`
- branch: `main`
- commit: `bfcb49d013ddf8909583b8c03674a6306048bba5`
- imported: `2026-07-15`

Imported source trees are `api/`, `controllers/`, `extensions/api/`,
`extensions/controllers/`, `internal/`, and the controller command. The
upstream CRDs, RBAC, Helm chart, and controller manifests were used as the
deployment baseline.

Local deltas are deliberately narrow:

1. The module and binary are named `cocoon-sandbox-operator`.
2. Extension controllers are enabled by default so every upstream API kind is
   functional after installation.
3. A Pod mutation seam keeps standard kubelet as the rollout default and
   supports explicit `vk-cocoon` selection per Sandbox.
4. The legacy `sandbox.cocoonstack.io/v1` direct-sandboxd controllers and CRDs
   moved here from MindOS for compatibility.
5. Kubernetes object names and the default namespace use the Cocoon operator
   identity.

When updating from upstream, import the same source trees, reapply the local
deltas above, run `make generate`, and require
`make all` plus conversion/controller tests to pass. Do not replace the Cocoon
compatibility API or Pod runtime adapter with upstream files.
