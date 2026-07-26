# Operator configuration

The operator is intentionally usable with its defaults: leader election and
all agent-sandbox extension controllers are enabled, and generated Pods use
standard kubelet scheduling.

## Runtime and API surface

- `--default-runtime` (`standard`): default backend for newly created Sandbox
  Pods. Valid values are `vk-cocoon` and `standard`.
- `--extensions` (`true`): enable `SandboxTemplate`, `SandboxWarmPool`, and
  `SandboxClaim` controllers and webhooks.
- `--cluster-domain` (`cluster.local`): suffix used to construct Sandbox
  Service FQDNs.

An explicit Pod-template `runtimeClassName` always selects standard kubelet.
An explicit `sandbox.cocoonstack.io/runtime` annotation takes precedence over
the default.

## Controller concurrency

- `--sandbox-concurrent-workers` (1)
- `--sandbox-claim-concurrent-workers` (50)
- `--sandbox-warm-pool-concurrent-workers` (1)
- `--sandbox-template-concurrent-workers` (1)
- `--sandbox-warm-pool-max-batch-size` (300)
- `--enable-warm-pool-eviction` (`true`)
- `--kube-api-qps` (-1, unlimited)
- `--kube-api-burst` (10)

## Webhook and leader election

- `--leader-elect` (`true`)
- `--leader-election-namespace` (auto-detected when empty)
- `--webhook-port` (9443)
- `--webhook-cert-dir` (`/tmp/k8s-webhook-server/serving-certs`)
- `--webhook-service-name` (`cocoon-sandbox-webhook-service`)
- `--webhook-namespace` (`cocoon-sandbox-system`)
- `--manage-webhook-certs` (`true`)

When `--manage-webhook-certs=true`, the operator creates serving certificates
and patches conversion-webhook CA bundles. Disable it only when the cluster
manages both externally.

## Observability

- `--metrics-bind-address` (`:8080`)
- `--health-probe-bind-address` (`:8081`)
- `--enable-tracing` (`false`)
- `--enable-pprof` (`false`)
- `--enable-pprof-debug` (`false`)

Use `--enable-pprof-debug` only in controlled environments because it exposes
process details and enables block/mutex sampling.

## Example patch

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sandbox-operator
  namespace: cocoon-sandbox-system
spec:
  template:
    spec:
      containers:
        - name: sandbox-operator
          args:
            - --leader-elect=true
            - --extensions=true
            - --default-runtime=standard
            - --sandbox-concurrent-workers=10
            - --sandbox-claim-concurrent-workers=100
```

Helm exposes the common flags under `controller.*`; use
`controller.extraArgs` for other flags.
