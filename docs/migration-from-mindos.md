# Migration from MindOS

MindOS remains an API consumer. This operator becomes the sole owner of sandbox
CRDs, status, finalizers, warm-pool reconciliation, and sandboxd communication.

## The invariant: never two active writers

The old MindOS sandbox reconcilers and this operator must **never** reconcile the
same `sandbox.cocoonstack.io` / `agents.x-k8s.io` objects at the same time. Their
leader-election identities differ (`cocoon-sandbox-operator.agents.x-k8s.io` here
vs. the MindOS controller-manager ID), so Kubernetes leader election **cannot**
prevent duplicate reconciliation across the two binaries. Double writers race on
status, finalizers, and sandboxd claims.

The safe handoff therefore **stops the old writer before starting the new one**. A
brief window in which *no* controller reconciles sandbox objects is acceptable
(objects simply hold their last state); a window in which *both* reconcile is not.

## Safe rollout order

1. **Quiesce the old writer first.** Roll out the MindOS build that no longer
   registers the Cocoon sandbox controllers and no longer installs the four
   `sandbox.cocoonstack.io` CRDs. Wait until every old MindOS replica is running
   that build (`kubectl rollout status`). At this point no controller is an active
   writer of sandbox objects — this is the intended, safe no-writer gap.
   - If your MindOS build cannot drop the reconcilers in one step, instead scale
     the old sandbox controller-manager to zero replicas and confirm it is not
     holding its leader lease before proceeding. Either way the old writer must be
     provably stopped before step 3.
2. Confirm the old writer is gone: no MindOS pod is reconciling sandbox objects
   (no recent MindOS-sourced updates to `CocoonSandbox`/pool/node/template status,
   old leader lease released).
3. **Only now enable the new writer.** Deploy `cocoon-sandbox-operator` with leader
   election enabled. It becomes the sole active writer.
4. Verify the operator can list all eight sandbox CRDs and its conversion webhook
   is ready (`kubectl get --raw` on the webhook, CRD `caBundle` populated).
5. Confirm existing `CocoonSandbox`, pool, node, and template objects retain their
   status and continue reconciling — and that the only controller now touching
   their status/finalizers is `cocoon-sandbox-operator`.
6. Remove any obsolete MindOS CRD release only after Helm ownership metadata is
   transferred to the operator release (see "CRD and resource ownership" below).

Rollback: if step 3–5 misbehaves, scale the operator to zero and re-enable the old
MindOS reconcilers. Because step 1 removed the old writer cleanly, at no point are
both active.

## CRD and resource ownership (avoid "invalid ownership metadata")

The eight CRDs ship in `helm/crds/` (Helm's unmanaged CRD directory): a fresh
`helm install` applies them once, and Helm never claims ownership of or upgrades
them. Upgrade them out of band with `kubectl apply -f helm/crds/` (also documented
in `helm/README.md`). Because Helm does not own them, they never raise the
`invalid ownership metadata` error.

The **templated** shared resources do carry Helm ownership: the install Namespace
`cocoon-sandbox-system`, the `cocoon-sandbox-operator` ServiceAccount, ClusterRole,
ClusterRoleBinding, and the operator/webhook Services. If a prior MindOS install
already created any object with one of these names (most likely the Namespace),
the first `helm upgrade --install` aborts with
`invalid ownership metadata ... missing key "app.kubernetes.io/managed-by"`. Handle
it one of these ways:

- Install with Helm 3.17+ and `--take-ownership` so Helm adopts the pre-existing
  objects; or
- Pre-stamp the colliding objects before installing:
  ```
  for obj in namespace/cocoon-sandbox-system \
             serviceaccount/cocoon-sandbox-operator \
             clusterrole/cocoon-sandbox-operator \
             clusterrolebinding/cocoon-sandbox-operator; do
    kubectl -n cocoon-sandbox-system annotate "$obj" \
      meta.helm.sh/release-name=cocoon-sandbox-operator \
      meta.helm.sh/release-namespace=cocoon-sandbox-system --overwrite
    kubectl -n cocoon-sandbox-system label "$obj" \
      app.kubernetes.io/managed-by=Helm --overwrite
  done
  ```
  (cluster-scoped objects omit `-n`); or
- Install into a namespace with no collisions using `namespace.create=false` and
  `namespace.name=<existing>`.

## Storage-version migration

Run `helm/files/migrate.sh --phase=bootstrap` before enabling the operator and
`--phase=migrate` after, to shadow cold-start pools and rewrite every sandbox
object into the v1beta1 storage format. No resource conversion is required for
`sandbox.cocoonstack.io/v1`: the schema and group are preserved. The upstream
agent-sandbox APIs use their existing v1alpha1-to-v1beta1 conversion webhooks.
