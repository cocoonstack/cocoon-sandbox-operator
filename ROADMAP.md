# Roadmap

Direction, not commitment — sequenced by current priority. Issues and PRs that
move any of these forward are welcome.

## Near term

- **Per-pool hypervisor engine axis (`ch` | `fc`).** sandboxd already keys
  warm pools by engine; surface the axis through
  `SandboxTemplate`/`SandboxWarmPool` and the warm-pool driver so a single
  node can run mixed-engine pools under operator control.
- **Prompt release for L3 claims.** Deleting a synthesized `Sandbox` should
  release the node-local claim immediately via its `claim_ref` instead of
  waiting for the claim TTL to reap it.
- **Engine-labeled pool metrics.** `sandboxd_pool_*` series currently collapse
  pools that share template/net/size but differ in engine.

## Medium term

- **L2 ClaimGateway hardening.** Inline `SubjectAccessReview` +
  `ResourceQuota` on the node-local claim path, promoting the benchmarked
  skeleton to a supported deployment mode.
- **Aggregated-apiserver HA.** Multi-replica scatter-gather with consistent
  claim routing (today: multiple replicas serve reads; claims prefer a single
  writer).
- **Watch as merged per-node streams.** Serve `watch` on
  `sandboxes.agents.x-k8s.io` from live per-node inventory streams rather than
  inventory re-list.

## Medium term (continued)

- **Checkpoints that survive node loss.** A checkpoint currently lives on the
  node that took it: peer healing moves a record to a node that cannot reach
  it, but nothing replicates one, so losing that node's disk loses the
  checkpoint (see [docs/snapshot-placement.md](docs/snapshot-placement.md)).
  Making them durable needs asynchronous replication to N peers plus a
  placement policy that tracks replica sets and repairs under-replication.
  **Not built, and deliberately so** — checkpoints are branch points for agent
  workloads, not backups, and replication would cost write amplification on
  every capture. Until this lands, promote anything that must outlive its node
  to a template and distribute it as one.

## Longer term

- **Million-sandbox validation.** Exercise the full L0–L3 design at fleet
  scale and publish the methodology alongside the existing benchmarks.
- **Upstream alignment.** Track `kubernetes-sigs/agent-sandbox` API evolution
  (provenance in [UPSTREAM.md](UPSTREAM.md)) and contribute conformance
  feedback upstream.
