// Copyright 2026 The CocoonStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package scale defines the decentralized scaling interfaces described in the
// README's "Scaling design" chapter. L0 (API hygiene) and L1 (claim ownership
// transfer) are implemented in the vk-cocoon provider and the extensions
// controllers respectively; this package holds the L2 and L3 contracts:
//
//   - ClaimGateway (L2): the node-local claim fast path over sandboxd. A claim
//     is served by the node that already holds a warm microVM; the SandboxClaim
//     object is reconciled to Bound asynchronously afterward (kubelet static-Pod
//     semantics: the node acts first, the apiserver records after).
//
//   - SandboxStore + NodeInventory (L3): the aggregated-apiserver storage
//     contract. sandboxes.agents.x-k8s.io is served by scatter-gathering live
//     node inventories; etcd stores only intent (warm-pool desired replicas plus
//     one O(nodes) NodeInventory object per node), the metrics.k8s.io pattern.
//
// Both contracts are implemented here: the sandboxd-backed ClaimGateway and its
// orphan reconciler (claimgateway_impl.go), and the scatter-gather store with
// its NodeInventory publisher and cache-fed inventory source
// (sandboxstore_impl.go), served by cmd/sandbox-apiserver via pkg/scale/apiserver.
package scale
