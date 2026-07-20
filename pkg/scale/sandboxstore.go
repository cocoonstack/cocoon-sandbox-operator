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

package scale

import (
	"context"

	"k8s.io/apimachinery/pkg/watch"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/extensions/api/v1beta1"
)

// ListOptions is the subset of client list parameters the aggregated store
// honors when fanning out to node inventories.
type ListOptions struct {
	Namespace     string
	LabelSelector string
	FieldSelector string
}

// PoolKey identifies a warm pool by the claim axes the aggregated Create path
// derives from a Sandbox: the template (blueprint image), the network mode, and
// the size class. A node advertises matching warm capacity as a NodeInventory
// PoolCapacity, and Create picks the node with the most warm capacity for the key.
type PoolKey struct {
	Template string
	Net      string
	Size     string
}

// PoolCapacity is one node's warm capacity for a single pool. It aliases the
// canonical extensions type so the scale contracts stay self-contained.
type PoolCapacity = extv1beta1.PoolCapacity

// SandboxStore is the L3 storage contract behind an aggregated apiserver serving
// sandboxes.agents.x-k8s.io. It holds NO per-sandbox etcd objects: List/Get/Watch
// scatter-gather live node inventories and Create/Delete are synchronous node-local
// claim/release, so etcd stores only intent (warm-pool desired replicas plus one
// O(nodes) NodeInventory per node). This is the metrics.k8s.io pattern applied to
// sandboxes, so object count drops from O(sandboxes) to O(pools+nodes) while
// kubectl/RBAC/watch keep working.
type SandboxStore interface {
	// List assembles a SandboxList by fanning out to node inventories.
	List(ctx context.Context, opts ListOptions) (*sandboxv1beta1.SandboxList, error)
	// Get routes to the owning node for an authoritative (read-after-write)
	// answer rather than the eventually-consistent summary.
	Get(ctx context.Context, namespace, name string) (*sandboxv1beta1.Sandbox, error)
	// Watch merges per-node inventory streams into a single sandbox watch.
	Watch(ctx context.Context, opts ListOptions) (watch.Interface, error)
	// Claim delivers a warm microVM for namespace/name from a node advertising warm
	// capacity for pool, returning the node-local assignment (claim id, node,
	// connection address). It writes NO per-sandbox etcd object: the claim is a
	// synchronous node-local ownership transfer via the owning node's sandboxd.
	// When no node has a warm microVM for the pool it returns an error for which
	// IsNoWarmCapacity is true, so the caller can surface a retryable 503.
	Claim(ctx context.Context, namespace, name string, pool PoolKey) (Assignment, error)
	// Release returns the claimed microVM id to its owning node's pool. It is
	// owner-authorized teardown only (the Sandbox resource itself being deleted);
	// it never destroys a VM on pod state alone. The node's sandboxd address is
	// resolved from its NodeInventory.
	Release(ctx context.Context, node, id string) error
}

// InventoryEntry is one live sandbox as summarized by its owning node.
type InventoryEntry = extv1beta1.InventoryEntry

// NodeInventory is the single O(nodes) etcd object per node: the durable summary
// of that node's live sandboxes, server-side-applied on a slow cadence. The
// per-sandbox truth lives in the node (the L0 node-scoped cache), not etcd; a
// lost NodeInventory is rebuilt from the node's own live state on next publish.
// The canonical type (and its CRD) lives in the extensions.agents.x-k8s.io
// group; these aliases keep the scale contracts self-contained for callers.
type NodeInventory = extv1beta1.NodeInventory
