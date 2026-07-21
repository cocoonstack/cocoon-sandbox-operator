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

// Package warmpool drives the official agents.x-k8s.io SandboxWarmPool CRD onto
// the L3 node-local warm pools. It is the control-plane surface for warm
// capacity: `kubectl apply` a SandboxWarmPool sets the target, editing
// spec.replicas scales it, deleting it drains — no SSH, no per-node poking.
//
// The design contract the whole L3 stack depends on: this reconcile is
// POOL-LEVEL, O(pools + nodes), NEVER O(sandboxes). It runs inside the
// aggregated apiserver process alongside the NodeInventory cache, so one
// component polls node state and writes back CR status. There is deliberately
// NO per-sandbox reconcile — individual Sandbox status is synthesized on read
// from NodeInventory, so the apiserver never carries a per-sandbox background
// load and cannot wedge as the sandbox count grows.
package warmpool

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	extv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale/sandboxd"
)

// netAnnotation selects the pool network mode; it mirrors the aggregated
// apiserver's NetAnnotation so a Create derives the same key the driver sets.
const netAnnotation = "sandbox.cocoonstack.io/net"

// defaultInterval is the pool resync cadence. Pools are O(single-digit), so a
// tight loop is cheap; it exists only to reconcile drift (a node that restarted,
// a target edited out of band).
const defaultInterval = 20 * time.Second

// maxNodeConcurrency bounds the per-node PUT /v1/pools fan-out per tick.
const maxNodeConcurrency = 16

// PoolSetter is the sandboxd surface the driver needs: replace a node's whole
// warm-target set. The concrete *sandboxd.Client satisfies it; tests inject a fake.
type PoolSetter interface {
	SetPools(ctx context.Context, pools []sandboxd.PoolSpec) (*sandboxd.NodeInfo, error)
}

// ClientFactory builds a PoolSetter for one node's advertise address and the
// uniform fleet api_token.
type ClientFactory func(addr, token string) PoolSetter

// NewSandboxdFactory returns the production factory backed by the real HTTP client.
func NewSandboxdFactory() ClientFactory {
	return func(addr, token string) PoolSetter { return sandboxd.New("http://"+addr, token) }
}

// Driver reconciles SandboxWarmPool CRs into node-local sandboxd warm targets.
type Driver struct {
	// kube lists SandboxWarmPools, gets SandboxTemplates, and writes pool status.
	kube client.Client
	// inv is the cache-fed NodeInventory reader (node list + address + live warm).
	inv scale.InventorySource
	// token is the uniform fleet-wide sandboxd api_token used on every PUT /v1/pools.
	token   string
	factory ClientFactory

	interval time.Duration
	log      logr.Logger
}

// Options configures a Driver.
type Options struct {
	Interval time.Duration
	Log      logr.Logger
}

// New builds a Driver. token empty leaves the driver fail-closed (it logs and
// sets no pools rather than driving sandboxd unauthenticated).
func New(kube client.Client, inv scale.InventorySource, token string, factory ClientFactory, opts Options) *Driver {
	if opts.Interval <= 0 {
		opts.Interval = defaultInterval
	}
	return &Driver{kube: kube, inv: inv, token: token, factory: factory, interval: opts.Interval, log: opts.Log}
}

// Run reconciles once immediately, then every interval until ctx is cancelled.
func (d *Driver) Run(ctx context.Context) {
	timer := time.NewTicker(d.interval)
	defer timer.Stop()
	if err := d.reconcileOnce(ctx); err != nil {
		d.log.Error(err, "warm-pool initial reconcile failed")
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := d.reconcileOnce(ctx); err != nil {
				d.log.Error(err, "warm-pool reconcile failed")
			}
		}
	}
}

// nodeView is one schedulable node: its name, sandboxd address, and current
// per-pool warm counts (for status write-back).
type nodeView struct {
	name    string
	addr    string
	warmBy  map[scale.PoolKey]int
}

// desiredPool is a resolved SandboxWarmPool: its key and per-node target.
type desiredPool struct {
	nn      types.NamespacedName
	key     scale.PoolKey
	targets map[string]int
}

// reconcileOnce is the whole loop: resolve pools, distribute targets, PUT every
// node's full pool set, write pool status from live inventory.
func (d *Driver) reconcileOnce(ctx context.Context) error {
	var pools extv1beta1.SandboxWarmPoolList
	if err := d.kube.List(ctx, &pools); err != nil {
		return fmt.Errorf("list SandboxWarmPools: %w", err)
	}
	nodes, err := d.schedulableNodes(ctx)
	if err != nil {
		return err
	}

	desired := make([]desiredPool, 0, len(pools.Items))
	for i := range pools.Items {
		p := &pools.Items[i]
		key, rerr := d.poolKey(ctx, p)
		if rerr != nil {
			d.log.Error(rerr, "resolve warm pool template", "pool", p.Namespace+"/"+p.Name)
			d.writeStatus(ctx, p, nodes, key) // status still reflects live warm (0 target)
			continue
		}
		replicas := int32(1)
		if p.Spec.Replicas != nil {
			replicas = *p.Spec.Replicas
		}
		desired = append(desired, desiredPool{
			nn:      types.NamespacedName{Namespace: p.Namespace, Name: p.Name},
			key:     key,
			targets: distribute(replicas, nodes),
		})
	}

	d.applyToNodes(ctx, nodes, desired)

	// Write each pool's status from freshly-read inventory (post-apply warm may
	// still be refilling; status reflects what the nodes report now).
	for i := range pools.Items {
		p := &pools.Items[i]
		key, kerr := d.poolKey(ctx, p)
		if kerr != nil {
			continue
		}
		d.writeStatus(ctx, p, nodes, key)
	}
	return nil
}

// schedulableNodes reads NodeInventory (O(nodes), cache-fed) into node views,
// keeping only nodes that advertise a sandboxd address.
func (d *Driver) schedulableNodes(ctx context.Context) ([]nodeView, error) {
	names, err := d.inv.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list node inventories: %w", err)
	}
	views := make([]nodeView, 0, len(names))
	for _, name := range names {
		inv, err := d.inv.NodeInventory(ctx, name)
		if err != nil {
			d.log.V(1).Info("skip node without readable inventory", "node", name, "err", err.Error())
			continue
		}
		if inv.Address == "" {
			continue
		}
		warmBy := make(map[scale.PoolKey]int, len(inv.Pools))
		for _, pc := range inv.Pools {
			warmBy[scale.PoolKey{Template: pc.Template, Net: pc.Net, Size: pc.Size}] = pc.Warm
		}
		views = append(views, nodeView{name: name, addr: inv.Address, warmBy: warmBy})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].name < views[j].name })
	return views, nil
}

// poolKey resolves a SandboxWarmPool's SandboxTemplate and derives the pool key
// via the shared scale.PoolKeyFor — identical to the key a Create derives.
func (d *Driver) poolKey(ctx context.Context, p *extv1beta1.SandboxWarmPool) (scale.PoolKey, error) {
	name := p.Spec.TemplateRef.Name
	if name == "" {
		return scale.PoolKey{}, errors.New("spec.sandboxTemplateRef.name is required")
	}
	var tmpl extv1beta1.SandboxTemplate
	if err := d.kube.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: name}, &tmpl); err != nil {
		return scale.PoolKey{}, fmt.Errorf("get SandboxTemplate %s/%s: %w", p.Namespace, name, err)
	}
	net := tmpl.Annotations[netAnnotation]
	if net == "" {
		net = tmpl.Spec.PodTemplate.ObjectMeta.Annotations[netAnnotation]
	}
	return scale.PoolKeyFor(tmpl.Spec.PodTemplate.Spec.Containers, net), nil
}

// applyToNodes builds each node's FULL desired pool set (one entry per pool) and
// PUTs it. The whole set goes in one request because sandboxd replaces its pool
// config wholesale — an omitted pool drains. Per-node failures are logged, not
// fatal: one unreachable node must not stall the rest.
func (d *Driver) applyToNodes(ctx context.Context, nodes []nodeView, desired []desiredPool) {
	if d.token == "" {
		d.log.Info("warm-pool driver has no sandboxd token; skipping pool apply (fail-closed)")
		return
	}
	sem := make(chan struct{}, maxNodeConcurrency)
	var wg sync.WaitGroup
	for i := range nodes {
		node := nodes[i]
		// Aggregate targets BY KEY: two SandboxWarmPools may resolve to the same
		// (template,net,size) — their warm targets sum, and sandboxd rejects a PUT
		// that repeats a key ("duplicate pool"). One spec per distinct key.
		byKey := make(map[scale.PoolKey]int, len(desired))
		for _, dp := range desired {
			byKey[dp.key] += dp.targets[node.name]
		}
		specs := make([]sandboxd.PoolSpec, 0, len(byKey))
		for key, warm := range byKey {
			specs = append(specs, sandboxd.PoolSpec{Template: key.Template, Net: key.Net, Size: key.Size, Warm: warm})
		}
		sort.Slice(specs, func(a, b int) bool {
			if specs[a].Template != specs[b].Template {
				return specs[a].Template < specs[b].Template
			}
			if specs[a].Net != specs[b].Net {
				return specs[a].Net < specs[b].Net
			}
			return specs[a].Size < specs[b].Size
		})
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := d.factory(node.addr, d.token).SetPools(ctx, specs); err != nil {
				d.log.Error(err, "set node warm pools", "node", node.name, "addr", node.addr)
			}
		}()
	}
	wg.Wait()
}

// writeStatus updates a SandboxWarmPool's status.replicas/readyReplicas from the
// live warm counts across all nodes for the pool's key. Warm VMs are claim-ready,
// so readyReplicas == replicas. Best-effort; a conflict is retried next tick.
func (d *Driver) writeStatus(ctx context.Context, p *extv1beta1.SandboxWarmPool, nodes []nodeView, key scale.PoolKey) {
	total := 0
	for i := range nodes {
		total += nodes[i].warmBy[key]
	}
	selector := "agents.x-k8s.io/warm-pool=" + p.Name
	if p.Status.Replicas == int32(total) && p.Status.ReadyReplicas == int32(total) && p.Status.Selector == selector {
		return
	}
	fresh := p.DeepCopy()
	fresh.Status.Replicas = int32(total)
	fresh.Status.ReadyReplicas = int32(total)
	fresh.Status.Selector = selector
	if err := d.kube.Status().Update(ctx, fresh); err != nil {
		d.log.V(1).Info("warm-pool status update deferred", "pool", p.Namespace+"/"+p.Name, "err", err.Error())
	}
}

// distribute spreads total warm targets evenly across nodes (base + remainder to
// the first nodes), matching the aggregated apiserver's most-warm-first node pick.
func distribute(replicas int32, nodes []nodeView) map[string]int {
	targets := make(map[string]int, len(nodes))
	if len(nodes) == 0 {
		return targets
	}
	total := int(replicas)
	if total < 0 {
		total = 0
	}
	base := total / len(nodes)
	remainder := total % len(nodes)
	for i := range nodes {
		t := base
		if i < remainder {
			t++
		}
		targets[nodes[i].name] = t
	}
	return targets
}
