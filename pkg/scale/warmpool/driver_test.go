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

package warmpool

import (
	"context"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale/sandboxd"
)

const testImage = "ghcr.io/cocoonstack/sandbox/rt@sha256:deadbeef"

// fakeSetter records the last pools set per node address.
type fakeSetter struct {
	mu   sync.Mutex
	byAddr map[string][]sandboxd.PoolSpec
}

func (f *fakeSetter) factory() ClientFactory {
	return func(addr, _ string) PoolSetter { return &fakeNode{addr: addr, parent: f} }
}

type fakeNode struct {
	addr   string
	parent *fakeSetter
}

func (n *fakeNode) SetPools(_ context.Context, pools []sandboxd.PoolSpec) (*sandboxd.NodeInfo, error) {
	n.parent.mu.Lock()
	defer n.parent.mu.Unlock()
	n.parent.byAddr[n.addr] = pools
	return &sandboxd.NodeInfo{}, nil
}

func newTestDriver(t *testing.T, objs ...client.Object) (*Driver, *fakeSetter, *scale.StaticInventorySource, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := extv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&extv1beta1.SandboxWarmPool{}).
		WithObjects(objs...).Build()
	inv := scale.NewStaticInventorySource()
	setter := &fakeSetter{byAddr: map[string][]sandboxd.PoolSpec{}}
	d := New(kube, inv, "tok", setter.factory(), Options{Interval: 0})
	return d, setter, inv, kube
}

func warmPool(name string, replicas int32) *extv1beta1.SandboxWarmPool {
	return &extv1beta1.SandboxWarmPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
		Spec: extv1beta1.SandboxWarmPoolSpec{
			Replicas:    ptr.To(replicas),
			TemplateRef: extv1beta1.SandboxTemplateRef{Name: "tpl"},
		},
	}
}

func template() *extv1beta1.SandboxTemplate {
	return &extv1beta1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "tpl"},
		Spec: extv1beta1.SandboxTemplateSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: testImage}}},
				},
			},
		},
	}
}

func putNodes(inv *scale.StaticInventorySource, n int) {
	for i := 0; i < n; i++ {
		name := string(rune('a' + i))
		inv.Put(&scale.NodeInventory{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Node:       name,
			Address:    "10.0.0." + name + ":7777",
		})
	}
}

// TestReconcileDistributesAndMatchesPoolKey pins the two load-bearing invariants:
// (1) a SandboxWarmPool's replicas are spread across all nodes summing to EXACTLY
// replicas, and (2) the pool key the driver sets is byte-identical to what the
// aggregated apiserver derives for a Sandbox with the same image — so a Create
// always finds the warm capacity (the G5 fix). A drift here means perpetual 503s.
func TestReconcileDistributesAndMatchesPoolKey(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p", 100), template())
	putNodes(inv, 26)

	if err := d.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Exactly 26 nodes were PUT, summing to exactly 100, spread evenly.
	if len(setter.byAddr) != 26 {
		t.Fatalf("PUT %d nodes, want 26", len(setter.byAddr))
	}
	want := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")
	sum, minWarm, maxWarm := 0, 1<<30, 0
	for _, specs := range setter.byAddr {
		if len(specs) != 1 {
			t.Fatalf("node got %d pool specs, want 1", len(specs))
		}
		s := specs[0]
		if s.Template != want.Template || s.Net != want.Net || s.Size != want.Size {
			t.Fatalf("pool key {%s,%s,%s} != apiserver-derived {%s,%s,%s}",
				s.Template, s.Net, s.Size, want.Template, want.Net, want.Size)
		}
		sum += s.Warm
		if s.Warm < minWarm {
			minWarm = s.Warm
		}
		if s.Warm > maxWarm {
			maxWarm = s.Warm
		}
	}
	if sum != 100 {
		t.Fatalf("targets sum to %d, want 100", sum)
	}
	if maxWarm-minWarm > 1 {
		t.Fatalf("uneven spread: min=%d max=%d", minWarm, maxWarm)
	}

	// Status is written back from live inventory warm (0 here — nothing refilled yet).
	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 0 {
		t.Fatalf("status.replicas = %d, want 0 (no warm reported yet)", got.Status.Replicas)
	}
}

// TestReconcileWritesWarmStatus verifies status reflects live NodeInventory warm.
func TestReconcileWritesWarmStatus(t *testing.T) {
	d, _, inv, kube := newTestDriver(t, warmPool("p", 8), template())
	key := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")
	// Two nodes each already reporting 4 warm of the pool's key → status 8.
	for _, name := range []string{"a", "b"} {
		inv.Put(&scale.NodeInventory{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Node:       name,
			Address:    "10.0.0." + name + ":7777",
			Pools:      []extv1beta1.PoolCapacity{{Template: key.Template, Net: key.Net, Size: key.Size, Warm: 4, Target: 4}},
		})
	}
	if err := d.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 8 || got.Status.ReadyReplicas != 8 {
		t.Fatalf("status replicas=%d ready=%d, want 8/8", got.Status.Replicas, got.Status.ReadyReplicas)
	}
}

// TestTwoPoolsSameKeyAggregate pins that two SandboxWarmPools resolving to the
// same pool key produce ONE spec per node with SUMMED warm — sandboxd rejects a
// PUT that repeats a key ("duplicate pool"), which silently stalled every node.
func TestTwoPoolsSameKeyAggregate(t *testing.T) {
	// Both pools reference the same template "tpl" → identical key.
	d, setter, inv, _ := newTestDriver(t, warmPool("p1", 3), warmPool2("p2", 5), template())
	putNodes(inv, 2)
	if err := d.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	sum := 0
	for addr, specs := range setter.byAddr {
		// The load-bearing invariant: ONE spec per node (keys aggregated, no
		// duplicate the PUT would reject), and every spec carries the shared key.
		if len(specs) != 1 {
			t.Fatalf("node %s got %d specs, want 1 aggregated (no duplicate key): %+v", addr, len(specs), specs)
		}
		if specs[0].Template != testImage {
			t.Fatalf("node %s wrong key %q", addr, specs[0].Template)
		}
		sum += specs[0].Warm
	}
	// p1(3) + p2(5) summed across the fleet = 8, regardless of per-node rounding.
	if sum != 8 {
		t.Fatalf("fleet warm total = %d, want 8 (3+5 summed)", sum)
	}
}

// warmPool2 builds a second pool referencing the same template.
func warmPool2(name string, replicas int32) *extv1beta1.SandboxWarmPool {
	p := warmPool(name, replicas)
	return p
}

// TestDrainOnZeroReplicas: replicas=0 sets every node's target to 0 (the
// control-plane drain — kubectl scale to 0 or delete recycles the pool).
func TestDrainOnZeroReplicas(t *testing.T) {
	d, setter, inv, _ := newTestDriver(t, warmPool("p", 0), template())
	putNodes(inv, 3)
	if err := d.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for addr, specs := range setter.byAddr {
		if len(specs) != 1 || specs[0].Warm != 0 {
			t.Fatalf("node %s target != 0 on drain: %+v", addr, specs)
		}
	}
}
