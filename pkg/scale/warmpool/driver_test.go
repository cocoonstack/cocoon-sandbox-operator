package warmpool

import (
	"context"
	"errors"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd"
)

const testImage = "ghcr.io/cocoonstack/sandbox/rt@sha256:deadbeef"

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

	// Status is written back from the warm each node reported (0 here — targets
	// accepted, nothing refilled yet).
	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 0 {
		t.Fatalf("status.replicas = %d, want 0 (no warm reported yet)", got.Status.Replicas)
	}
}

// TestReconcileWritesWarmStatus verifies status reflects the live warm each node
// reports in its PUT response.
func TestReconcileWritesWarmStatus(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p", 8), template())
	key := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")
	// Two nodes each already reporting 4 warm of the pool's key → status 8.
	for _, name := range []string{"a", "b"} {
		inv.Put(&scale.NodeInventory{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Node:       name,
			Address:    "10.0.0." + name + ":7777",
			Pools:      []extv1beta1.PoolCapacity{{Template: key.Template, Net: key.Net, Size: key.Size, Warm: 4, Target: 4}},
		})
		setter.reportWarm("10.0.0."+name+":7777", 4)
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

// TestStatusPrefersPutResponseOverStaleInventory pins WHY status is sourced from
// the PUT response: NodeInventory is published on its own cadence (30s by
// default) and is read before this tick's apply, so a fill in progress reads
// low. Sampling the response instead makes the reported total as fresh as the
// resync interval — that equality is what the 5s sampling period rests on.
func TestStatusPrefersPutResponseOverStaleInventory(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p", 20), template())
	key := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")
	for _, name := range []string{"a", "b"} {
		addr := "10.0.0." + name + ":7777"
		// Inventory still carries the pre-fill snapshot: 4 warm per node.
		inv.Put(&scale.NodeInventory{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Node:       name,
			Address:    addr,
			Pools:      []extv1beta1.PoolCapacity{{Template: key.Template, Net: key.Net, Size: key.Size, Warm: 4, Target: 10}},
		})
		// The node itself now reports 7 — the truth as of this tick.
		setter.reportWarm(addr, 7)
	}
	if err := d.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 14 {
		t.Fatalf("status.replicas = %d, want 14 (live 7+7, not stale 4+4)", got.Status.Replicas)
	}
}

// TestStatusFallsBackToInventoryWhenPutFails pins that a node the driver could
// not reach keeps contributing its last known inventory warm, so one unreachable
// node cannot make the fleet total collapse toward zero.
func TestStatusFallsBackToInventoryWhenPutFails(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p", 20), template())
	key := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")
	for _, name := range []string{"a", "b"} {
		addr := "10.0.0." + name + ":7777"
		inv.Put(&scale.NodeInventory{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Node:       name,
			Address:    addr,
			Pools:      []extv1beta1.PoolCapacity{{Template: key.Template, Net: key.Net, Size: key.Size, Warm: 5, Target: 10}},
		})
		setter.reportWarm(addr, 9)
	}
	setter.failAddr = "10.0.0.b:7777"
	if err := d.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 14 {
		t.Fatalf("status.replicas = %d, want 14 (live 9 from a, inventory 5 from unreachable b)", got.Status.Replicas)
	}
}

// TestTwoPoolsSameKeyAggregate pins that two SandboxWarmPools resolving to the
// same pool key produce ONE spec per node with SUMMED warm — sandboxd rejects a
// PUT that repeats a key ("duplicate pool"), which silently stalled every node.
func TestTwoPoolsSameKeyAggregate(t *testing.T) {
	// Both pools reference the same template "tpl" → identical key.
	d, setter, inv, _ := newTestDriver(t, warmPool("p1", 3), warmPool("p2", 5), template())
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

// TestApplyBoundsEachNodeCall pins the sandboxd client contract: the client has
// no HTTP timeout, so the driver must bound every SetPools with a deadline or a
// connected-but-silent node wedges the global reconcile forever.
func TestApplyBoundsEachNodeCall(t *testing.T) {
	d, setter, inv, _ := newTestDriver(t, warmPool("p", 3), template())
	putNodes(inv, 2)
	if err := d.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !setter.sawDeadline {
		t.Fatal("SetPools ran without a context deadline; a silent node would block reconcile forever")
	}
}

// fakeSetter records the last pools set per node address and plays back the warm
// counts a node reports in its PUT response — the driver's status source.
type fakeSetter struct {
	mu     sync.Mutex
	byAddr map[string][]sandboxd.PoolSpec
	// warm is the warm count each node echoes per pool; absent means 0 (target
	// accepted, nothing filled yet).
	warm map[string]int
	// failAddr, when set, makes that node's PUT fail.
	failAddr string
	// sawDeadline records whether the most recent PUT carried a context deadline.
	sawDeadline bool
}

// reportWarm makes addr answer every PUT reporting n warm for each pool it holds.
func (f *fakeSetter) reportWarm(addr string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.warm == nil {
		f.warm = map[string]int{}
	}
	f.warm[addr] = n
}

func (f *fakeSetter) factory() ClientFactory {
	return func(addr, _ string) PoolSetter { return &fakeNode{addr: addr, parent: f} }
}

type fakeNode struct {
	addr   string
	parent *fakeSetter
}

func (n *fakeNode) SetPools(ctx context.Context, pools []sandboxd.PoolSpec) (*sandboxd.NodeInfo, error) {
	n.parent.mu.Lock()
	defer n.parent.mu.Unlock()
	_, n.parent.sawDeadline = ctx.Deadline()
	if n.parent.failAddr == n.addr {
		return nil, errors.New("node unreachable")
	}
	n.parent.byAddr[n.addr] = pools
	// Mirror real sandboxd: the response echoes every pool the node now holds,
	// each with its live warm count.
	info := &sandboxd.NodeInfo{}
	for _, p := range pools {
		info.Pools = append(info.Pools, sandboxd.NodePool{
			Key:  sandboxd.PoolKey{Template: p.Template, Net: p.Net, Size: p.Size},
			Warm: n.parent.warm[n.addr],
			// Target echoes the requested warm watermark.
			Target: p.Warm,
		})
	}
	return info, nil
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
			Replicas:    new(replicas),
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
	for i := range n {
		name := string(rune('a' + i))
		inv.Put(&scale.NodeInventory{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Node:       name,
			Address:    "10.0.0." + name + ":7777",
		})
	}
}
