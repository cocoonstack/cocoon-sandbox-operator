package scale

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

// Fleet shapes: the deployed 26-node fleet and the 50k-microVM projection.
var benchFleets = []struct {
	name    string
	nodes   int
	perNode int
}{
	{"26x100", 26, 100},
	{"200x2000", 200, 2000},
}

// BenchmarkStoreGet resolves one sandbox living on the last node in enumeration
// order — the sequential worst case a Get pays on a miss-heavy sweep.
func BenchmarkStoreGet(b *testing.B) {
	for _, fleet := range benchFleets {
		b.Run(fleet.name, func(b *testing.B) {
			store, _ := benchStore(b, fleet.nodes, fleet.perNode)
			last := benchNodeName(fleet.nodes - 1)
			target := fmt.Sprintf("default/sb-%s-%d", last, fleet.perNode-1)
			ns, name := splitNamespacedName(target)
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := store.Get(ctx, ns, name); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}

// BenchmarkStoreWarmCandidates sweeps every node inventory for warm capacity —
// the node-pick cost every aggregated Create/claim pays.
func BenchmarkStoreWarmCandidates(b *testing.B) {
	for _, fleet := range benchFleets {
		b.Run(fleet.name, func(b *testing.B) {
			store, pool := benchStore(b, fleet.nodes, fleet.perNode)
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				candidates, err := store.warmCandidates(ctx, pool)
				if err != nil {
					b.Fatalf("warm candidates: %v", err)
				}
				if len(candidates) != fleet.nodes {
					b.Fatalf("got %d candidates, want %d", len(candidates), fleet.nodes)
				}
			}
		})
	}
}

// benchStore publishes nodes×perNode inventory entries, every node advertising
// warm capacity for the returned pool key.
func benchStore(b *testing.B, nodes, perNode int) (*scatterGatherStore, PoolKey) {
	b.Helper()
	pool := PoolKey{Template: "ghcr.io/cocoonstack/sandbox/rt:24.04", Net: NetDefault, Size: SizeClassSmall}
	src := NewStaticInventorySource()
	for n := range nodes {
		name := benchNodeName(n)
		entries := make([]InventoryEntry, perNode)
		for i := range entries {
			entries[i] = InventoryEntry{
				Name:    fmt.Sprintf("default/sb-%s-%d", name, i),
				ID:      fmt.Sprintf("sb_%s_%d", name, i),
				Phase:   "Running",
				Address: "10.0.0.1:7777",
			}
		}
		src.Put(&NodeInventory{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Node:       name,
			Address:    "10.0.0.1:7777",
			Entries:    entries,
			Pools:      []extv1beta1.PoolCapacity{{Template: pool.Template, Net: pool.Net, Size: pool.Size, Warm: 5, Target: 5}},
		})
	}
	return NewScatterGatherStore(src), pool
}

func benchNodeName(n int) string { return fmt.Sprintf("node-%03d", n) }
