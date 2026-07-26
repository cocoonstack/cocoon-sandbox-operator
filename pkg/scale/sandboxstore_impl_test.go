package scale

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

func TestScatterGatherList_FlattensAllNodes(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns-a/s1", "Running"), entry("ns-b/s2", "Pending")))
	src.Put(inv("n2", entry("ns-a/s3", "Running")))
	store := NewScatterGatherStore(src)

	list, err := store.List(context.Background(), ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 3)

	// Sorted by namespace then name.
	assert.Equal(t, "ns-a/s1", list.Items[0].Namespace+"/"+list.Items[0].Name)
	assert.Equal(t, "ns-a/s3", list.Items[1].Namespace+"/"+list.Items[1].Name)
	assert.Equal(t, "ns-b/s2", list.Items[2].Namespace+"/"+list.Items[2].Name)

	// The owning node is stamped into status and the node label.
	byName := map[string]sandboxStatusView{}
	for _, it := range list.Items {
		byName[it.Name] = sandboxStatusView{node: it.Status.NodeName, label: it.Labels[NodeLabel]}
	}
	assert.Equal(t, "n1", byName["s1"].node)
	assert.Equal(t, "n1", byName["s1"].label)
	assert.Equal(t, "n2", byName["s3"].node)
}

func TestScatterGatherList_HonorsNamespaceFilter(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("team-a/s1", "Running"), entry("team-b/s2", "Running")))
	src.Put(inv("n2", entry("team-a/s3", "Running")))
	store := NewScatterGatherStore(src)

	list, err := store.List(context.Background(), ListOptions{Namespace: "team-a"})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	for _, it := range list.Items {
		assert.Equal(t, "team-a", it.Namespace)
	}
}

func TestScatterGatherList_HonorsLabelSelector(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/running", "Running")))
	src.Put(inv("n2", entry("ns/pending", "Pending")))
	store := NewScatterGatherStore(src)

	list, err := store.List(context.Background(), ListOptions{LabelSelector: PhaseLabel + "=Running"})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "running", list.Items[0].Name)

	byNode, err := store.List(context.Background(), ListOptions{LabelSelector: NodeLabel + "=n2"})
	require.NoError(t, err)
	require.Len(t, byNode.Items, 1)
	assert.Equal(t, "pending", byNode.Items[0].Name)
}

func TestScatterGatherList_HonorsFieldSelector(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/a", "Running"), entry("ns/b", "Running")))
	src.Put(inv("n2", entry("ns/c", "Running")))
	store := NewScatterGatherStore(src)

	byName, err := store.List(context.Background(), ListOptions{FieldSelector: "metadata.name=b"})
	require.NoError(t, err)
	require.Len(t, byName.Items, 1)
	assert.Equal(t, "b", byName.Items[0].Name)

	byNode, err := store.List(context.Background(), ListOptions{FieldSelector: "status.nodeName=n2"})
	require.NoError(t, err)
	require.Len(t, byNode.Items, 1)
	assert.Equal(t, "c", byNode.Items[0].Name)
}

func TestScatterGatherList_ToleratesPartitionedNode(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/s1", "Running")))
	src.Put(inv("n2", entry("ns/s2", "Running")))
	src.Partition("n2") // listed by ListNodes but its inventory fetch fails
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()))

	// A partitioned node degrades to eventual consistency: its sandboxes are
	// omitted, but the list still succeeds with the reachable node's sandboxes.
	list, err := store.List(context.Background(), ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "s1", list.Items[0].Name)
}

func TestScatterGatherGet_RoutesToOwningNode(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/s1", "Running")))
	src.Put(inv("n2", entry("ns/s2", "Pending")))
	store := NewScatterGatherStore(src)

	got, err := store.Get(context.Background(), "ns", "s2")
	require.NoError(t, err)
	assert.Equal(t, "s2", got.Name)
	assert.Equal(t, "ns", got.Namespace)
	assert.Equal(t, "n2", got.Status.NodeName)

	_, err = store.Get(context.Background(), "ns", "missing")
	require.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err), "expected NotFound, got %v", err)
}

func TestScatterGatherGet_SynthesizesStatus(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", InventoryEntry{Name: "ns/s1", ID: "sb_abc123", Phase: "Running", ClaimRef: "ns/claim-1", Address: "10.1.2.3:7777"}))
	store := NewScatterGatherStore(src)

	got, err := store.Get(context.Background(), "ns", "s1")
	require.NoError(t, err)
	assert.Equal(t, []string{"10.1.2.3"}, got.Status.PodIPs)
	assert.Equal(t, "claim-1", got.Labels[ClaimLabel])
	// The sandboxd claim id rides as an annotation so the apiserver's Delete can
	// release exactly this microVM (never by k8s name).
	assert.Equal(t, "sb_abc123", got.Annotations[ClaimIDAnnotation])
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, metav1.ConditionTrue, got.Status.Conditions[0].Status)
	assert.Equal(t, "Running", got.Status.Conditions[0].Reason)
}

func TestEntryToSandbox_StampsClaimIDAnnotation(t *testing.T) {
	withID := entryToSandbox("n1", InventoryEntry{Name: "ns/s1", ID: "sb_abc", Phase: "Running"})
	assert.Equal(t, "sb_abc", withID.Annotations[ClaimIDAnnotation])

	// A node that has not published the id yet stamps no annotation, so Delete
	// refuses to release rather than guessing by name.
	noID := entryToSandbox("n1", InventoryEntry{Name: "ns/s1", Phase: "Running"})
	_, ok := noID.Annotations[ClaimIDAnnotation]
	assert.False(t, ok, "expected no claim-id annotation when the entry has no id")
}

func TestScatterGatherWatch_EmitsAddModifyDelete(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/s1", "Pending")))
	store := NewScatterGatherStore(src, WithWatchPollInterval(10*time.Millisecond))

	ctx := t.Context()
	w, err := store.Watch(ctx, ListOptions{})
	require.NoError(t, err)
	defer w.Stop()

	added := waitForType(t, w, watch.Added, time.Second)
	assert.Equal(t, "s1", added.Object.(*sandboxv1beta1.Sandbox).Name)

	// Phase change → Modified (a content-sensitive ResourceVersion changed).
	src.Put(inv("n1", entry("ns/s1", "Running")))
	waitForType(t, w, watch.Modified, 2*time.Second)

	// Entry disappears → Deleted.
	src.Put(inv("n1"))
	waitForType(t, w, watch.Deleted, 2*time.Second)
}

func TestScatterGather_ObjectCountIsPoolsPlusNodes(t *testing.T) {
	const nodes, perNode = 4, 250
	ctx := context.Background()
	src := NewStaticInventorySource()

	for k := range nodes {
		node := fmt.Sprintf("n%d", k)
		entries := make(sliceLive, 0, perNode)
		for i := range perNode {
			entries = append(entries, entry(fmt.Sprintf("ns/s-%d-%d", k, i), "Running"))
		}
		pub := NewNodeInventoryPublisher(node, entries, src, logr.Discard())
		n, err := pub.Publish(ctx)
		require.NoError(t, err)
		require.Equal(t, perNode, n)
	}

	store := NewScatterGatherStore(src)
	list, err := store.List(ctx, ListOptions{})
	require.NoError(t, err)

	// The store serves every sandbox...
	require.Len(t, list.Items, nodes*perNode)
	// ...while the durable object count is O(nodes) NodeInventory (pool intent
	// objects are separate and counted in the l3 aggregation evidence): the write
	// path is one server-side-apply per node, not per sandbox.
	assert.Equal(t, nodes, src.ObjectCount())
	assert.Equal(t, nodes, src.ApplyCount())
	// The etcd object count must NOT scale with the sandbox count.
	assert.Less(t, src.ObjectCount(), len(list.Items))
}

func TestPublisher_RebuildsFromLiveAfterLoss(t *testing.T) {
	ctx := context.Background()
	live := &mutableLive{entries: []InventoryEntry{entry("ns/a", "Running")}}
	src := NewStaticInventorySource()
	pub := NewNodeInventoryPublisher("n1", live, src, logr.Discard())

	_, err := pub.Publish(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, src.ObjectCount())

	// The node's NodeInventory object is lost.
	src.Remove("n1")
	require.Equal(t, 0, src.ObjectCount())

	// Live state moved on; the next publish rebuilds the object from live state.
	live.entries = []InventoryEntry{entry("ns/a", "Running"), entry("ns/b", "Running")}
	n, err := pub.Publish(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	got, err := src.NodeInventory(ctx, "n1")
	require.NoError(t, err)
	require.Len(t, got.Entries, 2)
}

func TestNodeInventory_DeepCopyIsIndependent(t *testing.T) {
	orig := inv("n1", entry("ns/a", "Running"))
	cp := orig.DeepCopy()
	cp.Entries[0].Phase = "Pending"
	cp.Node = "n2"
	assert.Equal(t, "Running", orig.Entries[0].Phase, "deep copy must not alias entries")
	assert.Equal(t, "n1", orig.Node)

	// DeepCopyObject returns an independent runtime.Object of the same type.
	obj := orig.DeepCopyObject()
	require.NotNil(t, obj)
	clone, ok := obj.(*NodeInventory)
	require.True(t, ok)
	assert.Equal(t, "n1", clone.Node)
}

func TestWatchSeesAShortLivedSandbox(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("sb-1", "Running")))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithWatchPollInterval(10*time.Millisecond))

	w, err := store.Watch(t.Context(), ListOptions{})
	require.NoError(t, err)
	defer w.Stop()
	require.Equal(t, watch.Added, (<-w.ResultChan()).Type)

	// A sandbox that comes and goes must still be observed. A watch that widened
	// its interval while quiet would step over this entirely.
	src.Put(inv("n1", entry("sb-1", "Running"), entry("sb-2", "Running")))

	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-w.ResultChan():
			if ev.Type == watch.Added {
				return
			}
		case <-deadline:
			t.Fatal("a short-lived sandbox produced no event")
		}
	}
}

func inv(node string, entries ...InventoryEntry) *NodeInventory {
	return &NodeInventory{
		ObjectMeta: metav1.ObjectMeta{Name: node},
		Node:       node,
		Entries:    entries,
	}
}

func entry(name, phase string) InventoryEntry { return InventoryEntry{Name: name, Phase: phase} }

// sliceLive is a NodeLiveSource fed from an in-memory slice.
type sliceLive []InventoryEntry

func (s sliceLive) LiveSandboxes(context.Context) ([]InventoryEntry, error) {
	return []InventoryEntry(s), nil
}

// mutableLive is a NodeLiveSource whose entries can change between publishes.
type mutableLive struct{ entries []InventoryEntry }

func (m *mutableLive) LiveSandboxes(context.Context) ([]InventoryEntry, error) {
	return m.entries, nil
}

type sandboxStatusView struct{ node, label string }

// waitForType drains events until one of type want arrives or the deadline hits.
func waitForType(t *testing.T, w watch.Interface, want watch.EventType, timeout time.Duration) watch.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-w.ResultChan():
			require.True(t, ok, "watch channel closed before %s event", want)
			if ev.Type == want {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s event", want)
			return watch.Event{}
		}
	}
}
