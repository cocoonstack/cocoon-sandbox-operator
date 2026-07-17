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
	"fmt"
	"hash/fnv"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
)

// Synthesized-Sandbox label keys. The aggregated store stamps these onto every
// Sandbox it materializes from a NodeInventory entry so label selectors (the
// `kubectl get sandboxes -l ...` path) have real axes to filter on without any
// per-sandbox etcd object.
const (
	// NodeLabel carries the owning node of a synthesized Sandbox.
	NodeLabel = "sandbox.cocoonstack.io/node"
	// PhaseLabel carries the entry phase of a synthesized Sandbox.
	PhaseLabel = "sandbox.cocoonstack.io/phase"
	// ClaimLabel carries the claim name a synthesized Sandbox is bound to.
	ClaimLabel = "sandbox.cocoonstack.io/claim"
)

// NodeInventoryGVK is the GroupVersionKind of the O(nodes) intent object the
// publisher server-side-applies. It shares the sandboxes group so one APIService
// and one RBAC surface cover both.
var NodeInventoryGVK = schema.GroupVersionKind{
	Group:   sandboxv1beta1.GroupVersion.Group,
	Version: sandboxv1beta1.GroupVersion.Version,
	Kind:    "NodeInventory",
}

// InventorySource enumerates the per-node NodeInventory objects that back the
// aggregated store. It is deliberately granular — a node enumeration plus a
// per-node fetch — rather than one cluster-wide read, so:
//
//   - List fans out per node with bounded concurrency and a single partitioned
//     node degrades to eventual consistency (its sandboxes are briefly absent)
//     instead of failing the whole list, and
//   - Get can route to the single owning node (the README "route Get to the
//     owning node, not the summary" contract).
//
// In production ListNodes/NodeInventory are served from a cache-fed client
// listing NodeInventory objects at ResourceVersion=0 (O(nodes), never a hot-path
// LIST off etcd); tests inject StaticInventorySource.
type InventorySource interface {
	// ListNodes returns the nodes that publish inventory. O(nodes), cache-fed.
	ListNodes(ctx context.Context) ([]string, error)
	// NodeInventory returns one node's authoritative inventory. A partitioned or
	// not-yet-published node returns an error, which List logs and skips.
	NodeInventory(ctx context.Context, node string) (*NodeInventory, error)
}

// StoreOption configures a scatterGatherStore.
type StoreOption func(*scatterGatherStore)

// WithLogger sets the store logger. The zero logr.Logger discards.
func WithLogger(log logr.Logger) StoreOption {
	return func(s *scatterGatherStore) { s.log = log }
}

// WithConcurrency bounds the per-node fan-out. Values <=0 leave it unbounded.
func WithConcurrency(n int) StoreOption {
	return func(s *scatterGatherStore) { s.concurrency = n }
}

// WithWatchPollInterval sets how often Watch re-derives node inventory to emit
// deltas. Defaults to one second.
func WithWatchPollInterval(d time.Duration) StoreOption {
	return func(s *scatterGatherStore) { s.watchPoll = d }
}

// scatterGatherStore is the concrete SandboxStore: List/Get/Watch synthesize
// Sandbox objects from live NodeInventory rather than reading any per-sandbox
// etcd object, exactly the metrics.k8s.io aggregation pattern.
type scatterGatherStore struct {
	src         InventorySource
	log         logr.Logger
	concurrency int
	watchPoll   time.Duration
}

// Compile-time assertions for the L3 contracts.
var (
	_ SandboxStore     = (*scatterGatherStore)(nil)
	_ InventorySource  = (*StaticInventorySource)(nil)
	_ InventorySource  = (*ClientInventorySource)(nil)
	_ InventoryApplier = (*StaticInventorySource)(nil)
	_ InventoryApplier = (*ssaInventoryApplier)(nil)
	_ runtime.Object   = (*NodeInventory)(nil)
)

// NewScatterGatherStore builds the aggregated store over src.
func NewScatterGatherStore(src InventorySource, opts ...StoreOption) *scatterGatherStore {
	s := &scatterGatherStore{
		src:         src,
		concurrency: 16,
		watchPoll:   time.Second,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// List assembles a SandboxList by fanning out to every node inventory with
// bounded concurrency, flattening entries into Sandboxes and honoring the
// namespace/label/field filters. A node whose inventory is unavailable
// (partitioned, or its NodeInventory lost before the next publish) is logged and
// omitted — eventual consistency, never a whole-list failure.
func (s *scatterGatherStore) List(ctx context.Context, opts ListOptions) (*sandboxv1beta1.SandboxList, error) {
	labelSel, fieldSel, err := parseSelectors(opts)
	if err != nil {
		return nil, err
	}
	nodes, err := s.src.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("scale: enumerate node inventories: %w", err)
	}

	perNode := make([][]sandboxv1beta1.Sandbox, len(nodes))
	g, gctx := errgroup.WithContext(ctx)
	if s.concurrency > 0 {
		g.SetLimit(s.concurrency)
	}
	for i, node := range nodes {
		g.Go(func() error {
			inv, err := s.src.NodeInventory(gctx, node)
			if err != nil {
				// Partitioned / lost inventory: skip this node's sandboxes rather
				// than failing the list. They reappear on the node's next publish.
				s.log.V(1).Info("node inventory unavailable; omitting from list (eventual consistency)",
					"node", node, "err", err.Error())
				return nil
			}
			perNode[i] = s.materialize(inv, opts.Namespace, labelSel, fieldSel)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("scale: fan-out to node inventories: %w", err)
	}

	total := 0
	for _, chunk := range perNode {
		total += len(chunk)
	}
	list := &sandboxv1beta1.SandboxList{}
	list.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("SandboxList"))
	list.Items = make([]sandboxv1beta1.Sandbox, 0, total)
	for _, chunk := range perNode {
		list.Items = append(list.Items, chunk...)
	}
	sort.Slice(list.Items, func(a, b int) bool {
		if list.Items[a].Namespace != list.Items[b].Namespace {
			return list.Items[a].Namespace < list.Items[b].Namespace
		}
		return list.Items[a].Name < list.Items[b].Name
	})
	return list, nil
}

// Get routes to the owning node's authoritative inventory. It resolves which
// node holds namespace/name and returns that entry synthesized as a Sandbox.
//
// In production this would RPC the owning node's live sandboxd for a
// read-after-write answer; in this substrate the node's published NodeInventory
// is the authoritative view available, so Get returns from it directly rather
// than from an eventually-consistent cluster-wide summary.
func (s *scatterGatherStore) Get(ctx context.Context, namespace, name string) (*sandboxv1beta1.Sandbox, error) {
	nodes, err := s.src.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("scale: enumerate node inventories: %w", err)
	}
	for _, node := range nodes {
		inv, err := s.src.NodeInventory(ctx, node)
		if err != nil {
			s.log.V(1).Info("node inventory unavailable during get; trying next node",
				"node", node, "err", err.Error())
			continue
		}
		for i := range inv.Entries {
			ens, ename := splitNamespacedName(inv.Entries[i].Name)
			if ens == namespace && ename == name {
				return entryToSandbox(inv.Node, inv.Entries[i]), nil
			}
		}
	}
	return nil, k8serrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), name)
}

// Watch merges per-node inventory into a single Sandbox event stream. This
// minimal-correct implementation re-derives the fanned-out list on a slow cadence
// and translates the diff into Added/Modified/Deleted events; a production
// implementation would merge real per-node watch streams instead of polling.
func (s *scatterGatherStore) Watch(ctx context.Context, opts ListOptions) (watch.Interface, error) {
	if _, _, err := parseSelectors(opts); err != nil {
		return nil, err
	}
	w := newInventoryWatcher()
	go s.runWatch(ctx, opts, w)
	return w, nil
}

func (s *scatterGatherStore) runWatch(ctx context.Context, opts ListOptions, w *inventoryWatcher) {
	defer w.terminate()

	known := map[string]*sandboxv1beta1.Sandbox{}
	emit := func(t watch.EventType, sb *sandboxv1beta1.Sandbox) bool {
		return w.send(ctx, watch.Event{Type: t, Object: sb})
	}

	// Initial synchronization: an Added for every currently-visible sandbox.
	if list, err := s.List(ctx, opts); err != nil {
		s.log.Error(err, "initial watch list failed")
	} else {
		for i := range list.Items {
			sb := list.Items[i].DeepCopy()
			known[objKey(sb)] = sb
			if !emit(watch.Added, sb) {
				return
			}
		}
	}

	ticker := time.NewTicker(s.watchPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case <-ticker.C:
			list, err := s.List(ctx, opts)
			if err != nil {
				s.log.Error(err, "watch poll list failed")
				continue
			}
			cur := make(map[string]*sandboxv1beta1.Sandbox, len(list.Items))
			for i := range list.Items {
				sb := list.Items[i].DeepCopy()
				k := objKey(sb)
				cur[k] = sb
				prev, ok := known[k]
				switch {
				case !ok:
					if !emit(watch.Added, sb) {
						return
					}
				case prev.ResourceVersion != sb.ResourceVersion:
					if !emit(watch.Modified, sb) {
						return
					}
				}
			}
			for k, prev := range known {
				if _, ok := cur[k]; !ok {
					if !emit(watch.Deleted, prev) {
						return
					}
				}
			}
			known = cur
		}
	}
}

// materialize turns one node's inventory entries into filtered Sandboxes.
func (s *scatterGatherStore) materialize(inv *NodeInventory, namespace string, labelSel labels.Selector, fieldSel fields.Selector) []sandboxv1beta1.Sandbox {
	out := make([]sandboxv1beta1.Sandbox, 0, len(inv.Entries))
	for i := range inv.Entries {
		sb := entryToSandbox(inv.Node, inv.Entries[i])
		if namespace != "" && sb.Namespace != namespace {
			continue
		}
		if !labelSel.Matches(labels.Set(sb.Labels)) {
			continue
		}
		if !fieldSel.Matches(sandboxFields(sb)) {
			continue
		}
		out = append(out, *sb)
	}
	return out
}

// parseSelectors turns the string selectors on ListOptions into matchers. Empty
// strings become everything-matchers.
func parseSelectors(opts ListOptions) (labels.Selector, fields.Selector, error) {
	labelSel, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("scale: parse label selector %q: %w", opts.LabelSelector, err)
	}
	fieldSel, err := fields.ParseSelector(opts.FieldSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("scale: parse field selector %q: %w", opts.FieldSelector, err)
	}
	return labelSel, fieldSel, nil
}

// entryToSandbox synthesizes the Sandbox object served for one inventory entry.
// The entry name is the sandbox's "<namespace>/<name>"; an unqualified name
// lands in the default namespace.
func entryToSandbox(node string, e InventoryEntry) *sandboxv1beta1.Sandbox {
	ns, name := splitNamespacedName(e.Name)
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            name,
			Labels:          synthLabels(node, e),
			ResourceVersion: resourceVersionFor(ns, name, e),
		},
		Status: sandboxv1beta1.SandboxStatus{
			NodeName: node,
			PodIPs:   addressIPs(e.Address),
			Conditions: []metav1.Condition{{
				Type:    string(sandboxv1beta1.SandboxConditionReady),
				Status:  readyStatus(e.Phase),
				Reason:  readyReason(e.Phase),
				Message: fmt.Sprintf("phase %q reported by node %q inventory", e.Phase, node),
			}},
		},
	}
	return sb
}

func synthLabels(node string, e InventoryEntry) map[string]string {
	l := map[string]string{NodeLabel: node}
	if e.Phase != "" {
		l[PhaseLabel] = e.Phase
	}
	if e.ClaimRef != "" {
		_, claim := splitNamespacedName(e.ClaimRef)
		if claim != "" {
			l[ClaimLabel] = claim
		}
	}
	return l
}

func sandboxFields(sb *sandboxv1beta1.Sandbox) fields.Set {
	return fields.Set{
		"metadata.name":      sb.Name,
		"metadata.namespace": sb.Namespace,
		"status.nodeName":    sb.Status.NodeName,
	}
}

func readyStatus(phase string) metav1.ConditionStatus {
	if strings.EqualFold(phase, "Running") || strings.EqualFold(phase, "Ready") {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func readyReason(phase string) string {
	if phase == "" {
		return "Unknown"
	}
	return phase
}

// addressIPs strips the port from a host:port address, yielding the pod IP list.
func addressIPs(addr string) []string {
	if addr == "" {
		return nil
	}
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		return []string{host}
	}
	return []string{addr}
}

// resourceVersionFor derives a deterministic, content-sensitive ResourceVersion
// so watch can detect a Modified entry and clients see a stable version for an
// unchanged one. It is opaque, as the API contract requires.
func resourceVersionFor(ns, name string, e InventoryEntry) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(ns + "/" + name + "|" + e.Phase + "|" + e.ClaimRef + "|" + e.Address))
	return fmt.Sprintf("%d", h.Sum64())
}

func splitNamespacedName(s string) (namespace, name string) {
	if idx := strings.IndexByte(s, '/'); idx >= 0 {
		return s[:idx], s[idx+1:]
	}
	return metav1.NamespaceDefault, s
}

func objKey(sb *sandboxv1beta1.Sandbox) string { return sb.Namespace + "/" + sb.Name }

// --- Watch plumbing ----------------------------------------------------------

// inventoryWatcher is a watch.Interface fed by the store's poll-diff goroutine.
type inventoryWatcher struct {
	result chan watch.Event
	done   chan struct{}
	once   sync.Once
}

func newInventoryWatcher() *inventoryWatcher {
	return &inventoryWatcher{
		result: make(chan watch.Event, 64),
		done:   make(chan struct{}),
	}
}

func (w *inventoryWatcher) ResultChan() <-chan watch.Event { return w.result }

// Stop signals the producer to exit; safe to call multiple times.
func (w *inventoryWatcher) Stop() {
	w.once.Do(func() { close(w.done) })
}

// terminate is called once by the producer goroutine as it exits, closing the
// result channel so consumers observe end-of-stream.
func (w *inventoryWatcher) terminate() { close(w.result) }

// send delivers ev unless the watch has been stopped or the context is done.
func (w *inventoryWatcher) send(ctx context.Context, ev watch.Event) bool {
	select {
	case w.result <- ev:
		return true
	case <-w.done:
		return false
	case <-ctx.Done():
		return false
	}
}

// --- NodeInventory as a runtime.Object ---------------------------------------

// DeepCopyInto copies the receiver into out.
func (in *NodeInventory) DeepCopyInto(out *NodeInventory) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	if in.Entries != nil {
		out.Entries = make([]InventoryEntry, len(in.Entries))
		copy(out.Entries, in.Entries)
	}
}

// DeepCopy returns a deep copy of the NodeInventory.
func (in *NodeInventory) DeepCopy() *NodeInventory {
	if in == nil {
		return nil
	}
	out := new(NodeInventory)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject satisfies runtime.Object so the inventory can flow through
// client machinery.
func (in *NodeInventory) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// --- NodeInventory publisher (the O(nodes) write path) -----------------------

// NodeLiveSource is a node's own live sandbox state — the sandboxd inventory /
// L0 node-scoped cache — NOT a cluster-wide LIST. A lost NodeInventory object is
// rebuilt from this on the next publish.
type NodeLiveSource interface {
	LiveSandboxes(ctx context.Context) ([]InventoryEntry, error)
}

// InventoryApplier server-side-applies a NodeInventory object. The default
// implementation resolves the resource through a RESTMapper (never a naive
// kind+"s"); tests inject a fake.
type InventoryApplier interface {
	Apply(ctx context.Context, inv *NodeInventory) error
}

// NodeInventoryPublisher server-side-applies one NodeInventory object for its
// node on a slow cadence, summarizing the node's live sandboxes. This is the
// entire L3 write path: O(nodes) applies, no per-sandbox etcd object.
type NodeInventoryPublisher struct {
	node    string
	live    NodeLiveSource
	applier InventoryApplier
	log     logr.Logger
}

// NewNodeInventoryPublisher builds a publisher for node, reading live state from
// live and applying via applier.
func NewNodeInventoryPublisher(node string, live NodeLiveSource, applier InventoryApplier, log logr.Logger) *NodeInventoryPublisher {
	return &NodeInventoryPublisher{node: node, live: live, applier: applier, log: log}
}

// Publish reads the node's live sandboxes and server-side-applies a single
// NodeInventory object for the node, returning the number of summarized entries.
func (p *NodeInventoryPublisher) Publish(ctx context.Context) (int, error) {
	entries, err := p.live.LiveSandboxes(ctx)
	if err != nil {
		return 0, fmt.Errorf("scale: read node %q live sandboxes: %w", p.node, err)
	}
	inv := &NodeInventory{
		TypeMeta: metav1.TypeMeta{
			Kind:       NodeInventoryGVK.Kind,
			APIVersion: NodeInventoryGVK.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: p.node},
		Node:       p.node,
		Entries:    entries,
	}
	if err := p.applier.Apply(ctx, inv); err != nil {
		return 0, fmt.Errorf("scale: apply node %q inventory: %w", p.node, err)
	}
	return len(entries), nil
}

// PublishPeriodically runs Publish on interval until ctx is cancelled. Publish
// failures are logged, not fatal — the next tick rebuilds from live state.
func (p *NodeInventoryPublisher) PublishPeriodically(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if n, err := p.Publish(ctx); err != nil {
			p.log.Error(err, "node inventory publish failed; will retry on next tick", "node", p.node)
		} else {
			p.log.V(1).Info("published node inventory", "node", p.node, "entries", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ssaInventoryApplier is the production applier: it server-side-applies the
// NodeInventory through a controller-runtime client, which resolves the resource
// via its RESTMapper from the object's GVK — never a naive kind+"s"
// pluralization. It works on any client (a cache-fed client in production).
type ssaInventoryApplier struct {
	c          client.Client
	fieldOwner string
}

// NewSSAInventoryApplier returns the default server-side-apply InventoryApplier.
func NewSSAInventoryApplier(c client.Client, fieldOwner string) InventoryApplier {
	if fieldOwner == "" {
		fieldOwner = "cocoon-node-inventory-publisher"
	}
	return &ssaInventoryApplier{c: c, fieldOwner: fieldOwner}
}

func (a *ssaInventoryApplier) Apply(ctx context.Context, inv *NodeInventory) error {
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(inv)
	if err != nil {
		return fmt.Errorf("scale: encode node inventory: %w", err)
	}
	u := &unstructured.Unstructured{Object: raw}
	u.SetGroupVersionKind(NodeInventoryGVK)
	u.SetName(inv.Node)
	// Server-side apply: the client resolves the GVR from the GVK via its
	// RESTMapper (never a naive kind+"s").
	ac := client.ApplyConfigurationFromUnstructured(u)
	if err := a.c.Apply(ctx, ac, client.FieldOwner(a.fieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("scale: server-side-apply node inventory: %w", err)
	}
	return nil
}

// --- Static in-memory InventorySource (tests, bench, and the publish target) --

// StaticInventorySource is an in-memory InventorySource that doubles as an
// InventoryApplier: publishers Apply into it (the O(nodes) "etcd" writes) and the
// store reads from it (the cache-fed NodeInventory reads). Production swaps in a
// client-backed source listing NodeInventory objects at ResourceVersion=0.
type StaticInventorySource struct {
	mu        sync.RWMutex
	inv       map[string]*NodeInventory
	partition map[string]struct{}
	applies   int
}

// NewStaticInventorySource returns an empty source.
func NewStaticInventorySource() *StaticInventorySource {
	return &StaticInventorySource{
		inv:       map[string]*NodeInventory{},
		partition: map[string]struct{}{},
	}
}

// Put stores a node inventory directly (test seeding).
func (s *StaticInventorySource) Put(inv *NodeInventory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inv[inv.Node] = inv.DeepCopy()
}

// Apply implements InventoryApplier: it stores the inventory and counts the
// apply, modeling the single O(nodes) server-side-apply write per node.
func (s *StaticInventorySource) Apply(_ context.Context, inv *NodeInventory) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inv[inv.Node] = inv.DeepCopy()
	s.applies++
	return nil
}

// Partition keeps node in ListNodes but makes NodeInventory(node) fail, modeling
// a node partitioned from the aggregated server.
func (s *StaticInventorySource) Partition(node string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partition[node] = struct{}{}
}

// Remove drops a node's inventory object entirely (lost inventory).
func (s *StaticInventorySource) Remove(node string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inv, node)
	delete(s.partition, node)
}

// ListNodes returns the known node names in stable order.
func (s *StaticInventorySource) ListNodes(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodes := make([]string, 0, len(s.inv))
	for n := range s.inv {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	return nodes, nil
}

// NodeInventory returns a copy of one node's inventory, or an error if the node
// is partitioned or unknown.
func (s *StaticInventorySource) NodeInventory(_ context.Context, node string) (*NodeInventory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.partition[node]; ok {
		return nil, fmt.Errorf("scale: node %q partitioned from aggregated server", node)
	}
	inv, ok := s.inv[node]
	if !ok {
		return nil, fmt.Errorf("scale: no inventory published for node %q", node)
	}
	return inv.DeepCopy(), nil
}

// ObjectCount is the number of durable NodeInventory objects held — the O(nodes)
// etcd object count backing every synthesized sandbox.
func (s *StaticInventorySource) ObjectCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.inv)
}

// ApplyCount is the number of Apply calls, i.e. the size of the write path.
func (s *StaticInventorySource) ApplyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applies
}

// --- Client-backed InventorySource (production read path) --------------------

// ClientInventorySource is the production InventorySource: it reads NodeInventory
// objects through a controller-runtime reader. Back it with a cache-fed reader
// (the manager's cache) so the O(nodes) enumeration is served from an informer,
// never a hot-path LIST off etcd. Objects are read as unstructured so the
// aggregated server needs no compiled NodeInventory CRD type.
type ClientInventorySource struct {
	reader client.Reader
}

// NewClientInventorySource builds a ClientInventorySource over reader (use a
// cache-fed client in production).
func NewClientInventorySource(reader client.Reader) *ClientInventorySource {
	return &ClientInventorySource{reader: reader}
}

// ListNodes lists NodeInventory objects (O(nodes)) and returns their names.
func (s *ClientInventorySource) ListNodes(ctx context.Context) ([]string, error) {
	ul := &unstructured.UnstructuredList{}
	ul.SetGroupVersionKind(NodeInventoryGVK.GroupVersion().WithKind(NodeInventoryGVK.Kind + "List"))
	if err := s.reader.List(ctx, ul); err != nil {
		return nil, fmt.Errorf("scale: list node inventories: %w", err)
	}
	nodes := make([]string, 0, len(ul.Items))
	for i := range ul.Items {
		nodes = append(nodes, ul.Items[i].GetName())
	}
	sort.Strings(nodes)
	return nodes, nil
}

// NodeInventory fetches and decodes one node's NodeInventory object.
func (s *ClientInventorySource) NodeInventory(ctx context.Context, node string) (*NodeInventory, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(NodeInventoryGVK)
	if err := s.reader.Get(ctx, types.NamespacedName{Name: node}, u); err != nil {
		return nil, fmt.Errorf("scale: get node %q inventory: %w", node, err)
	}
	inv := &NodeInventory{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, inv); err != nil {
		return nil, fmt.Errorf("scale: decode node %q inventory: %w", node, err)
	}
	return inv, nil
}
