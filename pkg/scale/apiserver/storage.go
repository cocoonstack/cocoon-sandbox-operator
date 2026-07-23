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

package apiserver

import (
	"context"
	"fmt"
	"net"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"
)

// Aggregated-Sandbox annotations. Create stamps the claim id + delivered address
// onto the returned object; the synthesized read path stamps the same claim id
// from node inventory, and Delete reads it to release exactly the node-local
// microVM that was handed over.
const (
	// ClaimIDAnnotation carries the sandboxd claim id of a claimed sandbox. It is
	// defined in package scale (whose store stamps it on synthesized reads); this
	// alias keeps apiserver call sites writing and reading the identical key.
	ClaimIDAnnotation = scale.ClaimIDAnnotation
	// AddressAnnotation carries the delivered sandbox connection address.
	AddressAnnotation = "sandbox.cocoonstack.io/address"
	// TokenAnnotation carries the per-sandbox ownership token so a caller can
	// exec/agent into the sandbox it claimed via the L3 apiserver.
	TokenAnnotation = "sandbox.cocoonstack.io/token"
	// NetAnnotation selects the pool network mode on Create (default "none").
	NetAnnotation = "sandbox.cocoonstack.io/net"
)

// sandboxREST is the aggregated-apiserver storage for sandboxes.agents.x-k8s.io.
// It is backed by a scale.SandboxStore (scatter-gather over node inventory), NOT
// by the etcd-backed generic registry — so List/Get/Watch are synthesized from
// live node state and Create/Delete are synchronous node-local claim/release; no
// per-sandbox object ever exists in etcd.
type sandboxREST struct {
	store          scale.SandboxStore
	tableConvertor rest.TableConvertor
}

// The verb set an aggregated, scatter-gather resource implements: the read triad
// plus a node-local claim (Create) and release (GracefulDeleter).
var (
	_ rest.Storage              = (*sandboxREST)(nil)
	_ rest.Scoper               = (*sandboxREST)(nil)
	_ rest.Lister               = (*sandboxREST)(nil)
	_ rest.Getter               = (*sandboxREST)(nil)
	_ rest.Watcher              = (*sandboxREST)(nil)
	_ rest.Creater              = (*sandboxREST)(nil) //nolint:misspell // rest.Creater is the upstream Kubernetes interface name.
	_ rest.GracefulDeleter      = (*sandboxREST)(nil)
	_ rest.TableConvertor       = (*sandboxREST)(nil)
	_ rest.SingularNameProvider = (*sandboxREST)(nil)
)

// NewSandboxREST builds the sandboxes REST storage over store.
func NewSandboxREST(store scale.SandboxStore) rest.Storage {
	return &sandboxREST{
		store:          store,
		tableConvertor: rest.NewDefaultTableConvertor(sandboxv1beta1.Resource("sandboxes")),
	}
}

// New returns an empty Sandbox for the request pipeline.
func (r *sandboxREST) New() runtime.Object { return &sandboxv1beta1.Sandbox{} }

// NewList returns an empty SandboxList for the request pipeline.
func (r *sandboxREST) NewList() runtime.Object { return &sandboxv1beta1.SandboxList{} }

// Destroy releases resources; the store is stateless so this is a no-op.
func (r *sandboxREST) Destroy() {}

// NamespaceScoped reports that sandboxes are namespaced, so RBAC and
// `kubectl get sandboxes -n <ns>` scope exactly as for any namespaced resource.
func (r *sandboxREST) NamespaceScoped() bool { return true }

// GetSingularName returns the singular resource name for discovery.
func (r *sandboxREST) GetSingularName() string { return "sandbox" }

// List scatter-gathers node inventory into a SandboxList, honoring the request
// namespace and label/field selectors.
func (r *sandboxREST) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	return r.store.List(ctx, toScaleListOptions(ctx, options))
}

// Get routes to the owning node for name in the request namespace.
func (r *sandboxREST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, genericapirequest.NamespaceValue(ctx), name)
}

// Watch merges per-node inventory streams into one Sandbox watch.
func (r *sandboxREST) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	return r.store.Watch(ctx, toScaleListOptions(ctx, options))
}

// Create is the L3 write path: it derives the warm pool from the submitted
// Sandbox and asks the store to hand over an already-running microVM from a node
// with warm capacity for that pool (a synchronous node-local claim). It writes NO
// per-sandbox object to etcd; it synthesizes and returns a Ready Sandbox carrying
// the claim id + connection address. With no warm node it returns a retryable 503.
func (r *sandboxREST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, _ *metav1.CreateOptions) (runtime.Object, error) {
	sb, ok := obj.(*sandboxv1beta1.Sandbox)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("expected a Sandbox object, got %T", obj))
	}
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}

	// Identity: namespace from the request path, name (honoring generateName) from
	// the submitted object.
	namespace := genericapirequest.NamespaceValue(ctx)
	if namespace == "" {
		namespace = sb.Namespace
	}
	name := sb.Name
	if name == "" && sb.GenerateName != "" {
		name = names.SimpleNameGenerator.GenerateName(sb.GenerateName)
	}
	if name == "" {
		return nil, apierrors.NewBadRequest("Sandbox has neither metadata.name nor metadata.generateName")
	}

	pool := poolKeyForSandbox(sb)
	assignment, err := r.store.Claim(ctx, namespace, name, pool)
	if err != nil {
		if scale.IsNoWarmCapacity(err) {
			// Retryable: warm capacity refills asynchronously (the node's sandboxd
			// pool), so tell the client to retry rather than surfacing a 500.
			return nil, apierrors.NewServiceUnavailable(fmt.Sprintf(
				"no warm sandbox available for pool (template=%q net=%q size=%q); retry as warm capacity refills",
				pool.Template, pool.Net, pool.Size))
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("claim sandbox %s/%s: %w", namespace, name, err))
	}
	return synthesizeClaimedSandbox(namespace, name, sb, assignment), nil
}

// Delete is the L3 release path. It resolves the sandbox from live node inventory
// (so it knows the owning node), reads the sandboxd claim id the node published
// on that entry, and releases the node-local claim via the store. Per the delete-
// authorization contract this is owner-authorized teardown of the Sandbox resource
// itself; it never destroys a VM on pod state alone. Returns (object, true).
func (r *sandboxREST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, _ *metav1.DeleteOptions) (runtime.Object, bool, error) {
	namespace := genericapirequest.NamespaceValue(ctx)
	sb, err := r.store.Get(ctx, namespace, name)
	if err != nil {
		// NotFound propagates unchanged so IsNotFound stays true for the caller.
		return nil, false, err
	}
	if deleteValidation != nil {
		if err := deleteValidation(ctx, sb); err != nil {
			return nil, false, err
		}
	}

	node := sb.Status.NodeName
	if node == "" {
		// No owning node resolved: nothing to release against. Report it deleted.
		return sb, true, nil
	}
	claimID := sb.Annotations[ClaimIDAnnotation]
	if claimID == "" {
		// The node knows the sandbox but has not published its sandboxd claim id.
		// Releasing by the k8s name would target the wrong claim (or none), so
		// fail loud rather than silently mis-releasing or leaking the microVM.
		return nil, false, apierrors.NewInternalError(fmt.Errorf(
			"cannot delete sandbox %s/%s: node %q inventory carries no %s (sandboxd claim id); refusing to release by name",
			namespace, name, node, ClaimIDAnnotation))
	}
	if err := r.store.Release(ctx, node, claimID); err != nil {
		return nil, false, apierrors.NewInternalError(
			fmt.Errorf("release sandbox %s/%s (node %q id %q): %w", namespace, name, node, claimID, err))
	}
	return sb, true, nil
}

// ConvertToTable renders the default Name/Age table for `kubectl get`.
func (r *sandboxREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.tableConvertor.ConvertToTable(ctx, object, tableOptions)
}

// toScaleListOptions lifts the request namespace and selectors into the store's
// ListOptions. The namespace comes from the request path (empty = all namespaces).
func toScaleListOptions(ctx context.Context, options *metainternalversion.ListOptions) scale.ListOptions {
	o := scale.ListOptions{Namespace: genericapirequest.NamespaceValue(ctx)}
	if options != nil {
		if options.LabelSelector != nil {
			o.LabelSelector = options.LabelSelector.String()
		}
		if options.FieldSelector != nil {
			o.FieldSelector = options.FieldSelector.String()
		}
	}
	return o
}

// poolKeyForSandbox derives the warm-pool key from a Sandbox: the template is the
// first container's image, the size is a t-shirt class mapped from that container's
// resources, and the net comes from the NetAnnotation (default "none"). It defers
// to scale.PoolKeyFor so the SandboxWarmPool driver derives an identical key.
func poolKeyForSandbox(sb *sandboxv1beta1.Sandbox) scale.PoolKey {
	return scale.PoolKeyFor(sb.Spec.PodTemplate.Spec.Containers, sb.Annotations[NetAnnotation])
}

// synthesizeClaimedSandbox builds the Sandbox object Create returns: the submitted
// spec echoed back under the request name/namespace, a fresh UID/creationTimestamp,
// the claim id + address annotations, and a Ready status pointing at the owning
// node. It is never persisted — it is the response for a node-local claim.
func synthesizeClaimedSandbox(namespace, name string, in *sandboxv1beta1.Sandbox, a scale.Assignment) *sandboxv1beta1.Sandbox {
	out := in.DeepCopy()
	out.Namespace = namespace
	out.Name = name
	out.GenerateName = ""
	out.UID = uuid.NewUUID()
	out.CreationTimestamp = metav1.Now()
	out.ResourceVersion = ""
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[ClaimIDAnnotation] = a.SandboxName
	if a.Address != "" {
		out.Annotations[AddressAnnotation] = a.Address
	}
	if a.Token != "" {
		out.Annotations[TokenAnnotation] = a.Token
	}
	out.Status = sandboxv1beta1.SandboxStatus{
		NodeName: a.Node,
		PodIPs:   addressHosts(a.Address),
	}
	apimeta.SetStatusCondition(&out.Status.Conditions, metav1.Condition{
		Type:    string(sandboxv1beta1.SandboxConditionReady),
		Status:  metav1.ConditionTrue,
		Reason:  sandboxv1beta1.SandboxReasonDependenciesReady,
		Message: fmt.Sprintf("warm sandbox %q delivered by node %q", a.SandboxName, a.Node),
	})
	return out
}

// addressHosts strips the port from a host:port connection address, yielding the
// pod IP list on the synthesized status.
func addressHosts(addr string) []string {
	if addr == "" {
		return nil
	}
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		return []string{host}
	}
	return []string{addr}
}
