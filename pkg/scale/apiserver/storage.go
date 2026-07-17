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

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"
)

// sandboxREST is the aggregated-apiserver storage for sandboxes.agents.x-k8s.io.
// It is backed by a scale.SandboxStore (scatter-gather over node inventory), NOT
// by the etcd-backed generic registry — so List/Get/Watch are synthesized from
// live node state and no per-sandbox object exists in etcd.
type sandboxREST struct {
	store          scale.SandboxStore
	tableConvertor rest.TableConvertor
}

// The read-only verb set an aggregated, scatter-gather resource implements.
var (
	_ rest.Storage              = (*sandboxREST)(nil)
	_ rest.Scoper               = (*sandboxREST)(nil)
	_ rest.Lister               = (*sandboxREST)(nil)
	_ rest.Getter               = (*sandboxREST)(nil)
	_ rest.Watcher              = (*sandboxREST)(nil)
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
