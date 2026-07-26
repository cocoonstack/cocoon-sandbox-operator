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
	"testing"

	"k8s.io/apimachinery/pkg/util/managedfields"
	genericapiserver "k8s.io/apiserver/pkg/server"
	apiservercompatibility "k8s.io/apiserver/pkg/util/compatibility"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kube-openapi/pkg/builder3"
	"k8s.io/kube-openapi/pkg/validation/spec"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

// TestManagedFieldsTypeConverterResolvesSandbox reproduces the create-path crux
// that produced "[SHOULD NOT HAPPEN] failed to update managedFields" on every
// write: the managed-fields TypeConverter must map the Sandbox GVK to a model
// (via the x-kubernetes-group-version-kind marker), else ObjectToTyped returns
// NoCorrespondingTypeError. With the old empty OpenAPI this failed; with
// sandboxOpenAPIDefinitions it must succeed for both Sandbox and SandboxList.
func TestManagedFieldsTypeConverterResolvesSandbox(t *testing.T) {
	cfg := NewOpenAPIV3Config()
	models, err := builder3.BuildOpenAPIDefinitionsForResources(cfg,
		sandboxDefPrefix+"Sandbox",
		sandboxDefPrefix+"SandboxList",
	)
	if err != nil {
		t.Fatalf("build openapi models: %v", err)
	}
	tc, err := managedfields.NewTypeConverter(models, false)
	if err != nil {
		t.Fatalf("new type converter: %v", err)
	}

	sb := &sandboxv1beta1.Sandbox{}
	sb.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("Sandbox"))
	sb.Name = "probe"
	sb.Namespace = "default"
	if _, err := tc.ObjectToTyped(sb); err != nil {
		t.Fatalf("ObjectToTyped(Sandbox) must succeed (root cause of [SHOULD NOT HAPPEN]): %v", err)
	}

	list := &sandboxv1beta1.SandboxList{}
	list.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("SandboxList"))
	if _, err := tc.ObjectToTyped(list); err != nil {
		t.Fatalf("ObjectToTyped(SandboxList) must succeed: %v", err)
	}
}

// TestEveryServedTypeHasAnOpenAPIModel reproduces a real deployment failure:
// the action subresources were registered in the Scheme but had no OpenAPI
// model, and InstallAPIGroup then refused to start the server outright with
//
//	unable to get openapi models: cannot find model definition for
//	io.k8s.apimachinery.pkg.apis.meta.v1.TypeMeta
//
// because the subresource bodies embed metav1.TypeMeta and openapinamer
// resolves them through this map, not the Scheme. A missing model is not a
// degraded feature — it is a crash loop on rollout, so every served kind must
// be present here.
func TestEveryServedTypeHasAnOpenAPIModel(t *testing.T) {
	defs := sandboxOpenAPIDefinitions(func(path string) spec.Ref { return spec.Ref{} })

	for _, kind := range []string{
		"Sandbox",
		"SandboxList",
		"SandboxPauseOptions",
		"SandboxResumeOptions",
		"SandboxForkOptions",
		"SandboxForkResult",
		"SandboxSnapshotOptions",
		"SandboxSnapshotResult",
	} {
		key := sandboxDefPrefix + kind
		def, ok := defs[key]
		if !ok {
			t.Errorf("no OpenAPI model for %s; InstallAPIGroup will fail to start the apiserver", kind)
			continue
		}
		gvks, ok := def.Schema.Extensions["x-kubernetes-group-version-kind"]
		if !ok {
			t.Errorf("%s has no x-kubernetes-group-version-kind; the managed-fields TypeConverter cannot map it", kind)
			continue
		}
		entries, ok := gvks.([]interface{})
		if !ok || len(entries) != 1 {
			t.Errorf("%s gvk extension = %v, want exactly one entry", kind, gvks)
			continue
		}
		m, _ := entries[0].(map[string]interface{})
		if m["kind"] != kind {
			t.Errorf("%s declares kind %v, want %s", kind, m["kind"], kind)
		}
	}
}

// TestInstallSandboxAPISucceeds is the production startup path: main.go builds
// a GenericAPIServer and calls InstallSandboxAPI. An incomplete OpenAPI model
// graph fails HERE, and the binary then crash-loops on rollout — so this must
// be exercised locally, not discovered in the cluster.
func TestInstallSandboxAPISucceeds(t *testing.T) {
	cfg := genericapiserver.NewConfig(Codecs)
	cfg.EffectiveVersion = apiservercompatibility.DefaultBuildEffectiveVersion()
	cfg.OpenAPIV3Config = NewOpenAPIV3Config()
	cfg.ExternalAddress = "localhost:6443"
	cfg.LoopbackClientConfig = &restclient.Config{Host: "localhost:6443"}

	server, err := cfg.Complete(nil).New("test-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		t.Fatalf("build generic apiserver: %v", err)
	}
	if err := InstallSandboxAPI(server, nil); err != nil {
		t.Fatalf("InstallSandboxAPI: %v", err)
	}
}
