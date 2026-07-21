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
	"k8s.io/kube-openapi/pkg/builder3"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
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
