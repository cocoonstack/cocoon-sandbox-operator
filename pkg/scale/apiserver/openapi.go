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
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
)

// sandboxDefPrefix is the canonical Go package name openapinamer derives from
// the Scheme for the aggregated types; the model map MUST be keyed by it so the
// namer resolves the sandboxes resource to these definitions.
const sandboxDefPrefix = "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1."

// gvkExtension is the x-kubernetes-group-version-kind marker the managed-fields
// TypeConverter (managedfields/internal.indexModels) reads to map a GVK to its
// model. WITHOUT it, ObjectToTyped(sandbox) returns NoCorrespondingTypeError,
// which the create handler swallows as a "[SHOULD NOT HAPPEN] failed to update
// managedFields" error on every write. This is the whole reason this file exists.
func gvkExtension(kind string) spec.Extensions {
	return spec.Extensions{
		"x-kubernetes-group-version-kind": []interface{}{
			map[string]interface{}{
				"group":   sandboxv1beta1.GroupVersion.Group,
				"version": sandboxv1beta1.GroupVersion.Version,
				"kind":    kind,
			},
		},
	}
}

func stringSchema() spec.Schema {
	return spec.Schema{SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}}
}

// preserveUnknownObject is an object whose interior is left untyped. The
// aggregated server synthesizes Sandboxes and stores NOTHING in etcd, so
// server-side-apply field tracking is meaningless here — a coarse schema that
// lets the TypeConverter build a valid model for the Sandbox GVK is all the
// field manager needs. Full-fidelity per-field SSA would require openapi-gen
// over the embedded core/v1.PodSpec graph and buy us nothing.
func preserveUnknownObject() spec.Schema {
	return spec.Schema{
		SchemaProps:      spec.SchemaProps{Type: spec.StringOrArray{"object"}},
		VendorExtensible: spec.VendorExtensible{Extensions: spec.Extensions{"x-kubernetes-preserve-unknown-fields": true}},
	}
}

// sandboxOpenAPIDefinitions supplies just enough OpenAPI to register the
// Sandbox and SandboxList GVKs with the managed-fields TypeConverter. Replaces
// the former empty map, which left the writable Create path logging
// [SHOULD NOT HAPPEN] on every request.
func sandboxOpenAPIDefinitions(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
	sandbox := openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			VendorExtensible: spec.VendorExtensible{Extensions: gvkExtension("Sandbox")},
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
				Properties: map[string]spec.Schema{
					"kind":       stringSchema(),
					"apiVersion": stringSchema(),
					"metadata":   preserveUnknownObject(),
					"spec":       preserveUnknownObject(),
					"status":     preserveUnknownObject(),
				},
			},
		},
	}
	list := openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			VendorExtensible: spec.VendorExtensible{Extensions: gvkExtension("SandboxList")},
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
				Properties: map[string]spec.Schema{
					"kind":       stringSchema(),
					"apiVersion": stringSchema(),
					"metadata":   preserveUnknownObject(),
					"items": {
						SchemaProps: spec.SchemaProps{
							Type: spec.StringOrArray{"array"},
							Items: &spec.SchemaOrArray{
								Schema: &spec.Schema{
									SchemaProps: spec.SchemaProps{Ref: ref(sandboxDefPrefix + "Sandbox")},
								},
							},
						},
					},
				},
			},
		},
		Dependencies: []string{sandboxDefPrefix + "Sandbox"},
	}
	return map[string]openapicommon.OpenAPIDefinition{
		sandboxDefPrefix + "Sandbox":     sandbox,
		sandboxDefPrefix + "SandboxList": list,
	}
}
