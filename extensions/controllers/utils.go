// Copyright 2026 The Kubernetes Authors.
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

package controllers

import (
	corev1 "k8s.io/api/core/v1"

	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/internal/hash"
)

// ApplySandboxSecureDefaults applies the controller's "Secure by Default" logic to a PodSpec.
func ApplySandboxSecureDefaults(template *extensionsv1beta1.SandboxTemplate, spec *corev1.PodSpec) {
	if spec.AutomountServiceAccountToken == nil {
		spec.AutomountServiceAccountToken = new(false)
	}

	management := template.Spec.NetworkPolicyManagement
	isManaged := management == "" || management == extensionsv1beta1.NetworkPolicyManagementManaged
	isSecureByDefault := isManaged && template.Spec.NetworkPolicy == nil

	// Public resolvers block internal DNS enumeration; custom rules or Unmanaged
	// keep the cluster's own DNS, which air-gapped and proxied clusters need.
	if isSecureByDefault && spec.DNSPolicy == "" {
		spec.DNSPolicy = corev1.DNSNone
		spec.DNSConfig = &corev1.PodDNSConfig{
			Nameservers: []string{"8.8.8.8", "1.1.1.1"},
		}
	}
}

// SandboxTemplateRefHash encapsulates the generation of the hash for a sandbox template ref.
func SandboxTemplateRefHash(templateRefName string) string {
	return hash.Name(templateRefName)
}
