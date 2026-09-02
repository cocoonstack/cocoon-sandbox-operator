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

package v1alpha1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"

	sandboxv1alpha1 "github.com/cocoonstack/sandbox-operator/api/v1alpha1"
	v1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

func TestSandboxTemplateConversion(t *testing.T) {
	bTrue := true
	src := &SandboxTemplate{
		Name:      "my-template",
		Namespace: "default",
		Labels: map[string]string{
			"foo": "bar",
		},
		Annotations: map[string]string{
			"baz":                                  "qux",
			v1alpha1SandboxTemplateStateAnnotation: "some-old-state",
		},
		Spec: SandboxTemplateSpec{
			PodTemplate: sandboxv1alpha1.PodTemplate{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "my-agent",
							Image: "my-image:latest",
						},
					},
				},
			},
			VolumeClaimTemplates: []sandboxv1alpha1.PersistentVolumeClaimTemplate{
				{
					Name: "data",
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
					},
				},
			},
			NetworkPolicy: &NetworkPolicySpec{
				Ingress: []networkingv1.NetworkPolicyIngressRule{
					{},
				},
			},
			NetworkPolicyManagement: NetworkPolicyManagementManaged,
			EnvVarsInjectionPolicy:  EnvVarsInjectionPolicyAllowed,
			Service:                 &bTrue,
		},
	}

	dst := &v1beta1.SandboxTemplate{}
	if err := src.ConvertTo(dst); err != nil {
		t.Fatalf("failed to convert to v1beta1: %v", err)
	}

	if val, ok := src.Annotations[v1alpha1SandboxTemplateStateAnnotation]; !ok || val != "some-old-state" {
		t.Errorf("src.Annotations was mutated during ConvertTo! expected 'some-old-state', got %q", val)
	}
	if len(src.Annotations) != 2 {
		t.Errorf("expected 2 annotations in src, got %d", len(src.Annotations))
	}
	if len(src.Labels) != 1 {
		t.Errorf("expected 1 label in src, got %d", len(src.Labels))
	}

	if _, ok := dst.Annotations[v1alpha1SandboxTemplateStateAnnotation]; ok {
		t.Errorf("dst.Annotations carries the v1alpha1 state stash; the conversion is lossless and must not persist a second copy")
	}

	if dst.Spec.PodTemplate.Spec.Containers[0].Image != "my-image:latest" {
		t.Errorf("unexpected image: %s", dst.Spec.PodTemplate.Spec.Containers[0].Image)
	}
	if string(dst.Spec.EnvVarsInjectionPolicy) != string(EnvVarsInjectionPolicyAllowed) {
		t.Errorf("unexpected EnvVarsInjectionPolicy: %s", dst.Spec.EnvVarsInjectionPolicy)
	}

	roundTrip := &SandboxTemplate{}
	if err := roundTrip.ConvertFrom(dst); err != nil {
		t.Fatalf("failed to convert back to v1alpha1: %v", err)
	}

	if _, ok := roundTrip.Annotations[v1alpha1SandboxTemplateStateAnnotation]; ok {
		t.Errorf("roundTrip.Annotations still contains the state annotation after ConvertFrom!")
	}

	if roundTrip.Spec.PodTemplate.Spec.Containers[0].Image != src.Spec.PodTemplate.Spec.Containers[0].Image {
		t.Errorf("roundtrip PodTemplate Image mismatch: expected %q, got %q", src.Spec.PodTemplate.Spec.Containers[0].Image, roundTrip.Spec.PodTemplate.Spec.Containers[0].Image)
	}
	if roundTrip.Spec.EnvVarsInjectionPolicy != src.Spec.EnvVarsInjectionPolicy {
		t.Errorf("roundtrip EnvVarsInjectionPolicy mismatch: expected %q, got %q", src.Spec.EnvVarsInjectionPolicy, roundTrip.Spec.EnvVarsInjectionPolicy)
	}
	if roundTrip.Spec.NetworkPolicyManagement != src.Spec.NetworkPolicyManagement {
		t.Errorf("roundtrip NetworkPolicyManagement mismatch: expected %q, got %q", src.Spec.NetworkPolicyManagement, roundTrip.Spec.NetworkPolicyManagement)
	}
	if roundTrip.Spec.Service == nil || *roundTrip.Spec.Service != *src.Spec.Service {
		t.Errorf("roundtrip Service mismatch")
	}
}

func TestSandboxTemplateVolumeClaimTemplatesPolicyConversion(t *testing.T) {
	src := &v1beta1.SandboxTemplate{
		Name:      "my-template",
		Namespace: "default",
		Spec: v1beta1.SandboxTemplateSpec{
			VolumeClaimTemplatesPolicy: v1beta1.VolumeClaimTemplatesPolicyAllowed,
		},
	}

	spoke := &SandboxTemplate{}
	if err := spoke.ConvertFrom(src); err != nil {
		t.Fatalf("failed to convert from v1beta1: %v", err)
	}

	if val, ok := spoke.Annotations["api.agents.x-k8s.io/v1beta1-volume-claim-templates-policy"]; !ok || val != string(v1beta1.VolumeClaimTemplatesPolicyAllowed) {
		t.Errorf("expected annotation api.agents.x-k8s.io/v1beta1-volume-claim-templates-policy to be 'Allowed', got %q", val)
	}

	dst := &v1beta1.SandboxTemplate{}
	if err := spoke.ConvertTo(dst); err != nil {
		t.Fatalf("failed to convert to v1beta1: %v", err)
	}

	if dst.Spec.VolumeClaimTemplatesPolicy != v1beta1.VolumeClaimTemplatesPolicyAllowed {
		t.Errorf("roundtrip VolumeClaimTemplatesPolicy mismatch: expected %q, got %q", v1beta1.VolumeClaimTemplatesPolicyAllowed, dst.Spec.VolumeClaimTemplatesPolicy)
	}
}

func TestSandboxTemplateVolumeClaimTemplatesPolicyStaleAnnotationClearing(t *testing.T) {
	spoke := &SandboxTemplate{
		Name:      "stale-template",
		Namespace: "default",
		Annotations: map[string]string{
			"api.agents.x-k8s.io/v1beta1-volume-claim-templates-policy": "Allowed",
		},
	}

	dst := &v1beta1.SandboxTemplate{}
	if err := spoke.ConvertTo(dst); err != nil {
		t.Fatalf("failed to convert to v1beta1: %v", err)
	}

	if dst.Spec.VolumeClaimTemplatesPolicy != v1beta1.VolumeClaimTemplatesPolicyAllowed {
		t.Fatalf("expected VolumeClaimTemplatesPolicy Allowed, got %q", dst.Spec.VolumeClaimTemplatesPolicy)
	}

	dst.Spec.VolumeClaimTemplatesPolicy = ""

	spokeCleared := &SandboxTemplate{}
	if err := spokeCleared.ConvertFrom(dst); err != nil {
		t.Fatalf("failed to convert from v1beta1: %v", err)
	}

	if val, ok := spokeCleared.Annotations["api.agents.x-k8s.io/v1beta1-volume-claim-templates-policy"]; ok {
		t.Errorf("expected stale annotation to be deleted, but it remained with value %q", val)
	}

	dstFinal := &v1beta1.SandboxTemplate{}
	if err := spokeCleared.ConvertTo(dstFinal); err != nil {
		t.Fatalf("failed to convert to v1beta1 final: %v", err)
	}

	if dstFinal.Spec.VolumeClaimTemplatesPolicy != "" {
		t.Errorf("expected VolumeClaimTemplatesPolicy to remain empty, got %q", dstFinal.Spec.VolumeClaimTemplatesPolicy)
	}
}
