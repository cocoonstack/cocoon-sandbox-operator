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

package podruntime

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

func TestMutatePod(t *testing.T) {
	runtimeClass := "gvisor"
	tests := []struct {
		name        string
		defaultMode string
		annotations map[string]string
		runtime     *string
		selector    map[string]string
		nodeName    string
		wantVKC     bool
		wantErr     bool
	}{
		{name: "default vk-cocoon", defaultMode: ModeVKCocoon, wantVKC: true},
		{name: "explicit standard", defaultMode: ModeVKCocoon, annotations: map[string]string{RuntimeAnnotation: ModeStandard}},
		{name: "runtime class selects standard kubelet", defaultMode: ModeVKCocoon, runtime: &runtimeClass},
		{name: "explicit vk conflicts with runtime class", defaultMode: ModeVKCocoon, annotations: map[string]string{RuntimeAnnotation: ModeVKCocoon}, runtime: &runtimeClass, wantErr: true},
		{name: "conflicting node selector fails", defaultMode: ModeVKCocoon, selector: map[string]string{vkNodeLabelKey: "worker"}, wantErr: true},
		{name: "pinned node name conflicts with vk-cocoon", defaultMode: ModeVKCocoon, nodeName: "worker-1", wantErr: true},
		{name: "operator default is standard", defaultMode: DefaultMode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutator, err := NewMutator(tt.defaultMode)
			if err != nil {
				t.Fatalf("NewMutator: %v", err)
			}
			sandbox := &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "agents"}}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations},
				Spec: corev1.PodSpec{
					RuntimeClassName: tt.runtime,
					NodeSelector:     tt.selector,
					NodeName:         tt.nodeName,
					Containers:       []corev1.Container{{Name: "agent", Image: "registry.example/agent:v1"}},
				},
			}
			err = mutator.MutatePod(context.Background(), sandbox, pod)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MutatePod error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			gotVKC := pod.Annotations[RuntimeAnnotation] == ModeVKCocoon
			if gotVKC != tt.wantVKC {
				t.Fatalf("vk-cocoon mutation = %v, want %v", gotVKC, tt.wantVKC)
			}
			if !tt.wantVKC {
				// Standard runtime must leave the Pod untouched: no virtual-node
				// selector, no vk toleration, and none of the cocoon runtime
				// annotations. This is the core --default-runtime=standard invariant.
				if _, found := pod.Spec.NodeSelector[vkNodeLabelKey]; found {
					t.Errorf("standard runtime leaked node selector %s", vkNodeLabelKey)
				}
				if toleratesVKCocoon(pod.Spec.Tolerations) {
					t.Errorf("standard runtime leaked vk-cocoon toleration")
				}
				for _, key := range []string{cocoonModeAnnotation, cocoonManagedAnnotation, cocoonImageAnnotation, cocoonOSAnnotation, cocoonVMNameAnnotation} {
					if _, found := pod.Annotations[key]; found {
						t.Errorf("standard runtime leaked annotation %s", key)
					}
				}
				return
			}
			if got := pod.Spec.NodeSelector[vkNodeLabelKey]; got != vkNodeLabelValue {
				t.Errorf("node selector = %q, want %q", got, vkNodeLabelValue)
			}
			if got := pod.Annotations[cocoonImageAnnotation]; got != "registry.example/agent:v1" {
				t.Errorf("image annotation = %q", got)
			}
			if got := pod.Annotations[cocoonVMNameAnnotation]; got == "" || len(got) > 63 {
				t.Errorf("invalid stable VM name %q", got)
			}
		})
	}
}

func TestMutatePodIsIdempotent(t *testing.T) {
	mutator, err := NewMutator(ModeVKCocoon)
	if err != nil {
		t.Fatalf("NewMutator: %v", err)
	}
	sandbox := &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "agents"}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Image: "agent:v1"}}}}
	if err := mutator.MutatePod(context.Background(), sandbox, pod); err != nil {
		t.Fatalf("first MutatePod: %v", err)
	}
	if err := mutator.MutatePod(context.Background(), sandbox, pod); err != nil {
		t.Fatalf("second MutatePod: %v", err)
	}
	count := 0
	for _, toleration := range pod.Spec.Tolerations {
		if toleration.Key == vkProviderTaintKey {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("vk-cocoon toleration count = %d, want 1", count)
	}
}

func TestNewMutatorRejectsUnknownMode(t *testing.T) {
	if DefaultMode != ModeStandard {
		t.Fatalf("DefaultMode = %q, want %q", DefaultMode, ModeStandard)
	}
	if _, err := NewMutator("unknown"); err == nil {
		t.Fatal("NewMutator succeeded for unknown mode")
	}
}
