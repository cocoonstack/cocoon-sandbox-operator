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

package controllers

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
	asmetrics "github.com/cocoonstack/cocoon-sandbox-operator/internal/metrics"
)

type podMutatorFunc func(context.Context, *sandboxv1beta1.Sandbox, *corev1.Pod) error

func (f podMutatorFunc) MutatePod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) error {
	return f(ctx, sandbox, pod)
}

func TestReconcilePodAppliesPodMutator(t *testing.T) {
	sandbox := testSandboxForPodMutation()
	kube := newFakeClient(sandbox)
	reconciler := &SandboxReconciler{
		Client: kube,
		Scheme: Scheme,
		Tracer: asmetrics.NewNoOp(),
		PodMutator: podMutatorFunc(func(_ context.Context, _ *sandboxv1beta1.Sandbox, pod *corev1.Pod) error {
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations["sandbox.cocoonstack.io/test-runtime"] = "applied"
			return nil
		}),
	}

	pod, err := reconciler.reconcilePod(context.Background(), sandbox, NameHash(sandbox.Name))
	if err != nil {
		t.Fatalf("reconcilePod: %v", err)
	}
	if got := pod.Annotations["sandbox.cocoonstack.io/test-runtime"]; got != "applied" {
		t.Fatalf("runtime annotation = %q, want applied", got)
	}
}

func TestReconcilePodDoesNotCreatePodWhenMutatorFails(t *testing.T) {
	sandbox := testSandboxForPodMutation()
	kube := newFakeClient(sandbox)
	reconciler := &SandboxReconciler{
		Client: kube,
		Scheme: Scheme,
		Tracer: asmetrics.NewNoOp(),
		PodMutator: podMutatorFunc(func(context.Context, *sandboxv1beta1.Sandbox, *corev1.Pod) error {
			return errors.New("runtime conflict")
		}),
	}

	if _, err := reconciler.reconcilePod(context.Background(), sandbox, NameHash(sandbox.Name)); err == nil {
		t.Fatal("reconcilePod succeeded when mutator failed")
	}
	var pod corev1.Pod
	if err := kube.Get(context.Background(), clientObjectKey(sandbox.Namespace, sandbox.Name), &pod); err == nil {
		t.Fatal("Pod was created after mutator failure")
	}
}

func testSandboxForPodMutation() *sandboxv1beta1.Sandbox {
	return &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime-test", Namespace: "default", UID: sandboxUID},
		Spec: sandboxv1beta1.SandboxSpec{
			OperatingMode: sandboxv1beta1.SandboxOperatingModeRunning,
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Image: "agent:v1"}}},
				},
			},
		},
	}
}

func clientObjectKey(namespace, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: namespace, Name: name}
}
