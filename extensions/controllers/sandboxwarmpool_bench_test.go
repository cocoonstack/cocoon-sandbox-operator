package controllers

import (
	"fmt"
	"testing"

	"github.com/cocoonstack/sandbox-operator/internal/hash"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
)

const benchPoolMembers = 2500

// BenchmarkWarmPoolReconcileSteady measures one full steady-state Reconcile of a
// pool at the validated scale — the cost every member event used to pay.
func BenchmarkWarmPoolReconcileSteady(b *testing.B) {
	r, pool, _ := newBenchPool(b)
	req := ctrl.Request{Namespace: pool.Namespace, Name: pool.Name}
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := r.Reconcile(ctx, req); err != nil {
			b.Fatalf("reconcile: %v", err)
		}
	}
}

// BenchmarkPoolMemberDeepCopy measures deep-copying the full member list — the
// per-List cost the informer cache pays for this controller unless the read is
// declared copy-free.
func BenchmarkPoolMemberDeepCopy(b *testing.B) {
	_, _, members := newBenchPool(b)
	b.ReportAllocs()
	for b.Loop() {
		for i := range members {
			_ = members[i].DeepCopy()
		}
	}
}

// newBenchPool builds a reconciler over a steady pool: desired == current ==
// benchPoolMembers, every member owned, warm-labeled, hash-fresh, and Ready.
func newBenchPool(b *testing.B) (*SandboxWarmPoolReconciler, *extensionsv1beta1.SandboxWarmPool, []sandboxv1beta1.Sandbox) {
	scheme := newScheme(b)
	template := &extensionsv1beta1.SandboxTemplate{
		Namespace: "default", Name: "bench-tpl",
		Spec: extensionsv1beta1.SandboxTemplateSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "ghcr.io/cocoonstack/sandbox/rt:24.04"}}},
				},
			},
		},
	}
	blueprintHash, err := computeSandboxBlueprintHash(template)
	if err != nil {
		b.Fatalf("blueprint hash: %v", err)
	}
	pool := &extensionsv1beta1.SandboxWarmPool{
		Namespace: "default", Name: "bench-pool", UID: "pool-uid",
		Spec: extensionsv1beta1.SandboxWarmPoolSpec{
			Replicas:    new(int32(benchPoolMembers)),
			TemplateRef: extensionsv1beta1.SandboxTemplateRef{Name: template.Name},
		},
	}
	poolNameHash := hash.Name(pool.Name)
	members := make([]sandboxv1beta1.Sandbox, benchPoolMembers)
	objs := make([]runtime.Object, 0, benchPoolMembers+2)
	objs = append(objs, pool, template)
	for i := range members {
		members[i] = sandboxv1beta1.Sandbox{
			Namespace: "default",
			Name:      fmt.Sprintf("warm-%d", i),
			Labels: map[string]string{
				warmPoolSandboxLabel:                    poolNameHash,
				sandboxTemplateRefHash:                  SandboxTemplateRefHash(template.Name),
				sandboxv1beta1.SandboxLaunchTypeLabel:   sandboxv1beta1.SandboxLaunchTypeWarm,
				sandboxv1beta1.SandboxTemplateHashLabel: blueprintHash,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: extensionsv1beta1.GroupVersion.String(),
				Kind:       "SandboxWarmPool",
				Name:       pool.Name,
				UID:        pool.UID,
				Controller: new(true),
			}},
			Spec: sandboxv1beta1.SandboxSpec{SandboxBlueprint: *template.Spec.SandboxBlueprint.DeepCopy()},
			Status: sandboxv1beta1.SandboxStatus{
				Conditions: []metav1.Condition{{
					Type:               string(sandboxv1beta1.SandboxConditionReady),
					Status:             metav1.ConditionTrue,
					Reason:             "Ready",
					LastTransitionTime: metav1.Now(),
				}},
			},
		}
		objs = append(objs, &members[i])
	}
	cl := newFakeClient(scheme, objs...)
	return &SandboxWarmPoolReconciler{Client: cl, Scheme: scheme, MaxBatchSize: sandboxCreateDeleteMaxBatchSize, Tracer: asmetrics.NewNoOp()}, pool, members
}
