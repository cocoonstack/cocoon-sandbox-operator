package controllers

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

// A pool mid-operation: most claims already bound, a tail still waiting.
const (
	benchClaimTotal   = 2500
	benchClaimUnbound = 50
)

func BenchmarkMapWarmPoolToClaims(b *testing.B) {
	scheme := newScheme(b)
	warmPool := &extensionsv1beta1.SandboxWarmPool{
		ObjectMeta: metav1.ObjectMeta{Name: "bench-pool", Namespace: "default"},
	}
	objs := make([]client.Object, 0, benchClaimTotal+1)
	objs = append(objs, warmPool)
	for i := range benchClaimTotal {
		claim := &extensionsv1beta1.SandboxClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("claim-%d", i), Namespace: "default"},
			Spec:       extensionsv1beta1.SandboxClaimSpec{WarmPoolRef: extensionsv1beta1.SandboxWarmPoolRef{Name: warmPool.Name}},
		}
		if i >= benchClaimUnbound {
			claim.Status.SandboxStatus.Name = fmt.Sprintf("sb-%d", i)
		}
		objs = append(objs, claim)
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&extensionsv1beta1.SandboxClaim{}, extensionsv1beta1.WarmPoolRefField, func(obj client.Object) []string {
			c := obj.(*extensionsv1beta1.SandboxClaim)
			if c.Spec.WarmPoolRef.Name == "" {
				return nil
			}
			return []string{c.Spec.WarmPoolRef.Name}
		}).
		Build()
	r := &SandboxClaimReconciler{Client: cl, Scheme: scheme}

	ctx := b.Context()
	b.ReportAllocs()
	var requests int
	for b.Loop() {
		requests = len(r.mapWarmPoolToClaims(ctx, warmPool))
	}
	b.ReportMetric(float64(requests), "reqs/event")
}
