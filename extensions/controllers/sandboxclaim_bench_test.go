package controllers

import (
	"fmt"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

const (
	benchClaimTotal   = 2500
	benchClaimUnbound = 50
)

func BenchmarkMapWarmPoolToClaims(b *testing.B) {
	scheme := newScheme(b)
	warmPool := &extensionsv1beta1.SandboxWarmPool{
		Name: "bench-pool", Namespace: "default",
	}
	objs := make([]client.Object, 0, benchClaimTotal+1)
	objs = append(objs, warmPool)
	for i := range benchClaimTotal {
		claim := &extensionsv1beta1.SandboxClaim{
			Name: fmt.Sprintf("claim-%d", i), Namespace: "default",
			Spec: extensionsv1beta1.SandboxClaimSpec{WarmPoolRef: extensionsv1beta1.SandboxWarmPoolRef{Name: warmPool.Name}},
		}
		if i >= benchClaimUnbound {
			claim.Status.SandboxStatus.Name = fmt.Sprintf("sb-%d", i)
		}
		objs = append(objs, claim)
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&extensionsv1beta1.SandboxClaim{}, extensionsv1beta1.WarmPoolRefField, warmPoolRefIndexer).
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
