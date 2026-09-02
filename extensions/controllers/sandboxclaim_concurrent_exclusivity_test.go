package controllers

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/internal/hash"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
	"github.com/cocoonstack/sandbox-operator/internal/queue"
)

func TestWarmPoolConcurrentClaimExclusivity(t *testing.T) {
	scheme := newScheme(t)
	ctx := t.Context()

	const (
		warmCount  = 12
		claimCount = 24
	)

	poolHash := hash.Name("pool")
	tmplHash := SandboxTemplateRefHash("tpl")
	warmPoolUID := types.UID("pool-uid")

	template := &extensionsv1beta1.SandboxTemplate{
		Name: "tpl", Namespace: "default",
		Spec: extensionsv1beta1.SandboxTemplateSpec{SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
		}}},
	}
	warmPool := &extensionsv1beta1.SandboxWarmPool{
		Name: "pool", Namespace: "default", UID: warmPoolUID,
		Spec: extensionsv1beta1.SandboxWarmPoolSpec{TemplateRef: extensionsv1beta1.SandboxTemplateRef{Name: "tpl"}},
	}

	warmSandbox := func(name string) *sandboxv1beta1.Sandbox {
		return &sandboxv1beta1.Sandbox{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
			Labels: map[string]string{
				sandboxv1beta1.SandboxWarmPoolLabel:        poolHash,
				sandboxv1beta1.SandboxTemplateRefHashLabel: tmplHash,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: extensionsv1beta1.GroupVersion.String(),
				Kind:       "SandboxWarmPool",
				Name:       "pool",
				UID:        warmPoolUID,
				Controller: new(true),
			}},
			Spec: sandboxv1beta1.SandboxSpec{SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
			}}},
			Status: sandboxv1beta1.SandboxStatus{Conditions: []metav1.Condition{{
				Type:               string(sandboxv1beta1.SandboxConditionReady),
				Status:             metav1.ConditionTrue,
				Reason:             "DependenciesReady",
				LastTransitionTime: metav1.NewTime(time.Now()),
			}}},
		}
	}

	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(template, warmPool).
		WithStatusSubresource(&extensionsv1beta1.SandboxClaim{})

	testQueue := queue.NewSimpleSandboxQueue()
	npn := queue.GetNamespacedWarmPoolName("default", "pool")
	for i := range warmCount {
		sb := warmSandbox(fmt.Sprintf("warm-%02d", i))
		builder = builder.WithObjects(sb)
		testQueue.Add(npn, queue.SandboxKey{Namespace: "default", Name: sb.Name})
	}

	claims := make([]*extensionsv1beta1.SandboxClaim, claimCount)
	for i := range claims {
		claims[i] = &extensionsv1beta1.SandboxClaim{
			Name:      fmt.Sprintf("claim-%02d", i),
			Namespace: "default",
			UID:       types.UID(fmt.Sprintf("claim-%02d-uid", i)),
			Spec:      extensionsv1beta1.SandboxClaimSpec{WarmPoolRef: extensionsv1beta1.SandboxWarmPoolRef{Name: "pool"}},
		}
		builder = builder.WithObjects(claims[i])
	}
	fc := builder.Build()

	reconciler := &SandboxClaimReconciler{
		Client:                  fc,
		Scheme:                  scheme,
		WarmSandboxQueue:        testQueue,
		Recorder:                events.NewFakeRecorder(1 << 12),
		Tracer:                  asmetrics.NewNoOp(),
		MaxConcurrentReconciles: claimCount,
	}

	var wg sync.WaitGroup
	for _, cl := range claims {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			req := reconcile.Request{Name: name, Namespace: "default"}
			for range 10 {
				if _, err := reconciler.Reconcile(ctx, req); err != nil {
					continue
				}
				cur := &extensionsv1beta1.SandboxClaim{}
				if err := fc.Get(ctx, req.NamespacedName, cur); err == nil && cur.Status.SandboxStatus.Name != "" {
					return
				}
			}
		}(cl.Name)
	}
	wg.Wait()

	var allSandboxes sandboxv1beta1.SandboxList
	require.NoError(t, fc.List(ctx, &allSandboxes, client.InNamespace("default")))

	sandboxToOwners := make(map[string][]string)
	warmAdopted := map[string]bool{}
	for i := range allSandboxes.Items {
		sb := &allSandboxes.Items[i]
		ref := metav1.GetControllerOf(sb)
		if ref != nil && ref.Kind == "SandboxClaim" {
			sandboxToOwners[sb.Name] = append(sandboxToOwners[sb.Name], ref.Name)
			if strings.HasPrefix(sb.Name, "warm-") {
				warmAdopted[sb.Name] = true
			}
		}
	}

	for sbName, owners := range sandboxToOwners {
		require.LessOrEqual(t, len(owners), 1,
			"sandbox %s adopted by multiple claims %v — pod-exclusivity violated under concurrency", sbName, owners)
	}

	claimToSandbox := make(map[string][]string)
	for sbName, owners := range sandboxToOwners {
		for _, owner := range owners {
			claimToSandbox[owner] = append(claimToSandbox[owner], sbName)
		}
	}
	for _, cl := range claims {
		require.LessOrEqual(t, len(claimToSandbox[cl.Name]), 1,
			"claim %s owns multiple sandboxes %v", cl.Name, claimToSandbox[cl.Name])
	}

	require.Len(t, warmAdopted, warmCount,
		"expected all %d warm sandboxes adopted exactly once, got %d: %v", warmCount, len(warmAdopted), warmAdopted)

	require.Zero(t, testQueue.Len(npn), "warm queue must be fully drained after all warm sandboxes are claimed")
}
