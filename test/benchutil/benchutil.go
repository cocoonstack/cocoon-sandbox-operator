// Package benchutil holds the plumbing shared by the test/* bench harnesses:
// fatal-error exit, truncating percentiles, and warm-pool fixture helpers.
package benchutil

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

// Must exits the harness when err is non-nil.
func Must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(2)
	}
}

// Pct returns the p-th percentile of xs, truncated by round.
func Pct(xs []float64, p float64, round func(float64) float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	if p >= 100 {
		return round(s[len(s)-1])
	}
	return round(s[int(float64(len(s)-1)*p/100)])
}

// Round0..Round4 truncate to the named number of decimals — truncation, not
// rounding, matching the values the harness reports have always emitted.
func Round0(f float64) float64 { return float64(int(f)) }
func Round1(f float64) float64 { return float64(int(f*10)) / 10 }
func Round2(f float64) float64 { return float64(int(f*100)) / 100 }
func Round3(f float64) float64 { return float64(int(f*1000)) / 1000 }
func Round4(f float64) float64 { return float64(int(f*10000)) / 10000 }

// EnsurePool creates the SandboxWarmPool (labels may be nil) or updates its
// replica count.
func EnsurePool(ctx context.Context, cl client.Client, ns, pool, template string, replicas int32, labels map[string]string) {
	p := &extv1beta1.SandboxWarmPool{}
	err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: pool}, p)
	if apierrors.IsNotFound(err) {
		Must(cl.Create(ctx, &extv1beta1.SandboxWarmPool{
			ObjectMeta: metav1.ObjectMeta{Name: pool, Namespace: ns, Labels: labels},
			Spec:       extv1beta1.SandboxWarmPoolSpec{Replicas: &replicas, TemplateRef: extv1beta1.SandboxTemplateRef{Name: template}},
		}))
		return
	}
	Must(err)
	p.Spec.Replicas = &replicas
	Must(cl.Update(ctx, p))
}

// ReadySandboxes counts the Sandboxes pool owns and how many are Ready.
func ReadySandboxes(ctx context.Context, cl client.Client, ns, pool string) (total, ready int) {
	sl := &sandboxv1beta1.SandboxList{}
	if err := cl.List(ctx, sl, client.InNamespace(ns)); err != nil {
		return 0, 0
	}
	for i := range sl.Items {
		if !OwnedByPool(&sl.Items[i], pool) {
			continue
		}
		total++
		for _, c := range sl.Items[i].Status.Conditions {
			if c.Type == "Ready" && c.Status == metav1.ConditionTrue {
				ready++
			}
		}
	}
	return
}

// OwnedByPool reports whether sb has an owner reference to the named warm pool.
func OwnedByPool(sb *sandboxv1beta1.Sandbox, pool string) bool {
	return slices.ContainsFunc(sb.OwnerReferences, func(o metav1.OwnerReference) bool {
		return o.Kind == "SandboxWarmPool" && o.Name == pool
	})
}

// WaitReady polls until pool has target Ready members or timeoutSec elapses,
// returning the final Ready count.
func WaitReady(ctx context.Context, cl client.Client, ns, pool string, target, timeoutSec int) int {
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	for time.Now().Before(deadline) {
		if _, ready := ReadySandboxes(ctx, cl, ns, pool); ready >= target {
			return ready
		}
		time.Sleep(3 * time.Second)
	}
	_, ready := ReadySandboxes(ctx, cl, ns, pool)
	return ready
}
