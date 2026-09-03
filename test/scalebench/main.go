//go:build scalebench

// Command scalebench proves the L1 claim fast-path is near-constant in warm-pool
// size (the acceptance criterion in the README "Scaling design" chapter: claim
// p50 must not degrade linearly from a 100-pool to a 2000+-pool).
//
// It runs entirely against the controller-runtime fake client (a fake apiserver;
// no cluster, no kubelet), so it measures algorithmic complexity rather than real
// microVM boot. Two candidate-selection strategies are compared on identical
// fixtures at each pool size N:
//
//   - fast:  the shipped SandboxClaimReconciler, whose warm-pool adoption selects
//     from the in-memory WarmSandboxQueue and adopts with a single ownership
//     PATCH — no full Sandbox LIST on the claim path.
//   - naive: the pre-L1 anti-pattern — a full LIST of every Sandbox in the
//     namespace to find one warm/unclaimed candidate, then the same adopt PATCH.
//     The fake client's LIST is O(N), so this reproduces the O(N) claim cost.
//
// The fake client's LIST is genuinely O(objects), so the contrast is a real
// measurement of the design choice, not a synthetic constant. pass=true requires
// the fast path to stay near-constant (p50 ratio under the threshold) while the
// naive path degrades clearly more steeply (the sensitivity check that keeps this
// from being a tautology).
//
//	Run: GOTOOLCHAIN=go1.26.3 go run -tags scalebench ./test/scalebench \
//	       -sizes 100,2000 -out /path/to/l1-scalability.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	ctrls "github.com/cocoonstack/sandbox-operator/extensions/controllers"
	"github.com/cocoonstack/sandbox-operator/internal/hash"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
	"github.com/cocoonstack/sandbox-operator/internal/queue"
	"github.com/cocoonstack/sandbox-operator/test/benchutil"
)

const (
	ns       = "scalebench"
	tmplName = "sb-tmpl"
	poolName = "sb-pool"
)

var (
	sizesFlag = flag.String("sizes", "100,2000", "comma-separated warm-pool sizes to measure")
	outFlag   = flag.String("out", "/tmp/l1-scalability.json", "results json path")
	threshold = flag.Float64("threshold", 3.0, "max fast-path p50 ratio (Nmax/Nmin) to call near-constant")
	maxPasses = flag.Int("max-passes", 8, "max reconcile passes per claim before giving up")
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	benchutil.Must(sandboxv1beta1.AddToScheme(s))
	benchutil.Must(extv1beta1.AddToScheme(s))
	benchutil.Must(corev1.AddToScheme(s))
	return s
}

// warmSandbox builds one Ready, warm-pool-owned, adoptable Sandbox.
func warmSandbox(idx int, poolHash, tmplHash string, poolUID types.UID) *sandboxv1beta1.Sandbox {
	return &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:              fmt.Sprintf("warm-%06d", idx),
			Namespace:         ns,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
			Labels: map[string]string{
				sandboxv1beta1.SandboxWarmPoolLabel:        poolHash,
				sandboxv1beta1.SandboxTemplateRefHashLabel: tmplHash,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: extv1beta1.GroupVersion.String(),
				Kind:       "SandboxWarmPool",
				Name:       poolName,
				UID:        poolUID,
				Controller: new(true),
			}},
		},
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

func claim(idx int) *extv1beta1.SandboxClaim {
	return &extv1beta1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("claim-%06d", idx),
			Namespace: ns,
			UID:       types.UID(fmt.Sprintf("claim-%06d-uid", idx)),
		},
		Spec: extv1beta1.SandboxClaimSpec{WarmPoolRef: extv1beta1.SandboxWarmPoolRef{Name: poolName}},
	}
}

// fixture returns a fake client seeded with N warm sandboxes + N claims, plus the
// warm-pool queue populated with the N sandbox keys (as the event handler would).
func fixture(n int) (ctrlclient.Client, *queue.SimpleSandboxQueue, []*extv1beta1.SandboxClaim, *runtime.Scheme) {
	scheme := newScheme()
	poolHash := hash.Name(poolName)
	tmplHash := ctrls.SandboxTemplateRefHash(tmplName)
	poolUID := types.UID("pool-uid")

	tmpl := &extv1beta1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns},
		Spec: extv1beta1.SandboxTemplateSpec{SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
		}}},
	}
	pool := &extv1beta1.SandboxWarmPool{
		ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: ns, UID: poolUID},
		Spec:       extv1beta1.SandboxWarmPoolSpec{TemplateRef: extv1beta1.SandboxTemplateRef{Name: tmplName}},
	}

	objs := []ctrlclient.Object{tmpl, pool}
	q := queue.NewSimpleSandboxQueue()
	npn := queue.GetNamespacedWarmPoolName(ns, poolName)
	for i := range n {
		sb := warmSandbox(i, poolHash, tmplHash, poolUID)
		objs = append(objs, sb)
		q.Add(npn, queue.SandboxKey{Namespace: ns, Name: sb.Name})
	}
	claims := make([]*extv1beta1.SandboxClaim, n)
	for i := range n {
		claims[i] = claim(i)
		objs = append(objs, claims[i])
	}

	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&extv1beta1.SandboxClaim{}).
		Build()
	return fc, q, claims, scheme
}

// measureFast reconciles each claim through the real controller and records the
// wall time to reach Bound (status.sandbox.name set).
func measureFast(ctx context.Context, n int) ([]float64, int) {
	fc, q, claims, scheme := fixture(n)
	r := &ctrls.SandboxClaimReconciler{
		Client:           fc,
		Scheme:           scheme,
		WarmSandboxQueue: q,
		Recorder:         events.NewFakeRecorder(1 << 12),
		Tracer:           asmetrics.NewNoOp(),
	}
	lat := make([]float64, 0, n)
	bound := 0
	for _, cl := range claims {
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: cl.Name, Namespace: ns}}
		start := time.Now()
		ok := false
		for range *maxPasses {
			if _, err := r.Reconcile(ctx, req); err != nil {
				break
			}
			cur := &extv1beta1.SandboxClaim{}
			if err := fc.Get(ctx, req.NamespacedName, cur); err != nil {
				break
			}
			if cur.Status.SandboxStatus.Name != "" {
				ok = true
				break
			}
		}
		if ok {
			lat = append(lat, float64(time.Since(start).Microseconds())/1000)
			bound++
		}
	}
	return lat, bound
}

// measureNaive adopts each claim via a full-namespace Sandbox LIST (the pre-L1
// O(N) candidate selection), then the same single ownership PATCH. It touches the
// same fixture the fast path would, so the only difference measured is selection.
func measureNaive(ctx context.Context, n int) ([]float64, int) {
	fc, _, claims, _ := fixture(n)
	lat := make([]float64, 0, n)
	bound := 0
	for _, cl := range claims {
		start := time.Now()
		if adoptViaList(ctx, fc, cl) {
			lat = append(lat, float64(time.Since(start).Microseconds())/1000)
			bound++
		}
	}
	return lat, bound
}

// adoptViaList is the O(N) anti-pattern: LIST every Sandbox, filter to a warm and
// still-warm-pool-owned candidate, then transfer ownership with one PATCH.
func adoptViaList(ctx context.Context, fc ctrlclient.Client, cl *extv1beta1.SandboxClaim) bool {
	sl := &sandboxv1beta1.SandboxList{}
	if err := fc.List(ctx, sl, ctrlclient.InNamespace(ns)); err != nil {
		return false
	}
	for i := range sl.Items {
		sb := &sl.Items[i]
		if _, warm := sb.Labels[sandboxv1beta1.SandboxWarmPoolLabel]; !warm {
			continue
		}
		ctrlRef := metav1.GetControllerOf(sb)
		if ctrlRef == nil || ctrlRef.Kind != "SandboxWarmPool" {
			continue
		}
		orig := sb.DeepCopy()
		delete(sb.Labels, sandboxv1beta1.SandboxWarmPoolLabel)
		sb.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: extv1beta1.GroupVersion.String(),
			Kind:       "SandboxClaim",
			Name:       cl.Name,
			UID:        cl.UID,
			Controller: new(true),
		}}
		if err := fc.Patch(ctx, sb, ctrlclient.MergeFrom(orig)); err != nil {
			continue // raced; try next candidate
		}
		cc := cl.DeepCopy()
		cc.Status.SandboxStatus.Name = sb.Name
		if err := fc.Status().Patch(ctx, cc, ctrlclient.MergeFrom(cl)); err != nil {
			return false
		}
		return true
	}
	return false
}

func pct(xs []float64, p float64) float64 { return benchutil.Pct(xs, p, benchutil.Round3) }

type sizeResult struct {
	N         int     `json:"n"`
	FastP50Ms float64 `json:"fast_p50_ms"`
	FastP95Ms float64 `json:"fast_p95_ms"`
	FastBound int     `json:"fast_bound"`
	NaiveP50  float64 `json:"naive_p50_ms"`
	NaiveP95  float64 `json:"naive_p95_ms"`
	NaiveBnd  int     `json:"naive_bound"`
}

func main() {
	flag.Parse()
	ctx := context.Background()
	var sizes []int
	for _, s := range strings.Split(*sizesFlag, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(s))
		benchutil.Must(err)
		sizes = append(sizes, v)
	}
	sort.Ints(sizes)
	if len(sizes) < 2 {
		benchutil.Must(fmt.Errorf("need at least two sizes to measure a ratio, got %v", sizes))
	}

	results := make([]sizeResult, 0, len(sizes))
	for _, n := range sizes {
		fastLat, fastBound := measureFast(ctx, n)
		naiveLat, naiveBound := measureNaive(ctx, n)
		r := sizeResult{
			N:         n,
			FastP50Ms: pct(fastLat, 50),
			FastP95Ms: pct(fastLat, 95),
			FastBound: fastBound,
			NaiveP50:  pct(naiveLat, 50),
			NaiveP95:  pct(naiveLat, 95),
			NaiveBnd:  naiveBound,
		}
		results = append(results, r)
		fmt.Printf("[N=%d] fast p50=%.3fms p95=%.3fms bound=%d/%d | naive p50=%.3fms p95=%.3fms bound=%d/%d\n",
			n, r.FastP50Ms, r.FastP95Ms, r.FastBound, n, r.NaiveP50, r.NaiveP95, r.NaiveBnd, n)
	}

	lo, hi := results[0], results[len(results)-1]
	fastRatio := ratio(hi.FastP50Ms, lo.FastP50Ms)
	naiveRatio := ratio(hi.NaiveP50, lo.NaiveP50)

	// Every claim must have bound at both extremes (correctness gate), the fast
	// path must stay near-constant, and the naive path must degrade clearly more
	// steeply (sensitivity: proves the measurement can see O(N) growth at all).
	allBound := lo.FastBound == lo.N && hi.FastBound == hi.N
	nearConstant := fastRatio <= *threshold
	sensitive := naiveRatio >= fastRatio*1.5
	pass := allBound && nearConstant && sensitive

	out := map[string]any{
		"pass":             pass,
		"goal":             "G-0131",
		"layer":            "L1",
		"claim":            "warm-pool claim p50 is near-constant in pool size (fast path) vs O(N) for a full-LIST selection",
		"substrate":        "controller-runtime fake apiserver; real SandboxClaimReconciler",
		"sizes":            sizes,
		"threshold_ratio":  *threshold,
		"fast_p50_ratio":   benchutil.Round3(fastRatio),
		"naive_p50_ratio":  benchutil.Round3(naiveRatio),
		"all_claims_bound": allBound,
		"near_constant":    nearConstant,
		"sensitive":        sensitive,
		"results":          results,
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	benchutil.Must(os.WriteFile(*outFlag, b, 0o644))
	fmt.Printf("fast p50 ratio (N=%d/N=%d) = %.3f (threshold %.1f); naive ratio = %.3f; pass=%v\n",
		hi.N, lo.N, fastRatio, *threshold, naiveRatio, pass)
	fmt.Printf("wrote %s\n", *outFlag)
	if !pass {
		os.Exit(1)
	}
}

func ratio(hi, lo float64) float64 {
	if lo <= 0 {
		return 0
	}
	return hi / lo
}
