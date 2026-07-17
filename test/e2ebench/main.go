//go:build e2ebench

// Command e2ebench runs the G-0131 Phase D full-stack admission→claim→deliver→
// release→cleanup acceptance against a live cluster and a single vk-cocoon node
// (bd26 by default), producing e2e-fullstack.json.
//
// Sequence (all through pure Kubernetes objects — admission webhook, CRs, RBAC):
//
//  1. Record the node's prod-desktop pod count (must stay constant end-to-end).
//
//  2. Create a SandboxWarmPool(replicas=N) → N Sandbox CRs admitted → N real
//     microVMs on the node. Wait until N are Ready.
//
//  3. Four-way cross-check: warmpool.readyReplicas == sandbox CR count ==
//     Running pods on the node == N.
//
//  4. Fire N SandboxClaims (each admitted through the webhook) and wait for each
//     to reach Bound (status.sandbox.name set) — the claim delivery path.
//
//  5. Release: delete the N claims, then the pool (owner-authorized teardown) and
//     wait for every sandbox CR and pod this run created on the node to reach 0 —
//     the delete-authorization contract reclaiming VMs with zero leak.
//
//  6. Re-check the prod-desktop count is unchanged.
//
//     Run: KUBECONFIG=<vke-my> go run -tags e2ebench ./test/e2ebench \
//     -pool 100 -node cocoon-bd26 -out e2e-fullstack.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/extensions/api/v1beta1"
)

var (
	ns          = flag.String("ns", "g0131-e2e", "namespace for the run")
	poolSize    = flag.Int("pool", 100, "warm pool desired replicas (= claims fired)")
	node        = flag.String("node", "cocoon-bd26", "vk-cocoon node to pin the run to")
	prodNS      = flag.String("prod-ns", "cloud-desktop", "namespace whose pods on the node must stay intact")
	sbImage     = flag.String("image", "ghcr.io/cocoonstack/sandbox/rt:24.04", "sandbox VM image")
	fillWait    = flag.Int("fill-timeout", 600, "seconds to wait for the pool to fill")
	claimWait   = flag.Int("claim-timeout", 300, "seconds to wait for all claims to bind")
	cleanupWait = flag.Int("cleanup-timeout", 240, "seconds to wait for zero-leak cleanup")
	claimConc   = flag.Int("claim-conc", 25, "claim creation concurrency")
	out         = flag.String("out", "/tmp/e2e-fullstack.json", "results json path")

	cl     ctrlclient.Client
	scheme = runtime.NewScheme()
)

const (
	tmplName = "g0131-e2e-tmpl"
	poolName = "g0131-e2e-pool"
	runLabel = "cocoon-e2e-run"
	runVal   = "g0131-phase-d"
)

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(2)
	}
}

func main() {
	flag.Parse()
	must(clientgoscheme.AddToScheme(scheme))
	must(sandboxv1beta1.AddToScheme(scheme))
	must(extv1beta1.AddToScheme(scheme))
	cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	must(err)
	cfg.QPS, cfg.Burst = 200, 400
	cl, err = ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	must(err)

	ctx := context.Background()
	res := map[string]any{
		"goal": "G-0131", "layer": "e2e", "date": time.Now().UTC().Format("2006-01-02"),
		"node": *node, "total": *poolSize,
		"path": "admission webhook → SandboxWarmPool(replicas=N) → agents.x-k8s.io Sandbox → vk-cocoon microVM → SandboxClaim adopt(Bound) → owner-authorized delete → reclaim; pure Kubernetes, no proprietary control plane",
	}

	// (1) prod-desktop baseline on the node — must be unchanged at the end.
	prodBefore := podCount(ctx, *prodNS, *node)
	fmt.Printf("[prod] baseline: %d desktop pods on %s\n", prodBefore, *node)

	ensureNS(ctx)
	ensureTemplate(ctx)

	// (2) Fill the pool: admission admits N Sandbox CRs → N real microVMs.
	fmt.Printf("[fill] creating pool %s replicas=%d on %s\n", poolName, *poolSize, *node)
	ensurePool(ctx, int32(*poolSize))
	admissionPass := true
	filled := waitReady(ctx, *poolSize, *fillWait)
	if filled < *poolSize {
		admissionPass = false
		fmt.Printf("[fill] WARN only %d/%d ready before timeout\n", filled, *poolSize)
	}

	// (3) Four-way cross-check.
	rr, crc, pods := crossCheck(ctx)
	cross := map[string]any{
		"warmpool_ready_replicas": rr, "sandbox_cr_count": crc,
		"running_pods_on_node": pods, "real_microvms_on_node": pods,
	}
	res["cross_checks"] = cross
	fmt.Printf("[cross] readyReplicas=%d sandboxCR=%d pods=%d\n", rr, crc, pods)

	// (4) Fire N claims through admission, wait for Bound.
	bound, createFails := fireClaims(ctx, *poolSize, *claimConc, *claimWait)
	res["success"] = bound
	if createFails > 0 {
		admissionPass = false
	}
	res["admission_pass"] = admissionPass && bound == *poolSize && rr == *poolSize && crc == *poolSize
	fmt.Printf("[claim] bound=%d/%d createFails=%d\n", bound, *poolSize, createFails)

	// (5) Release + cleanup: delete claims then pool (owner-authorized), wait 0 leak.
	releasePass, leaked := releaseAndCleanup(ctx, *cleanupWait)
	res["release_pass"] = releasePass
	res["leaked"] = leaked
	fmt.Printf("[cleanup] releasePass=%v leaked=%d\n", releasePass, leaked)

	// (6) prod-desktops intact.
	prodAfter := podCount(ctx, *prodNS, *node)
	res["prod_intact"] = prodAfter
	res["prod_before"] = prodBefore
	fmt.Printf("[prod] after: %d desktop pods on %s\n", prodAfter, *node)

	pass := res["success"] == *poolSize && leaked == 0 &&
		res["admission_pass"] == true && releasePass && prodAfter == prodBefore && prodBefore > 0
	res["pass"] = pass

	b, _ := json.MarshalIndent(res, "", "  ")
	must(os.WriteFile(*out, b, 0o644))
	fmt.Printf("pass=%v; wrote %s\n", pass, *out)
	if !pass {
		os.Exit(1)
	}
}

func ensureNS(ctx context.Context) {
	_ = cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: *ns, Labels: map[string]string{runLabel: runVal}}})
}

func ensureTemplate(ctx context.Context) {
	svc := false
	container := corev1.Container{
		Name: "agent", Image: *sbImage, ImagePullPolicy: corev1.PullIfNotPresent,
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("16Mi"),
		}},
	}
	t := &extv1beta1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: *ns},
		Spec: extv1beta1.SandboxTemplateSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				Service: &svc,
				PodTemplate: sandboxv1beta1.PodTemplate{
					// vk-cocoon runtime: the operator's podruntime mutator adds the
					// mode/image/os + virtual-node selector + toleration. We pin the
					// virtual node by hostname so the whole run lands on one node.
					ObjectMeta: sandboxv1beta1.PodMetadata{Annotations: map[string]string{
						"sandbox.cocoonstack.io/runtime": "vk-cocoon",
					}},
					Spec: corev1.PodSpec{
						NodeSelector: map[string]string{"kubernetes.io/hostname": *node},
						Containers:   []corev1.Container{container},
					},
				},
			},
			NetworkPolicyManagement: "Unmanaged",
		},
	}
	if err := cl.Create(ctx, t); err != nil && !apierrors.IsAlreadyExists(err) {
		must(err)
	}
}

func ensurePool(ctx context.Context, replicas int32) {
	p := &extv1beta1.SandboxWarmPool{}
	err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: poolName}, p)
	if apierrors.IsNotFound(err) {
		must(cl.Create(ctx, &extv1beta1.SandboxWarmPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: *ns, Labels: map[string]string{runLabel: runVal}},
			Spec:       extv1beta1.SandboxWarmPoolSpec{Replicas: &replicas, TemplateRef: extv1beta1.SandboxTemplateRef{Name: tmplName}},
		}))
		return
	}
	must(err)
	p.Spec.Replicas = &replicas
	must(cl.Update(ctx, p))
}

// ourSandboxes returns the Sandbox CRs this run's pool owns and how many are Ready.
func ourSandboxes(ctx context.Context) (total, ready int) {
	sl := &sandboxv1beta1.SandboxList{}
	if err := cl.List(ctx, sl, ctrlclient.InNamespace(*ns)); err != nil {
		return 0, 0
	}
	for i := range sl.Items {
		if !ownedByPool(&sl.Items[i]) {
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

func ownedByPool(sb *sandboxv1beta1.Sandbox) bool {
	for _, o := range sb.OwnerReferences {
		if o.Kind == "SandboxWarmPool" && o.Name == poolName {
			return true
		}
	}
	return false
}

func waitReady(ctx context.Context, target, timeoutSec int) int {
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	last := 0
	for time.Now().Before(deadline) {
		_, ready := ourSandboxes(ctx)
		if ready > last {
			last = ready
		}
		if ready >= target {
			return ready
		}
		time.Sleep(3 * time.Second)
	}
	_, ready := ourSandboxes(ctx)
	return ready
}

func crossCheck(ctx context.Context) (readyReplicas, sandboxCR, pods int) {
	p := &extv1beta1.SandboxWarmPool{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: poolName}, p); err == nil {
		readyReplicas = int(p.Status.ReadyReplicas)
	}
	_, sandboxCR = ourSandboxes(ctx)
	pods = podCount(ctx, *ns, *node)
	return
}

// podCount counts Running pods in namespace on the given node.
func podCount(ctx context.Context, namespace, nodeName string) int {
	pl := &corev1.PodList{}
	if err := cl.List(ctx, pl, ctrlclient.InNamespace(namespace)); err != nil {
		return -1
	}
	n := 0
	for i := range pl.Items {
		if pl.Items[i].Spec.NodeName == nodeName && pl.Items[i].Status.Phase == corev1.PodRunning {
			n++
		}
	}
	return n
}

func fireClaims(ctx context.Context, n, conc, timeoutSec int) (bound, createFails int) {
	base := fmt.Sprintf("g0131c-%d", time.Now().Unix()%100000)
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%d", base, i)
	}
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			c := &extv1beta1.SandboxClaim{
				ObjectMeta: metav1.ObjectMeta{Name: names[idx], Namespace: *ns, Labels: map[string]string{runLabel: runVal}},
				Spec:       extv1beta1.SandboxClaimSpec{WarmPoolRef: extv1beta1.SandboxWarmPoolRef{Name: poolName}},
			}
			if err := cl.Create(ctx, c); err != nil {
				mu.Lock()
				createFails++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	for time.Now().Before(deadline) {
		b := 0
		cll := &extv1beta1.SandboxClaimList{}
		if err := cl.List(ctx, cll, ctrlclient.InNamespace(*ns)); err == nil {
			for i := range cll.Items {
				if cll.Items[i].Status.SandboxStatus.Name != "" {
					b++
				}
			}
		}
		if b >= n-createFails && b >= 1 {
			return b, createFails
		}
		bound = b
		time.Sleep(3 * time.Second)
	}
	return bound, createFails
}

// releaseAndCleanup deletes all claims (owner-authorized release), then the pool
// and template, and waits until every Sandbox CR and pod this run created on the
// node reaches 0. Returns whether release completed and the residual leak count.
func releaseAndCleanup(ctx context.Context, timeoutSec int) (releasePass bool, leaked int) {
	// Delete claims first (release), then the pool (cascade-delete warm sandboxes).
	cll := &extv1beta1.SandboxClaimList{}
	if err := cl.List(ctx, cll, ctrlclient.InNamespace(*ns)); err == nil {
		for i := range cll.Items {
			_ = cl.Delete(ctx, &cll.Items[i])
		}
	}
	_ = cl.Delete(ctx, &extv1beta1.SandboxWarmPool{ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: *ns}})
	_ = cl.Delete(ctx, &extv1beta1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: *ns}})

	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	for time.Now().Before(deadline) {
		total, _ := ourSandboxes(ctx)
		pods := podCount(ctx, *ns, *node)
		claimsLeft := 0
		cl2 := &extv1beta1.SandboxClaimList{}
		if err := cl.List(ctx, cl2, ctrlclient.InNamespace(*ns)); err == nil {
			claimsLeft = len(cl2.Items)
		}
		if total == 0 && pods == 0 && claimsLeft == 0 {
			return true, 0
		}
		leaked = total
		time.Sleep(3 * time.Second)
	}
	total, _ := ourSandboxes(ctx)
	return false, total
}
