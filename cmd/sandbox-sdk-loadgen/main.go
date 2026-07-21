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

// Command sandbox-sdk-loadgen measures the REAL create latency of the writable
// L3 aggregated apiserver by creating agents.x-k8s.io Sandboxes through a single
// persistent typed client — the same path the official agent-sandbox Go client
// uses. Unlike `kubectl create` in a loop (which re-runs discovery/OpenAPI on
// every invocation and inflated p50 to ~12s), the client here builds its
// RESTMapper once, so the histogram reflects the apiserver round-trip (warm
// claim), not client bootstrap cost.
//
// Safety properties (hard, by construction):
//   - EXACTLY --total creates are issued, then the run stops. --total is
//     REQUIRED and positive; there is no unbounded mode.
//   - With --cleanup (default true), each worker BLOCKS until its sandbox's
//     release is confirmed before creating the next one, so live claims never
//     exceed --concurrency. A delete that returns NotFound is NOT success —
//     it means the read view has not published the object yet (vk lag, ~10s);
//     the worker retries until the delete lands or --release-timeout expires,
//     and an expiry is counted in sandbox_sdk_leaked_total and logged loudly.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
)

// version is stamped at build time (-ldflags "-X main.version=...").
var version = "dev"

var (
	createSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "sandbox_sdk_create_seconds",
		Help: "Latency of Create(Sandbox) against the aggregated apiserver (warm claim round-trip).",
		// 1ms .. ~32s: warm hits land in the low-ms buckets, cold/clone fallback in seconds.
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	deleteSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sandbox_sdk_delete_seconds",
		Help:    "Latency of the FIRST successful Delete call (exact-VM release), excluding read-view NotFound retries.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	createsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_creates_total",
		Help: "Sandboxes successfully created.",
	})
	deletesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_deletes_total",
		Help: "Sandboxes whose release was CONFIRMED (Delete accepted by the apiserver).",
	})
	deleteRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_delete_notfound_retries_total",
		Help: "Delete attempts that hit NotFound because the read view had not published the object yet (vk lag).",
	})
	leakedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_leaked_total",
		Help: "Sandboxes created but whose release could NOT be confirmed within --release-timeout. Anything >0 means node claims were left for the TTL reaper — investigate.",
	})
	createFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sandbox_sdk_create_failed_total",
		Help: "Create failures by reason.",
	}, []string{"reason"})
	inflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sandbox_sdk_inflight",
		Help: "In-flight Create calls.",
	})
	buildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sandbox_sdk_build_info",
		Help: "Loadgen build info; value is always 1.",
	}, []string{"version"})
)

var failReasons = []string{"no-warm-503", "throttled-429", "internal-500", "timeout", "conflict", "other"}

type options struct {
	namespace      string
	image          string
	namegen        string
	concurrency    int
	total          int
	interval       time.Duration
	timeout        time.Duration
	cleanup        bool
	releaseTimeout time.Duration
	metricsAddr    string
}

func main() {
	var o options
	flag.StringVar(&o.namespace, "namespace", "sandbox-sdk-loadgen", "namespace to create Sandboxes in")
	flag.StringVar(&o.image, "template", "ghcr.io/cocoonstack/sandbox/rt@sha256:c8cab53a1e1684e6c0c95a06855001b0535be2862b7d4f72658f9f0e784c8778", "container image; picks the warm pool (image=template, no resources=size small, no net annotation=none)")
	flag.StringVar(&o.namegen, "name-prefix", "sdklg", "generated Sandbox name prefix")
	flag.IntVar(&o.concurrency, "concurrency", 1, "parallel create workers; with --cleanup this is also the max live claims")
	flag.IntVar(&o.total, "total", 0, "REQUIRED: issue exactly this many creates, then stop (must be > 0; there is no unbounded mode)")
	flag.DurationVar(&o.interval, "interval", 0, "per-worker gap between creates (0 = as fast as possible)")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "per-create context timeout")
	flag.BoolVar(&o.cleanup, "cleanup", true, "delete each Sandbox after creating it and BLOCK until the release is confirmed (bounds live claims to --concurrency)")
	flag.DurationVar(&o.releaseTimeout, "release-timeout", 120*time.Second, "max time to retry a Delete past the read-view publish lag before counting the sandbox as leaked")
	flag.StringVar(&o.metricsAddr, "metrics-addr", ":9090", "Prometheus metrics listen address")
	flag.Parse()

	// Hard gate: never start an unbounded or ill-configured run. The one time
	// this binary ran with an unbounded total it drained the fleet's warm pools
	// and leaked ~19k claims. Exactly --total, or nothing.
	if o.total <= 0 {
		fatal("--total is required and must be > 0 (got %d): this loadgen issues exactly --total creates and refuses to run unbounded", o.total)
	}
	if o.concurrency <= 0 {
		fatal("--concurrency must be > 0 (got %d)", o.concurrency)
	}
	if o.concurrency > o.total {
		o.concurrency = o.total
	}

	buildInfo.WithLabelValues(version).Set(1)
	for _, r := range failReasons {
		createFailed.WithLabelValues(r).Add(0) // pre-init so panels render 0 not "No data"
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))

	cfg, err := config.GetConfig()
	if err != nil {
		fatal("load kubeconfig: %v", err)
	}
	// Raise client-side throttling well above the default 5 QPS so the loadgen,
	// not client-go's rate limiter, sets the offered load.
	cfg.QPS = 2000
	cfg.Burst = 4000

	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fatal("build client: %v", err)
	}

	// Serve metrics for the whole run (and after the bounded run finishes) so
	// vmagent always has a live endpoint to scrape.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
		if err := http.ListenAndServe(o.metricsAddr, mux); err != nil { //nolint:gosec // internal metrics endpoint
			fatal("metrics server: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Printf("sandbox-sdk-loadgen %s: ns=%s total=%d concurrency=%d cleanup=%v release-timeout=%s image=%s\n",
		version, o.namespace, o.total, o.concurrency, o.cleanup, o.releaseTimeout, o.image)

	summary := run(ctx, cl, &o)
	fmt.Println(summary)
	if summary.leaked > 0 {
		fmt.Printf("ERROR: %d sandbox(es) leaked — their node claims were left for the TTL reaper. Do NOT scale this run up until the leak is explained.\n", summary.leaked)
	}

	// Keep /metrics alive so the final histogram/counters remain scrapeable.
	fmt.Println("run complete; serving /metrics until terminated")
	<-context.Background().Done()
}

// runSummary is the end-of-run accounting printed to stdout.
type runSummary struct {
	issued, created, failed, released, leaked int64
	elapsed                                   time.Duration
}

func (s runSummary) String() string {
	return fmt.Sprintf("summary: issued=%d created=%d failed=%d released=%d leaked=%d elapsed=%s",
		s.issued, s.created, s.failed, s.released, s.leaked, s.elapsed.Round(time.Millisecond))
}

// run fans out o.concurrency workers. A worker RESERVES a slot from the shared
// issue counter before each create, so across all workers exactly o.total
// creates are issued — failures consume their slot and are not retried (a retry
// would exceed the requested count).
func run(ctx context.Context, cl client.Client, o *options) runSummary {
	var s runSummary
	var issued int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				slot := atomic.AddInt64(&issued, 1)
				if slot > int64(o.total) {
					return
				}
				name := fmt.Sprintf("%s-%d", o.namegen, slot)
				sb, err := createOne(ctx, cl, o, name)
				if err != nil {
					atomic.AddInt64(&s.failed, 1)
				} else {
					atomic.AddInt64(&s.created, 1)
					if o.cleanup {
						if releaseWithRetry(ctx, cl, o, sb) {
							atomic.AddInt64(&s.released, 1)
						} else {
							atomic.AddInt64(&s.leaked, 1)
						}
					}
				}
				if o.interval > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(o.interval):
					}
				}
			}
		}(w)
	}
	wg.Wait()
	s.issued = min64(atomic.LoadInt64(&issued), int64(o.total))
	s.elapsed = time.Since(start)
	return s
}

// createOne issues a single Create, records latency and outcome, and returns
// the created object (for cleanup) or the error.
func createOne(ctx context.Context, cl client.Client, o *options, name string) (*sandboxv1beta1.Sandbox, error) {
	sb := newSandbox(o.namespace, name, o.image)

	cctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	inflight.Inc()
	start := time.Now()
	err := cl.Create(cctx, sb)
	elapsed := time.Since(start).Seconds()
	inflight.Dec()

	if err != nil {
		reason := classify(err)
		createFailed.WithLabelValues(reason).Inc()
		logSampled(reason, "create %s/%s failed: %v", o.namespace, name, err)
		return nil, err
	}
	createSeconds.Observe(elapsed)
	createsTotal.Inc()
	return sb, nil
}

// releaseWithRetry deletes sb and BLOCKS until the apiserver accepts the
// delete, retrying NotFound: a freshly created sandbox is not deletable until
// the node's vk publishes it into the read view (~10s), and a NotFound before
// then means "not yet", NOT "already gone". Returns false — and counts the
// sandbox as leaked — only after --release-timeout.
func releaseWithRetry(ctx context.Context, cl client.Client, o *options, sb *sandboxv1beta1.Sandbox) bool {
	deadline := time.Now().Add(o.releaseTimeout)
	for attempt := 0; ; attempt++ {
		dctx, cancel := context.WithTimeout(ctx, o.timeout)
		start := time.Now()
		err := cl.Delete(dctx, sb)
		cancel()
		switch {
		case err == nil:
			deleteSeconds.Observe(time.Since(start).Seconds())
			deletesTotal.Inc()
			return true
		case apierrors.IsNotFound(err):
			// Read view lag — retry until the object appears.
			deleteRetries.Inc()
		default:
			logSampled("delete", "delete %s/%s failed (attempt %d): %v", sb.Namespace, sb.Name, attempt, err)
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			leakedTotal.Inc()
			fmt.Printf("ERROR: sandbox %s/%s leaked: release not confirmed within %s (last err: %v)\n",
				sb.Namespace, sb.Name, o.releaseTimeout, err)
			return false
		}
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

// newSandbox builds a Sandbox whose derived pool is (image, net=none, size=small)
// — matching the warm pool sandboxd maintains — so a create is a warm hit.
func newSandbox(ns, name, image string) *sandboxv1beta1.Sandbox {
	return &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			Labels:    map[string]string{"agents.x-k8s.io/created-by": "sdk-loadgen"},
		},
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "sandbox",
							Image: image,
						}},
					},
				},
			},
		},
	}
}

// classify buckets a Create error for the failed-by-reason counter. no-warm is
// the meaningful signal: the apiserver returns 503 ServiceUnavailable when the
// derived pool has no warm VM (NewServiceUnavailable in storage.Create); claim
// plumbing failures surface as 500 InternalError.
func classify(err error) string {
	switch {
	case apierrors.IsServiceUnavailable(err):
		return "no-warm-503"
	case apierrors.IsTooManyRequests(err):
		return "throttled-429"
	case apierrors.IsInternalError(err):
		return "internal-500"
	case apierrors.IsServerTimeout(err), apierrors.IsTimeout(err), err == context.DeadlineExceeded:
		return "timeout"
	case apierrors.IsConflict(err), apierrors.IsAlreadyExists(err):
		return "conflict"
	default:
		return "other"
	}
}

// logSampled prints at most one line per key per 5s so failure causes are always
// visible in the pod log without flooding it at load-test rates.
var (
	logMu   sync.Mutex
	lastLog = map[string]time.Time{}
)

func logSampled(key, format string, args ...interface{}) {
	logMu.Lock()
	defer logMu.Unlock()
	if time.Since(lastLog[key]) < 5*time.Second {
		return
	}
	lastLog[key] = time.Now()
	fmt.Printf(format+"\n", args...)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "sandbox-sdk-loadgen: "+format+"\n", args...)
	os.Exit(1)
}
