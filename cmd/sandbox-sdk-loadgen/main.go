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
		Help:    "Latency of Delete(Sandbox) against the aggregated apiserver (exact-VM release).",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	createsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_creates_total",
		Help: "Sandboxes successfully created.",
	})
	deletesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_deletes_total",
		Help: "Sandboxes successfully deleted (only when --cleanup).",
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

type options struct {
	namespace   string
	image       string
	namegen     string
	concurrency int
	total       int
	interval    time.Duration
	timeout     time.Duration
	cleanup     bool
	metricsAddr string
}

func main() {
	var o options
	flag.StringVar(&o.namespace, "namespace", "sandbox-sdk-loadgen", "namespace to create Sandboxes in")
	flag.StringVar(&o.image, "template", "ghcr.io/cocoonstack/sandbox/rt@sha256:c8cab53a1e1684e6c0c95a06855001b0535be2862b7d4f72658f9f0e784c8778", "container image; picks the warm pool (image=template, no resources=size small, no net annotation=none)")
	flag.StringVar(&o.namegen, "name-prefix", "sdklg", "generated Sandbox name prefix")
	flag.IntVar(&o.concurrency, "concurrency", 10, "parallel create workers (in-flight creates)")
	flag.IntVar(&o.total, "total", 0, "stop after this many successful creates (0 = run until signalled)")
	flag.DurationVar(&o.interval, "interval", 0, "per-worker gap between creates (0 = as fast as possible)")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "per-create context timeout")
	flag.BoolVar(&o.cleanup, "cleanup", false, "delete each Sandbox right after creating it (release the claim); measures sustained warm-claim latency without draining the pool")
	flag.StringVar(&o.metricsAddr, "metrics-addr", ":9090", "Prometheus metrics listen address")
	flag.Parse()

	buildInfo.WithLabelValues(version).Set(1)
	for _, r := range []string{"no-warm-503", "throttled-429", "timeout", "conflict", "other"} {
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

	// Serve metrics for the whole run (and after a bounded run finishes) so
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

	fmt.Printf("sandbox-sdk-loadgen %s: ns=%s concurrency=%d total=%d cleanup=%v image=%s\n",
		version, o.namespace, o.concurrency, o.total, o.cleanup, o.image)

	run(ctx, cl, &o)

	// Keep /metrics alive so the final histogram/counters remain scrapeable.
	fmt.Println("run complete; serving /metrics until terminated")
	<-context.Background().Done()
}

// run fans out o.concurrency workers that create Sandboxes until the context is
// cancelled or the shared counter reaches o.total.
func run(ctx context.Context, cl client.Client, o *options) {
	var created int64
	var wg sync.WaitGroup
	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var seq int
			for {
				if ctx.Err() != nil {
					return
				}
				if o.total > 0 && atomic.LoadInt64(&created) >= int64(o.total) {
					return
				}
				name := fmt.Sprintf("%s-w%d-%d", o.namegen, worker, seq)
				seq++
				if createOne(ctx, cl, o, name) {
					atomic.AddInt64(&created, 1)
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
}

// createOne issues a single Create (and optional Delete), records latency and
// outcome, and returns true on a successful create.
func createOne(ctx context.Context, cl client.Client, o *options, name string) bool {
	sb := newSandbox(o.namespace, name, o.image)

	cctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	inflight.Inc()
	start := time.Now()
	err := cl.Create(cctx, sb)
	elapsed := time.Since(start).Seconds()
	inflight.Dec()

	if err != nil {
		createFailed.WithLabelValues(classify(err)).Inc()
		return false
	}
	createSeconds.Observe(elapsed)
	createsTotal.Inc()

	if o.cleanup {
		deleteOne(ctx, cl, o, sb)
	}
	return true
}

func deleteOne(ctx context.Context, cl client.Client, o *options, sb *sandboxv1beta1.Sandbox) {
	dctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	start := time.Now()
	if err := cl.Delete(dctx, sb); err != nil && !apierrors.IsNotFound(err) {
		return
	}
	deleteSeconds.Observe(time.Since(start).Seconds())
	deletesTotal.Inc()
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
// derived pool has no warm VM (NewServiceUnavailable in storage.Create).
func classify(err error) string {
	switch {
	case apierrors.IsServiceUnavailable(err):
		return "no-warm-503"
	case apierrors.IsTooManyRequests(err):
		return "throttled-429"
	case apierrors.IsServerTimeout(err), apierrors.IsTimeout(err), err == context.DeadlineExceeded:
		return "timeout"
	case apierrors.IsConflict(err), apierrors.IsAlreadyExists(err):
		return "conflict"
	default:
		return "other"
	}
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "sandbox-sdk-loadgen: "+format+"\n", args...)
	os.Exit(1)
}
