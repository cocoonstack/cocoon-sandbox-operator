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

// Command sandbox-apiserver is the L3 aggregated apiserver for
// sandboxes.agents.x-k8s.io. It serves the resource by scatter-gathering
// per-node NodeInventory objects (the metrics.k8s.io pattern) and stores NO
// per-sandbox object in etcd. It is registered with the kube-apiserver via the
// APIService in config/apiservice/, exactly as metrics-server is.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	apiservercompatibility "k8s.io/apiserver/pkg/util/compatibility"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	extv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"
	sandboxapiserver "github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale/apiserver"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale/warmpool"
)

// inventoryCacheSyncTimeout bounds the startup wait for the NodeInventory
// informer to sync. If the NodeInventory CRD is not installed or the
// kube-apiserver is unreachable, the binary fails loud instead of serving
// empty sandbox lists.
const inventoryCacheSyncTimeout = 2 * time.Minute

// options are the standard aggregated-apiserver options: secure serving plus
// delegated authentication/authorization (token/SAR review against the host
// kube-apiserver). There is deliberately no etcd option — this server stores
// nothing. The sandboxd token wires the node-local claim/release write path.
type options struct {
	SecureServing  *genericoptions.SecureServingOptionsWithLoopback
	Authentication *genericoptions.DelegatingAuthenticationOptions
	Authorization  *genericoptions.DelegatingAuthorizationOptions
	Features       *genericoptions.FeatureOptions

	// SandboxdToken is the uniform fleet-wide sandboxd api_token presented on the
	// node-local claim/release verbs. SandboxdTokenFile, when set, is read at
	// startup (a Secret mount) and takes precedence. When both are empty the
	// Create/Delete write path stays disabled (fails closed).
	SandboxdToken     string
	SandboxdTokenFile string

	// WarmPoolDriver enables the in-process SandboxWarmPool → sandboxd pool
	// reconcile loop (the control-plane surface for warm capacity). Pool-level,
	// O(pools+nodes); never per-sandbox. WarmPoolInterval is its resync cadence.
	WarmPoolDriver   bool
	WarmPoolInterval time.Duration
}

func newOptions() *options {
	o := &options{
		SecureServing:  genericoptions.NewSecureServingOptions().WithLoopback(),
		Authentication: genericoptions.NewDelegatingAuthenticationOptions(),
		Authorization:  genericoptions.NewDelegatingAuthorizationOptions(),
		Features:       genericoptions.NewFeatureOptions(),
		WarmPoolDriver: true,
	}
	o.SecureServing.BindPort = 6443
	// Allow running without a remote kubeconfig (in-cluster service account).
	o.Authentication.RemoteKubeConfigFileOptional = true
	o.Authorization.RemoteKubeConfigFileOptional = true
	return o
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	o.Authorization.AddFlags(fs)
	o.Features.AddFlags(fs)
	fs.StringVar(&o.SandboxdToken, "sandboxd-token", o.SandboxdToken,
		"Uniform fleet-wide sandboxd api_token presented on node-local claim/release. Prefer --sandboxd-token-file for a Secret mount.")
	fs.StringVar(&o.SandboxdTokenFile, "sandboxd-token-file", o.SandboxdTokenFile,
		"Path to a file (Secret mount) holding the sandboxd api_token; overrides --sandboxd-token when set.")
	fs.BoolVar(&o.WarmPoolDriver, "enable-warm-pool-driver", o.WarmPoolDriver,
		"Run the in-process SandboxWarmPool → sandboxd pool reconcile loop (control-plane warm-capacity surface; pool-level, never per-sandbox).")
	fs.DurationVar(&o.WarmPoolInterval, "warm-pool-sync-interval", o.WarmPoolInterval,
		"Resync cadence for the SandboxWarmPool driver, and with it the sampling period of the warm count in pool status (0 = default 5s).")
}

// resolveSandboxdToken returns the sandboxd token, reading it from the token file
// (a Secret mount) when one is configured, else the literal flag value.
func (o *options) resolveSandboxdToken() (string, error) {
	if o.SandboxdTokenFile == "" {
		return o.SandboxdToken, nil
	}
	b, err := os.ReadFile(o.SandboxdTokenFile)
	if err != nil {
		return "", fmt.Errorf("read sandboxd token file %q: %w", o.SandboxdTokenFile, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// serverConfig assembles a GenericAPIServer config from the options.
func (o *options) serverConfig() (*genericapiserver.Config, error) {
	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, nil); err != nil {
		return nil, fmt.Errorf("create self-signed certificates: %w", err)
	}
	cfg := genericapiserver.NewConfig(sandboxapiserver.Codecs)
	cfg.EffectiveVersion = apiservercompatibility.DefaultBuildEffectiveVersion()
	cfg.OpenAPIV3Config = sandboxapiserver.NewOpenAPIV3Config()
	if err := o.SecureServing.ApplyTo(&cfg.SecureServing, &cfg.LoopbackClientConfig); err != nil {
		return nil, fmt.Errorf("apply secure serving: %w", err)
	}
	if err := o.Authentication.ApplyTo(&cfg.Authentication, cfg.SecureServing, nil); err != nil {
		return nil, fmt.Errorf("apply authentication: %w", err)
	}
	if err := o.Authorization.ApplyTo(&cfg.Authorization); err != nil {
		return nil, fmt.Errorf("apply authorization: %w", err)
	}
	return cfg, nil
}

func run() error {
	o := newOptions()
	fs := pflag.NewFlagSet("cocoon-sandbox-apiserver", pflag.ExitOnError)
	o.addFlags(fs)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	cfg, err := o.serverConfig()
	if err != nil {
		return err
	}
	// Route controller-runtime logs (the warm-pool driver's) through klog so they
	// land in the apiserver's own log stream instead of being silently discarded.
	ctrl.SetLogger(klog.NewKlogr())
	ctx := genericapiserver.SetupSignalContext()

	// The store reads NodeInventory objects through a cache-fed reader: the
	// O(nodes) enumeration is served from an informer, never a hot-path LIST
	// against the kube-apiserver/etcd.
	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}
	reader, err := startInventoryCache(ctx, restCfg)
	if err != nil {
		return err
	}
	token, err := o.resolveSandboxdToken()
	if err != nil {
		return err
	}
	// WithClaimRouting wires the node-local claim/release write path: the uniform
	// fleet token plus a per-node sandboxd HTTP client keyed on each node's
	// advertised address (NodeInventory.Address). Empty token leaves it fail-closed.
	invSource := scale.NewClientInventorySource(reader)
	store := scale.NewScatterGatherStore(
		invSource,
		scale.WithClaimRouting(token, scale.NewSandboxdClientFactory()),
	)

	// The SandboxWarmPool driver is the control-plane surface for warm capacity:
	// it reconciles official SandboxWarmPool CRs into node-local sandboxd pool
	// targets and writes their status back from live inventory. It runs in THIS
	// process alongside the NodeInventory cache — one component polls node state
	// and writes back CR status. It is pool-level (O(pools+nodes)); there is no
	// per-sandbox reconcile, so the apiserver never carries per-sandbox load.
	if o.WarmPoolDriver {
		if err := startWarmPoolDriver(ctx, restCfg, token, o.WarmPoolInterval); err != nil {
			return err
		}
	}

	server, err := cfg.Complete(nil).New("cocoon-sandbox-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("build generic apiserver: %w", err)
	}
	if err := sandboxapiserver.InstallSandboxAPI(server, store); err != nil {
		return err
	}
	return server.PrepareRun().RunWithContext(ctx)
}

// startInventoryCache builds, starts, and syncs a controller-runtime cache
// scoped to exactly the NodeInventory GVK, returning it as the store's reader.
// ReaderFailOnMissingInformer makes a read of any other GVK fail loudly instead
// of silently spawning a cluster-wide informer, so the cache watches nothing
// but the O(nodes) NodeInventory objects.
func startInventoryCache(ctx context.Context, restCfg *restclient.Config) (cache.Cache, error) {
	inv := &unstructured.Unstructured{}
	inv.SetGroupVersionKind(scale.NodeInventoryGVK)
	invCache, err := cache.New(restCfg, cache.Options{
		ByObject:                    map[client.Object]cache.ByObject{inv: {}},
		ReaderFailOnMissingInformer: true,
	})
	if err != nil {
		return nil, fmt.Errorf("build inventory cache: %w", err)
	}
	// Register the informer up front: with ReaderFailOnMissingInformer set,
	// reads never create informers implicitly.
	if _, err := invCache.GetInformer(ctx, inv); err != nil {
		return nil, fmt.Errorf("register node inventory informer: %w", err)
	}
	cacheErr := make(chan error, 1)
	go func() { cacheErr <- invCache.Start(ctx) }()
	syncCtx, cancel := context.WithTimeout(ctx, inventoryCacheSyncTimeout)
	defer cancel()
	if !invCache.WaitForCacheSync(syncCtx) {
		select {
		case err := <-cacheErr:
			if err != nil {
				return nil, fmt.Errorf("run inventory cache: %w", err)
			}
		default:
		}
		return nil, fmt.Errorf("node inventory cache did not sync within %s (is the %s CRD installed?)",
			inventoryCacheSyncTimeout, scale.NodeInventoryGVK.GroupKind())
	}
	return invCache, nil
}

// startWarmPoolDriver runs the SandboxWarmPool driver as a controller inside a
// controller-runtime manager: it WATCHES SandboxWarmPool and NodeInventory, so a
// `kubectl apply/patch/delete` reconciles in milliseconds instead of waiting for
// a poll tick (the only latency that ever mattered — the node side fills a pool
// in under a second). Leader election makes exactly one of the apiserver replicas
// drive the pools. The manager's own metrics/health servers are disabled; the
// aggregated apiserver owns the serving port.
func startWarmPoolDriver(ctx context.Context, restCfg *restclient.Config, token string, interval time.Duration) error {
	scheme := runtime.NewScheme()
	if err := extv1beta1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register extensions scheme: %w", err)
	}
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:  "0",
		LeaderElection:          true,
		LeaderElectionID:        "cocoon-warmpool-driver",
		LeaderElectionNamespace: currentNamespace(),
	})
	if err != nil {
		return fmt.Errorf("build warm-pool manager: %w", err)
	}
	driver := warmpool.New(nil, nil, token, warmpool.NewSandboxdFactory(), warmpool.Options{
		Interval: interval,
		Log:      ctrl.Log.WithName("warmpool"),
	})
	if err := driver.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("set up warm-pool controller: %w", err)
	}
	go func() {
		if err := mgr.Start(ctx); err != nil {
			klog.ErrorS(err, "warm-pool manager exited")
		}
	}()
	return nil
}

// currentNamespace returns the pod's namespace (for the leader-election lease),
// read from the service-account mount, defaulting to the deployment namespace.
func currentNamespace() string {
	if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(b)); ns != "" {
			return ns
		}
	}
	return "cocoon-sandbox-system"
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "cocoon-sandbox-apiserver:", err)
		os.Exit(1)
	}
}
