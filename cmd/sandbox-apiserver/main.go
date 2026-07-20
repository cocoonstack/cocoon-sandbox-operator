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
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	apiservercompatibility "k8s.io/apiserver/pkg/util/compatibility"
	restclient "k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"
	sandboxapiserver "github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale/apiserver"
)

// inventoryCacheSyncTimeout bounds the startup wait for the NodeInventory
// informer to sync. If the NodeInventory CRD is not installed or the
// kube-apiserver is unreachable, the binary fails loud instead of serving
// empty sandbox lists.
const inventoryCacheSyncTimeout = 2 * time.Minute

// options are the standard aggregated-apiserver options: secure serving plus
// delegated authentication/authorization (token/SAR review against the host
// kube-apiserver). There is deliberately no etcd option — this server stores
// nothing.
type options struct {
	SecureServing  *genericoptions.SecureServingOptionsWithLoopback
	Authentication *genericoptions.DelegatingAuthenticationOptions
	Authorization  *genericoptions.DelegatingAuthorizationOptions
	Features       *genericoptions.FeatureOptions
}

func newOptions() *options {
	o := &options{
		SecureServing:  genericoptions.NewSecureServingOptions().WithLoopback(),
		Authentication: genericoptions.NewDelegatingAuthenticationOptions(),
		Authorization:  genericoptions.NewDelegatingAuthorizationOptions(),
		Features:       genericoptions.NewFeatureOptions(),
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
	store := scale.NewScatterGatherStore(scale.NewClientInventorySource(reader))

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

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "cocoon-sandbox-apiserver:", err)
		os.Exit(1)
	}
}
