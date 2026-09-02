package apiserver

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/managedfields"
	genericapiserver "k8s.io/apiserver/pkg/server"
	apiservercompatibility "k8s.io/apiserver/pkg/util/compatibility"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kube-openapi/pkg/builder3"
	"k8s.io/kube-openapi/pkg/validation/spec"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

func TestManagedFieldsTypeConverterResolvesSandbox(t *testing.T) {
	cfg := NewOpenAPIV3Config()
	models, err := builder3.BuildOpenAPIDefinitionsForResources(cfg,
		sandboxDefPrefix+"Sandbox",
		sandboxDefPrefix+"SandboxList",
	)
	if err != nil {
		t.Fatalf("build openapi models: %v", err)
	}
	tc, err := managedfields.NewTypeConverter(models, false)
	if err != nil {
		t.Fatalf("new type converter: %v", err)
	}

	sb := &sandboxv1beta1.Sandbox{}
	sb.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("Sandbox"))
	sb.Name = "probe"
	sb.Namespace = "default"
	if _, err := tc.ObjectToTyped(sb); err != nil {
		t.Fatalf("ObjectToTyped(Sandbox) must succeed (root cause of [SHOULD NOT HAPPEN]): %v", err)
	}

	list := &sandboxv1beta1.SandboxList{}
	list.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("SandboxList"))
	if _, err := tc.ObjectToTyped(list); err != nil {
		t.Fatalf("ObjectToTyped(SandboxList) must succeed: %v", err)
	}
}

func TestEveryServedTypeHasAnOpenAPIModel(t *testing.T) {
	defs := sandboxOpenAPIDefinitions(func(path string) spec.Ref { return spec.Ref{} })

	for _, kind := range []string{
		"Sandbox",
		"SandboxList",
		"SandboxPauseOptions",
		"SandboxResumeOptions",
		"SandboxForkOptions",
		"SandboxForkResult",
		"SandboxSnapshotOptions",
		"SandboxSnapshotResult",
	} {
		key := sandboxDefPrefix + kind
		def, ok := defs[key]
		if !ok {
			t.Errorf("no OpenAPI model for %s; InstallAPIGroup will fail to start the apiserver", kind)
			continue
		}
		gvks, ok := def.Schema.Extensions["x-kubernetes-group-version-kind"]
		if !ok {
			t.Errorf("%s has no x-kubernetes-group-version-kind; the managed-fields TypeConverter cannot map it", kind)
			continue
		}
		entries, ok := gvks.([]any)
		if !ok || len(entries) != 1 {
			t.Errorf("%s gvk extension = %v, want exactly one entry", kind, gvks)
			continue
		}
		m, _ := entries[0].(map[string]any)
		if m["kind"] != kind {
			t.Errorf("%s declares kind %v, want %s", kind, m["kind"], kind)
		}
	}
}

func TestInstallSandboxAPISucceeds(t *testing.T) {
	cfg := genericapiserver.NewConfig(Codecs)
	cfg.EffectiveVersion = apiservercompatibility.DefaultBuildEffectiveVersion()
	cfg.OpenAPIV3Config = NewOpenAPIV3Config()
	cfg.ExternalAddress = "localhost:6443"
	cfg.LoopbackClientConfig = &restclient.Config{Host: "localhost:6443"}

	server, err := cfg.Complete(nil).New("test-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		t.Fatalf("build generic apiserver: %v", err)
	}
	if err := InstallSandboxAPI(server, nil); err != nil {
		t.Fatalf("InstallSandboxAPI: %v", err)
	}
}
