package v1alpha1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

func BenchmarkConvertRoundTrip(b *testing.B) {
	src := benchSandbox()
	b.ReportAllocs()
	var annBytes int
	for b.Loop() {
		dst := &v1beta1.Sandbox{}
		if err := src.ConvertTo(dst); err != nil {
			b.Fatalf("convert to: %v", err)
		}
		annBytes = len(dst.Annotations[v1alpha1SandboxStateAnnotation])
		back := &Sandbox{}
		if err := back.ConvertFrom(dst); err != nil {
			b.Fatalf("convert from: %v", err)
		}
	}
	b.ReportMetric(float64(annBytes), "bytes/ann")
}

func benchSandbox() *Sandbox {
	replicas := int32(1)
	return &Sandbox{
		Name:      "bench-sandbox",
		Namespace: "default",
		Labels:    map[string]string{"app": "agent", "team": "bench"},
		Annotations: map[string]string{
			"prometheus.io/scrape": "true",
		},
		Spec: SandboxSpec{
			Replicas: &replicas,
			PodTemplate: PodTemplate{
				ObjectMeta: PodMetadata{Labels: map[string]string{"pod": "agent"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "agent",
							Image:   "ghcr.io/cocoonstack/sandbox/rt:24.04",
							Command: []string{"/bin/agent", "--serve"},
							Env: []corev1.EnvVar{
								{Name: "MODE", Value: "warm"},
								{Name: "REGION", Value: "sg"},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("2"),
									corev1.ResourceMemory: resource.MustParse("8Gi"),
								},
							},
						},
						{Name: "sidecar", Image: "ghcr.io/cocoonstack/sandbox/proxy:1.2"},
					},
				},
			},
		},
		Status: SandboxStatus{
			ServiceFQDN:   "bench-sandbox.default.svc.cluster.local",
			LabelSelector: "sandbox=bench-sandbox",
			PodIPs:        []string{"10.244.1.17"},
			Replicas:      1,
			Conditions: []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "Ready",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
}
