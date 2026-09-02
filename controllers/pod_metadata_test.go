package controllers

import (
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

func TestUpdatePodMetadata(t *testing.T) {
	tests := []struct {
		name            string
		podLabels       map[string]string
		podAnnotations  map[string]string
		tmplLabels      map[string]string
		tmplAnnotations map[string]string
		sandboxLabels   map[string]string
		warmPoolOwned   bool
		wantLabels      map[string]string
		wantPropagated  string
		wantUpdated     bool
	}{
		{
			name:           "propagates template labels and records them",
			tmplLabels:     map[string]string{"team": "core", "tier": "web"},
			wantLabels:     map[string]string{sandboxLabel: "h1", "team": "core", "tier": "web"},
			wantPropagated: "team,tier",
			wantUpdated:    true,
		},
		{
			name:           "drops a label the template no longer carries",
			podLabels:      map[string]string{sandboxLabel: "h1", "team": "core", "gone": "yes"},
			podAnnotations: map[string]string{sandboxv1beta1.SandboxPropagatedLabelsAnnotation: "gone,team"},
			tmplLabels:     map[string]string{"team": "core"},
			wantLabels:     map[string]string{sandboxLabel: "h1", "team": "core"},
			wantPropagated: "team",
			wantUpdated:    true,
		},
		{
			name:            "refuses system-reserved keys from the template",
			tmplLabels:      map[string]string{sandboxv1beta1.SandboxWarmPoolLabel: "spoofed", "team": "core"},
			tmplAnnotations: map[string]string{sandboxv1beta1.SandboxPropagatedLabelsAnnotation: "spoofed"},
			wantLabels:      map[string]string{sandboxLabel: "h1", "team": "core"},
			wantPropagated:  "team",
			wantUpdated:     true,
		},
		{
			name:           "scrubs a system label an older controller propagated",
			podLabels:      map[string]string{sandboxLabel: "h1", sandboxv1beta1.SandboxWarmPoolLabel: "stale"},
			podAnnotations: map[string]string{sandboxv1beta1.SandboxPropagatedLabelsAnnotation: sandboxv1beta1.SandboxWarmPoolLabel},
			wantLabels:     map[string]string{sandboxLabel: "h1"},
			wantUpdated:    true,
		},
		{
			name:           "keeps the controller-owned name hash while scrubbing",
			podLabels:      map[string]string{sandboxLabel: "h1"},
			podAnnotations: map[string]string{sandboxv1beta1.SandboxPropagatedLabelsAnnotation: sandboxLabel},
			wantLabels:     map[string]string{sandboxLabel: "h1"},
			wantUpdated:    true,
		},
		{
			name:          "mirrors the warm-pool hash while an extensions owner holds the Sandbox",
			sandboxLabels: map[string]string{sandboxv1beta1.SandboxWarmPoolLabel: "wp1", sandboxv1beta1.SandboxTemplateRefHashLabel: "tr1"},
			warmPoolOwned: true,
			wantLabels: map[string]string{
				sandboxLabel:                               "h1",
				sandboxv1beta1.SandboxWarmPoolLabel:        "wp1",
				sandboxv1beta1.SandboxTemplateRefHashLabel: "tr1",
			},
			wantUpdated: true,
		},
		{
			name:          "removes the warm-pool hash once the Sandbox is unowned",
			podLabels:     map[string]string{sandboxLabel: "h1", sandboxv1beta1.SandboxWarmPoolLabel: "wp1"},
			sandboxLabels: map[string]string{sandboxv1beta1.SandboxWarmPoolLabel: "wp1"},
			wantLabels:    map[string]string{sandboxLabel: "h1"},
			wantUpdated:   true,
		},
		{
			name:      "reports no change when everything already matches",
			podLabels: map[string]string{sandboxLabel: "h1", "team": "core"},
			podAnnotations: map[string]string{
				sandboxv1beta1.SandboxPropagatedLabelsAnnotation:      "team",
				sandboxv1beta1.SandboxPropagatedAnnotationsAnnotation: "",
			},
			tmplLabels:     map[string]string{"team": "core"},
			wantLabels:     map[string]string{sandboxLabel: "h1", "team": "core"},
			wantPropagated: "team",
			wantUpdated:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{
				Name:        "p",
				Labels:      maps.Clone(tt.podLabels),
				Annotations: maps.Clone(tt.podAnnotations),
			}
			sandbox := &sandboxv1beta1.Sandbox{
				Name:   "s",
				Labels: tt.sandboxLabels,
			}
			sandbox.Spec.PodTemplate.ObjectMeta.Labels = tt.tmplLabels
			sandbox.Spec.PodTemplate.ObjectMeta.Annotations = tt.tmplAnnotations
			if tt.warmPoolOwned {
				sandbox.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: "extensions.agents.x-k8s.io/v1beta1",
					Kind:       "SandboxWarmPool",
					Name:       "wp",
					UID:        "wp-uid",
					Controller: new(true),
				}}
			}

			r := &SandboxReconciler{}
			got := r.updatePodMetadata(t.Context(), pod, sandbox, "h1")

			if got != tt.wantUpdated {
				t.Errorf("updated = %v, want %v", got, tt.wantUpdated)
			}
			if !maps.Equal(pod.Labels, tt.wantLabels) {
				t.Errorf("labels = %v, want %v", pod.Labels, tt.wantLabels)
			}
			if p := pod.Annotations[sandboxv1beta1.SandboxPropagatedLabelsAnnotation]; p != tt.wantPropagated {
				t.Errorf("propagated-labels = %q, want %q", p, tt.wantPropagated)
			}
		})
	}
}

func TestResourceOwnershipIotaValues(t *testing.T) {
	for _, c := range []struct {
		got  resourceOwnership
		want int
		name string
	}{
		{resourceOwnedBySandbox, 0, "resourceOwnedBySandbox"},
		{resourceUnowned, 1, "resourceUnowned"},
		{resourceOwnedByOther, 2, "resourceOwnedByOther"},
	} {
		if int(c.got) != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}
