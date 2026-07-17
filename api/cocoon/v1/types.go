/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// ConditionReady reports whether a Cocoon sandbox workload is ready for use.
	ConditionReady = "Ready"

	// PhasePending means the controller has not claimed a sandbox yet.
	PhasePending CocoonSandboxPhase = "Pending"
	// PhaseRunning means sandboxd has returned a live claim.
	PhaseRunning CocoonSandboxPhase = "Running"
	// PhaseFailed means the controller could not claim or release the sandbox.
	PhaseFailed CocoonSandboxPhase = "Failed"

	// SourceWarmPool means the claim consumed an already warm sandbox.
	SourceWarmPool CocoonSandboxSource = "WarmPool"
	// SourceGolden means the claim was likely restored from a golden template.
	SourceGolden CocoonSandboxSource = "Golden"
	// SourceCold means the claim likely fell back to cold provisioning.
	SourceCold CocoonSandboxSource = "Cold"

	// NetNone is the hardened default sandbox lane.
	NetNone CocoonSandboxNet = "none"
	// NetEgress attaches an egress-capable network lane.
	NetEgress CocoonSandboxNet = "egress"

	// SizeSmall is the default resource tier.
	SizeSmall CocoonSandboxSize = "small"
	// SizeMedium is the medium resource tier.
	SizeMedium CocoonSandboxSize = "medium"
	// SizeLarge is the large resource tier.
	SizeLarge CocoonSandboxSize = "large"
)

// CocoonSandboxPhase is the high-level lifecycle phase reported in status.
// +kubebuilder:validation:Enum=Pending;Running;Failed
type CocoonSandboxPhase string

// CocoonSandboxSource records which provisioning lane likely served a claim.
// +kubebuilder:validation:Enum=WarmPool;Golden;Cold
type CocoonSandboxSource string

// CocoonSandboxNet selects the sandbox network lane.
// +kubebuilder:validation:Enum=none;egress
type CocoonSandboxNet string

// CocoonSandboxSize selects a coarse resource tier.
// +kubebuilder:validation:Enum=small;medium;large
type CocoonSandboxSize string

// EnvVarsInjectionPolicy controls whether environment variables may be injected.
// +kubebuilder:validation:Enum=Allowed;Disabled
type EnvVarsInjectionPolicy string

// TemplateNetworkPolicy describes outbound network policy at template level.
type TemplateNetworkPolicy struct {
	// EgressPolicy is required when net=egress. The controller records it as
	// policy intent; lane-level enforcement lives in sandboxd/cocoon.
	// +optional
	EgressPolicy string `json:"egressPolicy,omitempty"`
}

// TemplateBootstrap declares template preparation commands.
type TemplateBootstrap struct {
	// Command is the optional process used when promoting a ready-to-run template.
	// +optional
	Command []string `json:"command,omitempty"`
}

// TemplatePromotePolicy declares whether this template may be promoted.
type TemplatePromotePolicy struct {
	// Enabled allows promote/checkpoint flows to publish this template.
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

// CocoonSandboxTemplateSpec describes a reusable hot sandbox environment.
type CocoonSandboxTemplateSpec struct {
	// Template is the sandboxd template key used in POST /v1/claim.
	// +required
	Template string `json:"template"`
	// Image documents the image used to build this template.
	// +optional
	Image string `json:"image,omitempty"`
	// Net selects the default network lane.
	// +optional
	// +kubebuilder:default=none
	Net CocoonSandboxNet `json:"net,omitempty"`
	// Size selects the default resource tier.
	// +optional
	// +kubebuilder:default=small
	Size CocoonSandboxSize `json:"size,omitempty"`
	// OS is informational in v1. Only linux is supported by CocoonSandbox v1.
	// +optional
	// +kubebuilder:default=linux
	OS string `json:"os,omitempty"`
	// EnvVarsInjectionPolicy controls whether env injection is allowed.
	// +optional
	EnvVarsInjectionPolicy EnvVarsInjectionPolicy `json:"envVarsInjectionPolicy,omitempty"`
	// Network captures egress policy intent.
	// +optional
	Network *TemplateNetworkPolicy `json:"network,omitempty"`
	// Bootstrap captures provisioning/start commands.
	// +optional
	Bootstrap *TemplateBootstrap `json:"bootstrap,omitempty"`
	// PromotePolicy captures template promotion intent.
	// +optional
	PromotePolicy *TemplatePromotePolicy `json:"promotePolicy,omitempty"`
}

// CocoonSandboxTemplateStatus reports template readiness.
type CocoonSandboxTemplateStatus struct {
	// GoldenReady reports whether at least one node has a golden for this template.
	// +optional
	GoldenReady bool `json:"goldenReady,omitempty"`
	// Digest records the template image or snapshot digest when known.
	// +optional
	Digest string `json:"digest,omitempty"`
	// Conditions describe template-level readiness.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Golden",type="boolean",JSONPath=".status.goldenReady"
// CocoonSandboxTemplate defines a reusable hot sandbox environment.
type CocoonSandboxTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec CocoonSandboxTemplateSpec `json:"spec,omitempty"`
	// +optional
	Status CocoonSandboxTemplateStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// CocoonSandboxTemplateList contains a list of templates.
type CocoonSandboxTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CocoonSandboxTemplate `json:"items"`
}

// CocoonSandboxPoolSpec declares warm capacity for one template.
type CocoonSandboxPoolSpec struct {
	// Replicas is the desired warm sandbox count.
	// +required
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`
	// TemplateRef references a CocoonSandboxTemplate in the same namespace.
	// +required
	TemplateRef corev1.LocalObjectReference `json:"templateRef"`
	// UpdateStrategy controls when pool membership is replaced.
	// +optional
	UpdateStrategy *CocoonSandboxPoolUpdateStrategy `json:"updateStrategy,omitempty"`
}

// CocoonSandboxPoolUpdateStrategy controls warm pool replacement.
type CocoonSandboxPoolUpdateStrategy struct {
	// Type is Recreate or OnReplenish.
	// +optional
	// +kubebuilder:validation:Enum=Recreate;OnReplenish
	Type string `json:"type,omitempty"`
}

// CocoonSandboxPoolStatus reports observed warm capacity.
type CocoonSandboxPoolStatus struct {
	// Replicas is the observed target.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// ReadyReplicas is the observed warm count.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// WarmByNode breaks warm capacity down by node.
	// +optional
	WarmByNode []CocoonSandboxPoolNodeStatus `json:"warmByNode,omitempty"`
	// Conditions describe pool readiness.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CocoonSandboxPoolNodeStatus reports one node's pool capacity.
type CocoonSandboxPoolNodeStatus struct {
	// NodeName is the CocoonSandboxNode name.
	// +required
	NodeName string `json:"nodeName"`
	// Warm is the current warm count.
	// +optional
	Warm int32 `json:"warm,omitempty"`
	// Target is the desired warm count.
	// +optional
	Target int32 `json:"target,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="Target",type="integer",JSONPath=".status.replicas"
// CocoonSandboxPool declares warm capacity.
type CocoonSandboxPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec CocoonSandboxPoolSpec `json:"spec,omitempty"`
	// +optional
	Status CocoonSandboxPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// CocoonSandboxPoolList contains a list of pools.
type CocoonSandboxPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CocoonSandboxPool `json:"items"`
}

// CocoonSandboxLifecycle controls sandbox TTL and shutdown behavior.
type CocoonSandboxLifecycle struct {
	// TTLSeconds bounds the sandbox lifetime on the owning node.
	// +optional
	// +kubebuilder:validation:Minimum=1
	TTLSeconds int32 `json:"ttlSeconds,omitempty"`
	// ShutdownPolicy controls what happens after TTL/idle expiry.
	// +optional
	// +kubebuilder:validation:Enum=Delete;Hibernate
	ShutdownPolicy string `json:"shutdownPolicy,omitempty"`
}

// CocoonSandboxAccess declares allowed access modes.
type CocoonSandboxAccess struct {
	// Modes lists access modes such as mcp, exec, pty, or files.
	// +optional
	Modes []string `json:"modes,omitempty"`
}

// CocoonSandboxPlacement constrains which CocoonSandboxNode may claim the sandbox.
type CocoonSandboxPlacement struct {
	// NodeName pins the sandbox to a named CocoonSandboxNode.
	// +optional
	NodeName string `json:"nodeName,omitempty"`
	// NodeSelector matches labels on CocoonSandboxNode and on the referenced Kubernetes Node.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// CocoonSandboxSpec declares one claimed hot sandbox.
type CocoonSandboxSpec struct {
	// TemplateRef references a CocoonSandboxTemplate in the same namespace.
	// +required
	TemplateRef corev1.LocalObjectReference `json:"templateRef"`
	// PoolPolicy selects the warm pool policy: default, none, or a pool name.
	// +optional
	PoolPolicy string `json:"poolPolicy,omitempty"`
	// Net overrides the template network lane.
	// +optional
	Net CocoonSandboxNet `json:"net,omitempty"`
	// Size overrides the template resource tier.
	// +optional
	Size CocoonSandboxSize `json:"size,omitempty"`
	// Lifecycle controls TTL and shutdown behavior.
	// +optional
	Lifecycle *CocoonSandboxLifecycle `json:"lifecycle,omitempty"`
	// Access declares desired access modes.
	// +optional
	Access *CocoonSandboxAccess `json:"access,omitempty"`
	// Placement constrains sandboxd node selection.
	// +optional
	Placement *CocoonSandboxPlacement `json:"placement,omitempty"`
}

// CocoonSandboxEndpoint reports data-plane URLs.
type CocoonSandboxEndpoint struct {
	// SilkdUpgradeURL is the direct HTTP upgrade URL to the owning node.
	// +optional
	SilkdUpgradeURL string `json:"silkdUpgradeURL,omitempty"`
}

// CocoonSandboxStatus reports the claimed sandbox handle.
type CocoonSandboxStatus struct {
	// Phase is the high-level sandbox phase.
	// +optional
	Phase CocoonSandboxPhase `json:"phase,omitempty"`
	// Source is the provisioning source inferred from node pool state.
	// +optional
	Source CocoonSandboxSource `json:"source,omitempty"`
	// SandboxID is the sandboxd id.
	// +optional
	SandboxID string `json:"sandboxID,omitempty"`
	// OwnerNode is the CocoonSandboxNode that owns the claim.
	// +optional
	OwnerNode string `json:"ownerNode,omitempty"`
	// OwnerAddr is the sandboxd data-plane address.
	// +optional
	OwnerAddr string `json:"ownerAddr,omitempty"`
	// Deadline is the sandboxd lease deadline.
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`
	// Hibernated reports whether the sandbox is currently hibernated.
	// +optional
	Hibernated bool `json:"hibernated,omitempty"`
	// TokenSecretRef points to the Secret key containing the per-sandbox token.
	// +optional
	TokenSecretRef *corev1.SecretKeySelector `json:"tokenSecretRef,omitempty"`
	// Endpoint reports direct data-plane URLs.
	// +optional
	Endpoint *CocoonSandboxEndpoint `json:"endpoint,omitempty"`
	// ClaimLatencyMs records the controller-observed claim latency in milliseconds.
	// +optional
	ClaimLatencyMs string `json:"claimLatencyMs,omitempty"`
	// Conditions describe readiness.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Source",type="string",JSONPath=".status.source"
// +kubebuilder:printcolumn:name="Owner",type="string",JSONPath=".status.ownerNode"
// CocoonSandbox is a Kubernetes-native hot Linux microVM sandbox claim.
type CocoonSandbox struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec CocoonSandboxSpec `json:"spec,omitempty"`
	// +optional
	Status CocoonSandboxStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// CocoonSandboxList contains a list of sandboxes.
type CocoonSandboxList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CocoonSandbox `json:"items"`
}

// CocoonSandboxNodeSpec declares one sandboxd node.
type CocoonSandboxNodeSpec struct {
	// NodeName is the Kubernetes node name this sandboxd runs on.
	// +required
	NodeName string `json:"nodeName"`
	// Address is the sandboxd host:port data-plane address.
	// +required
	Address string `json:"address"`
	// APITokenSecretRef references the node-level sandboxd API token.
	// +optional
	APITokenSecretRef *corev1.SecretKeySelector `json:"apiTokenSecretRef,omitempty"`
}

// CocoonSandboxNodePoolStatus reports one pool on a sandboxd node.
type CocoonSandboxNodePoolStatus struct {
	// Template is the sandboxd template key.
	// +required
	Template string `json:"template"`
	// Net is the pool network lane.
	// +optional
	Net CocoonSandboxNet `json:"net,omitempty"`
	// Size is the pool resource tier.
	// +optional
	Size CocoonSandboxSize `json:"size,omitempty"`
	// Warm is the current warm count.
	// +optional
	Warm int32 `json:"warm,omitempty"`
	// Target is the desired warm count.
	// +optional
	Target int32 `json:"target,omitempty"`
	// Refilling is the current refill count.
	// +optional
	Refilling int32 `json:"refilling,omitempty"`
	// Golden reports whether a golden template exists on this node.
	// +optional
	Golden bool `json:"golden,omitempty"`
}

// CocoonSandboxNodeStatus reports sandboxd node state.
type CocoonSandboxNodeStatus struct {
	// Address is the observed sandboxd address.
	// +optional
	Address string `json:"address,omitempty"`
	// Pools reports warm pools.
	// +optional
	Pools []CocoonSandboxNodePoolStatus `json:"pools,omitempty"`
	// Claimed is the active claim count.
	// +optional
	Claimed int32 `json:"claimed,omitempty"`
	// Hibernated is the hibernated claim count.
	// +optional
	Hibernated int32 `json:"hibernated,omitempty"`
	// Peers reports mesh peers known by sandboxd.
	// +optional
	Peers []string `json:"peers,omitempty"`
	// Conditions describe node readiness.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Address",type="string",JSONPath=".status.address"
// +kubebuilder:printcolumn:name="Claimed",type="integer",JSONPath=".status.claimed"
// CocoonSandboxNode inventories one sandboxd node.
type CocoonSandboxNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec CocoonSandboxNodeSpec `json:"spec,omitempty"`
	// +optional
	Status CocoonSandboxNodeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// CocoonSandboxNodeList contains a list of nodes.
type CocoonSandboxNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CocoonSandboxNode `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(
			GroupVersion,
			&CocoonSandboxTemplate{},
			&CocoonSandboxTemplateList{},
			&CocoonSandboxPool{},
			&CocoonSandboxPoolList{},
			&CocoonSandbox{},
			&CocoonSandboxList{},
			&CocoonSandboxNode{},
			&CocoonSandboxNodeList{},
		)
		return nil
	})
}
