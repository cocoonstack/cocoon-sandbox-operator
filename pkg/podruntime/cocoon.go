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

// Package podruntime adapts agent-sandbox Pods to Cocoon runtime backends.
package podruntime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
)

const (
	// RuntimeAnnotation selects the runtime integration for one Sandbox Pod.
	RuntimeAnnotation = "sandbox.cocoonstack.io/runtime"

	ModeVKCocoon = "vk-cocoon"
	ModeStandard = "standard"
	// DefaultMode keeps ordinary kubelet scheduling unless a Sandbox explicitly
	// opts into the vk-cocoon virtual-node contract.
	DefaultMode = ModeStandard

	vkProviderTaintKey = "virtual-kubelet.io/provider"
	vkNodeLabelKey     = "node.kubernetes.io/instance-type"
	vkNodeLabelValue   = "virtual-node"

	cocoonModeAnnotation    = "cocoonset.cocoonstack.io/mode"
	cocoonManagedAnnotation = "cocoonset.cocoonstack.io/managed"
	cocoonImageAnnotation   = "cocoonset.cocoonstack.io/image"
	cocoonOSAnnotation      = "cocoonset.cocoonstack.io/os"
	cocoonVMNameAnnotation  = "vm.cocoonstack.io/name"
)

// Mutator applies the selected Cocoon runtime defaults to newly-created Pods.
type Mutator struct {
	defaultMode string
}

// NewMutator validates defaultMode and returns a runtime Pod mutator.
func NewMutator(defaultMode string) (*Mutator, error) {
	mode := strings.TrimSpace(defaultMode)
	switch mode {
	case ModeVKCocoon, ModeStandard:
		return &Mutator{defaultMode: mode}, nil
	default:
		return nil, fmt.Errorf("unsupported default runtime %q", defaultMode)
	}
}

// MutatePod applies runtime defaults without replacing user-supplied values.
func (m *Mutator) MutatePod(_ context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) error {
	if sandbox == nil {
		return fmt.Errorf("sandbox is nil")
	}
	if pod == nil {
		return fmt.Errorf("pod is nil")
	}

	mode, explicit := requestedMode(pod, m.defaultMode)
	switch mode {
	case ModeStandard:
		return nil
	case ModeVKCocoon:
		if pod.Spec.RuntimeClassName != nil {
			if explicit {
				return fmt.Errorf("%s=%s conflicts with spec.runtimeClassName", RuntimeAnnotation, ModeVKCocoon)
			}
			return nil
		}
	default:
		return fmt.Errorf("unsupported %s value %q", RuntimeAnnotation, mode)
	}

	// A pinned NodeName bypasses the virtual-node selector entirely and would place
	// the sandbox on whatever node is named, defeating the vk-cocoon contract. Reject
	// it rather than silently producing an unschedulable/misrouted Pod.
	if pod.Spec.NodeName != "" {
		return fmt.Errorf("%s=%s conflicts with spec.nodeName=%s", RuntimeAnnotation, ModeVKCocoon, pod.Spec.NodeName)
	}

	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}
	if value, found := pod.Spec.NodeSelector[vkNodeLabelKey]; found && value != vkNodeLabelValue {
		return fmt.Errorf("node selector %s=%s conflicts with vk-cocoon value %s", vkNodeLabelKey, value, vkNodeLabelValue)
	}
	pod.Spec.NodeSelector[vkNodeLabelKey] = vkNodeLabelValue

	if !toleratesVKCocoon(pod.Spec.Tolerations) {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
			Key:      vkProviderTaintKey,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		})
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	setDefault(pod.Annotations, RuntimeAnnotation, ModeVKCocoon)
	setDefault(pod.Annotations, cocoonModeAnnotation, "run")
	setDefault(pod.Annotations, cocoonManagedAnnotation, "true")
	setDefault(pod.Annotations, cocoonVMNameAnnotation, stableVMName(sandbox.Namespace, sandbox.Name))
	if len(pod.Spec.Containers) > 0 {
		setDefault(pod.Annotations, cocoonImageAnnotation, pod.Spec.Containers[0].Image)
	}
	osName := string(corev1.Linux)
	if pod.Spec.OS != nil && pod.Spec.OS.Name != "" {
		osName = string(pod.Spec.OS.Name)
	}
	setDefault(pod.Annotations, cocoonOSAnnotation, strings.ToLower(osName))
	return nil
}

func requestedMode(pod *corev1.Pod, defaultMode string) (string, bool) {
	if pod.Annotations != nil {
		if value := strings.TrimSpace(pod.Annotations[RuntimeAnnotation]); value != "" {
			return value, true
		}
	}
	if pod.Spec.RuntimeClassName != nil {
		return ModeStandard, false
	}
	return defaultMode, false
}

func toleratesVKCocoon(tolerations []corev1.Toleration) bool {
	for _, toleration := range tolerations {
		if toleration.Key != vkProviderTaintKey || toleration.Operator != corev1.TolerationOpExists {
			continue
		}
		if toleration.Effect == "" || toleration.Effect == corev1.TaintEffectNoSchedule {
			return true
		}
	}
	return false
}

func setDefault(values map[string]string, key, value string) {
	if _, found := values[key]; !found && value != "" {
		values[key] = value
	}
}

func stableVMName(namespace, name string) string {
	raw := strings.ToLower(strings.Trim(strings.Join([]string{"sandbox", namespace, name}, "-"), "-"))
	var normalized strings.Builder
	lastDash := false
	for _, char := range raw {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
			normalized.WriteRune(char)
			lastDash = false
			continue
		}
		if !lastDash {
			normalized.WriteByte('-')
			lastDash = true
		}
	}
	prefix := strings.Trim(normalized.String(), "-")
	if prefix == "" {
		prefix = "sandbox"
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(namespace+"/"+name)))[:8]
	const maxPrefix = 63 - 1 - 8
	if len(prefix) > maxPrefix {
		prefix = strings.Trim(prefix[:maxPrefix], "-")
	}
	return prefix + "-" + digest
}
