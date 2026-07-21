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

package scale

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// NetDefault is the pool network mode used when none is annotated: the hardened,
// NIC-less Firecracker lane.
const NetDefault = "none"

// Coarse, documented t-shirt-size thresholds mapping a container's CPU/memory
// request (falling back to its limit) onto the warm pool size classes.
var (
	sizeCPUMedium = resource.MustParse("1")
	sizeMemMedium = resource.MustParse("2Gi")
	sizeCPULarge  = resource.MustParse("4")
	sizeMemLarge  = resource.MustParse("8Gi")
)

// PoolKeyFor derives the warm-pool key for a workload. It is the SINGLE source of
// pool-key truth: the aggregated apiserver derives it from a Sandbox's
// podTemplate on Create, and the SandboxWarmPool driver derives it from a
// SandboxTemplate's podTemplate when setting node warm targets. Both MUST agree
// or a Create would never match the warm capacity the pool driver provisions
// (perpetual 503). Template = the first container image; Net = the given net
// (defaulted to NetDefault); Size = the first container's t-shirt class.
func PoolKeyFor(containers []corev1.Container, net string) PoolKey {
	var template string
	if len(containers) > 0 {
		template = containers[0].Image
	}
	if net == "" {
		net = NetDefault
	}
	return PoolKey{Template: template, Net: net, Size: SizeClassForContainers(containers)}
}

// SizeClassForContainers maps the first container's CPU/memory onto small|medium|
// large. It prefers requests, falls back to limits, and defaults to "small" when
// neither is set. Thresholds: >4 CPU or >8Gi -> large; >1 CPU or >2Gi -> medium.
func SizeClassForContainers(containers []corev1.Container) string {
	if len(containers) == 0 {
		return "small"
	}
	res := containers[0].Resources
	cpu := res.Requests.Cpu()
	if cpu.IsZero() {
		cpu = res.Limits.Cpu()
	}
	mem := res.Requests.Memory()
	if mem.IsZero() {
		mem = res.Limits.Memory()
	}
	switch {
	case cpu.Cmp(sizeCPULarge) > 0 || mem.Cmp(sizeMemLarge) > 0:
		return "large"
	case cpu.Cmp(sizeCPUMedium) > 0 || mem.Cmp(sizeMemMedium) > 0:
		return "medium"
	default:
		return "small"
	}
}
