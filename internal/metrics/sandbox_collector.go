// Copyright 2026 The Kubernetes Authors.
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

package metrics

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

const (
	kindSandboxClaim    = "SandboxClaim"
	kindSandboxWarmPool = "SandboxWarmPool"

	metricsCollectTimeout = 5 * time.Second
)

// AgentSandboxesMetricKey is used to aggregate counts for identical Sandboxes metric label combinations.
type AgentSandboxesMetricKey struct {
	Namespace      string
	ReadyCondition string
	Expired        string
	LaunchType     string
	Template       string
	OwnedBy        string
	CreatedBy      string
}

// SandboxCollector is a custom Prometheus collector that dynamically fetches sandbox counts.
type SandboxCollector struct {
	// baseCtx is the process lifetime context; Collect derives its scrape
	// timeout from it because the prometheus.Collector interface carries none.
	baseCtx            context.Context
	client             client.Client
	logger             logr.Logger
	agentSandboxesDesc *prometheus.Desc
}

func NewSandboxCollector(ctx context.Context, c client.Client, logger logr.Logger) *SandboxCollector {
	return &SandboxCollector{
		baseCtx:            ctx,
		client:             c,
		logger:             logger,
		agentSandboxesDesc: AgentSandboxesDesc,
	}
}

func (c *SandboxCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.agentSandboxesDesc
}

func (c *SandboxCollector) Collect(ch chan<- prometheus.Metric) {
	var sandboxList sandboxv1beta1.SandboxList
	ctx, cancel := context.WithTimeout(c.baseCtx, metricsCollectTimeout)
	defer cancel()

	// Copy-free cache read: the loop below only reads label inputs, and neither
	// mutates nor retains an item.
	if err := c.client.List(ctx, &sandboxList, client.UnsafeDisableDeepCopy); err != nil {
		c.logger.Error(err, "Failed to list sandboxes for metrics collection")
		return
	}

	counts := make(map[AgentSandboxesMetricKey]int)
	for i := range sandboxList.Items {
		sandbox := &sandboxList.Items[i]
		readyConditionStr := "false"
		expiredStr := "false"
		readyCond := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
		if readyCond != nil {
			if readyCond.Status == metav1.ConditionTrue {
				readyConditionStr = "true"
			}
			if readyCond.Reason == sandboxv1beta1.SandboxReasonExpired {
				expiredStr = "true"
			}
		}

		launchTypeStr := LaunchTypeCold
		if sandbox.Labels[sandboxv1beta1.SandboxLaunchTypeLabel] == sandboxv1beta1.SandboxLaunchTypeWarm {
			launchTypeStr = LaunchTypeWarm
		}

		sandboxTemplateStr := "unknown"
		if template, ok := sandbox.Annotations[sandboxv1beta1.SandboxTemplateRefAnnotation]; ok && template != "" {
			sandboxTemplateStr = template
		}

		apiVersion := extensionsv1beta1.GroupVersion.String()
		ownedByStr := "None"
		if controllerRef := metav1.GetControllerOf(sandbox); controllerRef != nil {
			if controllerRef.APIVersion == apiVersion {
				switch controllerRef.Kind {
				case kindSandboxClaim:
					ownedByStr = kindSandboxClaim
				case kindSandboxWarmPool:
					ownedByStr = kindSandboxWarmPool
				}
			}
		}

		createdByStr := "unknown"
		if val, ok := sandbox.Labels[sandboxv1beta1.CreatedByLabel]; ok {
			createdByStr = NormalizeCreatedBy(val)
		}

		key := AgentSandboxesMetricKey{
			Namespace:      sandbox.Namespace,
			ReadyCondition: readyConditionStr,
			Expired:        expiredStr,
			LaunchType:     launchTypeStr,
			Template:       sandboxTemplateStr,
			OwnedBy:        ownedByStr,
			CreatedBy:      createdByStr,
		}
		counts[key]++
	}

	for key, count := range counts {
		ch <- NewAgentSandboxesConstMetric(count, key)
	}
}

// NewAgentSandboxesConstMetric creates a new Prometheus ConstMetric for the agent_sandboxes gauge.
func NewAgentSandboxesConstMetric(count int, key AgentSandboxesMetricKey) prometheus.Metric {
	return prometheus.MustNewConstMetric(
		AgentSandboxesDesc,
		prometheus.GaugeValue,
		float64(count),
		key.Namespace,
		key.ReadyCondition,
		key.Expired,
		key.LaunchType,
		key.Template,
		key.OwnedBy,
		key.CreatedBy,
	)
}

// RegisterSandboxCollector registers the custom Prometheus collector for sandbox counts.
func RegisterSandboxCollector(ctx context.Context, c client.Client, logger logr.Logger) {
	collector := NewSandboxCollector(ctx, c, logger)
	if err := metrics.Registry.Register(collector); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			logger.Info("SandboxCollector already registered, ignoring")
			return
		}
		logger.Error(err, "Failed to register SandboxCollector")
	}
}
