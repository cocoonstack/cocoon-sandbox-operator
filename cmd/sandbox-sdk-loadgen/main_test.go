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

package main

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sandboxv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/api/v1beta1"
)

// TestRunIssuesExactlyTotal encodes the safety property that caused the
// 2026-07-21 incident when it was absent: a run must issue EXACTLY --total
// creates across all workers — never more — and with cleanup on, every created
// sandbox must be released (leaked == 0). If this test fails, the loadgen can
// again run away and drain the fleet's warm pools.
func TestRunIssuesExactlyTotal(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	o := &options{
		namespace:      "t",
		image:          "img",
		namegen:        "t",
		concurrency:    4,
		total:          25,
		timeout:        5 * time.Second,
		cleanup:        true,
		releaseTimeout: 5 * time.Second,
	}
	s := run(context.Background(), cl, o)

	if s.issued != 25 {
		t.Fatalf("issued = %d, want exactly total (25)", s.issued)
	}
	if s.created != 25 {
		t.Fatalf("created = %d, want 25 (fake client never fails)", s.created)
	}
	if s.released != 25 {
		t.Fatalf("released = %d, want 25 (cleanup must confirm every release)", s.released)
	}
	if s.leaked != 0 {
		t.Fatalf("leaked = %d, want 0", s.leaked)
	}

	list := &sandboxv1beta1.SandboxList{}
	if err := cl.List(context.Background(), list); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("%d sandboxes left behind after a cleanup run, want 0", len(list.Items))
	}
}

// TestConcurrencyClampedToTotal guards the small-run path: asking for 2 creates
// with 10 workers must still issue exactly 2.
func TestConcurrencyClampedToTotal(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	o := &options{
		namespace:      "t",
		image:          "img",
		namegen:        "t",
		concurrency:    10,
		total:          2,
		timeout:        5 * time.Second,
		cleanup:        true,
		releaseTimeout: 5 * time.Second,
	}
	s := run(context.Background(), cl, o)
	if s.issued != 2 || s.created != 2 {
		t.Fatalf("issued=%d created=%d, want exactly 2/2", s.issued, s.created)
	}
}
