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

package e2bcompat

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// lifecycleStore records which verb the compat layer routed where, so the tests
// assert the translation rather than any node behavior.
type lifecycleStore struct {
	fakeStore

	pausedNode, pausedID   string
	resumedNode, resumedID string
	forkedID               string
	forkCount              int
	forkChildren           []scale.Assignment
	snapshotName           string
	snapshot               scale.Snapshot
	err                    error

	// nodePaused is what the owning node reports for Stats().Paused — the
	// authoritative answer isPaused now consults instead of the cached label.
	nodePaused bool
}

func (f *lifecycleStore) Stats(context.Context, string, string) (scale.SandboxStats, error) {
	return scale.SandboxStats{Paused: f.nodePaused, CPUCount: 1, MemTotalBytes: 512 << 20}, nil
}

func (f *lifecycleStore) Pause(_ context.Context, node, id string) error {
	f.pausedNode, f.pausedID = node, id
	return f.err
}

func (f *lifecycleStore) Resume(_ context.Context, node, id string) error {
	f.resumedNode, f.resumedID = node, id
	return f.err
}

func (f *lifecycleStore) Fork(_ context.Context, _, id string, count, _ int) ([]scale.Assignment, error) {
	f.forkedID, f.forkCount = id, count
	return f.forkChildren, f.err
}

func (f *lifecycleStore) Snapshot(_ context.Context, _, _, name string) (scale.Snapshot, error) {
	f.snapshotName = name
	return f.snapshot, f.err
}

// pausedSandbox is a sandbox as the node publishes it while hibernated.
func pausedSandbox(name, claimID, node, image, token string) sandboxv1beta1.Sandbox {
	sb := liveSandbox(name, claimID, node, image, token)
	sb.Labels = map[string]string{scale.PhaseLabel: phaseHibernated}
	return sb
}

// nodeReportsPaused makes the fake's owning node report the sandbox hibernated,
// which is what isPaused actually consults.
func nodeReportsPaused(s *lifecycleStore) { s.nodePaused = true }

// TestPauseAlreadyPausedIs409 is load-bearing for the SDK, not cosmetic:
// e2b's pause() returns a boolean, and it derives false ("was already paused")
// from a 409. Reporting anything else turns an idempotent no-op into an error
// the caller has to interpret.
func TestPauseAlreadyPausedIs409(t *testing.T) {
	store := &lifecycleStore{}
	nodeReportsPaused(store)
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", ``, testKey)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (the SDK reads it as already-paused): %s", w.Code, w.Body.String())
	}
	if store.pausedID != "" {
		t.Errorf("Pause was routed to the node for an already-paused sandbox (id %q)", store.pausedID)
	}
}

// TestPauseRoutesToOwningNode: the verb must address the node-local claim id on
// the owning node, never the public id the client spelled.
func TestPauseRoutesToOwningNode(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", ``, testKey)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if store.pausedNode != "node-a" || store.pausedID != "sb_abc" {
		t.Errorf("Pause(%q, %q), want (node-a, sb_abc) — the raw claim id, not the published one",
			store.pausedNode, store.pausedID)
	}
}

// TestPauseFilesystemOnlyIsRejected: e2b's memory=false asks for a
// filesystem-only snapshot whose resume cold-boots. The node always captures
// memory, so honoring the flag would hand back a different sandbox than the
// caller asked for — say so instead of silently doing the other thing.
func TestPauseFilesystemOnlyIsRejected(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", `{"memory":false}`, testKey)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unsupported pause mode", w.Code)
	}
	if store.pausedID != "" {
		t.Error("the sandbox was paused anyway; an unsupported mode must not fall through")
	}
}

// TestConnectRunningIs200 / TestConnectPausedIs201: e2b's connect IS the SDK's
// resume. 200 means it was already running, 201 that a restore actually
// happened — the SDK accepts either, and the distinction is what tells an
// operator whether the mmap restore path ran.
func TestConnectRunningIs200(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/connect", `{"timeout":30}`, testKey)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a running sandbox: %s", w.Code, w.Body.String())
	}
	if store.resumedID != "" {
		t.Errorf("a running sandbox was resumed (id %q); connect must be a no-op then", store.resumedID)
	}
	var got Sandbox
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SandboxID != "sb-abc" {
		t.Errorf("sandboxID = %q, want the DNS-safe published id", got.SandboxID)
	}
}

func TestConnectPausedIs201AndResumes(t *testing.T) {
	store := &lifecycleStore{}
	nodeReportsPaused(store)
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/connect", `{"timeout":30}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 when a paused sandbox is resumed: %s", w.Code, w.Body.String())
	}
	if store.resumedNode != "node-a" || store.resumedID != "sb_abc" {
		t.Errorf("Resume(%q, %q), want (node-a, sb_abc)", store.resumedNode, store.resumedID)
	}
}

// TestForkPausedIs409 mirrors e2b: a paused source cannot be forked, and the
// message must say to resume it first rather than failing opaquely.
func TestForkPausedIs409(t *testing.T) {
	store := &lifecycleStore{}
	nodeReportsPaused(store)
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/fork", `{"count":2}`, testKey)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 for forking a paused sandbox", w.Code)
	}
	if store.forkedID != "" {
		t.Error("fork was routed to the node for a paused source")
	}
}

// TestForkReturnsPerChildResults: e2b's fork reply is one entry per child, each
// carrying its own new sandbox id — children are fresh sandboxes, not replicas
// of the parent's identity.
func TestForkReturnsPerChildResults(t *testing.T) {
	store := &lifecycleStore{
		forkChildren: []scale.Assignment{
			{SandboxName: "sb_c1", Node: "node-a", Token: "t1"},
			{SandboxName: "sb_c2", Node: "node-a", Token: "t2"},
		},
	}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/fork", `{"count":2}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	var got []SandboxForkResult
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d results, want one per child", len(got))
	}
	if store.forkCount != 2 {
		t.Errorf("fork count routed = %d, want 2", store.forkCount)
	}
	for i, want := range []string{"sb-c1", "sb-c2"} {
		if got[i].Sandbox == nil || got[i].Sandbox.SandboxID != want {
			t.Errorf("child %d = %+v, want published id %q", i, got[i].Sandbox, want)
		}
	}
}

// TestForkDefaultsToOneChild: count is optional in e2b's schema.
func TestForkDefaultsToOneChild(t *testing.T) {
	store := &lifecycleStore{forkChildren: []scale.Assignment{{SandboxName: "sb_c1", Node: "node-a"}}}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/fork", ``, testKey); w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 for a body-less fork: %s", w.Code, w.Body.String())
	}
	if store.forkCount != 1 {
		t.Errorf("fork count = %d, want the schema default of 1", store.forkCount)
	}
}

// TestSnapshotReturns201WithID: the source keeps running; the reply carries the
// checkpoint id a later branch is taken from.
func TestSnapshotReturns201WithID(t *testing.T) {
	store := &lifecycleStore{snapshot: scale.Snapshot{ID: "ck_1234", Name: "before-migration", Node: "node-a"}}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img", "tok")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/snapshots", `{"name":"before-migration"}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	var got SnapshotInfo
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SnapshotID != "ck_1234" {
		t.Errorf("snapshotID = %q, want the checkpoint id", got.SnapshotID)
	}
	if len(got.Names) != 1 || got.Names[0] != "before-migration" {
		t.Errorf("names = %v, want the requested label echoed", got.Names)
	}
	if store.snapshotName != "before-migration" {
		t.Errorf("name routed = %q, want it passed through", store.snapshotName)
	}
}

// TestLifecycleVerbsOnUnknownSandboxAre404 keeps the SDK's not-found branches
// working: it raises SandboxNotFound on 404 rather than a generic error.
func TestLifecycleVerbsOnUnknownSandboxAre404(t *testing.T) {
	for _, path := range []string{
		"/sandboxes/sb-missing/pause",
		"/sandboxes/sb-missing/connect",
		"/sandboxes/sb-missing/fork",
		"/sandboxes/sb-missing/snapshots",
	} {
		t.Run(path, func(t *testing.T) {
			h := newTestServer(t, &lifecycleStore{})
			if w := do(t, h, http.MethodPost, path, `{}`, testKey); w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404: %s", w.Code, w.Body.String())
			}
		})
	}
}

// TestPauseTrustsTheNodeNotTheStaleView reproduces a bug found in the cluster:
// the read view is synthesized from NodeInventory, which the node republishes
// on a ~30 s cadence, so for up to half a minute after a pause the listed
// sandbox still carries phase=Running. Deciding "already paused" from that
// label let a second pause through with 204, telling the caller it had paused a
// sandbox that was already down — and the e2b SDK turns that 204 into "yes, I
// paused it". The owning node is authoritative and must win.
func TestPauseTrustsTheNodeNotTheStaleView(t *testing.T) {
	store := &lifecycleStore{}
	store.nodePaused = true // the node has it hibernated ...
	sb := liveSandbox("s1", "sb_abc", "node-a", "img", "tok")
	sb.Labels = map[string]string{scale.PhaseLabel: "Running"} // ... but the view still says Running
	store.items = []sandboxv1beta1.Sandbox{sb}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", ``, testKey)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: a stale Running label must not mask the node's hibernated state", w.Code)
	}
	if store.pausedID != "" {
		t.Error("pause was routed to the node for an already-hibernated sandbox")
	}
}

// TestConnectTrustsTheNodeNotTheStaleView is the same hazard on resume: a stale
// Running label would return 200 without restoring, handing the caller
// connection details for a VM that is not running.
func TestConnectTrustsTheNodeNotTheStaleView(t *testing.T) {
	store := &lifecycleStore{}
	store.nodePaused = true
	sb := liveSandbox("s1", "sb_abc", "node-a", "img", "tok")
	sb.Labels = map[string]string{scale.PhaseLabel: "Running"}
	store.items = []sandboxv1beta1.Sandbox{sb}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/connect", `{"timeout":30}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: the node says paused, so connect must actually resume", w.Code)
	}
	if store.resumedID != "sb_abc" {
		t.Errorf("resumed %q, want sb_abc — a stale label must not skip the restore", store.resumedID)
	}
}
