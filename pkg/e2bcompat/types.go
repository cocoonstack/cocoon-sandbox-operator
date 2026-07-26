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

// The wire types below mirror the e2b REST API schemas the e2b SDKs consume
// (e2b-dev/E2B spec/openapi.yml: NewSandbox, Sandbox, SandboxDetail,
// ListedSandbox, ResumedSandbox). Field names and JSON casing are fixed by that
// contract — the SDK unmarshals them directly — so they are reproduced exactly
// rather than restyled to this repo's own conventions.

// NewSandbox is the POST /sandboxes request body (spec: NewSandbox). Only
// templateID is required; the rest carry SDK defaults.
type NewSandbox struct {
	TemplateID string `json:"templateID"`
	// Timeout is the sandbox time-to-live in seconds (SDK default 15).
	Timeout *int32 `json:"timeout,omitempty"`
	// Metadata and EnvVars are echoed back on reads. They are recorded on the
	// compat side only: the node-local claim path takes neither today.
	Metadata map[string]string `json:"metadata,omitempty"`
	EnvVars  map[string]string `json:"envVars,omitempty"`
	// AutoPause and Secure are accepted and reported back so an SDK that sets
	// them does not fail; neither changes the claim.
	AutoPause *bool `json:"autoPause,omitempty"`
	Secure    *bool `json:"secure,omitempty"`
	// AllowInternetAccess selects the warm pool's network lane: true picks the
	// egress-capable pool, false/nil the isolated one.
	AllowInternetAccess *bool `json:"allow_internet_access,omitempty"`
}

// Sandbox is the POST /sandboxes response (spec: Sandbox). templateID,
// sandboxID, clientID and envdVersion are required by the schema.
type Sandbox struct {
	TemplateID string `json:"templateID"`
	SandboxID  string `json:"sandboxID"`
	ClientID   string `json:"clientID"`
	// EnvdVersion gates SDK behavior (it version-compares before choosing the
	// envd auth style), so it is always reported.
	EnvdVersion     string `json:"envdVersion"`
	EnvdAccessToken string `json:"envdAccessToken,omitempty"`
	Domain          string `json:"domain,omitempty"`
	Alias           string `json:"alias,omitempty"`
}

// SandboxDetail is the GET /sandboxes/{sandboxID} response (spec:
// SandboxDetail), and its field set also satisfies ListedSandbox.
type SandboxDetail struct {
	TemplateID          string            `json:"templateID"`
	SandboxID           string            `json:"sandboxID"`
	ClientID            string            `json:"clientID"`
	StartedAt           string            `json:"startedAt"`
	EndAt               string            `json:"endAt"`
	State               string            `json:"state"`
	EnvdVersion         string            `json:"envdVersion"`
	CPUCount            int32             `json:"cpuCount"`
	MemoryMB            int32             `json:"memoryMB"`
	DiskSizeMB          int32             `json:"diskSizeMB"`
	Alias               string            `json:"alias,omitempty"`
	Metadata            map[string]string `json:"metadata,omitempty"`
	EnvdAccessToken     string            `json:"envdAccessToken,omitempty"`
	Domain              string            `json:"domain,omitempty"`
	AllowInternetAccess *bool             `json:"allowInternetAccess,omitempty"`
}

// SandboxTimeoutRequest is the POST /sandboxes/{sandboxID}/timeout request body
// (spec: SandboxTimeoutRequest) — the SDK's setTimeout call.
type SandboxTimeoutRequest struct {
	Timeout int32 `json:"timeout"`
}

// SandboxRefreshRequest is the POST /sandboxes/{sandboxID}/refreshes request
// body (spec: SandboxRefreshRequest) — the keepalive.
type SandboxRefreshRequest struct {
	Duration *int32 `json:"duration,omitempty"`
}

// SandboxPauseRequest is the POST /sandboxes/{id}/pause body. memory=false
// takes a filesystem-only snapshot, whose resume cold-boots.
type SandboxPauseRequest struct {
	Memory *bool `json:"memory,omitempty"`
}

// ConnectSandbox is the POST /sandboxes/{id}/connect body — the SDK's resume.
// timeout is required by the schema; TTL is only ever extended.
type ConnectSandbox struct {
	Timeout int32 `json:"timeout"`
}

// SandboxForkRequest is the POST /sandboxes/{id}/fork body.
type SandboxForkRequest struct {
	Timeout *int32 `json:"timeout,omitempty"`
	Count   *int32 `json:"count,omitempty"`
}

// SandboxForkResult is one entry of the fork reply: exactly one of Sandbox or
// Error is set, so a partial failure still returns 201 with per-child detail.
type SandboxForkResult struct {
	Sandbox *Sandbox  `json:"sandbox,omitempty"`
	Error   *APIError `json:"error,omitempty"`
}

// SandboxSnapshotRequest is the POST /sandboxes/{id}/snapshots body.
type SandboxSnapshotRequest struct {
	Name string `json:"name,omitempty"`
}

// SnapshotInfo is the snapshot create/list reply.
type SnapshotInfo struct {
	SnapshotID string   `json:"snapshotID"`
	Names      []string `json:"names"`
}

// SandboxMetric is one metrics sample. Every field is required by the schema,
// so all are always emitted; the SDK reads the deprecated `timestamp`.
type SandboxMetric struct {
	Timestamp     string  `json:"timestamp"`
	TimestampUnix int64   `json:"timestampUnix"`
	CPUCount      int32   `json:"cpuCount"`
	CPUUsedPct    float32 `json:"cpuUsedPct"`
	MemUsed       int64   `json:"memUsed"`
	MemTotal      int64   `json:"memTotal"`
	MemCache      int64   `json:"memCache"`
	DiskUsed      int64   `json:"diskUsed"`
	DiskTotal     int64   `json:"diskTotal"`
}

// Template is the templates-listing entry.
type Template struct {
	TemplateID  string   `json:"templateID"`
	BuildID     string   `json:"buildID"`
	CPUCount    int32    `json:"cpuCount"`
	MemoryMB    int32    `json:"memoryMB"`
	DiskSizeMB  int32    `json:"diskSizeMB"`
	Public      bool     `json:"public"`
	Aliases     []string `json:"aliases"`
	Names       []string `json:"names"`
	CreatedAt   string   `json:"createdAt"`
	UpdatedAt   string   `json:"updatedAt"`
	SpawnCount  int64    `json:"spawnCount"`
	BuildCount  int32    `json:"buildCount"`
	EnvdVersion string   `json:"envdVersion"`
}

// APIError is the e2b error envelope. The SDK surfaces `message` on failures.
type APIError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

// Sandbox states reported to the SDK (spec: SandboxState).
const (
	StateRunning = "running"
	StatePaused  = "paused"
)
