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

// Package sandboxd is a small HTTP client for the node-local sandboxd warm-pool
// daemon (see external/sandbox/docs/sandboxd-api.md). It uses only the standard
// library. The L2 ClaimGateway fronts one of these clients per node: Claim
// transfers ownership of an already-running microVM in sub-millisecond time, and
// Release destroys a sandbox's VM on owner-authorized teardown.
package sandboxd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ErrNodeAtCapacity is returned by Claim when sandboxd answers 429 (the node is
// at max_claims, the calling tenant is at its own max_claims, or the node is
// draining), or when a 200 carries only a peer redirect rather than a delivered
// sandbox. In every case this node handed over no VM, so the L2 gateway maps this
// to its ErrNoNodeCapacity sentinel and falls back to the L1 Kubernetes path.
var ErrNodeAtCapacity = errors.New("sandboxd: node at capacity or draining")

// HTTPError carries a non-2xx sandboxd status that is not otherwise typed (e.g.
// 400 bad body, 401 bad api token, 409 egress mismatch, 500 provisioning failed).
type HTTPError struct {
	StatusCode int
	// Message is the decoded {"error": "..."} body when present.
	Message string
}

func (e *HTTPError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sandboxd: http %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("sandboxd: http %d", e.StatusCode)
}

// Client talks to a single sandboxd instance. It is safe for concurrent use.
type Client struct {
	baseURL string
	// token is the node api_token (root or tenant) presented on the claim verb.
	// Release authenticates with the sandbox's own token, passed per call.
	token string
	hc    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient overrides the default *http.Client (for custom transports or
// timeouts). The default client has no timeout: sandboxd keeps read/write
// timeouts at zero because cold claims legitimately block, so callers bound the
// call with the context instead.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.hc = hc }
}

// New returns a Client for the sandboxd at baseURL, authenticating resource verbs
// with the node api_token (may be empty when sandboxd runs without auth).
func New(baseURL, token string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		hc:      &http.Client{},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ClaimSpec is the POST /v1/claim body. Net defaults to "none" and Size to
// "small" server-side; TTLSeconds 0 means the server default.
type ClaimSpec struct {
	Template   string `json:"template"`
	Net        string `json:"net,omitempty"`
	Size       string `json:"size,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
	// NoRedirect is set by an SDK retrying at a redirect target; the gateway does
	// not chase redirects (it falls back to L1 instead), so it stays false.
	NoRedirect bool `json:"no_redirect,omitempty"`
	// ClaimRef is the k8s "<namespace>/<name>" of the Sandbox this claim is
	// created for. sandboxd records it on the claim and echoes it in its
	// operator index, so the aggregated read path can map a listed sandbox back
	// to the name it was claimed under. Empty for claims with no k8s identity.
	ClaimRef string `json:"claim_ref,omitempty"`
}

// ClaimResult is the POST /v1/claim success body.
type ClaimResult struct {
	ID        string `json:"id"`
	Token     string `json:"token"`
	Deadline  string `json:"deadline"`
	OwnerAddr string `json:"owner_addr"`
	// FromCheckpoint is the lineage edge when the claim branched from a checkpoint.
	FromCheckpoint string `json:"from_checkpoint,omitempty"`
	// Redirect, when non-empty on a 200, names warm peers to retry at instead of a
	// delivered sandbox. Claim treats this as a capacity miss (see ErrNodeAtCapacity).
	Redirect []string `json:"redirect,omitempty"`
}

// Claim performs POST /v1/claim, returning the delivered sandbox on success.
// A 429, or a 200 that carries only a peer redirect, yields ErrNodeAtCapacity.
func (c *Client) Claim(ctx context.Context, spec ClaimSpec) (ClaimResult, error) {
	body, err := json.Marshal(spec)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("sandboxd: encode claim: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/claim", bytes.NewReader(body))
	if err != nil {
		return ClaimResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("sandboxd: claim: %w", err)
	}
	defer drainAndClose(resp)

	switch resp.StatusCode {
	case http.StatusOK:
		var r ClaimResult
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return ClaimResult{}, fmt.Errorf("sandboxd: decode claim response: %w", err)
		}
		if r.ID == "" {
			// A redirect-only body (warm miss, or node at max_claims with a warm
			// peer) or an empty body: this node delivered no sandbox.
			return ClaimResult{}, ErrNodeAtCapacity
		}
		return r, nil
	case http.StatusTooManyRequests:
		return ClaimResult{}, ErrNodeAtCapacity
	default:
		return ClaimResult{}, statusError(resp)
	}
}

// PoolSpec is one entry of the PUT /v1/pools body: the desired warm watermark
// for a single (template, net, size) pool on this node. It mirrors the claim
// key so a SandboxWarmPool's target lands on the exact pool a Create claims from.
type PoolSpec struct {
	Template string `json:"template"`
	Net      string `json:"net,omitempty"`
	Size     string `json:"size,omitempty"`
	Warm     int    `json:"warm"`
}

// NodeInfo is the PUT /v1/pools (and GET /v1/info) response: the node's live
// per-pool warm state plus its lifecycle counters. Only the fields the warm-pool
// driver reports on are decoded.
type NodeInfo struct {
	Pools []struct {
		Key struct {
			Template string `json:"template"`
			Net      string `json:"net"`
			Size     string `json:"size"`
		} `json:"key"`
		Warm      int  `json:"warm"`
		Refilling int  `json:"refilling"`
		Target    int  `json:"target"`
		Golden    bool `json:"golden"`
	} `json:"pools"`
	Claimed    int `json:"claimed"`
	Hibernated int `json:"hibernated"`
	Archived   int `json:"archived"`
}

// SetPools performs PUT /v1/pools, replacing this node's desired warm targets
// with the supplied set (an omitted pool is drained). It authenticates with the
// node api_token. The whole set is sent in one request because sandboxd replaces
// its pool config wholesale — a partial list silently drains the rest.
func (c *Client) SetPools(ctx context.Context, pools []PoolSpec) (*NodeInfo, error) {
	if pools == nil {
		pools = []PoolSpec{}
	}
	body, err := json.Marshal(struct {
		Pools []PoolSpec `json:"pools"`
	}{Pools: pools})
	if err != nil {
		return nil, fmt.Errorf("sandboxd: encode pools: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/v1/pools", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sandboxd: set pools: %w", err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, statusError(resp)
	}
	var info NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("sandboxd: decode pools response: %w", err)
	}
	return &info, nil
}

// Release performs POST /v1/sandboxes/{id}/release, which DESTROYS the VM. It
// authenticates with the sandbox's own token. A 404 (unknown id or already gone)
// is treated as success, matching the SDK. Callers must only reach this on
// owner-authorized teardown — see the ClaimGateway.Release contract.
func (c *Client) Release(ctx context.Context, id, token string) error {
	if id == "" {
		return fmt.Errorf("sandboxd: release requires a sandbox id")
	}
	u := c.baseURL + "/v1/sandboxes/" + url.PathEscape(id) + "/release"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}
	// Release auth is the sandbox's own token, not the node api_token.
	c.authenticate(req, token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd: release: %w", err)
	}
	defer drainAndClose(resp)

	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusNotFound:
		// Already gone: the SDK treats a released/unknown sandbox as success.
		return nil
	default:
		return statusError(resp)
	}
}

func (c *Client) authenticate(req *http.Request, token string) {
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

// statusError reads the {"error": "..."} body and returns a typed *HTTPError.
func statusError(resp *http.Response) error {
	var payload struct {
		Error string `json:"error"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	_ = json.Unmarshal(b, &payload)
	return &HTTPError{StatusCode: resp.StatusCode, Message: payload.Error}
}

// drainAndClose fully reads and closes the body so the keep-alive connection can
// be reused — essential for stable sub-millisecond claim latency under load.
func drainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
