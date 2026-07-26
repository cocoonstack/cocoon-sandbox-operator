package sandboxd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// The lifecycle verbs below all authenticate with the node's root api_token
// (the fleet token this client already holds) and take sandboxd's operator
// path, which resolves the sandbox by id without a per-sandbox token. That is
// deliberate: keeping one secret per sandbox in the control plane would turn
// its O(nodes) storage into O(sandboxes), the property the whole design rests
// on. sandboxd authorizes these by the root token exactly as it does release.

// ForkSpec is the POST /v1/sandboxes/{id}/fork body. Token stays empty on the
// operator path; Count must be within the node's max_fork_count.
type ForkSpec struct {
	Token      string `json:"token,omitempty"`
	Count      int    `json:"count"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

// ForkResult carries one claim per child; children are fresh sandboxes with
// their own ids and leases, and the parent keeps running.
type ForkResult struct {
	Children []ClaimResult `json:"children"`
}

// CheckpointSpec is the POST /v1/sandboxes/{id}/checkpoint body.
type CheckpointSpec struct {
	Token string `json:"token,omitempty"`
	Name  string `json:"name,omitempty"`
}

// Checkpoint is a captured sandbox state that branches can be claimed from.
type Checkpoint struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	SandboxID string    `json:"sandbox_id"`
	Key       PoolKey   `json:"key"`
	CreatedAt time.Time `json:"created_at"`
}

// PoolKey is a checkpoint's or template's pool identity.
type PoolKey struct {
	Template string `json:"template"`
	Net      string `json:"net,omitempty"`
	Size     string `json:"size,omitempty"`
	Engine   string `json:"engine,omitempty"`
}

// CheckpointClaimSpec is the POST /v1/checkpoints/{id}/claim body.
type CheckpointClaimSpec struct {
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// PromoteSpec is the POST /v1/sandboxes/{id}/promote body: it publishes the
// sandbox's state as a node-local template future claims clone from.
type PromoteSpec struct {
	Token    string `json:"token,omitempty"`
	Template string `json:"template"`
}

// PromoteResult returns the promoted template's full key.
type PromoteResult struct {
	Key PoolKey `json:"key"`
}

// SandboxSummary is one live claim as the owning node reports it.
type SandboxSummary struct {
	ID             string    `json:"id"`
	Key            PoolKey   `json:"key"`
	Deadline       time.Time `json:"deadline"`
	Hibernated     bool      `json:"hibernated"`
	Archived       bool      `json:"archived,omitempty"`
	FromCheckpoint string    `json:"from_checkpoint,omitempty"`
	ClaimRef       string    `json:"claim_ref,omitempty"`
}

// SandboxStats is one sandbox's resource usage. CPUCount and MemTotalBytes are
// the tier the VM was booted with (authoritative); MemUsedBytes is the host
// VMM's resident set and is only meaningful when MemUsedMeasured is true — a
// hibernated sandbox has no process to measure.
type SandboxStats struct {
	ID              string    `json:"id"`
	CPUCount        int       `json:"cpu_count"`
	MemTotalBytes   int64     `json:"mem_total_bytes"`
	MemUsedBytes    int64     `json:"mem_used_bytes"`
	Hibernated      bool      `json:"hibernated"`
	MeasuredAt      time.Time `json:"measured_at"`
	MemUsedMeasured bool      `json:"mem_used_measured"`
}

// Hibernate performs POST /v1/sandboxes/{id}/hibernate: it snapshots the
// sandbox and stops its VM, freeing the node's memory. This is the pause verb;
// its cost is proportional to guest RAM because the memory is written out.
func (c *Client) Hibernate(ctx context.Context, id string) error {
	return c.sandboxVerb(ctx, id, "hibernate")
}

// Wake performs POST /v1/sandboxes/{id}/wake, restoring a hibernated sandbox
// through cocoon's mmap fast path (~55 ms) and leaving it running. Idempotent
// on a sandbox that is already awake.
func (c *Client) Wake(ctx context.Context, id string) error {
	return c.sandboxVerb(ctx, id, "wake")
}

// sandboxVerb posts a body-less sandbox-scoped verb and expects 204.
func (c *Client) sandboxVerb(ctx context.Context, id, verb string) error {
	if id == "" {
		return fmt.Errorf("sandboxd: %s requires a sandbox id", verb)
	}
	u := c.baseURL + "/v1/sandboxes/" + url.PathEscape(id) + "/" + verb
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd: %s: %w", verb, err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusNoContent {
		return statusError(resp)
	}
	return nil
}

// Fork performs POST /v1/sandboxes/{id}/fork, branching the sandbox into count
// children. The parent is checkpointed in place and keeps running; each child
// is a fresh claim with its own id and lease.
func (c *Client) Fork(ctx context.Context, id string, spec ForkSpec) (ForkResult, error) {
	var out ForkResult
	if id == "" {
		return out, fmt.Errorf("sandboxd: fork requires a sandbox id")
	}
	err := c.postJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id)+"/fork", spec, &out)
	return out, err
}

// Checkpoint performs POST /v1/sandboxes/{id}/checkpoint, capturing the
// sandbox's state under a fresh id. The source keeps running.
func (c *Client) Checkpoint(ctx context.Context, id string, spec CheckpointSpec) (Checkpoint, error) {
	if id == "" {
		return Checkpoint{}, fmt.Errorf("sandboxd: checkpoint requires a sandbox id")
	}
	var out struct {
		Checkpoint Checkpoint `json:"checkpoint"`
	}
	err := c.postJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id)+"/checkpoint", spec, &out)
	return out.Checkpoint, err
}

// ClaimCheckpoint performs POST /v1/checkpoints/{id}/claim, delivering a fresh
// sandbox branched from the checkpoint's exact state.
func (c *Client) ClaimCheckpoint(ctx context.Context, checkpointID string, spec CheckpointClaimSpec) (ClaimResult, error) {
	var out ClaimResult
	if checkpointID == "" {
		return out, fmt.Errorf("sandboxd: claim checkpoint requires a checkpoint id")
	}
	err := c.postJSON(ctx, "/v1/checkpoints/"+url.PathEscape(checkpointID)+"/claim", spec, &out)
	return out, err
}

// Checkpoints performs GET /v1/checkpoints, newest first.
func (c *Client) Checkpoints(ctx context.Context) ([]Checkpoint, error) {
	var out struct {
		Checkpoints []Checkpoint `json:"checkpoints"`
	}
	err := c.getJSON(ctx, "/v1/checkpoints", &out)
	return out.Checkpoints, err
}

// DeleteCheckpoint performs DELETE /v1/checkpoints/{id}. A 404 is success.
func (c *Client) DeleteCheckpoint(ctx context.Context, checkpointID string) error {
	if checkpointID == "" {
		return fmt.Errorf("sandboxd: delete checkpoint requires a checkpoint id")
	}
	u := c.baseURL + "/v1/checkpoints/" + url.PathEscape(checkpointID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd: delete checkpoint: %w", err)
	}
	defer drainAndClose(resp)

	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return statusError(resp)
	}
}

// Promote performs POST /v1/sandboxes/{id}/promote, publishing the sandbox as
// a node-local template that later claims for that key clone from.
func (c *Client) Promote(ctx context.Context, id string, spec PromoteSpec) (PoolKey, error) {
	if id == "" {
		return PoolKey{}, fmt.Errorf("sandboxd: promote requires a sandbox id")
	}
	var out PromoteResult
	err := c.postJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id)+"/promote", spec, &out)
	return out.Key, err
}

// Sandbox performs GET /v1/sandboxes/{id}, the single-sandbox read that avoids
// scanning the whole-node listing.
func (c *Client) Sandbox(ctx context.Context, id string) (SandboxSummary, error) {
	var out SandboxSummary
	if id == "" {
		return out, fmt.Errorf("sandboxd: sandbox read requires an id")
	}
	err := c.getJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id), &out)
	return out, err
}

// Stats performs GET /v1/sandboxes/{id}/stats.
func (c *Client) Stats(ctx context.Context, id string) (SandboxStats, error) {
	var out SandboxStats
	if id == "" {
		return out, fmt.Errorf("sandboxd: stats requires a sandbox id")
	}
	err := c.getJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id)+"/stats", &out)
	return out, err
}

// Sandboxes performs GET /v1/sandboxes, this node's live claims.
func (c *Client) Sandboxes(ctx context.Context) ([]SandboxSummary, error) {
	var out struct {
		Sandboxes []SandboxSummary `json:"sandboxes"`
	}
	err := c.getJSON(ctx, "/v1/sandboxes", &out)
	return out.Sandboxes, err
}

// postJSON sends body as JSON and decodes a 2xx reply into out.
func (c *Client) postJSON(ctx context.Context, path string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("sandboxd: encode %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd: %s: %w", path, err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode/100 != 2 {
		return statusError(resp)
	}
	return decodeInto(resp, out, path)
}

// getJSON decodes a 2xx GET reply into out.
func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd: %s: %w", path, err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}
	return decodeInto(resp, out, path)
}

// decodeInto reads a bounded reply body into out.
func decodeInto(resp *http.Response, out any, path string) error {
	if out == nil {
		return nil
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("sandboxd: read %s reply: %w", path, err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("sandboxd: decode %s reply: %w", path, err)
	}
	return nil
}
