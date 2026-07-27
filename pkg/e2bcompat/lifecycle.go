package e2bcompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// pauseSandbox hibernates the sandbox: its memory is written out and the VM
// stops, so the cost is proportional to guest RAM. e2b's contract is specific
// about the already-paused case — the SDK reads 409 as "already paused" and
// returns false rather than raising — so that state is reported, not retried.
func (s *Server) pauseSandbox(w http.ResponseWriter, r *http.Request) {
	var req SandboxPauseRequest
	if err := decodeOptional(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, err, id, "pause")
		return
	}
	if s.isPaused(r.Context(), sb) {
		writeError(w, http.StatusConflict, fmt.Sprintf("sandbox %q is already paused", id))
		return
	}
	// memory=false asks for a filesystem-only snapshot whose resume cold-boots.
	// The node's hibernate always captures memory, so honoring it would mean
	// silently giving back a different sandbox than asked for.
	if req.Memory != nil && !*req.Memory {
		writeError(w, http.StatusBadRequest,
			"filesystem-only pause (memory=false) is not supported; this backend always snapshots memory")
		return
	}
	if err := s.store.Pause(r.Context(), sb.Status.NodeName, claimIDOf(sb)); err != nil {
		s.opts.Log.Error(err, "e2b pause failed", "sandboxID", id)
		writeError(w, http.StatusInternalServerError, "failed to pause the sandbox")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// connectSandbox is the SDK's resume: it returns the sandbox's connection
// details, restoring it first when paused. 200 means it was already running,
// 201 that it was paused and got resumed — the SDK accepts either, and the
// distinction is what tells an operator whether a restore actually happened.
// A resume takes cocoon's mmap fast path (~55 ms), unlike the pause that
// preceded it.
func (s *Server) connectSandbox(w http.ResponseWriter, r *http.Request) {
	var req ConnectSandbox
	if err := decodeOptional(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, err, id, "connect")
		return
	}
	status := http.StatusOK
	if s.isPaused(r.Context(), sb) {
		if err := s.store.Resume(r.Context(), sb.Status.NodeName, claimIDOf(sb)); err != nil {
			s.opts.Log.Error(err, "e2b connect: resume failed", "sandboxID", id)
			writeError(w, http.StatusInternalServerError, "failed to resume the sandbox")
			return
		}
		status = http.StatusCreated
	}
	writeJSON(w, status, Sandbox{
		TemplateID:      templateOf(sb),
		SandboxID:       publicID(claimIDOf(sb)),
		ClientID:        sb.Status.NodeName,
		EnvdVersion:     s.opts.EnvdVersion,
		EnvdAccessToken: sb.Annotations[tokenAnnotation],
		Domain:          s.opts.Domain,
	})
}

// forkSandbox branches the sandbox into count children. The parent is
// checkpointed in place and keeps running; every child is a fresh sandbox with
// its own id and lease. Per e2b's contract a partial failure is still a 201
// carrying per-child detail — a non-201 means nothing was attempted — so the
// node's all-or-nothing fork is reported as a whole-request failure only when
// it rejects the request outright.
func (s *Server) forkSandbox(w http.ResponseWriter, r *http.Request) {
	var req SandboxForkRequest
	if err := decodeOptional(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	count := int32(1)
	if req.Count != nil {
		count = *req.Count
	}
	if count < 1 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("count must be >= 1, got %d", count))
		return
	}
	if req.Timeout != nil && *req.Timeout < 0 {
		writeError(w, http.StatusBadRequest, "timeout must be >= 0")
		return
	}
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, err, id, "fork")
		return
	}
	if s.isPaused(r.Context(), sb) {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("sandbox %q is paused and cannot be forked; resume it first", id))
		return
	}
	children, err := s.store.Fork(r.Context(), sb.Status.NodeName, claimIDOf(sb), int(count), timeoutSeconds(req.Timeout))
	if err != nil {
		s.opts.Log.Error(err, "e2b fork failed", "sandboxID", id, "count", count)
		writeError(w, http.StatusInternalServerError, "failed to fork the sandbox")
		return
	}
	template := templateOf(sb)
	out := make([]SandboxForkResult, 0, len(children))
	for i := range children {
		child := children[i]
		out = append(out, SandboxForkResult{Sandbox: &Sandbox{
			TemplateID:      template,
			SandboxID:       publicID(child.SandboxName),
			ClientID:        child.Node,
			EnvdVersion:     s.opts.EnvdVersion,
			EnvdAccessToken: child.Token,
			Domain:          s.opts.Domain,
		}})
	}
	writeJSON(w, http.StatusCreated, out)
}

// createSnapshot captures the sandbox's state as a checkpoint later sandboxes
// can branch from. The source keeps running.
func (s *Server) createSnapshot(w http.ResponseWriter, r *http.Request) {
	var req SandboxSnapshotRequest
	if err := decodeOptional(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, err, id, "snapshot")
		return
	}
	snap, err := s.store.Snapshot(r.Context(), sb.Status.NodeName, claimIDOf(sb), req.Name)
	if err != nil {
		s.opts.Log.Error(err, "e2b snapshot failed", "sandboxID", id)
		writeError(w, http.StatusInternalServerError, "failed to snapshot the sandbox")
		return
	}
	writeJSON(w, http.StatusCreated, snapshotInfo(snap))
}

// listSnapshots reports the checkpoints across the fleet's nodes.
func (s *Server) listSnapshots(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.nodesWithSandboxes(r)
	if err != nil {
		s.opts.Log.Error(err, "e2b list snapshots: resolve nodes failed")
		writeError(w, http.StatusInternalServerError, "failed to list snapshots")
		return
	}
	out := []SnapshotInfo{}
	for _, node := range nodes {
		snaps, err := s.store.Snapshots(r.Context(), node)
		if err != nil {
			// One unreachable node must not blank the whole listing.
			s.opts.Log.Error(err, "e2b list snapshots: node failed", "node", node)
			continue
		}
		for _, snap := range snaps {
			out = append(out, snapshotInfo(snap))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// deleteSnapshot removes a checkpoint. e2b addresses snapshots as templates on
// delete, so this serves DELETE /templates/{templateID} too.
func (s *Server) deleteSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshotID := r.PathValue("snapshotID")
	nodes, err := s.nodesWithSandboxes(r)
	if err != nil {
		s.opts.Log.Error(err, "e2b delete snapshot: resolve nodes failed")
		writeError(w, http.StatusInternalServerError, "failed to delete the snapshot")
		return
	}
	// Checkpoints are node-local and the id does not name its node, so the
	// delete is offered to each node; a node that does not hold it reports
	// success (delete is idempotent), which keeps this safe to fan out.
	for _, node := range nodes {
		if err := s.store.DeleteSnapshot(r.Context(), node, snapshotID); err != nil {
			s.opts.Log.Error(err, "e2b delete snapshot: node failed", "node", node, "snapshotID", snapshotID)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// sandboxMetrics reports one sandbox's resource usage. e2b's schema requires
// every field, so all are emitted; the ones this backend cannot measure are
// reported as zero rather than invented (see SandboxStats).
func (s *Server) sandboxMetrics(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, err, id, "metrics")
		return
	}
	st, err := s.store.Stats(r.Context(), sb.Status.NodeName, claimIDOf(sb))
	if err != nil {
		s.opts.Log.Error(err, "e2b metrics failed", "sandboxID", id)
		writeError(w, http.StatusInternalServerError, "failed to read sandbox metrics")
		return
	}
	at := st.MeasuredAt
	if at.IsZero() {
		at = time.Now()
	}
	writeJSON(w, http.StatusOK, []SandboxMetric{{
		Timestamp:     at.UTC().Format(time.RFC3339),
		TimestampUnix: at.Unix(),
		CPUCount:      int32(st.CPUCount), //nolint:gosec // a size tier's CPU count is single digits
		MemUsed:       st.MemUsedBytes,
		MemTotal:      st.MemTotalBytes,
	}})
}

// listTemplates reports the pools this fleet can serve claims from. e2b's
// templates are build artifacts with their own lifecycle; the equivalent here
// is the set of warm-pool keys nodes advertise, which is what a caller can
// actually pass as templateID on create.
func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.inventories(r)
	if err != nil {
		s.opts.Log.Error(err, "e2b list templates failed")
		writeError(w, http.StatusInternalServerError, "failed to list templates")
		return
	}
	seen := map[string]struct{}{}
	out := []Template{}
	for _, inv := range nodes {
		for _, pc := range inv.Pools {
			if pc.Template == "" {
				continue
			}
			if _, dup := seen[pc.Template]; dup {
				continue
			}
			seen[pc.Template] = struct{}{}
			out = append(out, Template{
				TemplateID:  pc.Template,
				BuildID:     pc.Template,
				Public:      true,
				Aliases:     []string{},
				Names:       []string{pc.Template},
				EnvdVersion: s.opts.EnvdVersion,
			})
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// inventories returns every node's published inventory, the fleet view the
// pool-derived surfaces (templates, snapshot listing) are assembled from.
func (s *Server) inventories(r *http.Request) ([]*scale.NodeInventory, error) {
	if s.opts.Inventory == nil {
		return nil, errors.New("e2bcompat: no inventory source configured")
	}
	nodes, err := s.opts.Inventory.ListNodes(r.Context())
	if err != nil {
		return nil, err
	}
	out := make([]*scale.NodeInventory, 0, len(nodes))
	for _, node := range nodes {
		inv, err := s.opts.Inventory.NodeInventory(r.Context(), node)
		if err != nil {
			// A partitioned node is skipped, not fatal — the same rule the
			// aggregated read path applies.
			s.opts.Log.Error(err, "e2b: node inventory unavailable", "node", node)
			continue
		}
		out = append(out, inv)
	}
	return out, nil
}

// nodesWithSandboxes lists the nodes a checkpoint could live on.
func (s *Server) nodesWithSandboxes(r *http.Request) ([]string, error) {
	if s.opts.Inventory == nil {
		return nil, errors.New("e2bcompat: no inventory source configured")
	}
	return s.opts.Inventory.ListNodes(r.Context())
}

// decodeOptional decodes a JSON body that the schema allows to be absent. An
// empty body leaves the target at its zero value rather than failing, which is
// what pause and fork require.
func decodeOptional(w http.ResponseWriter, r *http.Request, out any) error {
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(out)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// isPaused reports whether the sandbox is hibernated, asking its owning node
// rather than trusting the synthesized read view.
//
// The label on a listed sandbox comes from NodeInventory, which the node
// republishes on a ~30 s cadence, so it lags a pause by up to that long. e2b's
// contract needs a strong answer here — the SDK reads 409 as "already paused"
// and returns false — and a stale Running label turns that into a second 204,
// telling the caller it paused a sandbox that was already down. The per-sandbox
// node read is authoritative and costs one round trip to a node we are about to
// call anyway.
//
// A node that cannot be reached falls back to the cached label: degrading to the
// eventually-consistent answer is better than failing the request outright.
func (s *Server) isPaused(ctx context.Context, sb *sandboxv1beta1.Sandbox) bool {
	if node, id := sb.Status.NodeName, claimIDOf(sb); node != "" && id != "" {
		if st, err := s.store.Stats(ctx, node, id); err == nil {
			return st.Paused
		}
	}
	return sb.Labels[scale.PhaseLabel] == phaseHibernated
}

// claimIDOf reports the node-local claim id the store's verbs address.
func claimIDOf(sb *sandboxv1beta1.Sandbox) string {
	return sb.Annotations[scale.ClaimIDAnnotation]
}

// timeoutSeconds resolves an optional TTL to the e2b default.
func timeoutSeconds(v *int32) int {
	if v == nil {
		return DefaultTimeoutSeconds
	}
	return int(*v)
}

// snapshotInfo renders a checkpoint in e2b's snapshot shape.
func snapshotInfo(snap scale.Snapshot) SnapshotInfo {
	names := []string{}
	if snap.Name != "" {
		names = append(names, snap.Name)
	}
	return SnapshotInfo{SnapshotID: snap.ID, Names: names}
}
