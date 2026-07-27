package scale

import (
	"context"
	"fmt"

	"github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd"
)

// Pause hibernates a delivered sandbox on its owning node.
func (s *scatterGatherStore) Pause(ctx context.Context, node, id string) error {
	cl, err := s.nodeClient(ctx, node, "pause", id)
	if err != nil {
		return err
	}
	if err := cl.Hibernate(ctx, id); err != nil {
		return fmt.Errorf("scale: sandboxd pause of %q on node %q: %w", id, node, err)
	}
	return nil
}

// Resume restores a paused sandbox through cocoon's mmap fast path.
func (s *scatterGatherStore) Resume(ctx context.Context, node, id string) error {
	cl, err := s.nodeClient(ctx, node, "resume", id)
	if err != nil {
		return err
	}
	if err := cl.Wake(ctx, id); err != nil {
		return fmt.Errorf("scale: sandboxd resume of %q on node %q: %w", id, node, err)
	}
	return nil
}

// Fork branches a sandbox into count children on its owning node. Children are
// fresh claims with their own ids; the parent keeps running.
func (s *scatterGatherStore) Fork(ctx context.Context, node, id string, count, ttlSeconds int) ([]Assignment, error) {
	if count < 1 {
		return nil, fmt.Errorf("scale: fork count must be >= 1, got %d", count)
	}
	cl, err := s.nodeClient(ctx, node, "fork", id)
	if err != nil {
		return nil, err
	}
	res, err := cl.Fork(ctx, id, sandboxd.ForkSpec{Count: count, TTLSeconds: ttlSeconds})
	if err != nil {
		return nil, fmt.Errorf("scale: sandboxd fork of %q on node %q: %w", id, node, err)
	}
	out := make([]Assignment, 0, len(res.Children))
	for _, c := range res.Children {
		out = append(out, Assignment{
			SandboxName: c.ID,
			Node:        node,
			Address:     c.OwnerAddr,
			Token:       c.Token,
		})
	}
	return out, nil
}

// Snapshot captures a sandbox's state as a checkpoint on its owning node.
func (s *scatterGatherStore) Snapshot(ctx context.Context, node, id, name string) (Snapshot, error) {
	cl, err := s.nodeClient(ctx, node, "snapshot", id)
	if err != nil {
		return Snapshot{}, err
	}
	ck, err := cl.Checkpoint(ctx, id, sandboxd.CheckpointSpec{Name: name})
	if err != nil {
		return Snapshot{}, fmt.Errorf("scale: sandboxd snapshot of %q on node %q: %w", id, node, err)
	}
	return snapshotFrom(ck, node), nil
}

// Snapshots lists a node's checkpoints.
func (s *scatterGatherStore) Snapshots(ctx context.Context, node string) ([]Snapshot, error) {
	cl, err := s.nodeClient(ctx, node, "list snapshots", "")
	if err != nil {
		return nil, err
	}
	cks, err := cl.Checkpoints(ctx)
	if err != nil {
		return nil, fmt.Errorf("scale: sandboxd list snapshots on node %q: %w", node, err)
	}
	out := make([]Snapshot, 0, len(cks))
	for _, ck := range cks {
		out = append(out, snapshotFrom(ck, node))
	}
	return out, nil
}

// DeleteSnapshot removes a checkpoint from its node.
func (s *scatterGatherStore) DeleteSnapshot(ctx context.Context, node, snapshotID string) error {
	cl, err := s.nodeClient(ctx, node, "delete snapshot", snapshotID)
	if err != nil {
		return err
	}
	if err := cl.DeleteCheckpoint(ctx, snapshotID); err != nil {
		return fmt.Errorf("scale: sandboxd delete snapshot %q on node %q: %w", snapshotID, node, err)
	}
	return nil
}

// ClaimSnapshot delivers a fresh sandbox branched from a checkpoint.
func (s *scatterGatherStore) ClaimSnapshot(ctx context.Context, node, snapshotID string, ttlSeconds int) (Assignment, error) {
	cl, err := s.nodeClient(ctx, node, "claim snapshot", snapshotID)
	if err != nil {
		return Assignment{}, err
	}
	res, err := cl.ClaimCheckpoint(ctx, snapshotID, sandboxd.CheckpointClaimSpec{TTLSeconds: ttlSeconds})
	if err != nil {
		return Assignment{}, fmt.Errorf("scale: sandboxd claim snapshot %q on node %q: %w", snapshotID, node, err)
	}
	return Assignment{
		SandboxName: res.ID,
		Node:        node,
		Address:     res.OwnerAddr,
		Token:       res.Token,
	}, nil
}

// Promote publishes a sandbox as a node-local template.
func (s *scatterGatherStore) Promote(ctx context.Context, node, id, template string) (PoolKey, error) {
	if template == "" {
		return PoolKey{}, fmt.Errorf("scale: promote requires a template name")
	}
	cl, err := s.nodeClient(ctx, node, "promote", id)
	if err != nil {
		return PoolKey{}, err
	}
	key, err := cl.Promote(ctx, id, sandboxd.PromoteSpec{Template: template})
	if err != nil {
		return PoolKey{}, fmt.Errorf("scale: sandboxd promote of %q on node %q: %w", id, node, err)
	}
	return PoolKey{Template: key.Template, Net: key.Net, Size: key.Size}, nil
}

// Stats reports a sandbox's resource usage from its owning node.
func (s *scatterGatherStore) Stats(ctx context.Context, node, id string) (SandboxStats, error) {
	cl, err := s.nodeClient(ctx, node, "stats", id)
	if err != nil {
		return SandboxStats{}, err
	}
	st, err := cl.Stats(ctx, id)
	if err != nil {
		return SandboxStats{}, fmt.Errorf("scale: sandboxd stats of %q on node %q: %w", id, node, err)
	}
	return SandboxStats{
		CPUCount:        st.CPUCount,
		MemTotalBytes:   st.MemTotalBytes,
		MemUsedBytes:    st.MemUsedBytes,
		MemUsedMeasured: st.MemUsedMeasured,
		Paused:          st.Hibernated,
		MeasuredAt:      st.MeasuredAt,
	}, nil
}

// nodeClient resolves a node's advertised sandboxd address and returns a client
// for it, failing closed when claim routing was never configured. verb and id
// name the operation in the error so a routing failure is diagnosable.
func (s *scatterGatherStore) nodeClient(ctx context.Context, node, verb, id string) (SandboxdClient, error) {
	if s.sandboxdFactory == nil {
		return nil, fmt.Errorf("scale: claim routing not configured (call WithClaimRouting)")
	}
	if node == "" {
		return nil, fmt.Errorf("scale: %s requires an owning node", verb)
	}
	inv, err := s.src.NodeInventory(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("scale: resolve node %q for %s of %q: %w", node, verb, id, err)
	}
	if inv.Address == "" {
		return nil, fmt.Errorf("scale: node %q advertises no sandboxd address for %s of %q", node, verb, id)
	}
	return s.sandboxdFactory(inv.Address, s.sandboxdToken), nil
}

// snapshotFrom converts a node's checkpoint record to the store's shape.
func snapshotFrom(ck sandboxd.Checkpoint, node string) Snapshot {
	return Snapshot{
		ID:        ck.ID,
		Name:      ck.Name,
		SandboxID: ck.SandboxID,
		Pool:      PoolKey{Template: ck.Key.Template, Net: ck.Key.Net, Size: ck.Key.Size},
		CreatedAt: ck.CreatedAt,
		Node:      node,
	}
}
