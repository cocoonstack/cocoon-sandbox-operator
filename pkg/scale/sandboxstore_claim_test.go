package scale

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd"
)

func TestStoreClaim_RoutesToAWarmNode(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n2", "10.0.0.2:7777", PoolCapacity{Template: "img", Warm: 4, Target: 5}))
	deadline := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-abc", Token: "sbtok", OwnerAddr: "10.0.0.2:9000", Deadline: deadline}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("uniform-token", f.factory()))

	a, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 600)
	require.NoError(t, err)
	assert.Equal(t, "n2", a.Node)
	assert.Equal(t, "sb-abc", a.SandboxName)
	assert.Equal(t, "10.0.0.2:9000", a.Address)
	// The claim routed to the picked node's advertise address with the uniform token.
	assert.Equal(t, "10.0.0.2:7777", f.builtAddr)
	assert.Equal(t, "uniform-token", f.builtToken)
	assert.Equal(t, "img", f.claimSpec.Template)
	assert.Equal(t, 600, f.claimSpec.TTLSeconds, "the caller's TTL must reach sandboxd")
	assert.Equal(t, deadline, a.Deadline, "the node-granted deadline must ride the assignment back")
	// The claim carries the k8s "<namespace>/<name>" so the node echoes it into
	// its operator index and the aggregated read path can resolve this sandbox.
	assert.Equal(t, "ns/s1", f.claimSpec.ClaimRef)
	assert.Equal(t, 1, f.claimCalls)
}

func TestStoreClaim_NoWarmCapacityIsRetryable(t *testing.T) {
	src := NewStaticInventorySource()
	// Warm==0 everywhere: no node can hand over a microVM.
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	f := &recordingFactory{}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "want ErrNoWarmCapacity, got %v", err)
	assert.Equal(t, 0, f.claimCalls, "must not call sandboxd when no node is warm")
}

func TestStoreClaim_PoolKeyMatchingNormalizesDefaults(t *testing.T) {
	src := NewStaticInventorySource()
	// A pool advertised with unset net/size serves the default-named ("none"/"small") key.
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 2, Target: 2}))
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-1"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img", Net: "none", Size: "small"}, 0)
	require.NoError(t, err)

	// A different net finds no matching pool.
	_, err = store.Claim(t.Context(), "ns", "s2", PoolKey{Template: "img", Net: "egress"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "net mismatch must be no-capacity, got %v", err)
}

func TestStoreClaim_SandboxdCapacityRaceIsRetryable(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
	// The advertised warm count raced to zero: sandboxd answers at-capacity.
	f := &recordingFactory{claimErr: sandboxd.ErrNodeAtCapacity}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "sandboxd 429 must map to no-capacity, got %v", err)
}

func TestStoreRelease_RoutesToNodeAddressWithUniformToken(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n2", "10.0.0.2:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	f := &recordingFactory{}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("uniform-token", f.factory()))

	err := store.Release(t.Context(), "n2", "sb-abc")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2:7777", f.builtAddr, "release must route to the node's advertise address")
	assert.Equal(t, "sb-abc", f.releaseID)
	assert.Equal(t, "uniform-token", f.releaseToken, "release authenticates with the uniform token")
	assert.Equal(t, 1, f.releaseCalls)
}

func TestStoreClaimRelease_FailClosedWithoutRouting(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 1}))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard())) // no WithClaimRouting

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.False(t, IsNoWarmCapacity(err), "unconfigured routing is a config error, not no-capacity")

	require.Error(t, store.Release(t.Context(), "n1", "sb-1"))
}

func TestPickWarmNodeSpreadsAcrossTheFleet(t *testing.T) {
	src := NewStaticInventorySource()
	for _, n := range []struct {
		node string
		warm int
	}{{"n1", 4}, {"n2", 4}, {"n3", 4}, {"n4", 4}} {
		src.Put(poolInv(n.node, n.node+":7777", PoolCapacity{Template: "img", Warm: n.warm, Target: 5}))
	}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()))

	picked := map[string]int{}
	for range 200 {
		candidates, err := store.warmCandidates(t.Context(), PoolKey{Template: "img"})
		require.NoError(t, err)
		best, _ := pickPowerOfTwo(candidates)
		picked[best.node]++
	}
	// Deterministic max-first would put all 200 on one node. Equal warmth means
	// power-of-two sampling must reach every node.
	assert.Len(t, picked, 4, "burst funneled onto a subset: %v", picked)
}

func TestPickWarmNodePrefersTheWarmerSample(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("cold", "cold:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
	src.Put(poolInv("warm", "warm:7777", PoolCapacity{Template: "img", Warm: 100, Target: 200}))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()))

	warmPicks := 0
	for range 200 {
		candidates, err := store.warmCandidates(t.Context(), PoolKey{Template: "img"})
		require.NoError(t, err)
		best, _ := pickPowerOfTwo(candidates)
		if best.node == "warm" {
			warmPicks++
		}
	}
	// Two samples with replacement pick the cold node only when both land on it.
	assert.Greater(t, warmPicks, 130, "sampling lost its bias toward warm capacity")
}

func TestStoreClaimFallsBackWhenTheSampledNodeRacedToZero(t *testing.T) {
	// The classic stale-inventory shape: a node still advertising one warm
	// microVM it no longer has, next to a node that is genuinely warm. Sampling
	// picks the empty one often enough that giving up on it would 503 callers
	// the fleet can serve.
	src := NewStaticInventorySource()
	src.Put(poolInv("stale", "stale:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
	src.Put(poolInv("warm", "warm:7777", PoolCapacity{Template: "img", Warm: 100, Target: 200}))

	f := &raceFactory{emptyAddr: "stale:7777", result: sandboxd.ClaimResult{ID: "sb-ok", Token: "tok"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	for range 40 {
		a, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, 0)
		require.NoError(t, err, "a warm node was available but the claim reported no capacity")
		assert.Equal(t, "warm", a.Node)
	}
}

func TestStoreClaimReportsNoCapacityOnlyWhenEveryNodeRaced(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	src.Put(poolInv("n2", "n2:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))

	f := &raceFactory{emptyAll: true}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "exhausting every node must stay the retryable no-capacity signal")
	assert.Equal(t, 2, f.calls, "each node must be tried exactly once")
}

// raceFactory answers ErrNodeAtCapacity for nodes that advertised warm capacity
// they no longer hold.
type raceFactory struct {
	emptyAddr string
	emptyAll  bool
	result    sandboxd.ClaimResult
	calls     int
}

func (r *raceFactory) factory() SandboxdClientFactory {
	return func(addr, _ string) SandboxdClient {
		return &raceClient{recordingClient: &recordingClient{}, f: r, addr: addr}
	}
}

// raceClient embeds recordingClient for the rest of the SandboxdClient surface
// and only decides whether this node still has the warm microVM it advertised.
type raceClient struct {
	*recordingClient
	f    *raceFactory
	addr string
}

func (c *raceClient) Claim(context.Context, sandboxd.ClaimSpec) (sandboxd.ClaimResult, error) {
	c.f.calls++
	if c.f.emptyAll || c.addr == c.f.emptyAddr {
		return sandboxd.ClaimResult{}, sandboxd.ErrNodeAtCapacity
	}
	return c.f.result, nil
}

// poolInv builds a NodeInventory advertising a sandboxd address and pool capacities.
func poolInv(node, addr string, pools ...PoolCapacity) *NodeInventory {
	return &NodeInventory{
		ObjectMeta: metav1.ObjectMeta{Name: node},
		Node:       node,
		Address:    addr,
		Pools:      pools,
	}
}

// recordingFactory captures how the store built and called the per-node sandboxd
// client, so tests can assert claim/release routing (address + uniform token) and
// the derived claim spec without a live node.
type recordingFactory struct {
	builtAddr    string
	builtToken   string
	claimSpec    sandboxd.ClaimSpec
	claimCalls   int
	releaseID    string
	releaseToken string
	releaseCalls int

	claimResult sandboxd.ClaimResult
	claimErr    error
	releaseErr  error
}

func (f *recordingFactory) factory() SandboxdClientFactory {
	return func(addr, token string) SandboxdClient {
		f.builtAddr, f.builtToken = addr, token
		return &recordingClient{f: f}
	}
}

var _ SandboxdClient = (*recordingClient)(nil)

type recordingClient struct{ f *recordingFactory }

func (c *recordingClient) Claim(_ context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error) {
	c.f.claimCalls++
	c.f.claimSpec = spec
	if c.f.claimErr != nil {
		return sandboxd.ClaimResult{}, c.f.claimErr
	}
	return c.f.claimResult, nil
}

func (c *recordingClient) Release(_ context.Context, id, token string) error {
	c.f.releaseCalls++
	c.f.releaseID, c.f.releaseToken = id, token
	return c.f.releaseErr
}

// The lifecycle verbs are not exercised here; they satisfy the SandboxdClient
// port so the recorder stays a drop-in.
func (c *recordingClient) Hibernate(context.Context, string) error { return nil }

func (c *recordingClient) Wake(context.Context, string) error { return nil }

func (c *recordingClient) Fork(context.Context, string, sandboxd.ForkSpec) (sandboxd.ForkResult, error) {
	return sandboxd.ForkResult{}, nil
}

func (c *recordingClient) Checkpoint(context.Context, string, sandboxd.CheckpointSpec) (sandboxd.Checkpoint, error) {
	return sandboxd.Checkpoint{}, nil
}

func (c *recordingClient) Checkpoints(context.Context) ([]sandboxd.Checkpoint, error) {
	return nil, nil
}

func (c *recordingClient) DeleteCheckpoint(context.Context, string) error { return nil }

func (c *recordingClient) ClaimCheckpoint(context.Context, string, sandboxd.CheckpointClaimSpec) (sandboxd.ClaimResult, error) {
	return sandboxd.ClaimResult{}, nil
}

func (c *recordingClient) Promote(context.Context, string, sandboxd.PromoteSpec) (sandboxd.PoolKey, error) {
	return sandboxd.PoolKey{}, nil
}

func (c *recordingClient) Stats(context.Context, string) (sandboxd.SandboxStats, error) {
	return sandboxd.SandboxStats{}, nil
}
