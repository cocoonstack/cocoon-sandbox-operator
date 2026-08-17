package scale

import (
	"context"
	"time"
)

// ClaimRequest identifies a node-local warm-pool claim.
type ClaimRequest struct {
	Namespace string
	ClaimName string
	WarmPool  string
	// Selector optionally narrows which warm sandboxes are eligible (template
	// blueprint hash, node affinity, etc.).
	Selector map[string]string
}

// Assignment is the result of a successful claim: the warm sandbox whose
// ownership was transferred, the node serving it, and its connection address.
// The SandboxClaim is marked Bound asynchronously after this returns.
type Assignment struct {
	SandboxName string
	Node        string
	Address     string
	// Token is the per-sandbox ownership credential returned by sandboxd on the
	// claim. It authenticates agent/exec against the delivered VM; the L3 Create
	// path surfaces it as an annotation so a caller can exec into what it claimed.
	Token string
	// Deadline is when the granted lease expires, as reported by the owning
	// node. It is authoritative over whatever TTL was requested: the node
	// applies its default when none was asked and clamps to its own maximum.
	// Zero when the node did not report one.
	Deadline time.Time
}

// ClaimGateway is the L2 node-local fast path for warm-pool claims. A claim is
// served by the node that already holds a warm microVM (via sandboxd), which
// hands over an already-running guest in sub-millisecond time and returns
// connection info immediately; the SandboxClaim object is reconciled to Bound
// asynchronously afterward.
// Authorization stays central through SubjectAccessReview.
type ClaimGateway interface {
	// Claim transfers ownership of a node-local warm sandbox to the caller and
	// returns connection info. It performs the authorization check inline; it
	// does NOT block on writing the SandboxClaim status.
	Claim(ctx context.Context, req ClaimRequest) (Assignment, error)
	// Release returns a sandbox to the node-local pool, or tears it down when
	// the pool is over target. Never destroys a VM on pod-level state alone —
	// see the vk-cocoon delete-authorization contract.
	Release(ctx context.Context, assignment Assignment) error
}
