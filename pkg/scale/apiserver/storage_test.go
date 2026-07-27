package apiserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// TestDelete_ReleasesByClaimIDAnnotation proves Delete releases the microVM by
// the sandboxd claim id the node published (the claim-id annotation), never by
// the k8s object name.
func TestDelete_ReleasesByClaimIDAnnotation(t *testing.T) {
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        "s1",
			Annotations: map[string]string{ClaimIDAnnotation: "sb_abc123"},
		},
		Status: sandboxv1beta1.SandboxStatus{NodeName: "n1"},
	}
	store := &fakeStore{getSandbox: sb}
	r := NewSandboxREST(store).(*sandboxREST)

	obj, ok, err := r.Delete(deleteCtx("ns"), "s1", nil, &metav1.DeleteOptions{})
	require.NoError(t, err)
	assert.True(t, ok)
	assert.NotNil(t, obj)
	assert.True(t, store.released, "expected the node-local claim to be released")
	assert.Equal(t, "n1", store.releaseNode)
	assert.Equal(t, "sb_abc123", store.releaseID, "must release by the sandboxd claim id, not the k8s name")
}

// TestDelete_FailsLoudWithoutClaimID proves Delete refuses to release when the
// node has not published a claim id — releasing by the k8s name (the old bug)
// would target the wrong claim. It must error and release nothing.
func TestDelete_FailsLoudWithoutClaimID(t *testing.T) {
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "s1"},
		Status:     sandboxv1beta1.SandboxStatus{NodeName: "n1"},
	}
	store := &fakeStore{getSandbox: sb}
	r := NewSandboxREST(store).(*sandboxREST)

	_, ok, err := r.Delete(deleteCtx("ns"), "s1", nil, &metav1.DeleteOptions{})
	require.Error(t, err)
	assert.False(t, ok)
	assert.True(t, apierrors.IsInternalError(err), "expected an internal error, got %v", err)
	assert.False(t, store.released, "must not release when the sandboxd claim id is unknown")
}

// fakeStore is a scale.SandboxStore stub for the Delete path: Get returns a
// preset sandbox, Release records its arguments. The read-path verbs are unused.
type fakeStore struct {
	getSandbox *sandboxv1beta1.Sandbox
	getErr     error

	released    bool
	releaseNode string
	releaseID   string
	releaseErr  error
}

func (f *fakeStore) List(context.Context, scale.ListOptions) (*sandboxv1beta1.SandboxList, error) {
	return &sandboxv1beta1.SandboxList{}, nil
}

func (f *fakeStore) Get(context.Context, string, string) (*sandboxv1beta1.Sandbox, error) {
	return f.getSandbox, f.getErr
}

func (f *fakeStore) Watch(context.Context, scale.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

func (f *fakeStore) Claim(context.Context, string, string, scale.PoolKey, int) (scale.Assignment, error) {
	return scale.Assignment{}, nil
}

func (f *fakeStore) Release(_ context.Context, node, id string) error {
	f.released, f.releaseNode, f.releaseID = true, node, id
	return f.releaseErr
}

// The lifecycle verbs are not exercised by these tests; they satisfy the
// SandboxStore contract so the fake stays a drop-in.
func (f *fakeStore) Pause(context.Context, string, string) error { return nil }

func (f *fakeStore) Resume(context.Context, string, string) error { return nil }

func (f *fakeStore) Fork(context.Context, string, string, int, int) ([]scale.Assignment, error) {
	return nil, nil
}

func (f *fakeStore) Snapshot(context.Context, string, string, string) (scale.Snapshot, error) {
	return scale.Snapshot{}, nil
}

func (f *fakeStore) Snapshots(context.Context, string) ([]scale.Snapshot, error) { return nil, nil }

func (f *fakeStore) DeleteSnapshot(context.Context, string, string) error { return nil }

func (f *fakeStore) ClaimSnapshot(context.Context, string, string, int) (scale.Assignment, error) {
	return scale.Assignment{}, nil
}

func (f *fakeStore) Promote(context.Context, string, string, string) (scale.PoolKey, error) {
	return scale.PoolKey{}, nil
}

func (f *fakeStore) Stats(context.Context, string, string) (scale.SandboxStats, error) {
	return scale.SandboxStats{}, nil
}

func deleteCtx(ns string) context.Context {
	return genericapirequest.WithNamespace(context.Background(), ns)
}
