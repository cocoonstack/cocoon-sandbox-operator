package apiserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
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

	obj, ok, err := r.Delete(deleteCtx(t, "ns"), "s1", nil, &metav1.DeleteOptions{})
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

	_, ok, err := r.Delete(deleteCtx(t, "ns"), "s1", nil, &metav1.DeleteOptions{})
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

	claimCalls  int
	claimTTL    int
	claimAssign scale.Assignment

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

func (f *fakeStore) Claim(_ context.Context, _, _ string, _ scale.PoolKey, ttlSeconds int) (scale.Assignment, error) {
	f.claimCalls++
	f.claimTTL = ttlSeconds
	return f.claimAssign, nil
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

func deleteCtx(t *testing.T, ns string) context.Context {
	return genericapirequest.WithNamespace(t.Context(), ns)
}

// submittedSandbox builds the minimal Sandbox a client POSTs to Create: a name
// plus one container image for the pool key, with optional annotations.
func submittedSandbox(name string, anns map[string]string) *sandboxv1beta1.Sandbox {
	sb := &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: anns}}
	sb.Spec.PodTemplate.Spec.Containers = []corev1.Container{{Name: "c", Image: "img"}}
	return sb
}

// withShutdown returns sb with spec.shutdownTime set to t.
func withShutdown(sb *sandboxv1beta1.Sandbox, t time.Time) *sandboxv1beta1.Sandbox {
	sb.Spec.ShutdownTime = &metav1.Time{Time: t}
	return sb
}

// TestTTLSecondsForSandbox pins the lease derivation against a fixed clock:
// spec.shutdownTime wins and rounds up to whole seconds, the annotation covers
// clients that cannot set the field, and unusable lifetimes are errors so
// Create can 400 before a warm microVM is spent.
func TestTTLSecondsForSandbox(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		sb      *sandboxv1beta1.Sandbox
		want    int
		wantErr bool
	}{
		"shutdownTime":         {sb: withShutdown(submittedSandbox("s", nil), now.Add(90*time.Second)), want: 90},
		"sub-second rounds up": {sb: withShutdown(submittedSandbox("s", nil), now.Add(90*time.Second+500*time.Millisecond)), want: 91},
		"annotation":           {sb: submittedSandbox("s", map[string]string{TTLSecondsAnnotation: "120"}), want: 120},
		"spec wins over annotation": {
			sb:   withShutdown(submittedSandbox("s", map[string]string{TTLSecondsAnnotation: "10"}), now.Add(time.Hour)),
			want: 3600,
		},
		"explicit zero asks the node default": {sb: submittedSandbox("s", map[string]string{TTLSecondsAnnotation: "0"}), want: 0},
		"no lifetime asks the node default":   {sb: submittedSandbox("s", nil), want: 0},
		"expired shutdownTime":                {sb: withShutdown(submittedSandbox("s", nil), now), wantErr: true},
		"malformed annotation":                {sb: submittedSandbox("s", map[string]string{TTLSecondsAnnotation: "banana"}), wantErr: true},
		"negative annotation":                 {sb: submittedSandbox("s", map[string]string{TTLSecondsAnnotation: "-5"}), wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ttlSecondsForSandbox(tc.sb, now)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestCreate_TTLRidesTheClaim proves the derived lease reaches the store — the
// wiring the fixed-clock table cannot see. Before this fix Create claimed with
// a hardcoded 0 and every k8s-created sandbox got sandboxd's default lease.
func TestCreate_TTLRidesTheClaim(t *testing.T) {
	f := &fakeStore{claimAssign: scale.Assignment{SandboxName: "sb_1", Node: "n1"}}
	r := NewSandboxREST(f).(*sandboxREST)

	_, err := r.Create(deleteCtx(t, "ns"), submittedSandbox("s1", map[string]string{TTLSecondsAnnotation: "120"}), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, 120, f.claimTTL)

	_, err = r.Create(deleteCtx(t, "ns"), submittedSandbox("s2", nil), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, f.claimTTL, "no submitted lifetime asks for the node default")
}

// TestCreate_RejectsUnusableLifetime: a bad lifetime 400s before any microVM
// is claimed; the derivation table above covers the full reject matrix.
func TestCreate_RejectsUnusableLifetime(t *testing.T) {
	f := &fakeStore{}
	r := NewSandboxREST(f).(*sandboxREST)

	_, err := r.Create(deleteCtx(t, "ns"), submittedSandbox("s1", map[string]string{TTLSecondsAnnotation: "banana"}), nil, nil)
	require.Error(t, err)
	assert.True(t, apierrors.IsBadRequest(err), "expected BadRequest, got %v", err)
	assert.Equal(t, 0, f.claimCalls, "no claim may be spent on a rejected request")
}

// TestCreate_EchoesGrantedDeadline: the node fixes the real expiry (clamping to
// its default and maximum), so the returned object must report the node's
// deadline as spec.shutdownTime rather than echoing what the caller submitted.
func TestCreate_EchoesGrantedDeadline(t *testing.T) {
	deadline := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	f := &fakeStore{claimAssign: scale.Assignment{SandboxName: "sb_1", Node: "n1", Deadline: deadline}}
	r := NewSandboxREST(f).(*sandboxREST)

	obj, err := r.Create(deleteCtx(t, "ns"), submittedSandbox("s1", map[string]string{TTLSecondsAnnotation: "999999"}), nil, nil)
	require.NoError(t, err)
	out, ok := obj.(*sandboxv1beta1.Sandbox)
	require.True(t, ok)
	require.NotNil(t, out.Spec.ShutdownTime)
	assert.True(t, out.Spec.ShutdownTime.Time.Equal(deadline),
		"spec.shutdownTime = %s, want the node-granted deadline %s", out.Spec.ShutdownTime, deadline)
}
