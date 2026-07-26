package sandboxd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaimSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// assert (not require) inside a handler goroutine: t.Errorf is goroutine-safe.
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/claim", r.URL.Path)
		assert.Equal(t, "Bearer root-token", r.Header.Get("Authorization"))
		var spec ClaimSpec
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&spec))
		assert.Equal(t, "base:24.04", spec.Template)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ClaimResult{
			ID: "sb_abc", Token: "sbtok", Deadline: "2026-07-06T00:05:00Z", OwnerAddr: "10.0.0.5:7777",
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	res, err := c.Claim(context.Background(), ClaimSpec{Template: "base:24.04", Net: "none", Size: "small", TTLSeconds: 300})
	require.NoError(t, err)
	require.Equal(t, "sb_abc", res.ID)
	require.Equal(t, "sbtok", res.Token)
	require.Equal(t, "10.0.0.5:7777", res.OwnerAddr)
}

// TestSandboxdClaimFallbackOn429 asserts a 429 maps to ErrNodeAtCapacity, the
// signal the gateway turns into an L1 fallback.
func TestSandboxdClaimFallbackOn429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "node at max_claims"})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	_, err := c.Claim(context.Background(), ClaimSpec{Template: "base:24.04"})
	require.ErrorIs(t, err, ErrNodeAtCapacity)
}

// TestSandboxdClaimRedirectIsCapacityMiss asserts a 200 that carries only a peer
// redirect (no delivered id) is treated as a capacity miss, not a bogus success.
func TestSandboxdClaimRedirectIsCapacityMiss(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ClaimResult{Redirect: []string{"10.0.0.6:7777"}})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	_, err := c.Claim(context.Background(), ClaimSpec{Template: "base:24.04"})
	require.ErrorIs(t, err, ErrNodeAtCapacity)
}

func TestClaimServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "provisioning failed"})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	_, err := c.Claim(context.Background(), ClaimSpec{Template: "base:24.04"})
	require.Error(t, err)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusInternalServerError, he.StatusCode)
	require.Equal(t, "provisioning failed", he.Message)
}

func TestReleaseSuccessAndAlreadyGone(t *testing.T) {
	var releases atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := releases.Add(1)
		assert.Equal(t, "Bearer sbtok", r.Header.Get("Authorization"), "release authenticates with the sandbox's own token")
		assert.Equal(t, "/v1/sandboxes/sb_abc/release", r.URL.Path)
		if n == 1 {
			w.WriteHeader(http.StatusNoContent) // first: destroyed
		} else {
			w.WriteHeader(http.StatusNotFound) // second: already gone
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	require.NoError(t, c.Release(context.Background(), "sb_abc", "sbtok"))
	// 404 (already gone) is treated as success.
	require.NoError(t, c.Release(context.Background(), "sb_abc", "sbtok"))
	require.Equal(t, int64(2), releases.Load())
}
