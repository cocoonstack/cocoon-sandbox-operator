package sandboxd_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cocoonstack/sandbox-operator/cocoon/sandboxd"
)

func TestClientClaimFollowsOneRedirect(t *testing.T) {
	t.Parallel()

	var targetAddr string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/claim", r.URL.Path)
		assert.Equal(t, "Bearer node-token", r.Header.Get("Authorization"))

		var req sandboxd.ClaimRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "cocoon-rt-24-04", req.Template)
		assert.True(t, req.NoRedirect)

		assert.NoError(t, json.NewEncoder(w).Encode(sandboxd.ClaimResponse{
			ID:        "sb-1",
			Token:     "sandbox-token",
			OwnerAddr: targetAddr,
		}))
	}))
	defer target.Close()
	targetAddr = strings.TrimPrefix(target.URL, "http://")

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/claim", r.URL.Path)
		assert.NoError(t, json.NewEncoder(w).Encode(sandboxd.ClaimResponse{
			Redirect: []string{targetAddr},
		}))
	}))
	defer source.Close()

	got, dialed, err := sandboxd.NewClient(source.Client()).Claim(
		context.Background(),
		strings.TrimPrefix(source.URL, "http://"),
		"node-token",
		sandboxd.ClaimRequest{Template: "cocoon-rt-24-04"},
	)
	require.NoError(t, err)
	require.Equal(t, "sb-1", got.ID)
	require.Equal(t, "sandbox-token", got.Token)
	require.Equal(t, targetAddr, dialed)
}

func TestClientInfoDecodesPools(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/info", r.URL.Path)
		assert.NoError(t, json.NewEncoder(w).Encode(sandboxd.NodeInfo{
			Claimed: 2,
			Pools: []sandboxd.PoolStatus{{
				Key:    sandboxd.PoolKey{Template: "rt", Net: "none", Size: "small"},
				Warm:   3,
				Target: 4,
				Golden: true,
			}},
		}))
	}))
	defer server.Close()

	info, err := sandboxd.NewClient(server.Client()).Info(context.Background(), strings.TrimPrefix(server.URL, "http://"), "")
	require.NoError(t, err)
	require.Equal(t, 2, info.Claimed)
	require.Equal(t, 3, info.Pools[0].Warm)
	require.True(t, info.Pools[0].Golden)
}
