/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package sandboxd contains the HTTP client used by the Cocoon compatibility
// controllers to reconcile Kubernetes resources with node-local sandboxd.
package sandboxd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	maxErrorBodyBytes     = 4096
	defaultRequestTimeout = 15 * time.Second
)

// Client talks to one sandboxd node.
type Client struct {
	httpClient *http.Client
}

// NewClient returns a sandboxd HTTP client.
func NewClient(hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: defaultRequestTimeout}
	}
	return &Client{httpClient: hc}
}

// ClaimRequest is the POST /v1/claim request body.
type ClaimRequest struct {
	Template   string `json:"template"`
	Net        string `json:"net,omitempty"`
	Size       string `json:"size,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
	NoRedirect bool   `json:"no_redirect,omitempty"`
}

// ClaimResponse is the POST /v1/claim response body.
type ClaimResponse struct {
	ID        string    `json:"id,omitempty"`
	Token     string    `json:"token,omitempty"`
	Deadline  time.Time `json:"deadline,omitempty"`
	OwnerAddr string    `json:"owner_addr,omitempty"`
	Redirect  []string  `json:"redirect,omitempty"`
}

// NodeInfo is the GET /v1/info response body.
type NodeInfo struct {
	Pools      []PoolStatus `json:"pools"`
	Claimed    int          `json:"claimed"`
	Hibernated int          `json:"hibernated"`
	Peers      []string     `json:"peers,omitempty"`
}

// PoolStatus is one warm-pool entry from GET /v1/info.
type PoolStatus struct {
	Key       PoolKey `json:"key"`
	Warm      int     `json:"warm"`
	Refilling int     `json:"refilling"`
	Target    int     `json:"target"`
	Golden    bool    `json:"golden"`
}

// PoolSpec is one desired warm-pool target for PUT /v1/pools.
type PoolSpec struct {
	Template string `json:"template"`
	Net      string `json:"net,omitempty"`
	Size     string `json:"size,omitempty"`
	Warm     int    `json:"warm"`
}

// PoolKey is the sandboxd pool key.
type PoolKey struct {
	Template string `json:"template"`
	Net      string `json:"net"`
	Size     string `json:"size"`
}

// Claim claims a sandbox from sandboxd and follows one redirect hop.
func (c *Client) Claim(ctx context.Context, addr, apiToken string, req ClaimRequest) (ClaimResponse, string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ClaimResponse{}, "", fmt.Errorf("sandboxd address is required")
	}
	resp, err := c.claimAt(ctx, addr, apiToken, req)
	if err != nil {
		return ClaimResponse{}, "", err
	}
	if len(resp.Redirect) == 0 {
		return resp, addr, nil
	}
	req.NoRedirect = true
	var lastErr error
	for _, redirectAddr := range resp.Redirect {
		redirectAddr = strings.TrimSpace(redirectAddr)
		if redirectAddr == "" {
			continue
		}
		resp, err = c.claimAt(ctx, redirectAddr, apiToken, req)
		if err == nil {
			return resp, redirectAddr, nil
		}
		lastErr = err
	}
	return ClaimResponse{}, "", fmt.Errorf("claim redirect targets failed: %w", lastErr)
}

// Info reads one sandboxd node's inventory.
func (c *Client) Info(ctx context.Context, addr, apiToken string) (*NodeInfo, error) {
	resp, err := c.roundTrip(ctx, http.MethodGet, addr, "/v1/info", nil, apiToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("info", resp)
	}
	var out NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode info response: %w", err)
	}
	return &out, nil
}

// SetPools replaces one sandboxd node's desired warm-pool targets.
func (c *Client) SetPools(ctx context.Context, addr, apiToken string, pools []PoolSpec) (*NodeInfo, error) {
	body, err := json.Marshal(struct {
		Pools []PoolSpec `json:"pools"`
	}{Pools: pools})
	if err != nil {
		return nil, fmt.Errorf("encode pool update request: %w", err)
	}
	resp, err := c.roundTrip(ctx, http.MethodPut, addr, "/v1/pools", bytes.NewReader(body), apiToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("set pools", resp)
	}
	var out NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode pool update response: %w", err)
	}
	return &out, nil
}

// Release releases a sandbox. A 404 is treated as success by callers.
func (c *Client) Release(ctx context.Context, addr, sandboxID, token string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil
	}
	resp, err := c.roundTrip(ctx, http.MethodPost, addr, "/v1/sandboxes/"+sandboxID+"/release", nil, token)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return apiError("release", resp)
	}
}

func (c *Client) claimAt(ctx context.Context, addr, apiToken string, req ClaimRequest) (ClaimResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return ClaimResponse{}, fmt.Errorf("encode claim request: %w", err)
	}
	resp, err := c.roundTrip(ctx, http.MethodPost, addr, "/v1/claim", bytes.NewReader(body), apiToken)
	if err != nil {
		return ClaimResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return ClaimResponse{}, apiError("claim", resp)
	}
	var out ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return ClaimResponse{}, fmt.Errorf("decode claim response: %w", err)
	}
	return out, nil
}

func (c *Client) roundTrip(ctx context.Context, method, addr, path string, body io.Reader, bearer string) (*http.Response, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("sandboxd address is required")
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	return c.httpClient.Do(req) //nolint:gosec // sandboxd addr is an operator-controlled node address.
}

func apiError(verb string, resp *http.Response) error {
	var body struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBodyBytes)).Decode(&body)
	if body.Error != "" {
		return fmt.Errorf("%s: %s (http %d)", verb, body.Error, resp.StatusCode)
	}
	return fmt.Errorf("%s: http %d", verb, resp.StatusCode)
}
