/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonsandboxv1 "github.com/cocoonstack/sandbox-operator/api/cocoon/v1"
	"github.com/cocoonstack/sandbox-operator/cocoon/sandboxd"
)

func TestCocoonSandboxReconcileDoesNotReclaimReadySandbox(t *testing.T) {
	ctx := context.Background()
	var claims atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer node-api-token", r.Header.Get("Authorization"))
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/claim", r.URL.Path)

		var req sandboxd.ClaimRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "rt-24-04", req.Template)
		assert.Equal(t, "none", req.Net)
		assert.Equal(t, "small", req.Size)
		claims.Add(1)

		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(sandboxd.ClaimResponse{
			ID:        "sb-ready",
			Token:     "sandbox-token",
			OwnerAddr: strings.TrimPrefix(serverURL(r), "http://"),
		}))
	}))
	defer server.Close()

	scheme := newCocoonSandboxTestScheme(t)
	sb := newCocoonSandboxTestObject()
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cocoonsandboxv1.CocoonSandbox{}).
		WithObjects(newCocoonSandboxTestObjects(sb, server.URL)...).
		Build()

	r := &CocoonSandboxController{
		Client:  kube,
		Scheme:  scheme,
		Sandbox: sandboxd.NewClient(server.Client()),
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: sb.Namespace, Name: sb.Name}}

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	require.Equal(t, int32(1), claims.Load())

	var got cocoonsandboxv1.CocoonSandbox
	require.NoError(t, kube.Get(ctx, req.NamespacedName, &got))
	require.Equal(t, "sb-ready", got.Status.SandboxID)
	require.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, cocoonsandboxv1.ConditionReady))
	require.NotNil(t, got.Status.TokenSecretRef)

	var secret corev1.Secret
	require.NoError(t, kube.Get(ctx, types.NamespacedName{Namespace: sb.Namespace, Name: got.Status.TokenSecretRef.Name}, &secret))
	require.Equal(t, "sandbox-token", string(secret.Data[cocoonSandboxTokenKey]))
}

func TestCocoonSandboxClaimReleasesClaimWhenStatusUpdateFails(t *testing.T) {
	ctx := context.Background()
	var releases atomic.Int32
	var releaseAuth atomic.Value

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/claim":
			w.Header().Set("Content-Type", "application/json")
			assert.NoError(t, json.NewEncoder(w).Encode(sandboxd.ClaimResponse{
				ID:        "sb-rollback",
				Token:     "rollback-token",
				OwnerAddr: strings.TrimPrefix(serverURL(r), "http://"),
			}))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sandboxes/sb-rollback/release":
			releases.Add(1)
			releaseAuth.Store(r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sandboxd request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	scheme := newCocoonSandboxTestScheme(t)
	sb := newCocoonSandboxTestObject()
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cocoonsandboxv1.CocoonSandbox{}).
		WithObjects(newCocoonSandboxTestObjects(sb, server.URL)...).
		Build()
	failingKube := &statusUpdateFailClient{Client: kube, failStatus: true}

	r := &CocoonSandboxController{
		Client:  failingKube,
		Scheme:  scheme,
		Sandbox: sandboxd.NewClient(server.Client()),
	}

	err := r.claim(ctx, sb)
	require.Error(t, err)
	require.Contains(t, err.Error(), "update CocoonSandbox status")
	require.Equal(t, int32(1), releases.Load())
	require.Equal(t, "Bearer rollback-token", releaseAuth.Load())
}

func TestCocoonSandboxPoolReconcileSetsSandboxdTarget(t *testing.T) {
	ctx := context.Background()
	var gotAuth atomic.Value
	var gotPools atomic.Value

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/v1/pools", r.URL.Path)
		gotAuth.Store(r.Header.Get("Authorization"))

		var req struct {
			Pools []sandboxd.PoolSpec `json:"pools"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		gotPools.Store(req.Pools)

		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(sandboxd.NodeInfo{
			Pools: []sandboxd.PoolStatus{{
				Key:    sandboxd.PoolKey{Template: "rt-24-04", Net: string(cocoonsandboxv1.NetNone), Size: string(cocoonsandboxv1.SizeSmall)},
				Warm:   2,
				Target: 2,
				Golden: true,
			}},
		}))
	}))
	defer server.Close()

	scheme := newCocoonSandboxTestScheme(t)
	addr := strings.TrimPrefix(server.URL, "http://")
	poolObj := &cocoonsandboxv1.CocoonSandboxPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "default", Generation: 1},
		Spec: cocoonsandboxv1.CocoonSandboxPoolSpec{
			Replicas:    2,
			TemplateRef: corev1.LocalObjectReference{Name: "rt"},
		},
	}
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cocoonsandboxv1.CocoonSandboxPool{}, &cocoonsandboxv1.CocoonSandboxNode{}).
		WithObjects(
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "node-token", Namespace: "default"},
				Data:       map[string][]byte{cocoonSandboxTokenKey: []byte("node-api-token")},
			},
			&cocoonsandboxv1.CocoonSandboxTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "rt", Namespace: "default"},
				Spec: cocoonsandboxv1.CocoonSandboxTemplateSpec{
					Template: "rt-24-04",
					Net:      cocoonsandboxv1.NetNone,
					Size:     cocoonsandboxv1.SizeSmall,
				},
			},
			&cocoonsandboxv1.CocoonSandboxNode{
				ObjectMeta: metav1.ObjectMeta{Name: "node-a", Namespace: "default"},
				Spec: cocoonsandboxv1.CocoonSandboxNodeSpec{
					NodeName: "node-a",
					Address:  addr,
					APITokenSecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "node-token"},
						Key:                  cocoonSandboxTokenKey,
					},
				},
			},
			poolObj,
		).
		Build()

	r := &CocoonSandboxPoolController{
		Client:  kube,
		Sandbox: sandboxd.NewClient(server.Client()),
	}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pool"}})
	require.NoError(t, err)

	require.Equal(t, "Bearer node-api-token", gotAuth.Load())
	pools, ok := gotPools.Load().([]sandboxd.PoolSpec)
	require.True(t, ok)
	require.Equal(t, []sandboxd.PoolSpec{{
		Template: "rt-24-04",
		Net:      string(cocoonsandboxv1.NetNone),
		Size:     string(cocoonsandboxv1.SizeSmall),
		Warm:     2,
	}}, pools)

	var gotPool cocoonsandboxv1.CocoonSandboxPool
	require.NoError(t, kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pool"}, &gotPool))
	require.Equal(t, int32(2), gotPool.Status.Replicas)
	require.Equal(t, int32(2), gotPool.Status.ReadyReplicas)
	require.True(t, meta.IsStatusConditionTrue(gotPool.Status.Conditions, cocoonsandboxv1.ConditionReady))
	require.Equal(t, []cocoonsandboxv1.CocoonSandboxPoolNodeStatus{{
		NodeName: "node-a",
		Warm:     2,
		Target:   2,
	}}, gotPool.Status.WarmByNode)

	var gotNode cocoonsandboxv1.CocoonSandboxNode
	require.NoError(t, kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: "node-a"}, &gotNode))
	require.True(t, meta.IsStatusConditionTrue(gotNode.Status.Conditions, cocoonsandboxv1.ConditionReady))
	require.Equal(t, int32(2), gotNode.Status.Pools[0].Target)
}

func TestCocoonSandboxPoolEnqueuesPoolsForNode(t *testing.T) {
	ctx := context.Background()
	scheme := newCocoonSandboxTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			&cocoonsandboxv1.CocoonSandboxPool{ObjectMeta: metav1.ObjectMeta{Name: "pool-b", Namespace: "default"}},
			&cocoonsandboxv1.CocoonSandboxPool{ObjectMeta: metav1.ObjectMeta{Name: "pool-a", Namespace: "default"}},
			&cocoonsandboxv1.CocoonSandboxPool{ObjectMeta: metav1.ObjectMeta{Name: "pool-other", Namespace: "other"}},
		).
		Build()

	r := &CocoonSandboxPoolController{Client: kube}
	reqs := r.enqueuePoolsForNode(ctx, &cocoonsandboxv1.CocoonSandboxNode{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a", Namespace: "default"},
	})

	require.Equal(t, []ctrl.Request{
		{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pool-a"}},
		{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pool-b"}},
	}, reqs)
}

func TestIgnoreStatusConflict(t *testing.T) {
	nodeErr := apierrors.NewConflict(
		schema.GroupResource{Group: cocoonsandboxv1.GroupVersion.Group, Resource: "cocoonsandboxnodes"},
		"node-a",
		errors.New("injected conflict"),
	)
	poolErr := apierrors.NewConflict(
		schema.GroupResource{Group: cocoonsandboxv1.GroupVersion.Group, Resource: "cocoonsandboxpools"},
		"pool-a",
		errors.New("injected conflict"),
	)
	require.NoError(t, ignoreStatusConflict(fmt.Errorf("update CocoonSandboxNode status: %w", nodeErr)))
	require.NoError(t, ignoreStatusConflict(fmt.Errorf("update CocoonSandboxPool status: %w", poolErr)))
	require.Error(t, ignoreStatusConflict(errors.New("boom")))
}

type statusUpdateFailClient struct {
	client.Client
	failStatus bool
}

func (c *statusUpdateFailClient) Status() client.SubResourceWriter {
	return &statusUpdateFailWriter{
		SubResourceWriter: c.Client.Status(),
		fail:              &c.failStatus,
	}
}

type statusUpdateFailWriter struct {
	client.SubResourceWriter
	fail *bool
}

func (w *statusUpdateFailWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if *w.fail {
		*w.fail = false
		return apierrors.NewConflict(
			schema.GroupResource{Group: cocoonsandboxv1.GroupVersion.Group, Resource: "cocoonsandboxes"},
			obj.GetName(),
			errors.New("injected status conflict"),
		)
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func newCocoonSandboxTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, cocoonsandboxv1.AddToScheme(scheme))
	return scheme
}

func newCocoonSandboxTestObject() *cocoonsandboxv1.CocoonSandbox {
	return &cocoonsandboxv1.CocoonSandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "sandbox",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: cocoonsandboxv1.CocoonSandboxSpec{
			TemplateRef: corev1.LocalObjectReference{Name: "rt"},
			Net:         cocoonsandboxv1.NetNone,
			Size:        cocoonsandboxv1.SizeSmall,
			Lifecycle:   &cocoonsandboxv1.CocoonSandboxLifecycle{TTLSeconds: 60},
			Placement:   &cocoonsandboxv1.CocoonSandboxPlacement{NodeName: "node-a"},
		},
	}
}

func newCocoonSandboxTestObjects(sb *cocoonsandboxv1.CocoonSandbox, serverURL string) []client.Object {
	addr := strings.TrimPrefix(serverURL, "http://")
	return []client.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "node-token", Namespace: sb.Namespace},
			Data:       map[string][]byte{cocoonSandboxTokenKey: []byte("node-api-token")},
		},
		&cocoonsandboxv1.CocoonSandboxTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "rt", Namespace: sb.Namespace},
			Spec: cocoonsandboxv1.CocoonSandboxTemplateSpec{
				Template: "rt-24-04",
				Net:      cocoonsandboxv1.NetNone,
				Size:     cocoonsandboxv1.SizeSmall,
			},
		},
		&cocoonsandboxv1.CocoonSandboxNode{
			ObjectMeta: metav1.ObjectMeta{Name: "node-a", Namespace: sb.Namespace},
			Spec: cocoonsandboxv1.CocoonSandboxNodeSpec{
				NodeName: "node-a",
				Address:  addr,
				APITokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "node-token"},
					Key:                  cocoonSandboxTokenKey,
				},
			},
			Status: cocoonsandboxv1.CocoonSandboxNodeStatus{
				Address: addr,
				Pools: []cocoonsandboxv1.CocoonSandboxNodePoolStatus{{
					Template: "rt-24-04",
					Net:      cocoonsandboxv1.NetNone,
					Size:     cocoonsandboxv1.SizeSmall,
					Warm:     1,
					Golden:   true,
				}},
			},
		},
		sb,
	}
}

func serverURL(r *http.Request) string {
	return "http://" + r.Host
}
