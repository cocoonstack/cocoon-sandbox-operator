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
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cocoonsandboxv1 "github.com/cocoonstack/sandbox-operator/api/cocoon/v1"
	"github.com/cocoonstack/sandbox-operator/cocoon/sandboxd"
)

const (
	cocoonSandboxFinalizer = "sandbox.cocoonstack.io/finalizer"
	cocoonSandboxTokenKey  = "token"
	cocoonSandboxRequeue   = 15 * time.Second
	cocoonNodeRequeue      = 30 * time.Second
	cocoonPoolRequeue      = 30 * time.Second
)

// CocoonSandboxController reconciles sandbox.cocoonstack.io CocoonSandbox claims.
type CocoonSandboxController struct {
	Client  client.Client
	Scheme  *runtime.Scheme
	Sandbox *sandboxd.Client
}

// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxes/finalizers,verbs=update
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxnodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

func (r *CocoonSandboxController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Sandbox == nil {
		r.Sandbox = sandboxd.NewClient(nil)
	}

	var sb cocoonsandboxv1.CocoonSandbox
	if err := r.Client.Get(ctx, req.NamespacedName, &sb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !sb.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &sb)
	}

	if controllerutil.AddFinalizer(&sb, cocoonSandboxFinalizer) {
		if err := r.Client.Update(ctx, &sb); err != nil {
			return ctrl.Result{}, fmt.Errorf("add CocoonSandbox finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if sb.Status.SandboxID != "" && meta.IsStatusConditionTrue(sb.Status.Conditions, cocoonsandboxv1.ConditionReady) {
		return ctrl.Result{}, nil
	}

	if err := r.claim(ctx, &sb); err != nil {
		if statusErr := r.setSandboxReadyCondition(ctx, &sb, metav1.ConditionFalse, cocoonsandboxv1.PhaseFailed, "ClaimFailed", err.Error()); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: cocoonSandboxRequeue}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CocoonSandboxController) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.Sandbox == nil {
		r.Sandbox = sandboxd.NewClient(nil)
	}
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{NeedLeaderElection: new(true)}).
		For(&cocoonsandboxv1.CocoonSandbox{}).
		Named("cocoonsandbox").
		Complete(r)
}

func (r *CocoonSandboxController) reconcileDelete(ctx context.Context, sb *cocoonsandboxv1.CocoonSandbox) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(sb, cocoonSandboxFinalizer) {
		return ctrl.Result{}, nil
	}
	token := ""
	if ref := sb.Status.TokenSecretRef; ref != nil {
		v, err := r.secretValue(ctx, sb.Namespace, ref)
		if err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: cocoonSandboxRequeue}, fmt.Errorf("read sandbox token secret: %w", err)
		}
		token = v
	}
	if sb.Status.OwnerAddr != "" && sb.Status.SandboxID != "" && token != "" {
		if err := r.Sandbox.Release(ctx, sb.Status.OwnerAddr, sb.Status.SandboxID, token); err != nil {
			return ctrl.Result{RequeueAfter: cocoonSandboxRequeue}, err
		}
	}
	if ref := sb.Status.TokenSecretRef; ref != nil && ref.Name != "" {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: sb.Namespace}}
		if err := r.Client.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: cocoonSandboxRequeue}, fmt.Errorf("delete sandbox token secret: %w", err)
		}
	}
	controllerutil.RemoveFinalizer(sb, cocoonSandboxFinalizer)
	if err := r.Client.Update(ctx, sb); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove CocoonSandbox finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *CocoonSandboxController) claim(ctx context.Context, sb *cocoonsandboxv1.CocoonSandbox) error {
	var tmpl cocoonsandboxv1.CocoonSandboxTemplate
	tmplKey := types.NamespacedName{Namespace: sb.Namespace, Name: sb.Spec.TemplateRef.Name}
	if strings.TrimSpace(tmplKey.Name) == "" {
		return fmt.Errorf("spec.templateRef.name is required")
	}
	if err := r.Client.Get(ctx, tmplKey, &tmpl); err != nil {
		return fmt.Errorf("get CocoonSandboxTemplate %s: %w", tmplKey, err)
	}

	node, err := r.selectNode(ctx, sb, &tmpl)
	if err != nil {
		return err
	}
	addr := firstNonEmpty(node.Status.Address, node.Spec.Address)
	if addr == "" {
		return fmt.Errorf("CocoonSandboxNode %s/%s has no address", node.Namespace, node.Name)
	}
	apiToken, err := r.nodeAPIToken(ctx, node)
	if err != nil {
		return err
	}

	net := effectiveNet(sb, &tmpl)
	size := effectiveSize(sb, &tmpl)
	source := inferClaimSource(node, tmpl.Spec.Template, net, size)
	claimReq := sandboxd.ClaimRequest{
		Template:   tmpl.Spec.Template,
		Net:        string(net),
		Size:       string(size),
		TTLSeconds: int(effectiveTTLSeconds(sb)),
	}
	start := time.Now()
	claim, dialedAddr, err := r.Sandbox.Claim(ctx, addr, apiToken, claimReq)
	if err != nil {
		return err
	}
	if claim.ID == "" || claim.Token == "" {
		return fmt.Errorf("sandboxd claim returned incomplete handle")
	}
	ownerAddr := firstNonEmpty(claim.OwnerAddr, dialedAddr, addr)
	secretName := sb.Name + "-cocoon-sandbox-token"
	if err := r.upsertTokenSecret(ctx, sb, secretName, claim.Token); err != nil {
		return r.releaseFailedClaim(ctx, ownerAddr, claim.ID, claim.Token, fmt.Errorf("upsert token Secret: %w", err))
	}

	next := sb.DeepCopy()
	next.Status.Phase = cocoonsandboxv1.PhaseRunning
	next.Status.Source = source
	next.Status.SandboxID = claim.ID
	next.Status.OwnerNode = node.Name
	next.Status.OwnerAddr = ownerAddr
	next.Status.Hibernated = false
	next.Status.TokenSecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
		Key:                  cocoonSandboxTokenKey,
	}
	next.Status.Endpoint = &cocoonsandboxv1.CocoonSandboxEndpoint{
		SilkdUpgradeURL: fmt.Sprintf("http://%s/v1/sandboxes/%s/agent", ownerAddr, claim.ID),
	}
	next.Status.ClaimLatencyMs = fmt.Sprintf("%.3f", float64(time.Since(start).Microseconds())/1000)
	if !claim.Deadline.IsZero() {
		deadline := metav1.NewTime(claim.Deadline)
		next.Status.Deadline = &deadline
	}
	meta.SetStatusCondition(&next.Status.Conditions, metav1.Condition{
		Type:               cocoonsandboxv1.ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "SandboxReady",
		Message:            "sandboxd claim is ready",
		ObservedGeneration: sb.Generation,
	})
	if err := r.Client.Status().Update(ctx, next); err != nil {
		return r.releaseFailedClaim(ctx, ownerAddr, claim.ID, claim.Token, fmt.Errorf("update CocoonSandbox status: %w", err))
	}
	*sb = *next
	return nil
}

func (r *CocoonSandboxController) releaseFailedClaim(ctx context.Context, ownerAddr, sandboxID, token string, cause error) error {
	if releaseErr := r.Sandbox.Release(ctx, ownerAddr, sandboxID, token); releaseErr != nil {
		return errors.Join(cause, fmt.Errorf("release claimed sandbox after reconcile failure: %w", releaseErr))
	}
	return cause
}

func (r *CocoonSandboxController) upsertTokenSecret(ctx context.Context, sb *cocoonsandboxv1.CocoonSandbox, name, token string) error {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: sb.Namespace, Name: name}
	err := r.Client.Get(ctx, key, secret)
	if apierrors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: sb.Namespace},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{cocoonSandboxTokenKey: []byte(token)},
		}
		if err := controllerutil.SetControllerReference(sb, secret, r.Scheme); err != nil {
			return fmt.Errorf("set token Secret owner: %w", err)
		}
		if err := r.Client.Create(ctx, secret); err != nil {
			return fmt.Errorf("create sandbox token secret: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("get sandbox token secret: %w", err)
	}
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	if string(secret.Data[cocoonSandboxTokenKey]) == token {
		return nil
	}
	next := secret.DeepCopy()
	maps.Copy(next.Data, secret.Data)
	next.Data[cocoonSandboxTokenKey] = []byte(token)
	if err := controllerutil.SetControllerReference(sb, next, r.Scheme); err != nil {
		return fmt.Errorf("set token Secret owner: %w", err)
	}
	if err := r.Client.Update(ctx, next); err != nil {
		return fmt.Errorf("update sandbox token secret: %w", err)
	}
	return nil
}

func (r *CocoonSandboxController) setSandboxReadyCondition(ctx context.Context, sb *cocoonsandboxv1.CocoonSandbox, status metav1.ConditionStatus, phase cocoonsandboxv1.CocoonSandboxPhase, reason, message string) error {
	next := sb.DeepCopy()
	next.Status.Phase = phase
	meta.SetStatusCondition(&next.Status.Conditions, metav1.Condition{
		Type:               cocoonsandboxv1.ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: sb.Generation,
	})
	if err := r.Client.Status().Update(ctx, next); err != nil {
		return fmt.Errorf("update CocoonSandbox status: %w", err)
	}
	*sb = *next
	return nil
}

func (r *CocoonSandboxController) selectNode(ctx context.Context, sb *cocoonsandboxv1.CocoonSandbox, tmpl *cocoonsandboxv1.CocoonSandboxTemplate) (*cocoonsandboxv1.CocoonSandboxNode, error) {
	if sb.Spec.Placement != nil && strings.TrimSpace(sb.Spec.Placement.NodeName) != "" {
		var node cocoonsandboxv1.CocoonSandboxNode
		key := types.NamespacedName{Namespace: sb.Namespace, Name: sb.Spec.Placement.NodeName}
		if err := r.Client.Get(ctx, key, &node); err != nil {
			return nil, fmt.Errorf("get pinned CocoonSandboxNode %s: %w", key, err)
		}
		if ok, err := r.nodeMatchesPlacement(ctx, sb, &node); err != nil {
			return nil, err
		} else if !ok {
			return nil, fmt.Errorf("pinned CocoonSandboxNode %s does not match placement selector", key)
		}
		return &node, nil
	}

	var list cocoonsandboxv1.CocoonSandboxNodeList
	if err := r.Client.List(ctx, &list, client.InNamespace(sb.Namespace)); err != nil {
		return nil, fmt.Errorf("list CocoonSandboxNodes: %w", err)
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("no CocoonSandboxNode resources in namespace %s", sb.Namespace)
	}
	net := effectiveNet(sb, tmpl)
	size := effectiveSize(sb, tmpl)
	matches := make([]cocoonsandboxv1.CocoonSandboxNode, 0, len(list.Items))
	for i := range list.Items {
		ok, err := r.nodeMatchesPlacement(ctx, sb, &list.Items[i])
		if err != nil {
			return nil, err
		}
		if ok && firstNonEmpty(list.Items[i].Status.Address, list.Items[i].Spec.Address) != "" {
			matches = append(matches, list.Items[i])
		}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no CocoonSandboxNode matches placement selector")
	}
	sort.SliceStable(matches, func(i, j int) bool {
		return nodeScore(&matches[i], tmpl.Spec.Template, net, size) > nodeScore(&matches[j], tmpl.Spec.Template, net, size)
	})
	return &matches[0], nil
}

func (r *CocoonSandboxController) nodeMatchesPlacement(ctx context.Context, sb *cocoonsandboxv1.CocoonSandbox, node *cocoonsandboxv1.CocoonSandboxNode) (bool, error) {
	if sb.Spec.Placement == nil || len(sb.Spec.Placement.NodeSelector) == 0 {
		return true, nil
	}
	if labelsMatch(node.Labels, sb.Spec.Placement.NodeSelector) {
		return true, nil
	}
	if node.Spec.NodeName == "" {
		return false, nil
	}
	var k8sNode corev1.Node
	if err := r.Client.Get(ctx, types.NamespacedName{Name: node.Spec.NodeName}, &k8sNode); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get Kubernetes Node %s: %w", node.Spec.NodeName, err)
	}
	return labelsMatch(k8sNode.Labels, sb.Spec.Placement.NodeSelector), nil
}

func (r *CocoonSandboxController) nodeAPIToken(ctx context.Context, node *cocoonsandboxv1.CocoonSandboxNode) (string, error) {
	if node.Spec.APITokenSecretRef == nil {
		return "", nil
	}
	token, err := r.secretValue(ctx, node.Namespace, node.Spec.APITokenSecretRef)
	if err != nil {
		return "", fmt.Errorf("read CocoonSandboxNode API token secret: %w", err)
	}
	return token, nil
}

func (r *CocoonSandboxController) secretValue(ctx context.Context, namespace string, ref *corev1.SecretKeySelector) (string, error) {
	if ref == nil || ref.Name == "" {
		return "", nil
	}
	key := ref.Key
	if key == "" {
		key = cocoonSandboxTokenKey
	}
	var secret corev1.Secret
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &secret); err != nil {
		return "", err
	}
	return string(secret.Data[key]), nil
}

// CocoonSandboxNodeController refreshes sandboxd node inventory.
type CocoonSandboxNodeController struct {
	Client  client.Client
	Sandbox *sandboxd.Client
}

// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxnodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxnodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *CocoonSandboxNodeController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Sandbox == nil {
		r.Sandbox = sandboxd.NewClient(nil)
	}
	var node cocoonsandboxv1.CocoonSandboxNode
	if err := r.Client.Get(ctx, req.NamespacedName, &node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	token := ""
	if node.Spec.APITokenSecretRef != nil {
		v, err := secretValue(ctx, r.Client, node.Namespace, node.Spec.APITokenSecretRef)
		if err != nil {
			if statusErr := r.updateNodeStatus(ctx, &node, nil, metav1.ConditionFalse, "APITokenSecretError", err.Error()); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: cocoonNodeRequeue}, err
		}
		token = v
	}
	info, err := r.Sandbox.Info(ctx, node.Spec.Address, token)
	if err != nil {
		if statusErr := ignoreStatusConflict(r.updateNodeStatus(ctx, &node, nil, metav1.ConditionFalse, "InfoFailed", err.Error())); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: cocoonNodeRequeue}, err
	}
	if err := ignoreStatusConflict(r.updateNodeStatus(ctx, &node, info, metav1.ConditionTrue, "SandboxdReady", "sandboxd info is available")); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: cocoonNodeRequeue}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CocoonSandboxNodeController) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Sandbox == nil {
		r.Sandbox = sandboxd.NewClient(nil)
	}
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{NeedLeaderElection: new(true)}).
		For(&cocoonsandboxv1.CocoonSandboxNode{}).
		Named("cocoonsandboxnode").
		Complete(r)
}

func (r *CocoonSandboxNodeController) updateNodeStatus(ctx context.Context, node *cocoonsandboxv1.CocoonSandboxNode, info *sandboxd.NodeInfo, status metav1.ConditionStatus, reason, message string) error {
	next := node.DeepCopy()
	next.Status.Address = node.Spec.Address
	if info != nil {
		next.Status.Pools = make([]cocoonsandboxv1.CocoonSandboxNodePoolStatus, 0, len(info.Pools))
		for _, p := range info.Pools {
			next.Status.Pools = append(next.Status.Pools, cocoonsandboxv1.CocoonSandboxNodePoolStatus{
				Template:  p.Key.Template,
				Net:       cocoonsandboxv1.CocoonSandboxNet(p.Key.Net),
				Size:      cocoonsandboxv1.CocoonSandboxSize(p.Key.Size),
				Warm:      int32(p.Warm),
				Target:    int32(p.Target),
				Refilling: int32(p.Refilling),
				Golden:    p.Golden,
			})
		}
		next.Status.Claimed = int32(info.Claimed)
		next.Status.Hibernated = int32(info.Hibernated)
		next.Status.Peers = append([]string(nil), info.Peers...)
	}
	meta.SetStatusCondition(&next.Status.Conditions, metav1.Condition{
		Type:               cocoonsandboxv1.ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: node.Generation,
	})
	if equality.Semantic.DeepEqual(node.Status, next.Status) {
		return nil
	}
	if err := r.Client.Status().Update(ctx, next); err != nil {
		return fmt.Errorf("update CocoonSandboxNode status: %w", err)
	}
	*node = *next
	return nil
}

// CocoonSandboxPoolController reconciles declarative warm pool capacity into sandboxd nodes.
type CocoonSandboxPoolController struct {
	Client  client.Client
	Sandbox *sandboxd.Client
}

// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxnodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=sandbox.cocoonstack.io,resources=cocoonsandboxnodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *CocoonSandboxPoolController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Sandbox == nil {
		r.Sandbox = sandboxd.NewClient(nil)
	}
	if err := r.syncPoolNamespace(ctx, req.Namespace); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: cocoonPoolRequeue}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CocoonSandboxPoolController) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Sandbox == nil {
		r.Sandbox = sandboxd.NewClient(nil)
	}
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{NeedLeaderElection: new(true)}).
		For(&cocoonsandboxv1.CocoonSandboxPool{}).
		Watches(
			&cocoonsandboxv1.CocoonSandboxNode{},
			handler.EnqueueRequestsFromMapFunc(r.enqueuePoolsForNode),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Named("cocoonsandboxpool").
		Complete(r)
}

func (r *CocoonSandboxPoolController) enqueuePoolsForNode(ctx context.Context, obj client.Object) []reconcile.Request {
	if r.Client == nil || obj == nil {
		return nil
	}
	var pools cocoonsandboxv1.CocoonSandboxPoolList
	if err := r.Client.List(ctx, &pools, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	sort.SliceStable(pools.Items, func(i, j int) bool { return pools.Items[i].Name < pools.Items[j].Name })
	requests := make([]reconcile.Request, 0, len(pools.Items))
	for i := range pools.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: pools.Items[i].Namespace,
			Name:      pools.Items[i].Name,
		}})
	}
	return requests
}

type desiredPool struct {
	obj      *cocoonsandboxv1.CocoonSandboxPool
	template string
	net      cocoonsandboxv1.CocoonSandboxNet
	size     cocoonsandboxv1.CocoonSandboxSize
	targets  map[string]int
}

func (r *CocoonSandboxPoolController) syncPoolNamespace(ctx context.Context, namespace string) error {
	listOpts := []client.ListOption{}
	if namespace != "" {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}
	var pools cocoonsandboxv1.CocoonSandboxPoolList
	if err := r.Client.List(ctx, &pools, listOpts...); err != nil {
		return fmt.Errorf("list CocoonSandboxPools: %w", err)
	}
	var nodeList cocoonsandboxv1.CocoonSandboxNodeList
	if err := r.Client.List(ctx, &nodeList, listOpts...); err != nil {
		return fmt.Errorf("list CocoonSandboxNodes: %w", err)
	}
	nodes := schedulableSandboxNodes(nodeList.Items)

	desired := make([]desiredPool, 0, len(pools.Items))
	invalid := make(map[string]string)
	for i := range pools.Items {
		p := &pools.Items[i]
		tmplName := strings.TrimSpace(p.Spec.TemplateRef.Name)
		if tmplName == "" {
			invalid[p.Name] = "spec.templateRef.name is required"
			continue
		}
		var tmpl cocoonsandboxv1.CocoonSandboxTemplate
		if err := r.Client.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: tmplName}, &tmpl); err != nil {
			invalid[p.Name] = fmt.Sprintf("get CocoonSandboxTemplate %s/%s: %v", p.Namespace, tmplName, err)
			continue
		}
		targets := distributeWarmTargets(p.Spec.Replicas, nodes)
		desired = append(desired, desiredPool{
			obj:      p.DeepCopy(),
			template: tmpl.Spec.Template,
			net:      effectiveNet(nil, &tmpl),
			size:     effectiveSize(nil, &tmpl),
			targets:  targets,
		})
	}

	desiredByNode := make(map[string][]sandboxd.PoolSpec, len(nodes))
	for _, node := range nodes {
		for _, d := range desired {
			desiredByNode[node.Name] = append(desiredByNode[node.Name], sandboxd.PoolSpec{
				Template: d.template,
				Net:      string(d.net),
				Size:     string(d.size),
				Warm:     d.targets[node.Name],
			})
		}
	}

	nodeInfos := make(map[string]*sandboxd.NodeInfo, len(nodes))
	nodeErrors := make(map[string]string)
	nodeStatus := CocoonSandboxNodeController{Client: r.Client}
	for i := range nodes {
		node := &nodes[i]
		addr := firstNonEmpty(node.Status.Address, node.Spec.Address)
		if addr == "" {
			nodeErrors[node.Name] = "sandboxd address is required"
			continue
		}
		token, err := secretValue(ctx, r.Client, node.Namespace, node.Spec.APITokenSecretRef)
		if err != nil {
			msg := fmt.Sprintf("read CocoonSandboxNode API token secret: %v", err)
			nodeErrors[node.Name] = msg
			if statusErr := ignoreStatusConflict(nodeStatus.updateNodeStatus(ctx, node, nil, metav1.ConditionFalse, "APITokenSecretError", msg)); statusErr != nil {
				return statusErr
			}
			continue
		}
		info, err := r.Sandbox.SetPools(ctx, addr, token, desiredByNode[node.Name])
		if err != nil {
			nodeErrors[node.Name] = err.Error()
			if statusErr := ignoreStatusConflict(nodeStatus.updateNodeStatus(ctx, node, nil, metav1.ConditionFalse, "PoolSyncFailed", err.Error())); statusErr != nil {
				return statusErr
			}
			continue
		}
		nodeInfos[node.Name] = info
		if err := ignoreStatusConflict(nodeStatus.updateNodeStatus(ctx, node, info, metav1.ConditionTrue, "SandboxdReady", "sandboxd pools are synchronized")); err != nil {
			return err
		}
	}

	for i := range pools.Items {
		p := &pools.Items[i]
		if msg := invalid[p.Name]; msg != "" {
			if err := ignoreStatusConflict(r.updatePoolStatus(ctx, p, "", "", "", nil, nil, metav1.ConditionFalse, "TemplateInvalid", msg)); err != nil {
				return err
			}
		}
	}
	for _, d := range desired {
		status, reason, message := poolReadyCondition(d, nodes, nodeErrors, nodeInfos)
		if err := ignoreStatusConflict(r.updatePoolStatus(ctx, d.obj, d.template, d.net, d.size, d.targets, nodeInfos, status, reason, message)); err != nil {
			return err
		}
	}
	return nil
}

func (r *CocoonSandboxPoolController) updatePoolStatus(ctx context.Context, poolObj *cocoonsandboxv1.CocoonSandboxPool, template string, net cocoonsandboxv1.CocoonSandboxNet, size cocoonsandboxv1.CocoonSandboxSize, targets map[string]int, infos map[string]*sandboxd.NodeInfo, status metav1.ConditionStatus, reason, message string) error {
	next := poolObj.DeepCopy()
	next.Status.Replicas = poolObj.Spec.Replicas
	next.Status.ReadyReplicas = 0
	next.Status.WarmByNode = nil
	if targets != nil {
		nodeNames := make([]string, 0, len(targets))
		for nodeName := range targets {
			nodeNames = append(nodeNames, nodeName)
		}
		sort.Strings(nodeNames)
		for _, nodeName := range nodeNames {
			warm, target := observedPoolOnNode(template, net, size, nodeName, targets[nodeName], infos)
			next.Status.ReadyReplicas += int32(warm)
			next.Status.WarmByNode = append(next.Status.WarmByNode, cocoonsandboxv1.CocoonSandboxPoolNodeStatus{
				NodeName: nodeName,
				Warm:     int32(warm),
				Target:   int32(target),
			})
		}
	}
	meta.SetStatusCondition(&next.Status.Conditions, metav1.Condition{
		Type:               cocoonsandboxv1.ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: poolObj.Generation,
	})
	if equality.Semantic.DeepEqual(poolObj.Status, next.Status) {
		return nil
	}
	if err := r.Client.Status().Update(ctx, next); err != nil {
		return fmt.Errorf("update CocoonSandboxPool status: %w", err)
	}
	*poolObj = *next
	return nil
}

func schedulableSandboxNodes(items []cocoonsandboxv1.CocoonSandboxNode) []cocoonsandboxv1.CocoonSandboxNode {
	nodes := make([]cocoonsandboxv1.CocoonSandboxNode, 0, len(items))
	for i := range items {
		if !items[i].DeletionTimestamp.IsZero() {
			continue
		}
		if firstNonEmpty(items[i].Status.Address, items[i].Spec.Address) == "" {
			continue
		}
		nodes = append(nodes, items[i])
	}
	sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return nodes
}

func distributeWarmTargets(replicas int32, nodes []cocoonsandboxv1.CocoonSandboxNode) map[string]int {
	targets := make(map[string]int, len(nodes))
	if len(nodes) == 0 {
		return targets
	}
	total := max(int(replicas), 0)
	base := total / len(nodes)
	remainder := total % len(nodes)
	for i := range nodes {
		target := base
		if i < remainder {
			target++
		}
		targets[nodes[i].Name] = target
	}
	return targets
}

func poolReadyCondition(d desiredPool, nodes []cocoonsandboxv1.CocoonSandboxNode, nodeErrors map[string]string, infos map[string]*sandboxd.NodeInfo) (metav1.ConditionStatus, string, string) {
	if len(nodes) == 0 {
		return metav1.ConditionFalse, "NoNodes", "no CocoonSandboxNode has a sandboxd address"
	}
	if len(nodeErrors) > 0 {
		node, msg := firstNodeError(nodeErrors)
		return metav1.ConditionFalse, "SyncFailed", fmt.Sprintf("sync to CocoonSandboxNode %s failed: %s", node, msg)
	}
	ready := 0
	for _, node := range nodes {
		warm, _ := observedPoolOnNode(d.template, d.net, d.size, node.Name, d.targets[node.Name], infos)
		ready += warm
	}
	if int32(ready) >= d.obj.Spec.Replicas {
		return metav1.ConditionTrue, "PoolReady", fmt.Sprintf("warm pool ready (%d/%d)", ready, d.obj.Spec.Replicas)
	}
	return metav1.ConditionFalse, "Refilling", fmt.Sprintf("warm pool refilling (%d/%d)", ready, d.obj.Spec.Replicas)
}

func observedPoolOnNode(template string, net cocoonsandboxv1.CocoonSandboxNet, size cocoonsandboxv1.CocoonSandboxSize, nodeName string, expectedTarget int, infos map[string]*sandboxd.NodeInfo) (int, int) {
	info := infos[nodeName]
	if info == nil {
		return 0, expectedTarget
	}
	for _, p := range info.Pools {
		if p.Key.Template == template && p.Key.Net == string(net) && p.Key.Size == string(size) {
			return p.Warm, p.Target
		}
	}
	return 0, expectedTarget
}

func firstNodeError(nodeErrors map[string]string) (string, string) {
	names := make([]string, 0, len(nodeErrors))
	for name := range nodeErrors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names[0], nodeErrors[names[0]]
}

func ignoreStatusConflict(err error) error {
	if err == nil || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

func secretValue(ctx context.Context, cl client.Client, namespace string, ref *corev1.SecretKeySelector) (string, error) {
	if ref == nil || ref.Name == "" {
		return "", nil
	}
	key := ref.Key
	if key == "" {
		key = cocoonSandboxTokenKey
	}
	var secret corev1.Secret
	if err := cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &secret); err != nil {
		return "", err
	}
	return string(secret.Data[key]), nil
}

func labelsMatch(labels, selector map[string]string) bool {
	if len(selector) == 0 {
		return true
	}
	for key, want := range selector {
		if labels[key] != want {
			return false
		}
	}
	return true
}

func effectiveNet(sb *cocoonsandboxv1.CocoonSandbox, tmpl *cocoonsandboxv1.CocoonSandboxTemplate) cocoonsandboxv1.CocoonSandboxNet {
	if sb != nil && sb.Spec.Net != "" {
		return sb.Spec.Net
	}
	if tmpl != nil && tmpl.Spec.Net != "" {
		return tmpl.Spec.Net
	}
	return cocoonsandboxv1.NetNone
}

func effectiveSize(sb *cocoonsandboxv1.CocoonSandbox, tmpl *cocoonsandboxv1.CocoonSandboxTemplate) cocoonsandboxv1.CocoonSandboxSize {
	if sb != nil && sb.Spec.Size != "" {
		return sb.Spec.Size
	}
	if tmpl != nil && tmpl.Spec.Size != "" {
		return tmpl.Spec.Size
	}
	return cocoonsandboxv1.SizeSmall
}

func effectiveTTLSeconds(sb *cocoonsandboxv1.CocoonSandbox) int32 {
	if sb != nil && sb.Spec.Lifecycle != nil && sb.Spec.Lifecycle.TTLSeconds > 0 {
		return sb.Spec.Lifecycle.TTLSeconds
	}
	return 0
}

func inferClaimSource(node *cocoonsandboxv1.CocoonSandboxNode, template string, net cocoonsandboxv1.CocoonSandboxNet, size cocoonsandboxv1.CocoonSandboxSize) cocoonsandboxv1.CocoonSandboxSource {
	for _, p := range node.Status.Pools {
		if p.Template == template && p.Net == net && p.Size == size {
			if p.Warm > 0 {
				return cocoonsandboxv1.SourceWarmPool
			}
			if p.Golden {
				return cocoonsandboxv1.SourceGolden
			}
		}
	}
	return cocoonsandboxv1.SourceCold
}

func nodeScore(node *cocoonsandboxv1.CocoonSandboxNode, template string, net cocoonsandboxv1.CocoonSandboxNet, size cocoonsandboxv1.CocoonSandboxSize) int {
	score := 0
	if meta.IsStatusConditionTrue(node.Status.Conditions, cocoonsandboxv1.ConditionReady) {
		score += 1000
	}
	for _, p := range node.Status.Pools {
		if p.Template == template && p.Net == net && p.Size == size {
			score += int(p.Warm) * 10
			if p.Golden {
				score += 5
			}
		}
	}
	return score
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
