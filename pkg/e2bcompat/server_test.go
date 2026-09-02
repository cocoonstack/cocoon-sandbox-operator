package e2bcompat

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const testKey = "e2b_testkey"

// TestCreateClaimsFromTemplatePool is the core contract: an e2b create is the
// same node-local claim, keyed on the template the caller asked for, and the
// response carries the fields the SDK requires.
func TestCreateClaimsFromTemplatePool(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_abc123", Node: "node-a", Token: "tok-1"}}
	h := newTestServer(t, store, func(o *Options) { o.Domain = "sandbox.example.com" })

	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"registry/rt:24.04","timeout":300}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (the e2b create contract): %s", w.Code, w.Body.String())
	}
	var got Sandbox
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got.SandboxID != "sb-abc123" {
		t.Errorf("sandboxID = %q, want the DNS-safe rendering of the claim id", got.SandboxID)
	}
	if got.TemplateID != "registry/rt:24.04" {
		t.Errorf("templateID = %q, want it echoed back", got.TemplateID)
	}
	if got.EnvdAccessToken != "tok-1" {
		t.Errorf("envdAccessToken = %q, want the claim token", got.EnvdAccessToken)
	}
	if got.Domain != "sandbox.example.com" {
		t.Errorf("domain = %q, want the configured domain", got.Domain)
	}

	if got.EnvdVersion == "" {
		t.Error("envdVersion is empty; the e2b SDK kills the sandbox and throws when it cannot parse this")
	}
	if store.claimPool.Template != "registry/rt:24.04" {
		t.Errorf("claimed pool template = %q, want the requested templateID", store.claimPool.Template)
	}
	if store.claimNS != "sandboxes" {
		t.Errorf("claim namespace = %q, want the configured compat namespace", store.claimNS)
	}
	if !strings.HasPrefix(store.claimName, namePrefix) {
		t.Errorf("claim name = %q, want the %q prefix so compat claims are recognizable", store.claimName, namePrefix)
	}
	if store.claimTTL != 300 {
		t.Errorf("claim ttl = %d, want the requested timeout passed through to the node", store.claimTTL)
	}
}

// TestCreateNetworkLane checks e2b's internet flag selects the pool's net axis.
func TestCreateNetworkLane(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"default is the hardened lane", `{"templateID":"t"}`, scale.NetDefault},
		{"internet access picks egress", `{"templateID":"t","allow_internet_access":true}`, "egress"},
		{"explicit false stays hardened", `{"templateID":"t","allow_internet_access":false}`, scale.NetDefault},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}}
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, "/sandboxes", tc.body, testKey); w.Code != http.StatusCreated {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			if store.claimPool.Net != tc.want {
				t.Errorf("pool net = %q, want %q", store.claimPool.Net, tc.want)
			}
		})
	}
}

// TestCreateNoWarmCapacityIsRetryable keeps the retryable signal retryable: a
// drained pool refills asynchronously, so it must not surface as a 500.
func TestCreateNoWarmCapacityIsRetryable(t *testing.T) {
	store := &fakeStore{claimErr: scale.ErrNoWarmCapacity}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"t"}`, testKey)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 so the client retries as warm capacity refills", w.Code)
	}
}

func TestCreateRejectsMissingTemplate(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	if w := do(t, h, http.MethodPost, "/sandboxes", `{}`, testKey); w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a body with no templateID", w.Code)
	}
}

func TestCreateRejectsNegativeTimeout(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	body := `{"templateID":"img","timeout":-1}`
	if w := do(t, h, http.MethodPost, "/sandboxes", body, testKey); w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a negative timeout", w.Code)
	}
}

// TestAuthRequiresAPIKey proves the claim endpoint is closed without the key
// the SDK presents.
func TestAuthRequiresAPIKey(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}}
	h := newTestServer(t, store)

	for _, tc := range []struct{ name, key string }{
		{"no key", ""},
		{"wrong key", "e2b_wrong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"t"}`, tc.key)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", w.Code)
			}
			if store.claimName != "" {
				t.Fatal("an unauthenticated request reached the claim path")
			}
		})
	}
}

// TestNewServerRefusesOpenByDefault: forgetting to configure a key must fail
// loud at startup, not silently serve an open claim endpoint.
func TestNewServerRefusesOpenByDefault(t *testing.T) {
	if _, err := NewServer(&fakeStore{}, Options{Namespace: "x"}); err == nil {
		t.Fatal("NewServer accepted no API key without explicit anonymous access")
	}
	if _, err := NewServer(&fakeStore{}, Options{Namespace: "x", AllowAnonymous: true}); err != nil {
		t.Fatalf("NewServer rejected explicit anonymous access: %v", err)
	}
}

func TestHealthNeedsNoKey(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	if w := do(t, h, http.MethodGet, "/health", "", ""); w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 without a key so probes work", w.Code)
	}
}

// TestDeleteReleasesTheClaimedID checks release targets the node-local claim id
// rather than the Kubernetes name — releasing by name would free the wrong VM.
func TestDeleteReleasesTheClaimedID(t *testing.T) {
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{
		liveSandbox("e2b-aaa", "sb_one", "node-a", "img:1"),
		liveSandbox("e2b-bbb", "sb_two", "node-b", "img:1"),
	}}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodDelete, "/sandboxes/sb_two", "", testKey); w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if store.releasedID != "sb_two" || store.releasedNode != "node-b" {
		t.Errorf("released (node=%q id=%q), want (node-b, sb_two)", store.releasedNode, store.releasedID)
	}
}

func TestDeleteUnknownSandboxIs404(t *testing.T) {
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{liveSandbox("a", "sb_one", "node-a", "i")}}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodDelete, "/sandboxes/sb_missing", "", testKey); w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if store.releasedID != "" {
		t.Errorf("released %q for an unknown sandbox", store.releasedID)
	}
}

// TestGetReportsDetail checks the detail shape the SDK decodes on getInfo.
func TestGetReportsDetail(t *testing.T) {
	got := getDetail(t, liveSandbox("e2b-aaa", "sb_one", "node-a", "registry/rt:24.04"))
	if got.SandboxID != "sb-one" || got.TemplateID != "registry/rt:24.04" || got.ClientID != "node-a" {
		t.Errorf("detail = %+v, want the live sandbox's id/template/node", got)
	}
	if got.State != StateRunning {
		t.Errorf("state = %q, want %q", got.State, StateRunning)
	}

	if got.StartedAt == "" || got.EndAt == "" {
		t.Errorf("startedAt/endAt = %q/%q, want RFC3339 timestamps", got.StartedAt, got.EndAt)
	}
}

func TestGetReportsTheGrantedDeadlineAsEndAt(t *testing.T) {
	sb := liveSandbox("e2b-aaa", "sb_one", "node-a", "registry/rt:24.04")
	sb.Annotations[scale.DeadlineAnnotation] = "2030-01-02T03:04:05Z"
	if got := getDetail(t, sb); got.EndAt != "2030-01-02T03:04:05Z" {
		t.Errorf("endAt = %q, want the granted deadline", got.EndAt)
	}
}

func TestGetReportsTheDefaultEndAtWithoutADeadline(t *testing.T) {
	sb := liveSandbox("e2b-aaa", "sb_one", "node-a", "registry/rt:24.04")
	sb.CreationTimestamp = metav1.NewTime(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	if got := getDetail(t, sb); got.EndAt != "2030-01-02T03:04:20Z" {
		t.Errorf("endAt = %q, want startedAt + %ds", got.EndAt, DefaultTimeoutSeconds)
	}
}

// TestListReturnsArray: the SDK decodes a bare JSON array, not an object.
func TestListReturnsArray(t *testing.T) {
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{
		liveSandbox("a", "sb_one", "node-a", "i:1"),
		liveSandbox("b", "sb_two", "node-b", "i:2"),
	}}
	h := newTestServer(t, store)

	for _, path := range []string{"/sandboxes", "/v2/sandboxes"} {
		t.Run(path, func(t *testing.T) {
			w := do(t, h, http.MethodGet, path, "", testKey)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}
			var got []SandboxDetail
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode as array: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("len = %d, want 2", len(got))
			}
		})
	}
}

// TestListEmptyIsArrayNotNull: an empty pool must encode as [], since `null`
// breaks callers that iterate the result directly.
func TestListEmptyIsArrayNotNull(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	w := do(t, h, http.MethodGet, "/sandboxes", "", testKey)
	if body := strings.TrimSpace(w.Body.String()); body != "[]" {
		t.Fatalf("body = %s, want []", body)
	}
}

// TestTimeoutAndRefreshAckLiveSandbox: both verbs confirm the sandbox exists
// before acknowledging, so a caller never gets an OK for a dead sandbox.
func TestTimeoutAndRefreshAckLiveSandbox(t *testing.T) {
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{liveSandbox("a", "sb_one", "node-a", "i")}}
	h := newTestServer(t, store)

	for _, tc := range []struct{ name, path, body string }{
		{"timeout", "/sandboxes/sb_one/timeout", `{"timeout":60}`},
		{"refresh", "/sandboxes/sb_one/refreshes", `{"duration":30}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := do(t, h, http.MethodPost, tc.path, tc.body, testKey); w.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
			}
		})
	}
	t.Run("unknown sandbox is 404", func(t *testing.T) {
		w := do(t, h, http.MethodPost, "/sandboxes/sb_gone/timeout", `{"timeout":60}`, testKey)
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})
}

// TestErrorBodyCarriesMessage: the SDK surfaces `message` from the envelope.
func TestErrorBodyCarriesMessage(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	w := do(t, h, http.MethodPost, "/sandboxes", `{}`, testKey)
	var got APIError
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if got.Message == "" || got.Code != http.StatusBadRequest {
		t.Errorf("error = %+v, want code 400 and a message", got)
	}
}

// TestLookupUsesClaimIDResolver pins the id-keyed fast path over the real
// scatter-gather store: both id spellings resolve without a fleet List, other
// namespaces stay invisible, and a miss is a plain not-found.
func TestLookupUsesClaimIDResolver(t *testing.T) {
	src := scale.NewStaticInventorySource()
	src.Put(&scale.NodeInventory{
		Name:    "node-a",
		Node:    "node-a",
		Address: "10.0.0.1:7777",
		Entries: []scale.InventoryEntry{
			{Name: "sandboxes/sb-1", ID: "sb_0123abcd", Phase: "Running", Address: "10.0.0.1:7777"},
			{Name: "elsewhere/sb-2", ID: "sb_ffff0000", Phase: "Running", Address: "10.0.0.1:7777"},
		},
	})
	s, err := NewServer(scale.NewScatterGatherStore(src), Options{Namespace: "sandboxes", AllowAnonymous: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/x", nil)

	for _, id := range []string{"sb_0123abcd", publicID("sb_0123abcd")} {
		sb, err := s.lookup(req, id)
		if err != nil {
			t.Fatalf("lookup(%q): %v", id, err)
		}
		if sb.Name != "sb-1" || sb.Namespace != "sandboxes" {
			t.Fatalf("lookup(%q) = %s/%s, want sandboxes/sb-1", id, sb.Namespace, sb.Name)
		}
	}
	if _, err := s.lookup(req, "sb_ffff0000"); err != errSandboxNotFound {
		t.Fatalf("cross-namespace id resolved, want errSandboxNotFound, got %v", err)
	}
	if _, err := s.lookup(req, "sb_missing"); err != errSandboxNotFound {
		t.Fatalf("missing id: want errSandboxNotFound, got %v", err)
	}
}

// TestDetailStateFollowsThePhaseLabel pins the e2b state mapping: a Hibernated
// phase reports paused, anything else running.
func TestDetailStateFollowsThePhaseLabel(t *testing.T) {
	s, err := NewServer(&fakeStore{}, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sb := liveSandbox("sb-1", "sb_0123abcd", "node-a", "img")
	if got := s.detailFor(&sb).State; got != StateRunning {
		t.Fatalf("running sandbox state = %q, want %q", got, StateRunning)
	}
	sb.Labels = map[string]string{scale.PhaseLabel: phaseHibernated}
	if got := s.detailFor(&sb).State; got != StatePaused {
		t.Fatalf("hibernated sandbox state = %q, want %q", got, StatePaused)
	}
}

// fakeStore records what the compat layer asked of the store and replays canned
// answers, so the tests assert the translation rather than the node behavior.
type fakeStore struct {
	claimPool scale.PoolKey
	claimNS   string
	claimName string
	claimTTL  int
	claimErr  error
	assign    scale.Assignment

	items []sandboxv1beta1.Sandbox

	releasedNode string
	releasedID   string
	releaseErr   error
	listErr      error
}

func (f *fakeStore) List(context.Context, scale.ListOptions) (*sandboxv1beta1.SandboxList, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return &sandboxv1beta1.SandboxList{Items: f.items}, nil
}

func (f *fakeStore) Get(context.Context, string, string) (*sandboxv1beta1.Sandbox, error) {
	return nil, nil
}

func (f *fakeStore) GetByClaimID(_ context.Context, _, _ string, match func(string) bool) (*sandboxv1beta1.Sandbox, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	for i := range f.items {
		if match(f.items[i].Annotations[scale.ClaimIDAnnotation]) {
			return &f.items[i], nil
		}
	}
	return nil, k8serrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), "")
}

func (f *fakeStore) Watch(context.Context, scale.ListOptions) (watch.Interface, error) {
	return nil, nil
}

func (f *fakeStore) Claim(_ context.Context, ns, name string, pool scale.PoolKey, ttlSeconds int) (scale.Assignment, error) {
	f.claimNS, f.claimName, f.claimPool, f.claimTTL = ns, name, pool, ttlSeconds
	if f.claimErr != nil {
		return scale.Assignment{}, f.claimErr
	}
	return f.assign, nil
}

func (f *fakeStore) Release(_ context.Context, node, id string) error {
	f.releasedNode, f.releasedID = node, id
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

func (f *fakeStore) Stats(context.Context, string, string) (scale.SandboxStats, error) {
	return scale.SandboxStats{}, nil
}

func getDetail(t *testing.T, sb sandboxv1beta1.Sandbox) SandboxDetail {
	t.Helper()
	h := newTestServer(t, &fakeStore{items: []sandboxv1beta1.Sandbox{sb}})
	w := do(t, h, http.MethodGet, "/sandboxes/sb_one", "", testKey)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var got SandboxDetail
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

func newTestServer(t *testing.T, store scale.SandboxStore, opts ...func(*Options)) http.Handler {
	t.Helper()
	o := Options{Namespace: "sandboxes", APIKeys: []string{testKey}, Log: logr.Discard()}
	for _, fn := range opts {
		fn(&o)
	}
	s, err := NewServer(store, o)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s.Handler()
}

func do(t *testing.T, h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if key != "" {
		r.Header.Set(apiKeyHeader, key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func liveSandbox(name, claimID, node, template string) sandboxv1beta1.Sandbox {
	sb := sandboxv1beta1.Sandbox{
		Name:              name,
		Namespace:         "sandboxes",
		CreationTimestamp: metav1.Now(),
		Labels:            map[string]string{scale.NodeLabel: node, scale.TemplateLabel: template},
		Annotations:       map[string]string{scale.ClaimIDAnnotation: claimID},
	}
	sb.Status.NodeName = node
	return sb
}
