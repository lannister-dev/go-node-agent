package controlapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

var _ ports.ControlAPI = (*Client)(nil)

func newTestServer(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := New(Options{BaseURL: srv.URL, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, srv
}

func validReq() ports.InitialRequest {
	return ports.InitialRequest{
		BootstrapToken:  "boot-token-xyz",
		NodeKey:         "node-key-abc",
		AgentInstanceID: "01234567-89ab-cdef-0123-456789abcdef",
		NodeRole:        "entry",
	}
}

func TestInitial_Success(t *testing.T) {
	var gotHeaders http.Header
	c, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agent/initial" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("unexpected method: %s", r.Method)
		}
		gotHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(initialResponseDTO{
			NodeID:             "node-1",
			NodeAuthToken:      "tok-1",
			AgentInstanceID:    "01234567-89ab-cdef-0123-456789abcdef",
			FullResyncRequired: true,
		})
	})

	resp, err := c.Initial(t.Context(), validReq())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.Identity.NodeID != "node-1" || resp.Identity.AuthToken != "tok-1" {
		t.Errorf("identity mismatch: %+v", resp.Identity)
	}
	if !resp.FullResyncRequired {
		t.Error("expected full resync required")
	}
	if resp.Identity.BootstrappedAt.IsZero() {
		t.Error("BootstrappedAt should be set")
	}

	if got := gotHeaders.Get("Authorization"); got != "Bearer boot-token-xyz" {
		t.Errorf("Authorization header: %q", got)
	}
	if got := gotHeaders.Get(headerNodeKey); got != "node-key-abc" {
		t.Errorf("X-Node-Key header: %q", got)
	}
	if got := gotHeaders.Get(headerAgentID); got != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Errorf("X-Agent-Instance-ID header: %q", got)
	}
	if got := gotHeaders.Get(headerNodeRole); got != "entry" {
		t.Errorf("X-Node-Role header: %q", got)
	}
}

func TestInitial_OmitsNodeRoleWhenEmpty(t *testing.T) {
	var gotHeaders http.Header
	c, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(initialResponseDTO{
			NodeID: "n1", NodeAuthToken: "t1", AgentInstanceID: "a1",
		})
	})
	req := validReq()
	req.NodeRole = ""
	if _, err := c.Initial(t.Context(), req); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, ok := gotHeaders[headerNodeRole]; ok {
		t.Error("X-Node-Role should not be sent when empty")
	}
}

func TestInitial_5xxRetryable(t *testing.T) {
	c, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream down"))
	})
	_, err := c.Initial(t.Context(), validReq())
	var rerr *RetryableError
	if !errors.As(err, &rerr) {
		t.Fatalf("expected RetryableError, got %T: %v", err, err)
	}
	if rerr.Status != http.StatusBadGateway {
		t.Errorf("status: %d", rerr.Status)
	}
	if rerr.Body != "upstream down" {
		t.Errorf("body: %q", rerr.Body)
	}
}

func TestInitial_4xxNonRetryable(t *testing.T) {
	c, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":"X-Node-Key required"}`))
	})
	_, err := c.Initial(t.Context(), validReq())
	var nrerr *NonRetryableError
	if !errors.As(err, &nrerr) {
		t.Fatalf("expected NonRetryableError, got %T: %v", err, err)
	}
	if nrerr.Status != http.StatusBadRequest {
		t.Errorf("status: %d", nrerr.Status)
	}
}

func TestInitial_409Conflict(t *testing.T) {
	c, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("node already bootstrapped"))
	})
	_, err := c.Initial(t.Context(), validReq())
	var nrerr *NonRetryableError
	if !errors.As(err, &nrerr) {
		t.Fatalf("expected NonRetryableError, got %T: %v", err, err)
	}
	if !nrerr.IsConflict() {
		t.Error("expected IsConflict()=true")
	}
}

func TestInitial_BadJSONNonRetryable(t *testing.T) {
	c, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	})
	_, err := c.Initial(t.Context(), validReq())
	var nrerr *NonRetryableError
	if !errors.As(err, &nrerr) {
		t.Fatalf("expected NonRetryableError, got %T: %v", err, err)
	}
}

func TestInitial_MissingFieldsNonRetryable(t *testing.T) {
	c, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node_id":"n1"}`))
	})
	_, err := c.Initial(t.Context(), validReq())
	var nrerr *NonRetryableError
	if !errors.As(err, &nrerr) {
		t.Fatalf("expected NonRetryableError, got %T: %v", err, err)
	}
}

func TestInitial_ContextCancel(t *testing.T) {
	c, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{}`))
	})
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := c.Initial(ctx, validReq())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestInitial_RequiresArgs(t *testing.T) {
	c, _ := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("server should not be hit")
		w.WriteHeader(http.StatusOK)
	})

	cases := []ports.InitialRequest{
		{NodeKey: "k", AgentInstanceID: "a"},
		{BootstrapToken: "t", AgentInstanceID: "a"},
		{BootstrapToken: "t", NodeKey: "k"},
	}
	for _, req := range cases {
		_, err := c.Initial(t.Context(), req)
		var nrerr *NonRetryableError
		if !errors.As(err, &nrerr) {
			t.Errorf("expected NonRetryableError for %+v, got %v", req, err)
		}
	}
}

func TestNew_RejectsInvalidBaseURL(t *testing.T) {
	cases := []string{"", "api.example.com", "ftp://x"}
	for _, base := range cases {
		_, err := New(Options{BaseURL: base})
		if err == nil {
			t.Errorf("expected error for BaseURL=%q", base)
		}
	}
}
