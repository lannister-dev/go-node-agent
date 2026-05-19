package singbox

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

var _ ports.SingBox = (*Client)(nil)

type recordedRequest struct {
	Method string
	Path   string
	Query  string
	Body   []byte
	Auth   string
}

type fakeAPI struct {
	mu       sync.Mutex
	requests []recordedRequest
	handler  func(w http.ResponseWriter, r *http.Request)
}

func newFakeAPI(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (*Client, *fakeAPI, string) {
	t.Helper()
	f := &fakeAPI{handler: handler}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, recordedRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.RawQuery,
			Body:   body,
			Auth:   r.Header.Get("Authorization"),
		})
		f.mu.Unlock()
		if f.handler != nil {
			r.Body = io.NopCloser(nopReader{})
			f.handler(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	c, err := New(Options{
		APIURL:     srv.URL,
		ConfigPath: cfgPath,
		Timeout:    2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, f, cfgPath
}

type nopReader struct{}

func (nopReader) Read(_ []byte) (int, error) { return 0, io.EOF }

func (f *fakeAPI) snapshot() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedRequest, len(f.requests))
	copy(out, f.requests)
	return out
}

func TestHealthy_Ok(t *testing.T) {
	c, f, _ := newFakeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"version":"1.10.0"}`))
	})
	if err := c.Healthy(t.Context()); err != nil {
		t.Fatalf("healthy: %v", err)
	}
	reqs := f.snapshot()
	if len(reqs) != 1 || reqs[0].Path != "/version" || reqs[0].Method != http.MethodGet {
		t.Errorf("unexpected request: %+v", reqs)
	}
}

func TestHealthy_5xx(t *testing.T) {
	c, _, _ := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	if err := c.Healthy(t.Context()); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestWriteConfig_AtomicAndReadable(t *testing.T) {
	c, _, cfgPath := newFakeAPI(t, nil)
	cfg := ports.SingBoxConfig{Raw: []byte(`{"log":{"level":"info"}}`)}
	if err := c.WriteConfig(t.Context(), cfg); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != `{"log":{"level":"info"}}` {
		t.Errorf("contents: %s", got)
	}

	entries, _ := os.ReadDir(filepath.Dir(cfgPath))
	for _, e := range entries {
		name := e.Name()
		if name != "config.json" && len(name) > 0 && name[0] == '.' {
			t.Errorf("temp file left behind: %s", name)
		}
	}
}

func TestWriteConfig_RejectsEmpty(t *testing.T) {
	c, _, _ := newFakeAPI(t, nil)
	if err := c.WriteConfig(t.Context(), ports.SingBoxConfig{}); err == nil {
		t.Fatal("expected error for empty config")
	}
}

func TestReload_PutsPathBody(t *testing.T) {
	c, f, cfgPath := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	if err := c.Reload(t.Context()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	reqs := f.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	r := reqs[0]
	if r.Method != http.MethodPut || r.Path != "/configs" {
		t.Errorf("method/path: %s %s", r.Method, r.Path)
	}
	if r.Query != "force=true" {
		t.Errorf("query: %s", r.Query)
	}
	var body struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(r.Body, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Path != cfgPath {
		t.Errorf("body path: got %s, want %s", body.Path, cfgPath)
	}
}

func TestReload_4xxError(t *testing.T) {
	c, _, _ := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`invalid config`))
	})
	if err := c.Reload(t.Context()); err == nil {
		t.Fatal("expected error on 400")
	}
}

func TestConnections_Counts(t *testing.T) {
	c, _, _ := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"downloadTotal": 1000,
			"uploadTotal": 500,
			"connections": [
				{"id":"c1", "chains":["chain1","praha-02"]},
				{"id":"c2", "chains":["chain1","praha-02"]},
				{"id":"c3", "chains":["chain1","latvia-01"]},
				{"id":"c4", "chains":[]}
			]
		}`))
	})
	conns, err := c.Connections(t.Context())
	if err != nil {
		t.Fatalf("connections: %v", err)
	}
	if conns.Total != 4 {
		t.Errorf("total: %d", conns.Total)
	}
	if conns.PerOutbound["praha-02"] != 2 {
		t.Errorf("praha-02: %d", conns.PerOutbound["praha-02"])
	}
	if conns.PerOutbound["latvia-01"] != 1 {
		t.Errorf("latvia-01: %d", conns.PerOutbound["latvia-01"])
	}
	if conns.PerOutbound["_unknown"] != 1 {
		t.Errorf("_unknown: %d", conns.PerOutbound["_unknown"])
	}
}

func TestConnections_Non200(t *testing.T) {
	c, _, _ := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	if _, err := c.Connections(t.Context()); err == nil {
		t.Fatal("expected error")
	}
}

func TestAuth_SendsBearer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-tok" {
			http.Error(w, "no token", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	c, err := New(Options{
		APIURL:     srv.URL,
		ConfigPath: filepath.Join(dir, "config.json"),
		Token:      "secret-tok",
		Timeout:    2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Healthy(t.Context()); err != nil {
		t.Errorf("healthy with token: %v", err)
	}
}

func TestContextCancel(t *testing.T) {
	c, _, _ := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := c.Healthy(ctx); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestNew_Validates(t *testing.T) {
	cases := map[string]Options{
		"missing APIURL":     {ConfigPath: "/tmp/x"},
		"missing ConfigPath": {APIURL: "http://localhost:9090"},
		"bad scheme":         {APIURL: "localhost:9090", ConfigPath: "/tmp/x"},
	}
	for name, opts := range cases {
		if _, err := New(opts); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
