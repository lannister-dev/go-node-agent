package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

type staticStats struct {
	r, a, f uint32
}

func (s staticStats) Snapshot() (uint32, uint32, uint32) { return s.r, s.a, s.f }

type alwaysHealthy struct{ name string }

func (a alwaysHealthy) Name() string                { return a.name }
func (a alwaysHealthy) Check(context.Context) error { return nil }

type alwaysSick struct {
	name string
	err  error
}

func (a alwaysSick) Name() string                { return a.name }
func (a alwaysSick) Check(context.Context) error { return a.err }

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func startServer(t *testing.T, opts Options) string {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = silent()
	}
	if opts.Addr == "" {
		opts.Addr = "127.0.0.1:0"
	}
	s, err := New(opts)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() { _ = s.Run(ctx) }()

	addr := s.Addr()
	for range 50 {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not become reachable on %s", addr)
	return ""
}

func TestServer_Healthz(t *testing.T) {
	addr := startServer(t, Options{})
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestServer_Readyz_AllHealthy(t *testing.T) {
	addr := startServer(t, Options{Checks: []HealthCheck{alwaysHealthy{name: "nats"}, alwaysHealthy{name: "store"}}})
	resp, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestServer_Readyz_OneFailing(t *testing.T) {
	addr := startServer(t, Options{Checks: []HealthCheck{
		alwaysHealthy{name: "store"},
		alwaysSick{name: "nats", err: errors.New("not connected")},
	}})
	resp, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: %d, want 503", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "not connected") {
		t.Errorf("body should include error: %s", body)
	}
}

func TestServer_Metrics(t *testing.T) {
	addr := startServer(t, Options{Stats: staticStats{r: 100, a: 90, f: 10}})
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, want := range []string{
		"agent_placement_received_total 100",
		"agent_placement_applied_total 90",
		"agent_placement_failed_total 10",
		"go_goroutines",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestServer_PprofGated(t *testing.T) {
	addrNo := startServer(t, Options{EnablePprof: false})
	resp, err := http.Get("http://" + addrNo + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 when pprof disabled, got %d", resp.StatusCode)
	}

	addrYes := startServer(t, Options{EnablePprof: true})
	resp, err = http.Get("http://" + addrYes + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 with pprof enabled, got %d", resp.StatusCode)
	}
}

func TestNew_RejectsEmptyAddr(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("expected error for empty addr")
	}
}
