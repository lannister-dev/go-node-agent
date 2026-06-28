package entryproxy

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

func newRoutingProxy() *Proxy {
	return &Proxy{
		backends: map[string]ports.EntryBackend{
			"be1": {ID: "be1"}, "be2": {ID: "be2"}, "be3": {ID: "be3"},
		},
		route:  map[string][]string{},
		health: map[string]*backendHealth{},
	}
}

func TestConnBackends_StickyPerDestination(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "user-a", []string{"be1", "be2", "be3"})
	for _, host := range []string{"a.com", "b.com", "cdn.net", "x.io"} {
		first := p.connBackends("user-a", host)
		if len(first) == 0 {
			t.Fatalf("no backend for %s", host)
		}
		for range 50 {
			again := p.connBackends("user-a", host)
			if again[0].ID != first[0].ID {
				t.Fatalf("not sticky for %s: %s vs %s", host, first[0].ID, again[0].ID)
			}
		}
	}
}

func TestConnBackends_SpreadsAcrossBackends(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "user-a", []string{"be1", "be2", "be3"})
	seen := map[string]bool{}
	for i := range 300 {
		cands := p.connBackends("user-a", "host-"+strconv.Itoa(i)+".com")
		if len(cands) == 0 {
			t.Fatal("no backend")
		}
		seen[cands[0].ID] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected spread across backends, got %v", seen)
	}
}

func TestConnBackends_OnlyEligibleBackends(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "user-b", []string{"be1"})
	for i := range 100 {
		cands := p.connBackends("user-b", "h"+strconv.Itoa(i))
		if len(cands) != 1 || cands[0].ID != "be1" {
			t.Fatalf("must stay on eligible be1, got %v", cands)
		}
	}
}

func TestConnBackends_SkipsUnknownEligible(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "user-c", []string{"be1", "gone"})
	for i := range 100 {
		cands := p.connBackends("user-c", "h"+strconv.Itoa(i))
		if len(cands) != 1 || cands[0].ID != "be1" {
			t.Fatalf("unknown backend must be skipped, got %v", cands)
		}
	}
}

func TestConnBackends_UnhealthyGoesLast(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "u", []string{"be1", "be2", "be3"})
	for _, id := range []string{"be1", "be2"} {
		p.markBackendFailure(id)
		p.markBackendFailure(id)
	}
	cands := p.connBackends("u", "site.com")
	if len(cands) != 3 || cands[0].ID != "be3" {
		t.Fatalf("only-healthy be3 must be first, got %v", cands)
	}
	p.markBackendSuccess("be1")
	for _, c := range p.connBackends("u", "site.com") {
		_ = c
	}
	if p.health["be1"].fails != 0 {
		t.Fatalf("success must reset fails, got %d", p.health["be1"].fails)
	}
}

func TestDialAnyBackend_FailsOverToLive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			_ = c.Close()
		}
	}()
	port := uint16(ln.Addr().(*net.TCPAddr).Port)

	p := &Proxy{
		health:        map[string]*backendHealth{},
		dialTimeout:   2 * time.Second,
		dialAttemptTO: 500 * time.Millisecond,
		log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	dead := ports.EntryBackend{ID: "dead", Address: "127.0.0.1", Port: 1}
	live := ports.EntryBackend{ID: "live", Address: "127.0.0.1", Port: port}

	raw, be, ok := p.dialAnyBackend(context.Background(), []ports.EntryBackend{dead, live})
	if !ok || be.ID != "live" {
		t.Fatalf("expected failover to live, got id=%q ok=%v", be.ID, ok)
	}
	_ = raw.Close()
	if p.health["dead"] == nil || p.health["dead"].fails == 0 {
		t.Fatal("dead backend must be marked failed")
	}
}

func TestSelectBackendAccumulatesSet(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SelectBackend(context.Background(), "user-a", "be1")
	_ = p.SelectBackend(context.Background(), "user-a", "be2")
	_ = p.SelectBackend(context.Background(), "user-a", "be1")
	if got := p.route["user-a"]; len(got) != 2 {
		t.Fatalf("expected accumulate to 2 unique, got %v", got)
	}
}
