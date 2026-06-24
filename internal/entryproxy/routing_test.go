package entryproxy

import (
	"context"
	"strconv"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

func newRoutingProxy() *Proxy {
	return &Proxy{
		backends: map[string]ports.EntryBackend{
			"be1": {ID: "be1"}, "be2": {ID: "be2"}, "be3": {ID: "be3"},
		},
		route: map[string][]string{},
	}
}

func TestPickConnBackend_StickyPerDestination(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "user-a", []string{"be1", "be2", "be3"})
	for _, host := range []string{"a.com", "b.com", "cdn.net", "x.io"} {
		first, ok := p.pickConnBackend("user-a", host)
		if !ok {
			t.Fatalf("no backend for %s", host)
		}
		for i := 0; i < 50; i++ {
			again, _ := p.pickConnBackend("user-a", host)
			if again.ID != first.ID {
				t.Fatalf("not sticky for %s: %s vs %s", host, first.ID, again.ID)
			}
		}
	}
}

func TestPickConnBackend_SpreadsAcrossBackends(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "user-a", []string{"be1", "be2", "be3"})
	seen := map[string]bool{}
	for i := 0; i < 300; i++ {
		be, ok := p.pickConnBackend("user-a", "host-"+strconv.Itoa(i)+".com")
		if !ok {
			t.Fatal("no backend")
		}
		seen[be.ID] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected spread across backends, got %v", seen)
	}
}

func TestPickConnBackend_OnlyEligibleBackends(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "user-a", []string{"be1"})
	for i := 0; i < 100; i++ {
		be, ok := p.pickConnBackend("user-a", "h"+strconv.Itoa(i))
		if !ok || be.ID != "be1" {
			t.Fatalf("must stay on eligible be1, got %q %v", be.ID, ok)
		}
	}
}

func TestPickConnBackend_SkipsUnhealthyEligible(t *testing.T) {
	p := newRoutingProxy()
	_ = p.SetUserBackends(context.Background(), "user-a", []string{"be1", "gone"})
	for i := 0; i < 100; i++ {
		be, ok := p.pickConnBackend("user-a", "h"+strconv.Itoa(i))
		if !ok || be.ID != "be1" {
			t.Fatalf("unhealthy backend must be skipped, got %q %v", be.ID, ok)
		}
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
