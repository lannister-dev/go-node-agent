package executor

import (
	"context"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type fakeEntryProxy struct {
	mu       sync.Mutex
	added    map[string]string
	removed  []string
	route    map[string]string
	backends []ports.EntryBackend
	conns    uint64
}

func newFakeProxy() *fakeEntryProxy {
	return &fakeEntryProxy{added: map[string]string{}, route: map[string]string{}}
}

func (f *fakeEntryProxy) AddUser(_ context.Context, clientID, flow string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added[clientID] = flow
	return nil
}

func (f *fakeEntryProxy) RemoveUser(_ context.Context, clientID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, clientID)
	delete(f.added, clientID)
	delete(f.route, clientID)
	return nil
}

func (f *fakeEntryProxy) SelectBackend(_ context.Context, clientID, backendID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.route[clientID] = backendID
	return nil
}

func (f *fakeEntryProxy) SetBackends(_ context.Context, backends []ports.EntryBackend) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.backends = backends
	return nil
}

func (f *fakeEntryProxy) BackendConnections(_ context.Context, _ string) (uint64, error) {
	return f.conns, nil
}

func (f *fakeEntryProxy) ActiveConnections(_ context.Context) ([]ports.EntryConnection, error) {
	return nil, nil
}

func newProxyActions(t *testing.T, proxy ports.EntryProxy, store PlacementStore) *EntryProxyActions {
	t.Helper()
	a, err := NewEntryProxyActions(proxy, store, backendsRegistry(), silent())
	if err != nil {
		t.Fatalf("new proxy actions: %v", err)
	}
	return a
}

func TestProxyActions_SimpleApplyActiveAddsAndRoutes(t *testing.T) {
	proxy := newFakeProxy()
	store := newMemStore()
	a := newProxyActions(t, proxy, store)

	err := a.SimpleApply(context.Background(), domain.Placement{
		ID: "p1", ClientID: "user-a", BackendNodeID: "latvia-01",
		Desired: domain.DesiredActive, Transport: domain.TransportReality, OpVersion: 1,
	})
	if err != nil {
		t.Fatalf("SimpleApply: %v", err)
	}
	if proxy.added["user-a"] != "xtls-rprx-vision" {
		t.Fatalf("flow not set from transport: %q", proxy.added["user-a"])
	}
	if proxy.route["user-a"] != "latvia-01" {
		t.Fatalf("route not set: %q", proxy.route["user-a"])
	}
	if p, ok, _ := store.GetPlacement(context.Background(), "p1"); !ok || p.Applied != domain.AppliedOk {
		t.Fatalf("placement not persisted applied: %+v", p)
	}
}

func TestProxyActions_SimpleApplyInactiveRemoves(t *testing.T) {
	proxy := newFakeProxy()
	store := newMemStore()
	a := newProxyActions(t, proxy, store)

	err := a.SimpleApply(context.Background(), domain.Placement{
		ID: "p1", ClientID: "user-a", BackendNodeID: "latvia-01",
		Desired: domain.DesiredInactive, OpVersion: 1,
	})
	if err != nil {
		t.Fatalf("SimpleApply: %v", err)
	}
	if len(proxy.removed) != 1 || proxy.removed[0] != "user-a" {
		t.Fatalf("user not removed: %v", proxy.removed)
	}
}

func TestProxyActions_SimpleApplyUnknownBackendFails(t *testing.T) {
	a := newProxyActions(t, newFakeProxy(), newMemStore())
	err := a.SimpleApply(context.Background(), domain.Placement{
		ID: "p1", ClientID: "user-a", BackendNodeID: "nope",
		Desired: domain.DesiredActive, OpVersion: 1,
	})
	if err == nil {
		t.Fatal("expected error for unknown backend")
	}
}

func TestProxyActions_SimpleApplyStaleOpVersionSkips(t *testing.T) {
	proxy := newFakeProxy()
	store := newMemStore(domain.Placement{ID: "p1", ClientID: "user-a", OpVersion: 5})
	a := newProxyActions(t, proxy, store)

	err := a.SimpleApply(context.Background(), domain.Placement{
		ID: "p1", ClientID: "user-a", BackendNodeID: "latvia-01",
		Desired: domain.DesiredActive, OpVersion: 3,
	})
	if err != nil {
		t.Fatalf("SimpleApply: %v", err)
	}
	if len(proxy.added) != 0 {
		t.Fatalf("stale op_version should not apply: %v", proxy.added)
	}
}

func TestProxyActions_SwapRouteSelectsBackend(t *testing.T) {
	proxy := newFakeProxy()
	store := newMemStore(domain.Placement{ID: "p1", ClientID: "user-a", BackendNodeID: "latvia-01", OpVersion: 1})
	a := newProxyActions(t, proxy, store)

	plan := domain.FlipPlan{
		PlacementID: "p1", OldBackend: "latvia-01", NewBackend: "praha-02", OpVersion: 2,
		Desired: domain.Placement{ID: "p1", ClientID: "user-a", BackendNodeID: "praha-02", Desired: domain.DesiredActive, OpVersion: 2},
	}
	if err := a.SwapRoute(context.Background(), plan); err != nil {
		t.Fatalf("SwapRoute: %v", err)
	}
	if proxy.route["user-a"] != "praha-02" {
		t.Fatalf("route not swapped: %q", proxy.route["user-a"])
	}
}

func TestProxyActions_RebuildFromStorePushesState(t *testing.T) {
	proxy := newFakeProxy()
	store := newMemStore(
		domain.Placement{ID: "p1", ClientID: "user-a", BackendNodeID: "latvia-01", Desired: domain.DesiredActive, Transport: domain.TransportReality},
		domain.Placement{ID: "p2", ClientID: "user-b", BackendNodeID: "praha-02", Desired: domain.DesiredInactive},
	)
	a := newProxyActions(t, proxy, store)

	if err := a.RebuildFromStore(context.Background()); err != nil {
		t.Fatalf("RebuildFromStore: %v", err)
	}
	if len(proxy.backends) != 2 {
		t.Fatalf("backends not synced: %d", len(proxy.backends))
	}
	if _, ok := proxy.added["user-a"]; !ok {
		t.Fatal("active user not added")
	}
	if _, ok := proxy.added["user-b"]; ok {
		t.Fatal("inactive user should not be added")
	}
	if proxy.route["user-a"] != "latvia-01" {
		t.Fatalf("active user route wrong: %q", proxy.route["user-a"])
	}
}

func TestProxyActions_OldBackendConnections(t *testing.T) {
	proxy := newFakeProxy()
	proxy.conns = 4
	a := newProxyActions(t, proxy, newMemStore())
	n, err := a.OldBackendConnections(context.Background(), domain.FlipPlan{OldBackend: "latvia-01"})
	if err != nil || n != 4 {
		t.Fatalf("OldBackendConnections = %d, %v; want 4, nil", n, err)
	}
}

var _ ports.EntryProxy = (*fakeEntryProxy)(nil)
