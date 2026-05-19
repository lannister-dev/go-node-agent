package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type fakeSingBox struct {
	mu          sync.Mutex
	configs     [][]byte
	reloads     int
	writeErr    error
	reloadErr   error
	connections ports.SingBoxConnections
	connErr     error
}

func (f *fakeSingBox) WriteConfig(_ context.Context, cfg ports.SingBoxConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil {
		return f.writeErr
	}
	cp := make([]byte, len(cfg.Raw))
	copy(cp, cfg.Raw)
	f.configs = append(f.configs, cp)
	return nil
}

func (f *fakeSingBox) Reload(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reloadErr != nil {
		return f.reloadErr
	}
	f.reloads++
	return nil
}

func (f *fakeSingBox) Connections(_ context.Context) (ports.SingBoxConnections, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connections, f.connErr
}

func (f *fakeSingBox) lastConfig() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.configs) == 0 {
		return nil
	}
	return f.configs[len(f.configs)-1]
}

func (f *fakeSingBox) configCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.configs)
}

func (f *fakeSingBox) reloadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reloads
}

type memStore struct {
	mu   sync.Mutex
	data map[domain.PlacementID]domain.Placement
	err  error
}

func newMemStore(seed ...domain.Placement) *memStore {
	s := &memStore{data: map[domain.PlacementID]domain.Placement{}}
	for _, p := range seed {
		s.data[p.ID] = p
	}
	return s
}

func (m *memStore) GetPlacement(_ context.Context, id domain.PlacementID) (domain.Placement, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return domain.Placement{}, false, m.err
	}
	p, ok := m.data[id]
	return p, ok, nil
}

func (m *memStore) PutPlacement(_ context.Context, p domain.Placement) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.data[p.ID] = p
	return nil
}

func (m *memStore) ListPlacements(_ context.Context) ([]domain.Placement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	out := make([]domain.Placement, 0, len(m.data))
	for _, p := range m.data {
		out = append(out, p)
	}
	return out, nil
}

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func backendsRegistry() *StaticBackends {
	return NewStaticBackends([]singboxgen.BackendSpec{
		{ID: "praha-02", Address: "10.0.0.2", Port: 9000, Transport: domain.TransportWS},
		{ID: "latvia-01", Address: "10.0.0.1", Port: 9000, Transport: domain.TransportWS},
	})
}

func defaultInbound() singboxgen.InboundSpec {
	return singboxgen.InboundSpec{
		Tag:    "vless-in",
		Listen: singboxgen.ListenSpec{Address: "::", Port: 443, Sniff: true},
	}
}

func newActions(t *testing.T, sb SingBoxControl, store PlacementStore, backends BackendLookup) *EntryActions {
	t.Helper()
	a, err := NewEntryActions(EntryActionsConfig{
		Inbound: defaultInbound(),
		LogCfg:  singboxgen.LogSpec{Level: "info"},
	}, sb, store, backends, silent())
	if err != nil {
		t.Fatalf("new actions: %v", err)
	}
	return a
}

func samplePlacement() domain.Placement {
	return domain.Placement{
		ID:            "p-1",
		KeyID:         "k-1",
		ClientID:      "uuid-a",
		NodeID:        "lv-01",
		BackendNodeID: "praha-02",
		OpVersion:     1,
		Desired:       domain.DesiredActive,
		Applied:       domain.AppliedOk,
		Protocol:      domain.ProtocolVLESS,
		Transport:     domain.TransportWS,
	}
}

func TestWarmBackend_OkWhenKnown(t *testing.T) {
	a := newActions(t, &fakeSingBox{}, newMemStore(), backendsRegistry())
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "praha-02", NewBackend: "latvia-01"}
	if err := a.WarmBackend(t.Context(), plan); err != nil {
		t.Fatalf("warm: %v", err)
	}
}

func TestWarmBackend_FailsForUnknownBackend(t *testing.T) {
	a := newActions(t, &fakeSingBox{}, newMemStore(), backendsRegistry())
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "praha-02", NewBackend: "ghost"}
	if err := a.WarmBackend(t.Context(), plan); err == nil {
		t.Fatal("expected error for unknown backend")
	}
}

func TestSwapRoute_PersistsAndReloads(t *testing.T) {
	sb := &fakeSingBox{}
	existing := samplePlacement()
	store := newMemStore(existing)
	a := newActions(t, sb, store, backendsRegistry())

	target := existing
	target.BackendNodeID = "latvia-01"
	target.OpVersion = 2
	plan := domain.FlipPlan{
		PlacementID: existing.ID,
		OldBackend:  "praha-02",
		NewBackend:  "latvia-01",
		Desired:     target,
	}
	if err := a.SwapRoute(t.Context(), plan); err != nil {
		t.Fatalf("swap: %v", err)
	}

	if sb.configCount() != 1 {
		t.Errorf("WriteConfig calls: %d", sb.configCount())
	}
	if sb.reloadCount() != 1 {
		t.Errorf("Reload calls: %d", sb.reloadCount())
	}

	stored, _, _ := store.GetPlacement(t.Context(), existing.ID)
	if stored.BackendNodeID != "latvia-01" || stored.OpVersion != 2 {
		t.Errorf("stored not updated: %+v", stored)
	}
	if stored.Applied != domain.AppliedOk {
		t.Errorf("Applied: %s", stored.Applied)
	}

	var cfg map[string]any
	if err := json.Unmarshal(sb.lastConfig(), &cfg); err != nil {
		t.Fatalf("config: %v", err)
	}
	rules := cfg["route"].(map[string]any)["rules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("rules: %d", len(rules))
	}
	r := rules[0].(map[string]any)
	if r["outbound"] != "backend-latvia-01" {
		t.Errorf("route after swap: %v", r)
	}
}

func TestSwapRoute_StoreErrorAbortsBeforeSingBox(t *testing.T) {
	sb := &fakeSingBox{}
	store := newMemStore(samplePlacement())
	store.err = errors.New("disk full")
	a := newActions(t, sb, store, backendsRegistry())
	target := samplePlacement()
	target.BackendNodeID = "latvia-01"
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "praha-02", NewBackend: "latvia-01", Desired: target}
	if err := a.SwapRoute(t.Context(), plan); err == nil {
		t.Fatal("expected error")
	}
	if sb.configCount() != 0 {
		t.Error("WriteConfig should not be called after store error")
	}
}

func TestSwapRoute_ReloadFailureSurfaces(t *testing.T) {
	sb := &fakeSingBox{reloadErr: errors.New("singbox 500")}
	store := newMemStore(samplePlacement())
	a := newActions(t, sb, store, backendsRegistry())
	target := samplePlacement()
	target.BackendNodeID = "latvia-01"
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "praha-02", NewBackend: "latvia-01", Desired: target}
	err := a.SwapRoute(t.Context(), plan)
	if err == nil {
		t.Fatal("expected reload error")
	}
}

func TestSwapRoute_RejectsBadDesired(t *testing.T) {
	a := newActions(t, &fakeSingBox{}, newMemStore(), backendsRegistry())
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "a", NewBackend: "b"}
	if err := a.SwapRoute(t.Context(), plan); err == nil {
		t.Fatal("missing Desired.ID should error")
	}

	plan.Desired = domain.Placement{ID: "different"}
	if err := a.SwapRoute(t.Context(), plan); err == nil {
		t.Fatal("Desired.ID != PlacementID should error")
	}
}

func TestOldBackendConnections_CountsByTag(t *testing.T) {
	sb := &fakeSingBox{
		connections: ports.SingBoxConnections{
			Total: 7,
			PerOutbound: map[string]uint64{
				"backend-praha-02":  5,
				"backend-latvia-01": 2,
			},
		},
	}
	a := newActions(t, sb, newMemStore(), backendsRegistry())
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "praha-02", NewBackend: "latvia-01"}
	n, err := a.OldBackendConnections(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("got %d, want 5", n)
	}
}

func TestOldBackendConnections_ZeroForUnknownTag(t *testing.T) {
	sb := &fakeSingBox{connections: ports.SingBoxConnections{PerOutbound: map[string]uint64{}}}
	a := newActions(t, sb, newMemStore(), backendsRegistry())
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "praha-02", NewBackend: "latvia-01"}
	n, err := a.OldBackendConnections(t.Context(), plan)
	if err != nil || n != 0 {
		t.Errorf("got n=%d err=%v", n, err)
	}
}

func TestOldBackendConnections_PropagatesError(t *testing.T) {
	sb := &fakeSingBox{connErr: errors.New("clash api down")}
	a := newActions(t, sb, newMemStore(), backendsRegistry())
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "praha-02", NewBackend: "latvia-01"}
	if _, err := a.OldBackendConnections(t.Context(), plan); err == nil {
		t.Fatal("expected error")
	}
}

func TestCoolOldBackend_RendersAndReloads(t *testing.T) {
	sb := &fakeSingBox{}
	store := newMemStore(samplePlacement())
	a := newActions(t, sb, store, backendsRegistry())
	plan := domain.FlipPlan{PlacementID: "p-1", OldBackend: "praha-02", NewBackend: "latvia-01"}
	if err := a.CoolOldBackend(t.Context(), plan); err != nil {
		t.Fatal(err)
	}
	if sb.configCount() != 1 || sb.reloadCount() != 1 {
		t.Errorf("expected 1 write + 1 reload, got %d/%d", sb.configCount(), sb.reloadCount())
	}
}

func TestNew_ValidatesArgs(t *testing.T) {
	good := EntryActionsConfig{Inbound: defaultInbound()}
	if _, err := NewEntryActions(good, nil, newMemStore(), backendsRegistry(), silent()); err == nil {
		t.Error("nil singbox should error")
	}
	if _, err := NewEntryActions(good, &fakeSingBox{}, nil, backendsRegistry(), silent()); err == nil {
		t.Error("nil store should error")
	}
	if _, err := NewEntryActions(good, &fakeSingBox{}, newMemStore(), nil, silent()); err == nil {
		t.Error("nil backends should error")
	}
	bad := EntryActionsConfig{}
	if _, err := NewEntryActions(bad, &fakeSingBox{}, newMemStore(), backendsRegistry(), silent()); err == nil {
		t.Error("empty Inbound.Tag should error")
	}
}

func TestStaticBackends_AllPreservesInsertionOrder(t *testing.T) {
	specs := []singboxgen.BackendSpec{
		{ID: "z", Address: "1", Port: 1},
		{ID: "a", Address: "2", Port: 2},
		{ID: "m", Address: "3", Port: 3},
	}
	reg := NewStaticBackends(specs)
	all := reg.All()
	if len(all) != 3 {
		t.Fatalf("len: %d", len(all))
	}
	if all[0].ID != "z" || all[1].ID != "a" || all[2].ID != "m" {
		t.Errorf("order broken: %+v", all)
	}
}

func TestStaticBackends_GetMissingReturnsFalse(t *testing.T) {
	reg := NewStaticBackends(nil)
	if _, ok := reg.Get("nope"); ok {
		t.Error("expected ok=false")
	}
}
