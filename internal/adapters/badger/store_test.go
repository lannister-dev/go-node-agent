package badger

import (
	"context"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func samplePlacement() domain.Placement {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	return domain.Placement{
		ID:            "p-001",
		KeyID:         "k-001",
		ClientID:      "01234567-89ab-cdef-0123-456789abcdef",
		NodeID:        "lv-01",
		BackendNodeID: "praha-02",
		OpVersion:     42,
		Desired:       domain.DesiredActive,
		Applied:       domain.AppliedPending,
		Transport:     domain.TransportWS,
		Protocol:      domain.ProtocolVLESS,
		IsRevoked:     false,
		ValidUntil:    now.Add(24 * time.Hour),
		UpdatedAt:     now,
		LastAppliedAt: time.Time{},
	}
}

func TestStore_PutGetPlacement(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	want := samplePlacement()

	if err := s.PutPlacement(ctx, want); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, found, err := s.GetPlacement(ctx, want.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if got.ID != want.ID || got.OpVersion != want.OpVersion || got.Desired != want.Desired ||
		got.Transport != want.Transport || got.IsRevoked != want.IsRevoked {
		t.Fatalf("placement mismatch\n got=%+v\nwant=%+v", got, want)
	}
	if !got.ValidUntil.Equal(want.ValidUntil) || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("time mismatch got valid_until=%v updated_at=%v", got.ValidUntil, got.UpdatedAt)
	}
	if !got.LastAppliedAt.IsZero() {
		t.Fatalf("last_applied_at should be zero, got %v", got.LastAppliedAt)
	}
}

func TestStore_GetPlacement_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, found, err := s.GetPlacement(t.Context(), "missing")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if found {
		t.Fatal("expected found=false")
	}
}

func TestStore_DeletePlacement(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	p := samplePlacement()
	if err := s.PutPlacement(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := s.DeletePlacement(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	_, found, err := s.GetPlacement(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected found=false after delete")
	}
}

func TestStore_ListPlacements(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	p1 := samplePlacement()
	p1.ID = "p-001"
	p2 := samplePlacement()
	p2.ID = "p-002"
	p2.OpVersion = 99
	for _, p := range []domain.Placement{p1, p2} {
		if err := s.PutPlacement(ctx, p); err != nil {
			t.Fatal(err)
		}
	}

	list, err := s.ListPlacements(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2, got %d: %+v", len(list), list)
	}
	seen := map[domain.PlacementID]domain.OpVersion{}
	for _, p := range list {
		seen[p.ID] = p.OpVersion
	}
	if seen["p-001"] != 42 || seen["p-002"] != 99 {
		t.Fatalf("unexpected list contents: %+v", seen)
	}
}

func TestStore_Cursor(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	seq, err := s.GetCursor(ctx, "placement_commands")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 {
		t.Fatalf("expected zero cursor, got %d", seq)
	}

	if err := s.PutCursor(ctx, "placement_commands", 1234); err != nil {
		t.Fatal(err)
	}
	seq, err = s.GetCursor(ctx, "placement_commands")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1234 {
		t.Fatalf("expected 1234, got %d", seq)
	}
}

func TestStore_Snapshot_InMemoryNoop(t *testing.T) {
	s := newTestStore(t)
	if err := s.Snapshot(t.Context()); err != nil {
		t.Fatalf("snapshot in-memory should be no-op, got: %v", err)
	}
}

func TestStore_Snapshot_FileBacked(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Path: dir})
	if err != nil {
		t.Fatalf("open file-backed: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := range 10 {
		p := samplePlacement()
		p.ID = domain.PlacementID("p-" + string(rune('a'+i)))
		if err := s.PutPlacement(t.Context(), p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Snapshot(t.Context()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
}

func TestStore_ContextCancellation(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.PutPlacement(ctx, samplePlacement()); err == nil {
		t.Fatal("expected cancellation error")
	}
}

var _ ports.Store = (*Store)(nil)
