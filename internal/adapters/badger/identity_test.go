package badger

import (
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func TestStore_Identity(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	_, found, err := s.GetIdentity(ctx)
	if err != nil {
		t.Fatalf("get empty: %v", err)
	}
	if found {
		t.Fatal("expected no identity on fresh store")
	}

	want := domain.NodeIdentity{
		NodeID:          "lv-01",
		AgentInstanceID: "01234567-89ab-cdef-0123-456789abcdef",
		AuthToken:       "tok-xyz",
		BootstrappedAt:  time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC),
	}
	if err := s.PutIdentity(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.GetIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected identity after put")
	}
	if got.NodeID != want.NodeID || got.AgentInstanceID != want.AgentInstanceID || got.AuthToken != want.AuthToken {
		t.Fatalf("mismatch:\n got=%+v\nwant=%+v", got, want)
	}
	if !got.BootstrappedAt.Equal(want.BootstrappedAt) {
		t.Fatalf("bootstrapped_at mismatch: got=%v want=%v", got.BootstrappedAt, want.BootstrappedAt)
	}
}
