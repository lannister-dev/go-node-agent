package executor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type fakeXray struct {
	mu      sync.Mutex
	added   []ports.XrayUser
	removed []domain.ClientID
	addErr  error
	rmErr   error
}

func (f *fakeXray) AddUser(_ context.Context, u ports.XrayUser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.addErr != nil {
		return f.addErr
	}
	f.added = append(f.added, u)
	return nil
}

func (f *fakeXray) RemoveUser(_ context.Context, c domain.ClientID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rmErr != nil {
		return f.rmErr
	}
	f.removed = append(f.removed, c)
	return nil
}

func (f *fakeXray) addedSnap() []ports.XrayUser {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ports.XrayUser, len(f.added))
	copy(out, f.added)
	return out
}

func (f *fakeXray) removedSnap() []domain.ClientID {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.ClientID, len(f.removed))
	copy(out, f.removed)
	return out
}

func silentBE() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newBackendExec(t *testing.T, x XrayUserManager, st PlacementStore) *BackendExecutor {
	t.Helper()
	e, err := NewBackendExecutor(x, st, silentBE())
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func placement(id, client string, desired domain.DesiredState, transport domain.TransportKind) domain.Placement {
	return domain.Placement{
		ID:        domain.PlacementID(id),
		ClientID:  domain.ClientID(client),
		NodeID:    "praha-02",
		Desired:   desired,
		Transport: transport,
		OpVersion: 1,
	}
}

func TestBackendExec_ActivateAddsUserAndPersists(t *testing.T) {
	x := &fakeXray{}
	st := newMemStore()
	e := newBackendExec(t, x, st)

	desired := placement("p-1", "uuid-a", domain.DesiredActive, domain.TransportReality)
	_, err := e.Apply(t.Context(), desired, domain.Placement{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(x.addedSnap()) != 1 || x.addedSnap()[0].ClientID != "uuid-a" {
		t.Errorf("expected one AddUser call for uuid-a, got %+v", x.addedSnap())
	}
	if len(x.removedSnap()) != 0 {
		t.Error("RemoveUser should not be called on activation")
	}
	stored, ok, _ := st.GetPlacement(t.Context(), "p-1")
	if !ok || stored.Applied != domain.AppliedOk {
		t.Errorf("stored: %+v", stored)
	}
}

func TestBackendExec_DeactivateRemovesUser(t *testing.T) {
	x := &fakeXray{}
	st := newMemStore()
	e := newBackendExec(t, x, st)

	prev := placement("p-1", "uuid-a", domain.DesiredActive, domain.TransportWS)
	prev.Applied = domain.AppliedOk

	desired := placement("p-1", "uuid-a", domain.DesiredInactive, domain.TransportWS)
	_, err := e.Apply(t.Context(), desired, prev, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(x.removedSnap()) != 1 || x.removedSnap()[0] != "uuid-a" {
		t.Errorf("expected RemoveUser uuid-a, got %+v", x.removedSnap())
	}
	if len(x.addedSnap()) != 0 {
		t.Error("AddUser should not be called on deactivation")
	}
	stored, _, _ := st.GetPlacement(t.Context(), "p-1")
	if stored.Desired != domain.DesiredInactive {
		t.Errorf("Desired not persisted: %s", stored.Desired)
	}
}

func TestBackendExec_RevocationRemovesUser(t *testing.T) {
	x := &fakeXray{}
	st := newMemStore()
	e := newBackendExec(t, x, st)

	prev := placement("p-1", "uuid-a", domain.DesiredActive, domain.TransportWS)
	prev.Applied = domain.AppliedOk

	desired := placement("p-1", "uuid-a", domain.DesiredActive, domain.TransportWS)
	desired.IsRevoked = true

	_, err := e.Apply(t.Context(), desired, prev, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(x.removedSnap()) != 1 {
		t.Errorf("revoked user should be removed; got removed=%+v added=%+v", x.removedSnap(), x.addedSnap())
	}
}

func TestBackendExec_NoOpWhenAlreadyActive(t *testing.T) {
	x := &fakeXray{}
	st := newMemStore()
	e := newBackendExec(t, x, st)

	prev := placement("p-1", "uuid-a", domain.DesiredActive, domain.TransportWS)
	prev.Applied = domain.AppliedOk

	desired := placement("p-1", "uuid-a", domain.DesiredActive, domain.TransportWS)
	desired.OpVersion = 2

	_, err := e.Apply(t.Context(), desired, prev, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(x.addedSnap()) != 0 || len(x.removedSnap()) != 0 {
		t.Errorf("no-op expected; added=%+v removed=%+v", x.addedSnap(), x.removedSnap())
	}
	stored, _, _ := st.GetPlacement(t.Context(), "p-1")
	if stored.OpVersion != 2 {
		t.Errorf("placement should still be persisted with new op_version: %d", stored.OpVersion)
	}
}

func TestBackendExec_AddErrorSurfaces(t *testing.T) {
	x := &fakeXray{addErr: errors.New("xray rejected")}
	st := newMemStore()
	e := newBackendExec(t, x, st)

	desired := placement("p-1", "uuid-a", domain.DesiredActive, domain.TransportWS)
	_, err := e.Apply(t.Context(), desired, domain.Placement{}, false)
	if err == nil {
		t.Fatal("expected error to surface")
	}
}

func TestBackendExec_RejectsBadInputs(t *testing.T) {
	x := &fakeXray{}
	st := newMemStore()
	e := newBackendExec(t, x, st)

	if _, err := e.Apply(t.Context(), domain.Placement{ClientID: "c"}, domain.Placement{}, false); err == nil {
		t.Error("missing ID should error")
	}
	if _, err := e.Apply(t.Context(), domain.Placement{ID: "p"}, domain.Placement{}, false); err == nil {
		t.Error("missing ClientID should error")
	}
}

func TestNewBackendExecutor_Validates(t *testing.T) {
	if _, err := NewBackendExecutor(nil, newMemStore(), silentBE()); err == nil {
		t.Error("nil xray should error")
	}
	if _, err := NewBackendExecutor(&fakeXray{}, nil, silentBE()); err == nil {
		t.Error("nil store should error")
	}
}
