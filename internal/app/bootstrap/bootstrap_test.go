package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type fakeStore struct {
	mu       sync.Mutex
	identity domain.NodeIdentity
	hasID    bool
	getErr   error
	putErr   error
	puts     int
}

func (f *fakeStore) GetIdentity(ctx context.Context) (domain.NodeIdentity, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return domain.NodeIdentity{}, false, f.getErr
	}
	return f.identity, f.hasID, nil
}

func (f *fakeStore) PutIdentity(ctx context.Context, id domain.NodeIdentity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return f.putErr
	}
	f.identity = id
	f.hasID = true
	f.puts++
	return nil
}

type fakeCtl struct {
	gotReq ports.InitialRequest
	resp   ports.InitialResponse
	err    error
	calls  int
}

func (f *fakeCtl) Initial(ctx context.Context, req ports.InitialRequest) (ports.InitialResponse, error) {
	f.gotReq = req
	f.calls++
	return f.resp, f.err
}

type fakeIDs struct{ id string }

func (f fakeIDs) NewID() string { return f.id }

func silentLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestBootstrap_FreshCallsInitialAndPersists(t *testing.T) {
	store := &fakeStore{}
	ctl := &fakeCtl{
		resp: ports.InitialResponse{
			Identity: domain.NodeIdentity{
				NodeID:          "lv-01",
				AgentInstanceID: "agent-id-from-server",
				AuthToken:       "tok",
				BootstrappedAt:  time.Now(),
			},
			FullResyncRequired: true,
		},
	}
	b := New(Config{
		BootstrapToken: "boot",
		NodeKey:        "key",
		NodeRole:       "entry",
	}, store, ctl, fakeIDs{id: "agent-id-local"}, silentLogger())

	res, err := b.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !res.WasFresh {
		t.Error("expected WasFresh=true")
	}
	if !res.FullResyncRequired {
		t.Error("expected FullResyncRequired=true")
	}
	if res.Identity.NodeID != "lv-01" {
		t.Errorf("identity.NodeID = %q", res.Identity.NodeID)
	}
	if store.puts != 1 {
		t.Errorf("expected 1 put, got %d", store.puts)
	}
	if ctl.calls != 1 {
		t.Errorf("expected 1 initial call, got %d", ctl.calls)
	}
	if ctl.gotReq.AgentInstanceID != "agent-id-local" {
		t.Errorf("agent_instance_id sent = %q, expected agent-id-local", ctl.gotReq.AgentInstanceID)
	}
	if ctl.gotReq.NodeRole != "entry" {
		t.Errorf("node_role sent = %q", ctl.gotReq.NodeRole)
	}
}

func TestBootstrap_ExistingIdentityReused(t *testing.T) {
	existing := domain.NodeIdentity{
		NodeID:          "lv-01",
		AgentInstanceID: "saved-agent-id",
		AuthToken:       "saved-tok",
		BootstrappedAt:  time.Now().Add(-time.Hour),
	}
	store := &fakeStore{identity: existing, hasID: true}
	ctl := &fakeCtl{}
	b := New(Config{}, store, ctl, fakeIDs{}, silentLogger())

	res, err := b.Run(t.Context())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.WasFresh {
		t.Error("expected WasFresh=false")
	}
	if res.Identity.AgentInstanceID != "saved-agent-id" {
		t.Errorf("identity mismatch: %+v", res.Identity)
	}
	if ctl.calls != 0 {
		t.Error("control-api should not be called when identity exists")
	}
	if store.puts != 0 {
		t.Error("store should not be written when identity exists")
	}
}

func TestBootstrap_ExpectedNodeIDMismatch_Existing(t *testing.T) {
	store := &fakeStore{
		identity: domain.NodeIdentity{NodeID: "lv-01"},
		hasID:    true,
	}
	b := New(Config{ExpectedNodeID: "praha-02"}, store, &fakeCtl{}, fakeIDs{}, silentLogger())
	_, err := b.Run(t.Context())
	if err == nil {
		t.Fatal("expected mismatch error")
	}
}

func TestBootstrap_ExpectedNodeIDMismatch_Server(t *testing.T) {
	ctl := &fakeCtl{
		resp: ports.InitialResponse{
			Identity: domain.NodeIdentity{NodeID: "lv-01", AgentInstanceID: "a", AuthToken: "t"},
		},
	}
	store := &fakeStore{}
	b := New(Config{
		ExpectedNodeID: "praha-02",
		BootstrapToken: "boot", NodeKey: "key",
	}, store, ctl, fakeIDs{id: "a"}, silentLogger())
	_, err := b.Run(t.Context())
	if err == nil {
		t.Fatal("expected mismatch error")
	}
	if store.puts != 0 {
		t.Error("should not persist on mismatch")
	}
}

func TestBootstrap_CtlErrorPropagates(t *testing.T) {
	store := &fakeStore{}
	ctl := &fakeCtl{err: errors.New("boom")}
	b := New(Config{
		BootstrapToken: "boot", NodeKey: "key",
	}, store, ctl, fakeIDs{id: "a"}, silentLogger())
	_, err := b.Run(t.Context())
	if err == nil || !errors.Is(err, ctl.err) {
		t.Fatalf("expected wrapped boom, got: %v", err)
	}
	if store.puts != 0 {
		t.Error("should not persist on ctl error")
	}
}

func TestBootstrap_StoreErrorPropagates(t *testing.T) {
	store := &fakeStore{getErr: errors.New("disk gone")}
	b := New(Config{}, store, &fakeCtl{}, fakeIDs{}, silentLogger())
	_, err := b.Run(t.Context())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBootstrap_ContextCancel(t *testing.T) {
	store := &fakeStore{}
	ctl := &fakeCtl{}
	b := New(Config{
		BootstrapToken: "b", NodeKey: "k",
	}, store, ctl, fakeIDs{id: "a"}, silentLogger())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	store.getErr = ctx.Err()

	_, err := b.Run(ctx)
	if err == nil {
		t.Fatal("expected context error")
	}
}
