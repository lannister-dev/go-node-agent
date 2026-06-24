package entryproxy_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/adapters/entryproxyclient"
	"github.com/lannister-dev/go-node-agent/internal/entryproxy"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type fakeProxy struct {
	mu           sync.Mutex
	users        map[string]string
	route        map[string]string
	userBackends map[string][]string
	backends     []ports.EntryBackend
	failSel      bool
}

func newFake() *fakeProxy {
	return &fakeProxy{users: map[string]string{}, route: map[string]string{}}
}

func (f *fakeProxy) AddUser(_ context.Context, clientID, flow string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.users[clientID] = flow
	return nil
}

func (f *fakeProxy) RemoveUser(_ context.Context, clientID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.users, clientID)
	return nil
}

func (f *fakeProxy) SelectBackend(_ context.Context, clientID, backendID string) error {
	if f.failSel {
		return errors.New("unknown backend")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.route[clientID] = backendID
	return nil
}

func (f *fakeProxy) SetUserBackends(_ context.Context, clientID string, backendIDs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.userBackends == nil {
		f.userBackends = map[string][]string{}
	}
	f.userBackends[clientID] = append([]string(nil), backendIDs...)
	return nil
}

func (f *fakeProxy) SetBackends(_ context.Context, backends []ports.EntryBackend) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.backends = backends
	return nil
}

func (f *fakeProxy) BackendConnections(_ context.Context, _ string) (uint64, error) {
	return 7, nil
}

func (f *fakeProxy) ActiveConnections(_ context.Context) ([]ports.EntryConnection, error) {
	return []ports.EntryConnection{
		{ID: "1", ClientID: "user-a", BackendID: "b1"},
		{ID: "2", ClientID: "user-a", BackendID: "b1"},
	}, nil
}

func (f *fakeProxy) Epoch() int64 { return 42 }

func startControl(t *testing.T, proxy entryproxy.Controller) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "control.sock")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = entryproxy.NewControlServer(proxy, nil).Serve(ctx, socket) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("unix", socket); err == nil {
			_ = c.Close()
			return socket
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("control socket not ready")
	return ""
}

func TestControlRoundTrip(t *testing.T) {
	fake := newFake()
	client := entryproxyclient.New(startControl(t, fake))
	ctx := context.Background()

	if err := client.AddUser(ctx, "user-a", "xtls-rprx-vision"); err != nil {
		t.Fatalf("AddUser: %v", err)
	}
	if err := client.SetBackends(ctx, []ports.EntryBackend{{ID: "b1", Address: "10.10.0.3", Port: 443}}); err != nil {
		t.Fatalf("SetBackends: %v", err)
	}
	if err := client.SelectBackend(ctx, "user-a", "b1"); err != nil {
		t.Fatalf("SelectBackend: %v", err)
	}

	fake.mu.Lock()
	gotFlow := fake.users["user-a"]
	gotRoute := fake.route["user-a"]
	gotBackends := len(fake.backends)
	fake.mu.Unlock()
	if gotFlow != "xtls-rprx-vision" {
		t.Fatalf("user flow not propagated: %q", gotFlow)
	}
	if gotRoute != "b1" {
		t.Fatalf("route not propagated: %q", gotRoute)
	}
	if gotBackends != 1 {
		t.Fatalf("backends not propagated: %d", gotBackends)
	}

	n, err := client.BackendConnections(ctx, "b1")
	if err != nil || n != 7 {
		t.Fatalf("BackendConnections = %d, %v; want 7, nil", n, err)
	}

	conns, err := client.ActiveConnections(ctx)
	if err != nil || len(conns) != 2 {
		t.Fatalf("ActiveConnections = %+v, %v", conns, err)
	}
	snap, err := client.Connections(ctx)
	if err != nil || len(snap.Conns) != 2 {
		t.Fatalf("Connections = %d conns, %v; want 2", len(snap.Conns), err)
	}

	if e, err := client.Epoch(ctx); err != nil || e != 42 {
		t.Fatalf("Epoch = %d, %v; want 42", e, err)
	}

	if err := client.RemoveUser(ctx, "user-a"); err != nil {
		t.Fatalf("RemoveUser: %v", err)
	}
	fake.mu.Lock()
	_, stillThere := fake.users["user-a"]
	fake.mu.Unlock()
	if stillThere {
		t.Fatal("user not removed")
	}
}

func TestControlPropagatesError(t *testing.T) {
	fake := newFake()
	fake.failSel = true
	client := entryproxyclient.New(startControl(t, fake))

	err := client.SelectBackend(context.Background(), "user-a", "nope")
	if err == nil {
		t.Fatal("expected error from SelectBackend")
	}
}
