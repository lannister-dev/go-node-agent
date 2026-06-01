//go:build with_utls

package entryproxy_test

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/app/executor"
	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/entryproxy"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type memPlacementStore struct {
	mu   sync.Mutex
	data map[domain.PlacementID]domain.Placement
}

func newMemPlacementStore(seed ...domain.Placement) *memPlacementStore {
	s := &memPlacementStore{data: map[domain.PlacementID]domain.Placement{}}
	for _, p := range seed {
		s.data[p.ID] = p
	}
	return s
}

func (s *memPlacementStore) GetPlacement(_ context.Context, id domain.PlacementID) (domain.Placement, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.data[id]
	return p, ok, nil
}

func (s *memPlacementStore) PutPlacement(_ context.Context, p domain.Placement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[p.ID] = p
	return nil
}

func (s *memPlacementStore) ListPlacements(_ context.Context) ([]domain.Placement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Placement, 0, len(s.data))
	for _, p := range s.data {
		out = append(out, p)
	}
	return out, nil
}

func startProxy(t *testing.T) (*entryproxy.Proxy, string, string) {
	t.Helper()
	priv, pub := genRealityKeys(t)
	target := tlsTarget(t)
	host, portStr, _ := net.SplitHostPort(target)
	port, _ := net.LookupPort("tcp", portStr)
	p, err := entryproxy.New(entryproxy.Config{
		ListenAddr:      "127.0.0.1:0",
		RealityKey:      priv,
		ShortID:         smokeShortID,
		ServerName:      smokeServerName,
		HandshakeServer: host,
		HandshakePort:   uint16(port),
	}, nil)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := p.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, p.Addr(), pub
}

func spec(id, addr string) singboxgen.BackendSpec {
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := net.LookupPort("tcp", portStr)
	return singboxgen.BackendSpec{ID: domain.BackendID(id), Name: id, Address: host, Port: uint16(port)}
}

// TestIntegrationAgentRoutesThroughProxy drives the real EntryProxyActions
// against the real Proxy: store + override -> RebuildFromStore -> a REALITY
// client lands on the override backend; a runtime SimpleApply adds a new user
// who connects and routes immediately without dropping a live connection.
func TestIntegrationAgentRoutesThroughProxy(t *testing.T) {
	const (
		userA    = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		userB    = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		userLive = "dddddddd-dddd-dddd-dddd-dddddddddddd"
	)
	all := []string{userA, userB, userLive}
	// The executor registers REALITY users with the vision flow; a real client
	// connects with the same flow, so the test client must too. (Smoke drives
	// AddUser directly with an empty flow, hence its plain helper.)
	visionFlow := singboxgen.FlowForTransport(domain.TransportReality)

	rixBE := taggedBackend(t, "RIX", all...)
	zrhBE := taggedBackend(t, "ZRH", all...)
	echoBE := fakeBackend(t, all...)
	echo := echoServer(t)

	proxy, proxyAddr, pub := startProxy(t)
	reg := executor.NewStaticBackends([]singboxgen.BackendSpec{
		spec("rix-backend-01", rixBE),
		spec("zrh-backend-01", zrhBE),
		spec("echo-backend-01", echoBE),
	})

	store := newMemPlacementStore(
		// userA multi-homed on rix+zrh, pinned to zrh by the load-balancer override.
		domain.Placement{ID: "a-rix", ClientID: userA, BackendNodeID: "rix-backend-01", Desired: domain.DesiredActive, Transport: domain.TransportReality, OpVersion: 9, EntryOverrideTag: "backend-zrh-backend-01"},
		domain.Placement{ID: "a-zrh", ClientID: userA, BackendNodeID: "zrh-backend-01", Desired: domain.DesiredActive, Transport: domain.TransportReality, OpVersion: 2, EntryOverrideTag: "backend-zrh-backend-01"},
		// userLive on the echo backend, for the no-drop check.
		domain.Placement{ID: "live", ClientID: userLive, BackendNodeID: "echo-backend-01", Desired: domain.DesiredActive, Transport: domain.TransportReality, OpVersion: 1, EntryOverrideTag: "backend-echo-backend-01"},
	)

	actions, err := executor.NewEntryProxyActions(proxy, store, reg, nil)
	if err != nil {
		t.Fatalf("new actions: %v", err)
	}
	ctx := context.Background()

	// (A) RebuildFromStore must route userA to the override backend (zrh), not the
	// higher-opversion rix candidate.
	if err := actions.RebuildFromStore(ctx); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if tag := readTag(t, realityVlessClientFlow(t, proxyAddr, pub, userA, visionFlow, "1.1.1.1:80")); tag != "ZRH" {
		t.Fatalf("override not honored: userA landed on %q, want ZRH", tag)
	}

	// Open a live connection for userLive and prove it survives a runtime add.
	live := realityVlessClientFlow(t, proxyAddr, pub, userLive, visionFlow, echo)
	roundtrip(t, live, "live-before")

	// (B) A brand-new user arrives at runtime via SimpleApply -> must connect and
	// route immediately, with no reload.
	if err := actions.SimpleApply(ctx, domain.Placement{
		ID: "b-rix", ClientID: userB, BackendNodeID: "rix-backend-01",
		Desired: domain.DesiredActive, Transport: domain.TransportReality, OpVersion: 1,
	}); err != nil {
		t.Fatalf("simple-apply new user: %v", err)
	}
	if tag := readTag(t, realityVlessClientFlow(t, proxyAddr, pub, userB, visionFlow, "1.1.1.1:80")); tag != "RIX" {
		t.Fatalf("runtime user routed to %q, want RIX", tag)
	}

	// (D) The live connection must not have dropped when userB was added.
	roundtrip(t, live, "live-after")

	// (C) Live route change: the load-balancer moves userA to rix via a fresh
	// command -> new connections must follow.
	if err := actions.SimpleApply(ctx, domain.Placement{
		ID: "a-rix", ClientID: userA, BackendNodeID: "rix-backend-01",
		Desired: domain.DesiredActive, Transport: domain.TransportReality, OpVersion: 20,
	}); err != nil {
		t.Fatalf("simple-apply route change: %v", err)
	}
	if tag := readTag(t, realityVlessClientFlow(t, proxyAddr, pub, userA, visionFlow, "1.1.1.1:80")); tag != "RIX" {
		t.Fatalf("route switch failed: userA landed on %q, want RIX", tag)
	}
}
