package traffic

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type fakeConns struct {
	mu    sync.Mutex
	calls int
	snaps []ports.SingBoxConnections
	err   error
}

func (f *fakeConns) Connections(_ context.Context) (ports.SingBoxConnections, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return ports.SingBoxConnections{}, f.err
	}
	i := f.calls
	f.calls++
	if i < len(f.snaps) {
		return f.snaps[i], nil
	}
	return f.snaps[len(f.snaps)-1], nil
}

type capturedMsg struct {
	subject string
	data    []byte
}

type fakePub struct {
	mu   sync.Mutex
	msgs []capturedMsg
}

func (p *fakePub) Publish(_ context.Context, subject string, _ map[string]string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	p.msgs = append(p.msgs, capturedMsg{subject: subject, data: cp})
	return nil
}

func silentLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPublisher_PerBackendDelta(t *testing.T) {
	const (
		entryID = "11111111-1111-1111-1111-111111111111"
		user1   = "22222222-2222-2222-2222-222222222222"
		user2   = "33333333-3333-3333-3333-333333333333"
		bA      = "44444444-4444-4444-4444-444444444444"
		bB      = "55555555-5555-5555-5555-555555555555"
	)
	tagU1A := "b-" + user1 + "-" + bA
	tagU2B := "b-" + user2 + "-" + bB

	cs := &fakeConns{snaps: []ports.SingBoxConnections{
		{Conns: []ports.SingBoxConn{
			{ID: "c1", Chains: []string{tagU1A}, Upload: 1000, Download: 500},
			{ID: "c2", Chains: []string{tagU2B}, Upload: 200, Download: 100},
		}},
		{Conns: []ports.SingBoxConn{
			{ID: "c1", Chains: []string{tagU1A}, Upload: 1500, Download: 800},
			{ID: "c2", Chains: []string{tagU2B}, Upload: 700, Download: 400},
			{ID: "c3", Chains: []string{tagU1A}, Upload: 50, Download: 30},
		}},
	}}
	pub := &fakePub{}
	p, err := NewPublisher(PublisherConfig{
		NodeID: entryID, NodeRole: "entry", Subject: "nodes.traffic",
	}, pub, cs, silentLog())
	if err != nil {
		t.Fatal(err)
	}

	if err := p.tick(t.Context()); err != nil {
		t.Fatalf("tick1: %v", err)
	}
	if err := p.tick(t.Context()); err != nil {
		t.Fatalf("tick2: %v", err)
	}

	if len(pub.msgs) != 2 {
		t.Fatalf("expected 2 publishes, got %d", len(pub.msgs))
	}

	var t2 []nodeTrafficPayload
	if err := json.Unmarshal(pub.msgs[1].data, &t2); err != nil {
		t.Fatalf("decode tick2: %v", err)
	}
	byBackend := map[string]nodeTrafficPayload{}
	for _, d := range t2 {
		byBackend[d.BackendNodeID] = d
	}
	dA, ok := byBackend[bA]
	if !ok {
		t.Fatalf("backend A missing: %+v", t2)
	}
	// c1 delta = 500/300; c3 first-seen this tick → no delta yet (matches Python semantics)
	if dA.BytesOut != 500 {
		t.Errorf("A bytes_out: got %d, want 500", dA.BytesOut)
	}
	if dA.BytesIn != 300 {
		t.Errorf("A bytes_in: got %d, want 300", dA.BytesIn)
	}
	if dA.ActiveSessions != 2 {
		t.Errorf("A active: %d", dA.ActiveSessions)
	}
	if dA.EntryNodeID != entryID {
		t.Errorf("A entry: %q", dA.EntryNodeID)
	}
	dB := byBackend[bB]
	if dB.BytesOut != 500 || dB.BytesIn != 300 || dB.ActiveSessions != 1 {
		t.Errorf("B: %+v", dB)
	}
}

func TestPublisher_SkipsDirectAndEmptyTick(t *testing.T) {
	cs := &fakeConns{snaps: []ports.SingBoxConnections{
		{Conns: []ports.SingBoxConn{
			{ID: "c1", Chains: []string{"direct"}, Upload: 100, Download: 100},
		}},
		{Conns: []ports.SingBoxConn{}},
	}}
	pub := &fakePub{}
	p, _ := NewPublisher(PublisherConfig{
		NodeID: "11111111-1111-1111-1111-111111111111", NodeRole: "entry",
	}, pub, cs, silentLog())
	if err := p.tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p.tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(pub.msgs) != 0 {
		t.Errorf("expected 0 publishes, got %d", len(pub.msgs))
	}
}

func TestPublisher_UrltestChain(t *testing.T) {
	const (
		user = "22222222-2222-2222-2222-222222222222"
		b1   = "44444444-4444-4444-4444-444444444444"
	)
	tagU := "b-" + user + "-" + b1
	cs := &fakeConns{snaps: []ports.SingBoxConnections{
		{Conns: []ports.SingBoxConn{
			{ID: "c1", Chains: []string{tagU, "auto-" + user}, Upload: 100, Download: 200},
		}},
		{Conns: []ports.SingBoxConn{
			{ID: "c1", Chains: []string{tagU, "auto-" + user}, Upload: 250, Download: 350},
		}},
	}}
	pub := &fakePub{}
	p, _ := NewPublisher(PublisherConfig{
		NodeID: "11111111-1111-1111-1111-111111111111", NodeRole: "entry",
	}, pub, cs, silentLog())
	_ = p.tick(t.Context())
	_ = p.tick(t.Context())
	if len(pub.msgs) != 2 {
		t.Fatalf("publishes: %d", len(pub.msgs))
	}
	var deltas []nodeTrafficPayload
	_ = json.Unmarshal(pub.msgs[1].data, &deltas)
	if len(deltas) != 1 || deltas[0].BackendNodeID != b1 {
		t.Errorf("got %+v, want backend=%s", deltas, b1)
	}
}

func TestPublisher_BackendModeOmitsEntryID(t *testing.T) {
	const u = "22222222-2222-2222-2222-222222222222"
	const b = "44444444-4444-4444-4444-444444444444"
	cs := &fakeConns{snaps: []ports.SingBoxConnections{
		{Conns: []ports.SingBoxConn{{ID: "c1", Chains: []string{"b-" + u + "-" + b}, Upload: 10}}},
		{Conns: []ports.SingBoxConn{{ID: "c1", Chains: []string{"b-" + u + "-" + b}, Upload: 30}}},
	}}
	pub := &fakePub{}
	p, _ := NewPublisher(PublisherConfig{
		NodeID: "11111111-1111-1111-1111-111111111111", NodeRole: "backend",
	}, pub, cs, silentLog())
	_ = p.tick(t.Context())
	_ = p.tick(t.Context())
	if len(pub.msgs) < 1 {
		t.Fatalf("no publishes")
	}
	for i, m := range pub.msgs {
		if strings.Contains(string(m.data), `"entry_node_id"`) {
			t.Errorf("publish[%d] backend role must omit entry_node_id: %s", i, m.data)
		}
	}
}

func TestPickBackendID(t *testing.T) {
	const (
		u = "22222222-2222-2222-2222-222222222222"
		b = "44444444-4444-4444-4444-444444444444"
	)
	if got := pickBackendID([]string{"b-" + u + "-" + b}); got != b {
		t.Errorf("direct: got %q, want %q", got, b)
	}
	if got := pickBackendID([]string{"auto-" + u, "b-" + u + "-" + b}); got != b {
		t.Errorf("urltest order: got %q", got)
	}
	if got := pickBackendID([]string{"direct"}); got != "" {
		t.Errorf("direct should yield empty: %q", got)
	}
	if got := pickBackendID([]string{"b-too-short"}); got != "" {
		t.Errorf("malformed tag should yield empty: %q", got)
	}
}
