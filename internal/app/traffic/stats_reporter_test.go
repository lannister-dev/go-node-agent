package traffic

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type kvCapture struct {
	mu      sync.Mutex
	entries map[string][]byte
}

func (k *kvCapture) KVPut(_ context.Context, bucket, key string, value []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.entries == nil {
		k.entries = map[string][]byte{}
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	k.entries[bucket+"/"+key] = cp
	return nil
}

func TestStatsReporter_AggregatesByBackendAndClient(t *testing.T) {
	const (
		nodeID = "11111111-1111-1111-1111-111111111111"
		u1     = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		u2     = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		bA     = "44444444-4444-4444-4444-444444444444"
		bB     = "55555555-5555-5555-5555-555555555555"
	)
	tagU1A := "b-" + u1 + "-" + bA
	tagU2B := "b-" + u2 + "-" + bB

	conns := &fakeConns{snaps: []ports.SingBoxConnections{
		{Conns: []ports.SingBoxConn{
			{ID: "c1", Chains: []string{tagU1A}},
			{ID: "c2", Chains: []string{tagU1A, "auto-" + u1}},
			{ID: "c3", Chains: []string{tagU2B}},
			{ID: "c4", Chains: []string{"direct"}},
		}},
	}}
	kv := &kvCapture{}
	registry := &fakeBackends{names: map[string]string{
		bA: "alpha-backend-01",
		bB: "beta-backend-01",
	}}
	r, err := NewStatsReporter(StatsReporterConfig{NodeID: nodeID}, kv, conns, registry, silentLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	raw := kv.entries[StatsKvBucket+"/node."+nodeID]
	if raw == nil {
		t.Fatal("no kv put")
	}
	var p statsPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if p.Total != 4 {
		t.Errorf("total=%d want 4", p.Total)
	}
	if p.ByBackend["backend-alpha-backend-01"] != 2 {
		t.Errorf("backend-alpha count=%d want 2", p.ByBackend["backend-alpha-backend-01"])
	}
	if p.ByBackend["backend-beta-backend-01"] != 1 {
		t.Errorf("backend-beta count=%d", p.ByBackend["backend-beta-backend-01"])
	}
	if p.ByBackend[tagDirect] != 1 {
		t.Errorf("direct count=%d", p.ByBackend[tagDirect])
	}
	if p.ByClientID[u1] != 2 || p.ByClientID[u2] != 1 {
		t.Errorf("by_client_id wrong: %+v", p.ByClientID)
	}
	if p.UniqueUsers != 2 {
		t.Errorf("unique_users=%d want 2", p.UniqueUsers)
	}
	if p.NodeID != nodeID {
		t.Errorf("node_id mismatch")
	}
}

func TestStatsReporter_ExcludesProbe(t *testing.T) {
	const (
		nodeID = "11111111-1111-1111-1111-111111111111"
		user   = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		probe  = "99999999-9999-9999-9999-999999999999"
		bA     = "44444444-4444-4444-4444-444444444444"
	)
	conns := &fakeConns{snaps: []ports.SingBoxConnections{
		{Conns: []ports.SingBoxConn{
			{ID: "c1", Chains: []string{"b-" + user + "-" + bA}},
			{ID: "c2", Chains: []string{"b-" + probe + "-" + bA}},
		}},
	}}
	kv := &kvCapture{}
	registry := &fakeBackends{names: map[string]string{bA: "alpha-backend-01"}}
	r, err := NewStatsReporter(StatsReporterConfig{NodeID: nodeID, ProbeClientIDs: []string{probe}}, kv, conns, registry, silentLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	var p statsPayload
	if err := json.Unmarshal(kv.entries[StatsKvBucket+"/node."+nodeID], &p); err != nil {
		t.Fatal(err)
	}
	if p.Total != 1 {
		t.Errorf("total=%d want 1 (probe excluded)", p.Total)
	}
	if _, ok := p.ByClientID[probe]; ok {
		t.Errorf("probe must not appear in by_client_id: %+v", p.ByClientID)
	}
	if p.UniqueUsers != 1 || p.ByClientID[user] != 1 {
		t.Errorf("real user miscounted: unique=%d byClient=%+v", p.UniqueUsers, p.ByClientID)
	}
}

func TestAggregatedBackendTag(t *testing.T) {
	const (
		u = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		b = "44444444-4444-4444-4444-444444444444"
	)
	resolver := &fakeBackends{names: map[string]string{b: "alpha-backend-01"}}
	r := &StatsReporter{backends: resolver}
	cases := []struct {
		name   string
		chains []string
		want   string
	}{
		{"per-user single", []string{"b-" + u + "-" + b}, "backend-alpha-backend-01"},
		{"urltest order: per-user first", []string{"b-" + u + "-" + b, "auto-" + u}, "backend-alpha-backend-01"},
		{"urltest order: auto first", []string{"auto-" + u, "b-" + u + "-" + b}, "backend-alpha-backend-01"},
		{"unknown backend falls back to id", []string{"b-" + u + "-99999999-9999-9999-9999-999999999999"}, "backend-99999999-9999-9999-9999-999999999999"},
		{"direct only", []string{"direct"}, "direct"},
		{"empty", []string{}, "direct"},
		{"malformed b-", []string{"b-too-short"}, "b-too-short"},
	}
	for _, c := range cases {
		got := r.aggregatedBackendTag(c.chains)
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

type fakeBackends struct {
	names map[string]string
}

func (f *fakeBackends) Get(id domain.BackendID) (singboxgen.BackendSpec, bool) {
	name, ok := f.names[string(id)]
	if !ok {
		return singboxgen.BackendSpec{}, false
	}
	return singboxgen.BackendSpec{ID: id, Name: name}, true
}
