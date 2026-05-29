package traffic

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/ports"
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
	r, err := NewStatsReporter(StatsReporterConfig{NodeID: nodeID}, kv, conns, silentLog())
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
	if p.ByBackend["backend-"+bA] != 2 {
		t.Errorf("backend-A count=%d want 2", p.ByBackend["backend-"+bA])
	}
	if p.ByBackend["backend-"+bB] != 1 {
		t.Errorf("backend-B count=%d", p.ByBackend["backend-"+bB])
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

func TestAggregatedBackendTag(t *testing.T) {
	const (
		u = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		b = "44444444-4444-4444-4444-444444444444"
	)
	cases := []struct {
		name   string
		chains []string
		want   string
	}{
		{"per-user single", []string{"b-" + u + "-" + b}, "backend-" + b},
		{"urltest order: per-user first", []string{"b-" + u + "-" + b, "auto-" + u}, "backend-" + b},
		{"urltest order: auto first", []string{"auto-" + u, "b-" + u + "-" + b}, "backend-" + b},
		{"direct only", []string{"direct"}, "direct"},
		{"empty", []string{}, "direct"},
		{"malformed b-", []string{"b-too-short"}, "b-too-short"},
	}
	for _, c := range cases {
		got := aggregatedBackendTag(c.chains)
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}
