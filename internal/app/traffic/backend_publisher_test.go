package traffic

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/adapters/xray"
)

type fakeXrayStats struct {
	mu    sync.Mutex
	calls int
	snaps [][]xray.UserStat
	err   error
}

func (f *fakeXrayStats) QueryUserStats(_ context.Context, _ bool) ([]xray.UserStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	i := f.calls
	f.calls++
	if i < len(f.snaps) {
		return f.snaps[i], nil
	}
	return nil, nil
}

func TestBackendPublisher_AggregatesPerUserAndNodeTotal(t *testing.T) {
	const (
		backendID = "44444444-4444-4444-4444-444444444444"
		u1        = "user-aaaa"
		u2        = "user-bbbb"
	)
	stats := &fakeXrayStats{snaps: [][]xray.UserStat{{
		{ClientID: u1, Link: "uplink", Value: 100},
		{ClientID: u1, Link: "downlink", Value: 200},
		{ClientID: u2, Link: "uplink", Value: 50},
		{ClientID: u2, Link: "downlink", Value: 70},
	}}}
	pub := &fakePub{}
	p, err := NewBackendPublisher(BackendPublisherConfig{
		NodeID:             backendID,
		NodeTrafficSubject: "nodes.traffic",
		UserTrafficSubject: "users.traffic",
	}, pub, &kvCapture{}, stats, silentLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(pub.msgs) != 2 {
		t.Fatalf("expected 2 publishes (node+users), got %d", len(pub.msgs))
	}
	// node-level
	var nodeMsg []nodeTrafficPayload
	for _, m := range pub.msgs {
		if m.subject == "nodes.traffic" {
			_ = json.Unmarshal(m.data, &nodeMsg)
		}
	}
	if len(nodeMsg) != 1 {
		t.Fatalf("node msg: %+v", nodeMsg)
	}
	if nodeMsg[0].BackendNodeID != backendID || nodeMsg[0].EntryNodeID != "" {
		t.Errorf("node msg id: %+v", nodeMsg[0])
	}
	if nodeMsg[0].BytesIn != 150 || nodeMsg[0].BytesOut != 270 {
		t.Errorf("node msg bytes: in=%d out=%d (want in=150 out=270)", nodeMsg[0].BytesIn, nodeMsg[0].BytesOut)
	}
	// user-level
	var userMsg []userTrafficDelta
	for _, m := range pub.msgs {
		if m.subject == "users.traffic" {
			_ = json.Unmarshal(m.data, &userMsg)
		}
	}
	if len(userMsg) != 2 {
		t.Fatalf("user msg: %+v", userMsg)
	}
	byID := map[string]userTrafficDelta{}
	for _, u := range userMsg {
		byID[u.Identifier] = u
	}
	if byID[u1].DeltaBytes != 300 {
		t.Errorf("u1: %+v (want delta_bytes=300)", byID[u1])
	}
	if byID[u2].DeltaBytes != 120 {
		t.Errorf("u2: %+v (want delta_bytes=120)", byID[u2])
	}
}

func TestBackendPublisher_ExcludesProbe(t *testing.T) {
	const (
		backendID = "44444444-4444-4444-4444-444444444444"
		user      = "user-real"
		probe     = "user-probe"
	)
	stats := &fakeXrayStats{snaps: [][]xray.UserStat{{
		{ClientID: user, Link: "uplink", Value: 100},
		{ClientID: user, Link: "downlink", Value: 200},
		{ClientID: probe, Link: "uplink", Value: 999},
		{ClientID: probe, Link: "downlink", Value: 999},
	}}}
	pub := &fakePub{}
	kv := &kvCapture{}
	p, err := NewBackendPublisher(BackendPublisherConfig{
		NodeID:             backendID,
		NodeTrafficSubject: "nodes.traffic",
		UserTrafficSubject: "users.traffic",
		ProbeClientIDs:     []string{probe},
	}, pub, kv, stats, silentLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, m := range pub.msgs {
		if m.subject == "nodes.traffic" {
			var nm []nodeTrafficPayload
			_ = json.Unmarshal(m.data, &nm)
			if nm[0].BytesIn != 100 || nm[0].BytesOut != 200 {
				t.Errorf("probe leaked into node totals: %+v", nm[0])
			}
		}
		if m.subject == "users.traffic" {
			var um []userTrafficDelta
			_ = json.Unmarshal(m.data, &um)
			if len(um) != 1 || um[0].Identifier != user {
				t.Errorf("probe leaked into user deltas: %+v", um)
			}
		}
	}
	var live backendLivePayload
	if err := json.Unmarshal(kv.entries[StatsKvBucket+"/backend."+backendID], &live); err != nil {
		t.Fatal(err)
	}
	for _, id := range live.ActiveClientIDs {
		if id == probe {
			t.Errorf("probe leaked into active client ids: %+v", live.ActiveClientIDs)
		}
	}
}

func TestBackendPublisher_NoPublishOnEmpty(t *testing.T) {
	stats := &fakeXrayStats{snaps: [][]xray.UserStat{{}}}
	pub := &fakePub{}
	p, _ := NewBackendPublisher(BackendPublisherConfig{
		NodeID:             "44444444-4444-4444-4444-444444444444",
		NodeTrafficSubject: "nodes.traffic",
		UserTrafficSubject: "users.traffic",
	}, pub, &kvCapture{}, stats, silentLog())
	_ = p.tick(t.Context())
	if len(pub.msgs) != 0 {
		t.Errorf("expected no publishes, got %d", len(pub.msgs))
	}
}

func TestBackendPublisher_SkipsZeroValues(t *testing.T) {
	stats := &fakeXrayStats{snaps: [][]xray.UserStat{{
		{ClientID: "u1", Link: "uplink", Value: 0},
		{ClientID: "u1", Link: "downlink", Value: 0},
	}}}
	pub := &fakePub{}
	p, _ := NewBackendPublisher(BackendPublisherConfig{
		NodeID:             "44444444-4444-4444-4444-444444444444",
		NodeTrafficSubject: "nodes.traffic",
	}, pub, &kvCapture{}, stats, silentLog())
	_ = p.tick(t.Context())
	if len(pub.msgs) != 0 {
		t.Errorf("expected no publishes, got %d", len(pub.msgs))
	}
}
