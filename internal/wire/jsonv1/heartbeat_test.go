package jsonv1

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func ptrF(v float64) *float64 { return &v }

func TestMarshalHeartbeatEvent_Minimal(t *testing.T) {
	h := domain.Heartbeat{
		NodeID:    "lv-01",
		At:        time.Date(2026, 5, 19, 10, 30, 0, 0, time.UTC),
		IsHealthy: true,
		Ready:     true,
		PollCount: 7,
	}
	raw, err := MarshalHeartbeatEvent(h, "evt-1", "1.2.3")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	expect := map[string]any{
		"schema_version": float64(1),
		"event_id":       "evt-1",
		"node_id":        "lv-01",
		"emitted_at":     "2026-05-19T10:30:00Z",
		"agent_version":  "1.2.3",
		"is_healthy":     true,
		"ready":          true,
		"poll_count":     float64(7),
		"applied":        float64(0),
		"failed":         float64(0),
	}
	for k, v := range expect {
		if got[k] != v {
			t.Errorf("%s: got %v (%T), want %v", k, got[k], got[k], v)
		}
	}
	for _, k := range []string{"last_error", "cpu_pct", "mem_pct", "bandwidth_pct", "pool", "upstream"} {
		if _, ok := got[k]; ok {
			t.Errorf("%s should be omitted when nil/empty, got %v", k, got[k])
		}
	}
}

func TestMarshalHeartbeatEvent_WithMetricsAndPool(t *testing.T) {
	h := domain.Heartbeat{
		NodeID:       "lv-01",
		At:           time.Date(2026, 5, 19, 10, 30, 0, 0, time.UTC),
		IsHealthy:    true,
		Ready:        true,
		LastError:    "previous tick failed",
		PollCount:    100,
		Applied:      85,
		Failed:       2,
		CPUPct:       ptrF(13.5),
		MemPct:       ptrF(42.7),
		BandwidthPct: ptrF(7.1),
		Pool: &domain.HeartbeatPool{
			SlotsTotal:      4,
			SlotsActive:     3,
			DesiredBackends: 4,
			LastApplyOk:     true,
			LastAppliedAt:   time.Date(2026, 5, 19, 10, 29, 50, 0, time.UTC),
		},
		Upstream: &domain.HeartbeatUpstream{
			Configured:     true,
			LastApplyOk:    true,
			UpstreamNodeID: "praha-02",
			UpstreamHost:   "praha-02.vpn.example.com",
		},
	}
	raw, err := MarshalHeartbeatEvent(h, "evt-2", "1.2.3")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got["cpu_pct"].(float64) != 13.5 || got["mem_pct"].(float64) != 42.7 || got["bandwidth_pct"].(float64) != 7.1 {
		t.Errorf("metric values: %+v", got)
	}
	if got["last_error"].(string) != "previous tick failed" {
		t.Errorf("last_error: %v", got["last_error"])
	}
	pool, ok := got["pool"].(map[string]any)
	if !ok {
		t.Fatalf("expected pool object, got %T %v", got["pool"], got["pool"])
	}
	if pool["slots_total"].(float64) != 4 || pool["slots_active"].(float64) != 3 {
		t.Errorf("pool slots: %+v", pool)
	}
	if pool["last_applied_at"].(string) != "2026-05-19T10:29:50Z" {
		t.Errorf("pool.last_applied_at: %v", pool["last_applied_at"])
	}
	up, ok := got["upstream"].(map[string]any)
	if !ok {
		t.Fatalf("expected upstream object")
	}
	if up["upstream_node_id"].(string) != "praha-02" {
		t.Errorf("upstream_node_id: %v", up["upstream_node_id"])
	}
	if _, ok := up["upstream_addr"]; ok {
		t.Errorf("empty upstream_addr should be omitted, got %v", up["upstream_addr"])
	}
}

func TestMarshalHeartbeatEvent_ValidatesInputs(t *testing.T) {
	at := time.Now().UTC()
	cases := []struct {
		name string
		h    domain.Heartbeat
		eid  string
		ver  string
	}{
		{"missing event_id", domain.Heartbeat{NodeID: "n", At: at}, "", "v"},
		{"missing node_id", domain.Heartbeat{At: at}, "e", "v"},
		{"zero time", domain.Heartbeat{NodeID: "n"}, "e", "v"},
	}
	for _, c := range cases {
		if _, err := MarshalHeartbeatEvent(c.h, c.eid, c.ver); err == nil {
			t.Errorf("%s: expected error", c.name)
		}
	}
}
