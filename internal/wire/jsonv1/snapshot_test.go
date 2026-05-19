package jsonv1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func TestMarshalSnapshotRequestEvent_Minimal(t *testing.T) {
	raw, err := MarshalSnapshotRequestEvent(SnapshotRequest{
		NodeID:      "lv-01",
		RequestedAt: time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC),
		Reason:      SnapshotReasonStartup,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["node_id"] != "lv-01" || got["reason"] != "startup" {
		t.Errorf("required fields: %+v", got)
	}
	if got["schema_version"].(float64) != 1 {
		t.Errorf("schema_version: %v", got["schema_version"])
	}
	for _, k := range []string{"known_snapshot_id", "last_command_stream_seq", "last_seen_xray_uptime"} {
		if _, has := got[k]; has {
			t.Errorf("%s should be omitted when unset, got %v", k, got[k])
		}
	}
}

func TestMarshalSnapshotRequestEvent_WithOptionals(t *testing.T) {
	raw, err := MarshalSnapshotRequestEvent(SnapshotRequest{
		NodeID:               "lv-01",
		RequestedAt:          time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC),
		Reason:               SnapshotReasonXrayRestart,
		KnownSnapshotID:      "snap-7",
		LastCommandStreamSeq: 12345,
		LastSeenXrayUptime:   9999,
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if got["known_snapshot_id"] != "snap-7" {
		t.Errorf("known_snapshot_id: %v", got["known_snapshot_id"])
	}
	if got["last_command_stream_seq"].(float64) != 12345 {
		t.Errorf("seq: %v", got["last_command_stream_seq"])
	}
	if got["last_seen_xray_uptime"].(float64) != 9999 {
		t.Errorf("uptime: %v", got["last_seen_xray_uptime"])
	}
}

func TestMarshalSnapshotRequestEvent_Rejects(t *testing.T) {
	cases := map[string]SnapshotRequest{
		"missing node_id": {Reason: SnapshotReasonStartup},
		"bad reason":      {NodeID: "n", Reason: "weird"},
		"empty reason":    {NodeID: "n"},
	}
	for name, req := range cases {
		if _, err := MarshalSnapshotRequestEvent(req); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestUnmarshalSnapshotChunkEvent_TwoItems(t *testing.T) {
	body := []byte(`{
		"schema_version": 1,
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"snapshot_id": "snap-7",
		"chunk_index": 0,
		"is_last_chunk": false,
		"items": [
			{
				"schema_version": 1,
				"node_id": "lv-01",
				"emitted_at": "2026-05-19T10:00:00Z",
				"event_id": "evt-1",
				"placement_id": "p-1",
				"key_id": "k-1",
				"op_version": 7,
				"desired_state": "active",
				"backend_node_id": "praha-02",
				"protocol": "vless",
				"transport": "ws",
				"client_id": "uuid-a",
				"is_revoked": false,
				"snapshot_complete": false
			},
			{
				"schema_version": 1,
				"node_id": "lv-01",
				"emitted_at": "2026-05-19T10:00:00Z",
				"event_id": "evt-2",
				"placement_id": "p-2",
				"key_id": "k-2",
				"op_version": 3,
				"desired_state": "inactive",
				"backend_node_id": "latvia-01",
				"protocol": "vless",
				"transport": "reality",
				"client_id": "uuid-b",
				"is_revoked": true,
				"snapshot_complete": false
			}
		]
	}`)
	chunk, err := UnmarshalSnapshotChunkEvent(body)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if chunk.NodeID != "lv-01" || chunk.SnapshotID != "snap-7" || chunk.IsLastChunk {
		t.Errorf("envelope: %+v", chunk)
	}
	if len(chunk.Items) != 2 {
		t.Fatalf("items: %d", len(chunk.Items))
	}
	if chunk.Items[0].Placement.ID != "p-1" || chunk.Items[1].Placement.ID != "p-2" {
		t.Errorf("items shape: %+v", chunk.Items)
	}
	if chunk.Items[0].Placement.Desired != domain.DesiredActive {
		t.Errorf("item 0 state: %s", chunk.Items[0].Placement.Desired)
	}
	if !chunk.Items[1].Placement.IsRevoked {
		t.Error("item 1 IsRevoked should be true")
	}
}

func TestUnmarshalSnapshotChunkEvent_RejectsBadItem(t *testing.T) {
	body := []byte(`{
		"schema_version": 1,
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"chunk_index": 0,
		"is_last_chunk": true,
		"items": [{
			"event_id": "evt-x",
			"placement_id": "p-x",
			"node_id": "lv-01",
			"emitted_at": "2026-05-19T10:00:00Z",
			"op_version": 0,
			"desired_state": "weird"
		}]
	}`)
	if _, err := UnmarshalSnapshotChunkEvent(body); err == nil {
		t.Fatal("expected error from bad item")
	}
}

func TestUnmarshalSnapshotChunkEvent_RejectsMissingNodeID(t *testing.T) {
	body := []byte(`{"chunk_index":0,"is_last_chunk":true,"items":[],"emitted_at":"2026-05-19T10:00:00Z"}`)
	_, err := UnmarshalSnapshotChunkEvent(body)
	if err == nil || !strings.Contains(err.Error(), "node_id") {
		t.Fatalf("expected node_id error, got %v", err)
	}
}
