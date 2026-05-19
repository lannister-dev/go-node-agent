package jsonv1

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMarshalSyncReportEvent_FullResync(t *testing.T) {
	raw, err := MarshalSyncReportEvent(SyncReport{
		EventID:             "evt-sync-1",
		NodeID:              "lv-01",
		EmittedAt:           time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC),
		SyncedCount:         12,
		FullResyncCompleted: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if got["full_resync_completed"] != true {
		t.Errorf("full_resync_completed: %v", got["full_resync_completed"])
	}
	if got["synced_count"].(float64) != 12 {
		t.Errorf("synced_count: %v", got["synced_count"])
	}
	for _, k := range []string{"config_version", "inventory_hash", "inventory_count"} {
		if _, has := got[k]; has {
			t.Errorf("%s should be omitted when unset", k)
		}
	}
}

func TestMarshalSyncReportEvent_WithInventory(t *testing.T) {
	raw, err := MarshalSyncReportEvent(SyncReport{
		EventID:        "evt-sync-2",
		NodeID:         "lv-01",
		SyncedCount:    100,
		InventoryHash:  "sha256:abc",
		InventoryCount: 100,
		ConfigVersion:  42,
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if got["inventory_hash"] != "sha256:abc" || got["inventory_count"].(float64) != 100 {
		t.Errorf("inventory: %+v", got)
	}
	if got["config_version"].(float64) != 42 {
		t.Errorf("config_version: %v", got["config_version"])
	}
}

func TestMarshalSyncReportEvent_Rejects(t *testing.T) {
	cases := map[string]SyncReport{
		"missing event_id": {NodeID: "n"},
		"missing node_id":  {EventID: "e"},
	}
	for name, r := range cases {
		if _, err := MarshalSyncReportEvent(r); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
