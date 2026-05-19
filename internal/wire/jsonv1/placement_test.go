package jsonv1

import (
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func TestUnmarshalPlacementCommandEvent_Full(t *testing.T) {
	data := []byte(`{
		"schema_version": 1,
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00.123456+00:00",
		"snapshot_id": "snap-7",
		"epoch": 42,
		"event_id": "evt-1",
		"placement_id": "p-001",
		"key_id": "k-001",
		"op_version": 7,
		"desired_state": "active",
		"backend_node_id": "praha-02",
		"protocol": "vless",
		"transport": "ws",
		"client_id": "01234567-89ab-cdef-0123-456789abcdef",
		"is_revoked": false,
		"snapshot_complete": true,
		"valid_until": "2027-01-01T00:00:00Z",
		"updated_at": "2026-05-19T09:59:55Z"
	}`)
	cmd, err := UnmarshalPlacementCommandEvent(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cmd.EventID != "evt-1" || cmd.SnapshotID != "snap-7" || !cmd.SnapshotComplete {
		t.Errorf("envelope: %+v", cmd)
	}
	if cmd.Placement.ID != "p-001" || cmd.Placement.OpVersion != 7 || cmd.Placement.Desired != domain.DesiredActive {
		t.Errorf("placement: %+v", cmd.Placement)
	}
	if cmd.Placement.Transport != domain.TransportWS || cmd.Placement.Protocol != domain.ProtocolVLESS {
		t.Errorf("transport/protocol: %+v", cmd.Placement)
	}
	if cmd.Placement.ValidUntil.IsZero() {
		t.Error("ValidUntil should be parsed")
	}
}

func TestUnmarshalPlacementCommandEvent_OmitsOptional(t *testing.T) {
	data := []byte(`{
		"schema_version": 1,
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"event_id": "evt-2",
		"placement_id": "p-002",
		"key_id": "k-002",
		"op_version": 1,
		"desired_state": "inactive",
		"backend_node_id": "praha-02",
		"protocol": "vless",
		"transport": "reality",
		"client_id": "01234567-89ab-cdef-0123-456789abcdef",
		"is_revoked": true,
		"snapshot_complete": false
	}`)
	cmd, err := UnmarshalPlacementCommandEvent(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cmd.Placement.Desired != domain.DesiredInactive {
		t.Errorf("desired_state: %v", cmd.Placement.Desired)
	}
	if !cmd.Placement.IsRevoked {
		t.Error("is_revoked should be true")
	}
	if !cmd.Placement.ValidUntil.IsZero() || !cmd.Placement.UpdatedAt.IsZero() {
		t.Errorf("optional times should be zero: %v %v", cmd.Placement.ValidUntil, cmd.Placement.UpdatedAt)
	}
	if cmd.SnapshotID != "" {
		t.Errorf("snapshot_id should be empty: %q", cmd.SnapshotID)
	}
}

func TestUnmarshalPlacementCommandEvent_Rejects(t *testing.T) {
	cases := map[string]string{
		"bad json":         `{`,
		"missing event_id": `{"placement_id":"p","node_id":"n","emitted_at":"2026-01-01T00:00:00Z","op_version":1,"desired_state":"active"}`,
		"zero op_version":  `{"event_id":"e","placement_id":"p","node_id":"n","emitted_at":"2026-01-01T00:00:00Z","op_version":0,"desired_state":"active"}`,
		"bad desired":      `{"event_id":"e","placement_id":"p","node_id":"n","emitted_at":"2026-01-01T00:00:00Z","op_version":1,"desired_state":"weird"}`,
		"bad transport":    `{"event_id":"e","placement_id":"p","node_id":"n","emitted_at":"2026-01-01T00:00:00Z","op_version":1,"desired_state":"active","transport":"smoke"}`,
	}
	for name, body := range cases {
		if _, err := UnmarshalPlacementCommandEvent([]byte(body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
