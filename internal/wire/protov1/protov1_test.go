package protov1

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	agentv1 "github.com/lannister-dev/go-node-agent/pkg/proto/vpn/agent/v1"
)

func ptrF(v float64) *float64 { return &v }

func TestHeartbeat_RoundTrip(t *testing.T) {
	at := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	src := domain.Heartbeat{
		NodeID:       "lv-01",
		At:           at,
		IsHealthy:    true,
		Ready:        true,
		LastError:    "prev failed",
		PollCount:    100,
		Applied:      85,
		Failed:       2,
		CPUPct:       ptrF(13.5),
		MemPct:       ptrF(42.7),
		BandwidthPct: ptrF(7.1),
		Pool: &domain.HeartbeatPool{
			SlotsTotal: 4, SlotsActive: 3, LastApplyOk: true,
			LastAppliedAt: at.Add(-5 * time.Second),
		},
		Upstream: &domain.HeartbeatUpstream{
			Configured: true, LastApplyOk: true, UpstreamNodeID: "praha-02",
		},
	}
	data, err := MarshalHeartbeatEvent(src, "evt-1", "1.2.3")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, eventID, version, err := UnmarshalHeartbeatEvent(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if eventID != "evt-1" || version != "1.2.3" {
		t.Errorf("eventID=%s version=%s", eventID, version)
	}
	if got.NodeID != src.NodeID || got.PollCount != src.PollCount || got.Failed != src.Failed {
		t.Errorf("counters: %+v", got)
	}
	if *got.CPUPct != *src.CPUPct || *got.BandwidthPct != *src.BandwidthPct {
		t.Errorf("metrics: cpu=%v bw=%v", got.CPUPct, got.BandwidthPct)
	}
	if got.Pool == nil || got.Pool.SlotsActive != 3 {
		t.Errorf("pool: %+v", got.Pool)
	}
	if got.Upstream == nil || got.Upstream.UpstreamNodeID != "praha-02" {
		t.Errorf("upstream: %+v", got.Upstream)
	}
}

func TestHeartbeat_RejectsInvalid(t *testing.T) {
	at := time.Now()
	cases := map[string]struct {
		h   domain.Heartbeat
		eid string
		ver string
	}{
		"missing event_id": {domain.Heartbeat{NodeID: "n", At: at}, "", "v"},
		"missing node_id":  {domain.Heartbeat{At: at}, "e", "v"},
		"zero time":        {domain.Heartbeat{NodeID: "n"}, "e", "v"},
	}
	for name, c := range cases {
		if _, err := MarshalHeartbeatEvent(c.h, c.eid, c.ver); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func buildSamplePlacementCommandEvent() []byte {
	pb := &agentv1.PlacementCommandEvent{
		Envelope: &agentv1.AgentEnvelope{
			SchemaVersion: 1,
			NodeId:        "lv-01",
			EmittedAt:     timeToProto(time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC)),
		},
		EventId:       "evt-cmd",
		PlacementId:   "p-1",
		KeyId:         "k-1",
		OpVersion:     7,
		DesiredState:  agentv1.DesiredState_DESIRED_STATE_ACTIVE,
		BackendNodeId: "praha-02",
		Protocol:      agentv1.Protocol_PROTOCOL_VLESS,
		Transport:     agentv1.TransportKind_TRANSPORT_KIND_REALITY,
		ClientId:      "uuid-a",
		IsRevoked:     false,
	}
	data, _ := proto.Marshal(pb)
	return data
}

func TestPlacementCommand_Unmarshal(t *testing.T) {
	cmd, err := UnmarshalPlacementCommandEvent(buildSamplePlacementCommandEvent())
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cmd.EventID != "evt-cmd" || cmd.Placement.ID != "p-1" || cmd.Placement.OpVersion != 7 {
		t.Errorf("cmd: %+v", cmd)
	}
	if cmd.Placement.Desired != domain.DesiredActive {
		t.Errorf("desired: %v", cmd.Placement.Desired)
	}
	if cmd.Placement.Transport != domain.TransportReality {
		t.Errorf("transport: %v", cmd.Placement.Transport)
	}
}

func TestPlacementCommand_RejectsMissingFields(t *testing.T) {
	pb := &agentv1.PlacementCommandEvent{
		Envelope: &agentv1.AgentEnvelope{},
	}
	data, _ := proto.Marshal(pb)
	if _, err := UnmarshalPlacementCommandEvent(data); err == nil {
		t.Fatal("expected error for missing ids")
	}
}

func TestApplyResult_Marshal(t *testing.T) {
	at := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	r := domain.PlacementReport{
		PlacementID:  "p-1",
		NodeID:       "lv-01",
		OpVersion:    7,
		AppliedState: domain.AppliedOk,
		Status:       domain.ReportApplied,
	}
	data, err := MarshalApplyResultEvent(r, "evt-r", at)
	if err != nil {
		t.Fatal(err)
	}
	var pb agentv1.PlacementApplyResultEvent
	if err := proto.Unmarshal(data, &pb); err != nil {
		t.Fatal(err)
	}
	if pb.GetPlacementId() != "p-1" || pb.GetOpVersion() != 7 {
		t.Errorf("result: %+v", &pb)
	}
	if pb.GetAppliedState() != agentv1.AppliedState_APPLIED_STATE_APPLIED {
		t.Errorf("applied_state: %v", pb.GetAppliedState())
	}
	if pb.GetReportStatus() != agentv1.ReportStatus_REPORT_STATUS_APPLIED {
		t.Errorf("report_status: %v", pb.GetReportStatus())
	}
}

func TestSnapshotRequest_Marshal(t *testing.T) {
	data, err := MarshalSnapshotRequestEvent(SnapshotRequest{
		NodeID:      "lv-01",
		RequestedAt: time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC),
		Reason:      SnapshotReasonStartup,
	})
	if err != nil {
		t.Fatal(err)
	}
	var pb agentv1.SnapshotRequestEvent
	_ = proto.Unmarshal(data, &pb)
	if pb.GetReason() != agentv1.SnapshotRequestReason_SNAPSHOT_REQUEST_REASON_STARTUP {
		t.Errorf("reason: %v", pb.GetReason())
	}
}

func TestSnapshotRequest_RejectsBadReason(t *testing.T) {
	if _, err := MarshalSnapshotRequestEvent(SnapshotRequest{NodeID: "n", Reason: "weird"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestSnapshotChunk_RoundTrip(t *testing.T) {
	item := &agentv1.PlacementCommandEvent{
		Envelope: &agentv1.AgentEnvelope{
			SchemaVersion: 1,
			NodeId:        "lv-01",
			EmittedAt:     timeToProto(time.Now()),
		},
		EventId: "evt", PlacementId: "p-1", KeyId: "k", OpVersion: 1,
		DesiredState: agentv1.DesiredState_DESIRED_STATE_ACTIVE,
		ClientId:     "uuid-a",
	}
	chunk := &agentv1.SnapshotChunkEvent{
		Envelope: &agentv1.AgentEnvelope{
			SchemaVersion: 1,
			NodeId:        "lv-01",
			EmittedAt:     timeToProto(time.Now()),
		},
		ChunkIndex:  0,
		IsLastChunk: true,
		Items:       []*agentv1.PlacementCommandEvent{item},
	}
	data, _ := proto.Marshal(chunk)
	got, err := UnmarshalSnapshotChunkEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsLastChunk || len(got.Items) != 1 || got.Items[0].Placement.ID != "p-1" {
		t.Errorf("chunk: %+v", got)
	}
}

func TestSyncReport_Marshal(t *testing.T) {
	data, err := MarshalSyncReportEvent(SyncReport{
		EventID: "e", NodeID: "lv-01", SyncedCount: 5, FullResyncCompleted: true,
		InventoryHash: "sha256:abc",
	})
	if err != nil {
		t.Fatal(err)
	}
	var pb agentv1.SyncReportEvent
	_ = proto.Unmarshal(data, &pb)
	if !pb.GetFullResyncCompleted() || pb.GetSyncedCount() != 5 {
		t.Errorf("sync: %+v", &pb)
	}
	if pb.GetInventoryHash() != "sha256:abc" {
		t.Errorf("inventory_hash: %v", pb.GetInventoryHash())
	}
}

func TestUpstreamChanged_Unmarshal(t *testing.T) {
	pb := &agentv1.UpstreamChangedPayload{
		SchemaVersion:        1,
		EventId:              "e",
		NodeId:               "lv-01",
		EmittedAt:            timeToProto(time.Now()),
		UpstreamNodeId:       "praha-02",
		UpstreamPublicDomain: "praha-02.vpn.example.com",
		UpstreamRealityIp:    proto.String("10.0.0.42"),
	}
	data, _ := proto.Marshal(pb)
	got, err := UnmarshalUpstreamChanged(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.BackendID != "praha-02" || got.RealityIP != "10.0.0.42" {
		t.Errorf("upstream: %+v", got)
	}
}
