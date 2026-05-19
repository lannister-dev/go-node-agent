package wire

import (
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func TestSubjects(t *testing.T) {
	s := NewSubjects(SubjectPrefixes{
		Command:    "agent.placements",
		Result:     "agent.placement_results",
		Snapshot:   "agent.snapshots",
		Heartbeat:  "agent.heartbeats",
		SyncReport: "agent.sync_reports",
	})
	const node domain.NodeID = "lv-01"

	cases := map[string]string{
		s.PlacementCommand(node):   "agent.placements.lv-01.commands",
		s.PlacementResult(node):    "agent.placement_results.lv-01.results",
		s.PlacementResultAck(node): "agent.placement_results.lv-01.acks",
		s.SnapshotRequest(node):    "agent.snapshots.lv-01.request",
		s.SnapshotChunk(node):      "agent.snapshots.lv-01.chunks",
		s.Heartbeat(node):          "agent.heartbeats.lv-01.events",
		s.SyncReport(node):         "agent.sync_reports.lv-01.events",
		s.SyncReportAck(node):      "agent.sync_reports.lv-01.acks",
		s.UpstreamChanged(node):    "agent.placements.lv-01.upstream",
		s.PoolChanged(node):        "agent.placements.lv-01.pool",
	}

	for got, want := range cases {
		if got != want {
			t.Errorf("subject mismatch:\n got=%q\nwant=%q", got, want)
		}
	}
}

func TestSubjects_TrimsTrailingDot(t *testing.T) {
	s := NewSubjects(SubjectPrefixes{
		Command: "agent.placements.",
	})
	got := s.PlacementCommand("n1")
	want := "agent.placements.n1.commands"
	if got != want {
		t.Fatalf("trailing dot not trimmed:\n got=%q\nwant=%q", got, want)
	}
}
