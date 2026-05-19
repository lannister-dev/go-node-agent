package wire

import (
	"strings"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type SubjectPrefixes struct {
	Command    string
	Result     string
	Snapshot   string
	Heartbeat  string
	SyncReport string
}

func (p SubjectPrefixes) normalized() SubjectPrefixes {
	return SubjectPrefixes{
		Command:    strings.TrimRight(p.Command, "."),
		Result:     strings.TrimRight(p.Result, "."),
		Snapshot:   strings.TrimRight(p.Snapshot, "."),
		Heartbeat:  strings.TrimRight(p.Heartbeat, "."),
		SyncReport: strings.TrimRight(p.SyncReport, "."),
	}
}

type Subjects struct {
	p SubjectPrefixes
}

func NewSubjects(p SubjectPrefixes) Subjects {
	return Subjects{p: p.normalized()}
}

func (s Subjects) PlacementCommand(nodeID domain.NodeID) string {
	return s.p.Command + "." + string(nodeID) + ".commands"
}

func (s Subjects) PlacementResult(nodeID domain.NodeID) string {
	return s.p.Result + "." + string(nodeID) + ".results"
}

func (s Subjects) PlacementResultAck(nodeID domain.NodeID) string {
	return s.p.Result + "." + string(nodeID) + ".acks"
}

func (s Subjects) SnapshotRequest(nodeID domain.NodeID) string {
	return s.p.Snapshot + "." + string(nodeID) + ".request"
}

func (s Subjects) SnapshotChunk(nodeID domain.NodeID) string {
	return s.p.Snapshot + "." + string(nodeID) + ".chunks"
}

func (s Subjects) Heartbeat(nodeID domain.NodeID) string {
	return s.p.Heartbeat + "." + string(nodeID) + ".events"
}

func (s Subjects) SyncReport(nodeID domain.NodeID) string {
	return s.p.SyncReport + "." + string(nodeID) + ".events"
}

func (s Subjects) SyncReportAck(nodeID domain.NodeID) string {
	return s.p.SyncReport + "." + string(nodeID) + ".acks"
}

func (s Subjects) UpstreamChanged(nodeID domain.NodeID) string {
	return s.p.Command + "." + string(nodeID) + ".upstream"
}

func (s Subjects) PoolChanged(nodeID domain.NodeID) string {
	return s.p.Command + "." + string(nodeID) + ".pool"
}
