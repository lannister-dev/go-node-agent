package protov1

import (
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	agentv1 "github.com/lannister-dev/go-node-agent/pkg/proto/vpn/agent/v1"
)

type SyncReport struct {
	EventID             string
	NodeID              domain.NodeID
	EmittedAt           time.Time
	SyncedCount         uint32
	ConfigVersion       uint64
	InventoryHash       string
	InventoryCount      uint64
	FullResyncCompleted bool
}

func MarshalSyncReportEvent(rep SyncReport) ([]byte, error) {
	if rep.EventID == "" {
		return nil, errors.New("protov1: sync_report event_id required")
	}
	if rep.NodeID == "" {
		return nil, errors.New("protov1: sync_report node_id required")
	}
	at := rep.EmittedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	pb := &agentv1.SyncReportEvent{
		SchemaVersion:       1,
		EventId:             rep.EventID,
		NodeId:              string(rep.NodeID),
		EmittedAt:           timeToProto(at),
		SyncedCount:         rep.SyncedCount,
		FullResyncCompleted: rep.FullResyncCompleted,
	}
	if rep.ConfigVersion > 0 {
		pb.ConfigVersion = proto.Uint64(rep.ConfigVersion)
	}
	if rep.InventoryHash != "" {
		pb.InventoryHash = proto.String(rep.InventoryHash)
	}
	if rep.InventoryCount > 0 {
		pb.InventoryCount = proto.Uint64(rep.InventoryCount)
	}
	return proto.Marshal(pb)
}
