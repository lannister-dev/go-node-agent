package jsonv1

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type syncReportDTO struct {
	SchemaVersion       int     `json:"schema_version"`
	EventID             string  `json:"event_id"`
	NodeID              string  `json:"node_id"`
	EmittedAt           string  `json:"emitted_at"`
	SyncedCount         uint32  `json:"synced_count"`
	ConfigVersion       *uint64 `json:"config_version,omitempty"`
	InventoryHash       *string `json:"inventory_hash,omitempty"`
	InventoryCount      *uint64 `json:"inventory_count,omitempty"`
	FullResyncCompleted bool    `json:"full_resync_completed"`
}

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
		return nil, errors.New("jsonv1: sync_report event_id required")
	}
	if rep.NodeID == "" {
		return nil, errors.New("jsonv1: sync_report node_id required")
	}
	at := rep.EmittedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	dto := syncReportDTO{
		SchemaVersion:       schemaVersion,
		EventID:             rep.EventID,
		NodeID:              string(rep.NodeID),
		EmittedAt:           at.UTC().Format(time.RFC3339Nano),
		SyncedCount:         rep.SyncedCount,
		FullResyncCompleted: rep.FullResyncCompleted,
	}
	if rep.ConfigVersion > 0 {
		v := rep.ConfigVersion
		dto.ConfigVersion = &v
	}
	if rep.InventoryHash != "" {
		s := rep.InventoryHash
		dto.InventoryHash = &s
	}
	if rep.InventoryCount > 0 {
		v := rep.InventoryCount
		dto.InventoryCount = &v
	}
	return json.Marshal(dto)
}
