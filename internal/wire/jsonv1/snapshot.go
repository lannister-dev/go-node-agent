package jsonv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

const (
	SnapshotReasonStartup        = "startup"
	SnapshotReasonXrayRestart    = "xray_restart"
	SnapshotReasonRedeliveryGap  = "redelivery_gap"
	SnapshotReasonOperatorForced = "operator_forced"
)

type snapshotRequestDTO struct {
	SchemaVersion        int     `json:"schema_version"`
	NodeID               string  `json:"node_id"`
	RequestedAt          string  `json:"requested_at"`
	Reason               string  `json:"reason"`
	KnownSnapshotID      *string `json:"known_snapshot_id,omitempty"`
	LastCommandStreamSeq *uint64 `json:"last_command_stream_seq,omitempty"`
	LastSeenXrayUptime   *uint64 `json:"last_seen_xray_uptime,omitempty"`
}

type SnapshotRequest struct {
	NodeID               domain.NodeID
	RequestedAt          time.Time
	Reason               string
	KnownSnapshotID      string
	LastCommandStreamSeq uint64
	LastSeenXrayUptime   uint64
}

func MarshalSnapshotRequestEvent(req SnapshotRequest) ([]byte, error) {
	if req.NodeID == "" {
		return nil, errors.New("jsonv1: snapshot request node_id required")
	}
	if !validSnapshotReason(req.Reason) {
		return nil, fmt.Errorf("jsonv1: invalid snapshot reason %q", req.Reason)
	}
	at := req.RequestedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	dto := snapshotRequestDTO{
		SchemaVersion: schemaVersion,
		NodeID:        string(req.NodeID),
		RequestedAt:   at.UTC().Format(time.RFC3339Nano),
		Reason:        req.Reason,
	}
	if req.KnownSnapshotID != "" {
		s := req.KnownSnapshotID
		dto.KnownSnapshotID = &s
	}
	if req.LastCommandStreamSeq > 0 {
		v := req.LastCommandStreamSeq
		dto.LastCommandStreamSeq = &v
	}
	if req.LastSeenXrayUptime > 0 {
		v := req.LastSeenXrayUptime
		dto.LastSeenXrayUptime = &v
	}
	return json.Marshal(dto)
}

func validSnapshotReason(r string) bool {
	switch r {
	case SnapshotReasonStartup, SnapshotReasonXrayRestart, SnapshotReasonRedeliveryGap, SnapshotReasonOperatorForced:
		return true
	}
	return false
}

type snapshotChunkDTO struct {
	SchemaVersion uint32                `json:"schema_version"`
	NodeID        string                `json:"node_id"`
	EmittedAt     string                `json:"emitted_at"`
	SnapshotID    *string               `json:"snapshot_id,omitempty"`
	Epoch         *uint64               `json:"epoch,omitempty"`
	ChunkIndex    uint32                `json:"chunk_index"`
	IsLastChunk   bool                  `json:"is_last_chunk"`
	Items         []placementCommandDTO `json:"items"`
}

type SnapshotChunk struct {
	NodeID      domain.NodeID
	EmittedAt   time.Time
	SnapshotID  string
	ChunkIndex  uint32
	IsLastChunk bool
	Items       []domain.PlacementCommand
}

func UnmarshalSnapshotChunkEvent(data []byte) (SnapshotChunk, error) {
	var dto snapshotChunkDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return SnapshotChunk{}, fmt.Errorf("jsonv1: decode snapshot_chunk: %w", err)
	}
	if dto.NodeID == "" {
		return SnapshotChunk{}, errors.New("jsonv1: snapshot chunk missing node_id")
	}
	emittedAt, err := parseTime(dto.EmittedAt)
	if err != nil {
		return SnapshotChunk{}, fmt.Errorf("jsonv1: snapshot chunk emitted_at: %w", err)
	}
	out := SnapshotChunk{
		NodeID:      domain.NodeID(dto.NodeID),
		EmittedAt:   emittedAt,
		ChunkIndex:  dto.ChunkIndex,
		IsLastChunk: dto.IsLastChunk,
		Items:       make([]domain.PlacementCommand, 0, len(dto.Items)),
	}
	if dto.SnapshotID != nil {
		out.SnapshotID = *dto.SnapshotID
	}
	for i, itemDTO := range dto.Items {
		raw, err := json.Marshal(itemDTO)
		if err != nil {
			return SnapshotChunk{}, fmt.Errorf("jsonv1: re-marshal snapshot item %d: %w", i, err)
		}
		cmd, err := UnmarshalPlacementCommandEvent(raw)
		if err != nil {
			return SnapshotChunk{}, fmt.Errorf("jsonv1: snapshot item %d: %w", i, err)
		}
		out.Items = append(out.Items, cmd)
	}
	return out, nil
}
