package protov1

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	agentv1 "github.com/lannister-dev/go-node-agent/pkg/proto/vpn/agent/v1"
)

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
		return nil, errors.New("protov1: snapshot request node_id required")
	}
	reason := snapshotReasonToProto(req.Reason)
	if reason == agentv1.SnapshotRequestReason_SNAPSHOT_REQUEST_REASON_UNSPECIFIED {
		return nil, fmt.Errorf("protov1: invalid snapshot reason %q", req.Reason)
	}
	at := req.RequestedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	pb := &agentv1.SnapshotRequestEvent{
		SchemaVersion: 1,
		NodeId:        string(req.NodeID),
		RequestedAt:   timeToProto(at),
		Reason:        reason,
	}
	if req.KnownSnapshotID != "" {
		pb.KnownSnapshotId = proto.String(req.KnownSnapshotID)
	}
	if req.LastCommandStreamSeq > 0 {
		pb.LastCommandStreamSeq = proto.Uint64(req.LastCommandStreamSeq)
	}
	if req.LastSeenXrayUptime > 0 {
		pb.LastSeenXrayUptime = proto.Uint64(req.LastSeenXrayUptime)
	}
	return proto.Marshal(pb)
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
	var pb agentv1.SnapshotChunkEvent
	if err := proto.Unmarshal(data, &pb); err != nil {
		return SnapshotChunk{}, fmt.Errorf("protov1: decode snapshot_chunk: %w", err)
	}
	if pb.GetEnvelope().GetNodeId() == "" {
		return SnapshotChunk{}, errors.New("protov1: snapshot chunk missing node_id")
	}
	out := SnapshotChunk{
		NodeID:      domain.NodeID(pb.GetEnvelope().GetNodeId()),
		EmittedAt:   timeFromProto(pb.GetEnvelope().GetEmittedAt()),
		ChunkIndex:  pb.GetChunkIndex(),
		IsLastChunk: pb.GetIsLastChunk(),
		Items:       make([]domain.PlacementCommand, 0, len(pb.GetItems())),
	}
	if pb.GetEnvelope().GetSnapshotId() != "" {
		out.SnapshotID = pb.GetEnvelope().GetSnapshotId()
	}
	for i, item := range pb.GetItems() {
		raw, err := proto.Marshal(item)
		if err != nil {
			return SnapshotChunk{}, fmt.Errorf("protov1: re-marshal item %d: %w", i, err)
		}
		cmd, err := UnmarshalPlacementCommandEvent(raw)
		if err != nil {
			return SnapshotChunk{}, fmt.Errorf("protov1: snapshot item %d: %w", i, err)
		}
		out.Items = append(out.Items, cmd)
	}
	return out, nil
}
