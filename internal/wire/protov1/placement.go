package protov1

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	agentv1 "github.com/lannister-dev/go-node-agent/pkg/proto/vpn/agent/v1"
)

func UnmarshalPlacementCommandEvent(data []byte) (domain.PlacementCommand, error) {
	var pb agentv1.PlacementCommandEvent
	if err := proto.Unmarshal(data, &pb); err != nil {
		return domain.PlacementCommand{}, fmt.Errorf("protov1: decode placement command: %w", err)
	}
	if pb.GetEventId() == "" || pb.GetPlacementId() == "" || pb.GetEnvelope().GetNodeId() == "" {
		return domain.PlacementCommand{}, errors.New("protov1: placement command missing ids")
	}
	if pb.GetOpVersion() == 0 {
		return domain.PlacementCommand{}, errors.New("protov1: placement command op_version must be >= 1")
	}
	desired := desiredFromProto(pb.GetDesiredState())
	if !desired.Valid() {
		return domain.PlacementCommand{}, fmt.Errorf("protov1: invalid desired_state %s", pb.GetDesiredState())
	}

	emittedAt := timeFromProto(pb.GetEnvelope().GetEmittedAt())
	cmd := domain.PlacementCommand{
		EventID:          pb.GetEventId(),
		NodeID:           domain.NodeID(pb.GetEnvelope().GetNodeId()),
		EmittedAt:        emittedAt,
		SnapshotComplete: pb.GetSnapshotComplete(),
		Placement: domain.Placement{
			ID:            domain.PlacementID(pb.GetPlacementId()),
			KeyID:         domain.KeyID(pb.GetKeyId()),
			ClientID:      domain.ClientID(pb.GetClientId()),
			NodeID:        domain.NodeID(pb.GetEnvelope().GetNodeId()),
			BackendNodeID: domain.BackendID(pb.GetBackendNodeId()),
			OpVersion:     domain.OpVersion(pb.GetOpVersion()),
			Desired:       desired,
			Protocol:      protocolFromProto(pb.GetProtocol()),
			Transport:     transportFromProto(pb.GetTransport()),
			IsRevoked:     pb.GetIsRevoked(),
			ValidUntil:    timeFromProto(pb.GetValidUntil()),
			UpdatedAt:     timeFromProto(pb.GetUpdatedAt()),
		},
	}
	if pb.GetEnvelope().GetSnapshotId() != "" {
		cmd.SnapshotID = pb.GetEnvelope().GetSnapshotId()
	}
	return cmd, nil
}

func MarshalApplyResultEvent(r domain.PlacementReport, eventID string, emittedAt time.Time) ([]byte, error) {
	if eventID == "" {
		return nil, errors.New("protov1: event_id required")
	}
	if r.PlacementID == "" || r.NodeID == "" {
		return nil, errors.New("protov1: result missing ids")
	}
	if r.OpVersion == 0 {
		return nil, errors.New("protov1: result op_version must be >= 1")
	}
	if !r.AppliedState.Valid() {
		return nil, fmt.Errorf("protov1: invalid applied_state %q", r.AppliedState)
	}
	if emittedAt.IsZero() {
		emittedAt = time.Now().UTC()
	}
	pb := &agentv1.PlacementApplyResultEvent{
		Envelope: &agentv1.AgentEnvelope{
			SchemaVersion: 1,
			NodeId:        string(r.NodeID),
			EmittedAt:     timeToProto(emittedAt),
		},
		EventId:      eventID,
		PlacementId:  string(r.PlacementID),
		OpVersion:    uint64(r.OpVersion),
		AppliedState: appliedToProto(r.AppliedState),
		Retryable:    r.Retryable,
	}
	if r.Status != "" {
		if !r.Status.Valid() {
			return nil, fmt.Errorf("protov1: invalid report_status %q", r.Status)
		}
		s := reportStatusToProto(r.Status)
		pb.ReportStatus = &s
	}
	if r.Err != "" {
		pb.Error = proto.String(r.Err)
	}
	return proto.Marshal(pb)
}
