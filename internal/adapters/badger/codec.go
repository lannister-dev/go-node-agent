package badger

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	agentv1 "github.com/lannister-dev/go-node-agent/pkg/proto/vpn/agent/v1"
)

func marshalIdentity(id domain.NodeIdentity) ([]byte, error) {
	pb := &agentv1.StoredIdentity{
		NodeId:          string(id.NodeID),
		AgentInstanceId: id.AgentInstanceID,
		AuthToken:       id.AuthToken,
		BootstrappedAt:  timeToProto(id.BootstrappedAt),
	}
	return proto.Marshal(pb)
}

func unmarshalIdentity(data []byte) (domain.NodeIdentity, error) {
	pb := &agentv1.StoredIdentity{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return domain.NodeIdentity{}, fmt.Errorf("unmarshal stored identity: %w", err)
	}
	return domain.NodeIdentity{
		NodeID:          domain.NodeID(pb.GetNodeId()),
		AgentInstanceID: pb.GetAgentInstanceId(),
		AuthToken:       pb.GetAuthToken(),
		BootstrappedAt:  timeFromProto(pb.BootstrappedAt),
	}, nil
}

func marshalPlacement(p domain.Placement) ([]byte, error) {
	pb := &agentv1.StoredPlacement{
		Id:            string(p.ID),
		KeyId:         string(p.KeyID),
		ClientId:      string(p.ClientID),
		NodeId:        string(p.NodeID),
		BackendNodeId: string(p.BackendNodeID),
		OpVersion:     uint64(p.OpVersion),
		DesiredState:  desiredToProto(p.Desired),
		AppliedState:  appliedToProto(p.Applied),
		Transport:     transportToProto(p.Transport),
		Protocol:      protocolToProto(p.Protocol),
		IsRevoked:     p.IsRevoked,
		ValidUntil:    timeToProto(p.ValidUntil),
		UpdatedAt:     timeToProto(p.UpdatedAt),
		LastAppliedAt: timeToProto(p.LastAppliedAt),
	}
	return proto.Marshal(pb)
}

func unmarshalPlacement(data []byte) (domain.Placement, error) {
	pb := &agentv1.StoredPlacement{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return domain.Placement{}, fmt.Errorf("unmarshal stored placement: %w", err)
	}
	return domain.Placement{
		ID:            domain.PlacementID(pb.GetId()),
		KeyID:         domain.KeyID(pb.GetKeyId()),
		ClientID:      domain.ClientID(pb.GetClientId()),
		NodeID:        domain.NodeID(pb.GetNodeId()),
		BackendNodeID: domain.BackendID(pb.GetBackendNodeId()),
		OpVersion:     domain.OpVersion(pb.GetOpVersion()),
		Desired:       desiredFromProto(pb.GetDesiredState()),
		Applied:       appliedFromProto(pb.GetAppliedState()),
		Transport:     transportFromProto(pb.GetTransport()),
		Protocol:      protocolFromProto(pb.GetProtocol()),
		IsRevoked:     pb.GetIsRevoked(),
		ValidUntil:    timeFromProto(pb.ValidUntil),
		UpdatedAt:     timeFromProto(pb.UpdatedAt),
		LastAppliedAt: timeFromProto(pb.LastAppliedAt),
	}, nil
}

func desiredToProto(d domain.DesiredState) agentv1.DesiredState {
	switch d {
	case domain.DesiredActive:
		return agentv1.DesiredState_DESIRED_STATE_ACTIVE
	case domain.DesiredInactive:
		return agentv1.DesiredState_DESIRED_STATE_INACTIVE
	}
	return agentv1.DesiredState_DESIRED_STATE_UNSPECIFIED
}

func desiredFromProto(p agentv1.DesiredState) domain.DesiredState {
	switch p {
	case agentv1.DesiredState_DESIRED_STATE_ACTIVE:
		return domain.DesiredActive
	case agentv1.DesiredState_DESIRED_STATE_INACTIVE:
		return domain.DesiredInactive
	default:
		return ""
	}
}

func appliedToProto(a domain.AppliedState) agentv1.AppliedState {
	switch a {
	case domain.AppliedPending:
		return agentv1.AppliedState_APPLIED_STATE_PENDING
	case domain.AppliedOk:
		return agentv1.AppliedState_APPLIED_STATE_APPLIED
	case domain.AppliedError:
		return agentv1.AppliedState_APPLIED_STATE_ERROR
	}
	return agentv1.AppliedState_APPLIED_STATE_UNSPECIFIED
}

func appliedFromProto(p agentv1.AppliedState) domain.AppliedState {
	switch p {
	case agentv1.AppliedState_APPLIED_STATE_PENDING:
		return domain.AppliedPending
	case agentv1.AppliedState_APPLIED_STATE_APPLIED:
		return domain.AppliedOk
	case agentv1.AppliedState_APPLIED_STATE_ERROR:
		return domain.AppliedError
	default:
		return ""
	}
}

func transportToProto(t domain.TransportKind) agentv1.TransportKind {
	switch t {
	case domain.TransportWS:
		return agentv1.TransportKind_TRANSPORT_KIND_WS
	case domain.TransportXHTTP:
		return agentv1.TransportKind_TRANSPORT_KIND_XHTTP
	case domain.TransportTCP:
		return agentv1.TransportKind_TRANSPORT_KIND_TCP
	case domain.TransportReality:
		return agentv1.TransportKind_TRANSPORT_KIND_REALITY
	}
	return agentv1.TransportKind_TRANSPORT_KIND_UNSPECIFIED
}

func transportFromProto(p agentv1.TransportKind) domain.TransportKind {
	switch p {
	case agentv1.TransportKind_TRANSPORT_KIND_WS:
		return domain.TransportWS
	case agentv1.TransportKind_TRANSPORT_KIND_XHTTP:
		return domain.TransportXHTTP
	case agentv1.TransportKind_TRANSPORT_KIND_TCP:
		return domain.TransportTCP
	case agentv1.TransportKind_TRANSPORT_KIND_REALITY:
		return domain.TransportReality
	default:
		return ""
	}
}

func protocolToProto(p domain.Protocol) agentv1.Protocol {
	if p == domain.ProtocolVLESS {
		return agentv1.Protocol_PROTOCOL_VLESS
	}
	return agentv1.Protocol_PROTOCOL_UNSPECIFIED
}

func protocolFromProto(p agentv1.Protocol) domain.Protocol {
	if p == agentv1.Protocol_PROTOCOL_VLESS {
		return domain.ProtocolVLESS
	}
	return ""
}

func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func timeFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
