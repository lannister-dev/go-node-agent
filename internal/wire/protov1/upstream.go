package protov1

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	agentv1 "github.com/lannister-dev/go-node-agent/pkg/proto/vpn/agent/v1"
)

type UpstreamChange struct {
	EventID      string
	NodeID       domain.NodeID
	BackendID    domain.BackendID
	PublicDomain string
	RealityIP    string
	Removed      bool
}

func UnmarshalUpstreamChanged(data []byte) (UpstreamChange, error) {
	var pb agentv1.UpstreamChangedPayload
	if err := proto.Unmarshal(data, &pb); err != nil {
		return UpstreamChange{}, fmt.Errorf("protov1: decode upstream_changed: %w", err)
	}
	if pb.GetUpstreamNodeId() == "" {
		return UpstreamChange{}, errors.New("protov1: upstream_changed missing upstream_node_id")
	}
	out := UpstreamChange{
		EventID:      pb.GetEventId(),
		NodeID:       domain.NodeID(pb.GetNodeId()),
		BackendID:    domain.BackendID(pb.GetUpstreamNodeId()),
		PublicDomain: pb.GetUpstreamPublicDomain(),
	}
	if pb.UpstreamRealityIp != nil {
		out.RealityIP = pb.GetUpstreamRealityIp()
	}
	return out, nil
}

var _ = proto.Marshal
