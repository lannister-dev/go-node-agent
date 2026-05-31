package jsonv1

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type upstreamChangedPayloadDTO struct {
	SchemaVersion        int     `json:"schema_version"`
	EventID              string  `json:"event_id"`
	NodeID               string  `json:"node_id"`
	EmittedAt            string  `json:"emitted_at"`
	UpstreamNodeID       string  `json:"upstream_node_id"`
	UpstreamName         string  `json:"upstream_name,omitempty"`
	UpstreamPublicHost   string  `json:"upstream_public_domain"`
	UpstreamRealityIP    *string `json:"upstream_reality_ip,omitempty"`
	UpstreamInternalWgIP *string `json:"upstream_internal_wg_ip,omitempty"`
	UpstreamAgentPort    *int    `json:"upstream_agent_port,omitempty"`
	Removed              bool    `json:"removed,omitempty"`
}

type UpstreamChange struct {
	EventID      string
	NodeID       domain.NodeID
	BackendID    domain.BackendID
	BackendName  string
	PublicDomain string
	RealityIP    string
	InternalWgIP string
	AgentPort    uint16
	Removed      bool
}

func UnmarshalUpstreamChanged(data []byte) (UpstreamChange, error) {
	var dto upstreamChangedPayloadDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return UpstreamChange{}, fmt.Errorf("jsonv1: decode upstream_changed: %w", err)
	}
	if dto.UpstreamNodeID == "" {
		return UpstreamChange{}, errors.New("jsonv1: upstream_changed missing upstream_node_id")
	}
	out := UpstreamChange{
		EventID:      dto.EventID,
		NodeID:       domain.NodeID(dto.NodeID),
		BackendID:    domain.BackendID(dto.UpstreamNodeID),
		BackendName:  dto.UpstreamName,
		PublicDomain: dto.UpstreamPublicHost,
		Removed:      dto.Removed,
	}
	if dto.UpstreamRealityIP != nil {
		out.RealityIP = *dto.UpstreamRealityIP
	}
	if dto.UpstreamInternalWgIP != nil {
		out.InternalWgIP = *dto.UpstreamInternalWgIP
	}
	if dto.UpstreamAgentPort != nil && *dto.UpstreamAgentPort > 0 && *dto.UpstreamAgentPort <= 65535 {
		out.AgentPort = uint16(*dto.UpstreamAgentPort)
	}
	return out, nil
}
