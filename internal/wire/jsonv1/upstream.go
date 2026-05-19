package jsonv1

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type upstreamChangedPayloadDTO struct {
	SchemaVersion      int     `json:"schema_version"`
	EventID            string  `json:"event_id"`
	NodeID             string  `json:"node_id"`
	EmittedAt          string  `json:"emitted_at"`
	UpstreamNodeID     string  `json:"upstream_node_id"`
	UpstreamPublicHost string  `json:"upstream_public_domain"`
	UpstreamRealityIP  *string `json:"upstream_reality_ip,omitempty"`
	Removed            bool    `json:"removed,omitempty"`
}

type UpstreamChange struct {
	EventID      string
	NodeID       domain.NodeID
	BackendID    domain.BackendID
	PublicDomain string
	RealityIP    string
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
		PublicDomain: dto.UpstreamPublicHost,
		Removed:      dto.Removed,
	}
	if dto.UpstreamRealityIP != nil {
		out.RealityIP = *dto.UpstreamRealityIP
	}
	return out, nil
}
