package jsonv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type placementCommandDTO struct {
	SchemaVersion    int     `json:"schema_version"`
	NodeID           string  `json:"node_id"`
	EmittedAt        string  `json:"emitted_at"`
	SnapshotID       *string `json:"snapshot_id,omitempty"`
	Epoch            *uint64 `json:"epoch,omitempty"`
	EventID          string  `json:"event_id"`
	PlacementID      string  `json:"placement_id"`
	KeyID            string  `json:"key_id"`
	OpVersion        uint64  `json:"op_version"`
	DesiredState     string  `json:"desired_state"`
	BackendNodeID    string  `json:"backend_node_id"`
	Protocol         string  `json:"protocol"`
	Transport        string  `json:"transport"`
	ClientID         string  `json:"client_id"`
	IsRevoked        bool    `json:"is_revoked"`
	SnapshotComplete bool    `json:"snapshot_complete"`
	ValidUntil       *string `json:"valid_until,omitempty"`
	UpdatedAt        *string `json:"updated_at,omitempty"`
}

func UnmarshalPlacementCommandEvent(data []byte) (domain.PlacementCommand, error) {
	var dto placementCommandDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return domain.PlacementCommand{}, fmt.Errorf("jsonv1: decode placement command: %w", err)
	}
	if dto.EventID == "" || dto.PlacementID == "" || dto.NodeID == "" {
		return domain.PlacementCommand{}, fmt.Errorf("jsonv1: placement command missing required ids (event_id=%q placement_id=%q node_id=%q)", dto.EventID, dto.PlacementID, dto.NodeID)
	}
	if dto.OpVersion == 0 {
		return domain.PlacementCommand{}, errors.New("jsonv1: placement command op_version must be >= 1")
	}
	desired := domain.DesiredState(dto.DesiredState)
	if !desired.Valid() {
		return domain.PlacementCommand{}, fmt.Errorf("jsonv1: invalid desired_state %q", dto.DesiredState)
	}
	transport := domain.TransportKind(dto.Transport)
	if dto.Transport != "" && !transport.Valid() {
		return domain.PlacementCommand{}, fmt.Errorf("jsonv1: invalid transport %q", dto.Transport)
	}

	emittedAt, err := parseTime(dto.EmittedAt)
	if err != nil {
		return domain.PlacementCommand{}, fmt.Errorf("jsonv1: emitted_at: %w", err)
	}
	validUntil, err := parseOptTime(dto.ValidUntil)
	if err != nil {
		return domain.PlacementCommand{}, fmt.Errorf("jsonv1: valid_until: %w", err)
	}
	updatedAt, err := parseOptTime(dto.UpdatedAt)
	if err != nil {
		return domain.PlacementCommand{}, fmt.Errorf("jsonv1: updated_at: %w", err)
	}

	cmd := domain.PlacementCommand{
		EventID:          dto.EventID,
		NodeID:           domain.NodeID(dto.NodeID),
		EmittedAt:        emittedAt,
		SnapshotComplete: dto.SnapshotComplete,
		Placement: domain.Placement{
			ID:            domain.PlacementID(dto.PlacementID),
			KeyID:         domain.KeyID(dto.KeyID),
			ClientID:      domain.ClientID(dto.ClientID),
			NodeID:        domain.NodeID(dto.NodeID),
			BackendNodeID: domain.BackendID(dto.BackendNodeID),
			OpVersion:     domain.OpVersion(dto.OpVersion),
			Desired:       desired,
			Protocol:      domain.Protocol(dto.Protocol),
			Transport:     transport,
			IsRevoked:     dto.IsRevoked,
			ValidUntil:    validUntil,
			UpdatedAt:     updatedAt,
		},
	}
	if dto.SnapshotID != nil {
		cmd.SnapshotID = *dto.SnapshotID
	}
	return cmd, nil
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("empty time")
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return t.UTC(), nil
	}
	t, err = time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func parseOptTime(s *string) (time.Time, error) {
	if s == nil || *s == "" {
		return time.Time{}, nil
	}
	return parseTime(*s)
}
