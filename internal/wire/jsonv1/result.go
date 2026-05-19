package jsonv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type placementApplyResultDTO struct {
	SchemaVersion  int     `json:"schema_version"`
	NodeID         string  `json:"node_id"`
	EmittedAt      string  `json:"emitted_at"`
	EventID        string  `json:"event_id"`
	PlacementID    string  `json:"placement_id"`
	OpVersion      uint64  `json:"op_version"`
	AppliedState   string  `json:"applied_state"`
	ReportStatus   *string `json:"report_status,omitempty"`
	Retryable      bool    `json:"retryable"`
	Error          *string `json:"error,omitempty"`
	InventoryHash  *string `json:"inventory_hash,omitempty"`
	InventoryCount *uint64 `json:"inventory_count,omitempty"`
}

func MarshalApplyResultEvent(r domain.PlacementReport, eventID string, emittedAt time.Time) ([]byte, error) {
	if eventID == "" {
		return nil, errors.New("jsonv1: event_id required")
	}
	if r.PlacementID == "" || r.NodeID == "" {
		return nil, errors.New("jsonv1: result missing ids")
	}
	if r.OpVersion == 0 {
		return nil, errors.New("jsonv1: result op_version must be >= 1")
	}
	if !r.AppliedState.Valid() {
		return nil, fmt.Errorf("jsonv1: invalid applied_state %q", r.AppliedState)
	}
	if emittedAt.IsZero() {
		emittedAt = time.Now().UTC()
	}

	dto := placementApplyResultDTO{
		SchemaVersion: schemaVersion,
		NodeID:        string(r.NodeID),
		EmittedAt:     emittedAt.UTC().Format(time.RFC3339Nano),
		EventID:       eventID,
		PlacementID:   string(r.PlacementID),
		OpVersion:     uint64(r.OpVersion),
		AppliedState:  string(r.AppliedState),
		Retryable:     r.Retryable,
	}
	if r.Status != "" {
		if !r.Status.Valid() {
			return nil, fmt.Errorf("jsonv1: invalid report_status %q", r.Status)
		}
		s := string(r.Status)
		dto.ReportStatus = &s
	}
	if r.Err != "" {
		e := r.Err
		dto.Error = &e
	}
	return json.Marshal(dto)
}
