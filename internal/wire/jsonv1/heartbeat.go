package jsonv1

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

const schemaVersion = 1

type heartbeatEventDTO struct {
	SchemaVersion int                   `json:"schema_version"`
	EventID       string                `json:"event_id"`
	NodeID        string                `json:"node_id"`
	EmittedAt     string                `json:"emitted_at"`
	AgentVersion  string                `json:"agent_version"`
	IsHealthy     bool                  `json:"is_healthy"`
	Ready         bool                  `json:"ready"`
	LastError     *string               `json:"last_error,omitempty"`
	PollCount     uint32                `json:"poll_count"`
	Applied       uint32                `json:"applied"`
	Failed        uint32                `json:"failed"`
	CPUPct        *float64              `json:"cpu_pct,omitempty"`
	MemPct        *float64              `json:"mem_pct,omitempty"`
	BandwidthPct  *float64              `json:"bandwidth_pct,omitempty"`
	Pool          *heartbeatPoolDTO     `json:"pool,omitempty"`
	Upstream      *heartbeatUpstreamDTO `json:"upstream,omitempty"`
}

type heartbeatPoolDTO struct {
	SlotsTotal               uint32  `json:"slots_total"`
	SlotsActive              uint32  `json:"slots_active"`
	DesiredBackends          uint32  `json:"desired_backends"`
	DroppedOverflow          uint32  `json:"dropped_overflow"`
	LastApplyOk              bool    `json:"last_apply_ok"`
	LastApplyError           *string `json:"last_apply_error,omitempty"`
	ConsecutiveApplyFailures uint32  `json:"consecutive_apply_failures"`
	LastAppliedGeneration    uint32  `json:"last_applied_generation"`
	LastAppliedAt            *string `json:"last_applied_at,omitempty"`
}

type heartbeatUpstreamDTO struct {
	Configured               bool    `json:"configured"`
	LastApplyOk              bool    `json:"last_apply_ok"`
	LastApplyError           *string `json:"last_apply_error,omitempty"`
	ConsecutiveApplyFailures uint32  `json:"consecutive_apply_failures"`
	UpstreamNodeID           *string `json:"upstream_node_id,omitempty"`
	UpstreamHost             *string `json:"upstream_host,omitempty"`
	UpstreamAddr             *string `json:"upstream_addr,omitempty"`
	LastAppliedAt            *string `json:"last_applied_at,omitempty"`
}

func MarshalHeartbeatEvent(h domain.Heartbeat, eventID, agentVersion string) ([]byte, error) {
	if eventID == "" {
		return nil, errors.New("jsonv1: event_id required")
	}
	if h.NodeID == "" {
		return nil, errors.New("jsonv1: node_id required")
	}
	if h.At.IsZero() {
		return nil, errors.New("jsonv1: At required")
	}

	dto := heartbeatEventDTO{
		SchemaVersion: schemaVersion,
		EventID:       eventID,
		NodeID:        string(h.NodeID),
		EmittedAt:     h.At.UTC().Format(time.RFC3339Nano),
		AgentVersion:  agentVersion,
		IsHealthy:     h.IsHealthy,
		Ready:         h.Ready,
		PollCount:     h.PollCount,
		Applied:       h.Applied,
		Failed:        h.Failed,
		CPUPct:        h.CPUPct,
		MemPct:        h.MemPct,
		BandwidthPct:  h.BandwidthPct,
	}
	if h.LastError != "" {
		s := h.LastError
		dto.LastError = &s
	}
	if h.Pool != nil {
		dto.Pool = poolToDTO(h.Pool)
	}
	if h.Upstream != nil {
		dto.Upstream = upstreamToDTO(h.Upstream)
	}
	return json.Marshal(dto)
}

func poolToDTO(p *domain.HeartbeatPool) *heartbeatPoolDTO {
	dto := &heartbeatPoolDTO{
		SlotsTotal:               p.SlotsTotal,
		SlotsActive:              p.SlotsActive,
		DesiredBackends:          p.DesiredBackends,
		DroppedOverflow:          p.DroppedOverflow,
		LastApplyOk:              p.LastApplyOk,
		ConsecutiveApplyFailures: p.ConsecutiveApplyFailures,
		LastAppliedGeneration:    p.LastAppliedGeneration,
	}
	if p.LastApplyError != "" {
		s := p.LastApplyError
		dto.LastApplyError = &s
	}
	if !p.LastAppliedAt.IsZero() {
		s := p.LastAppliedAt.UTC().Format(time.RFC3339Nano)
		dto.LastAppliedAt = &s
	}
	return dto
}

func upstreamToDTO(u *domain.HeartbeatUpstream) *heartbeatUpstreamDTO {
	dto := &heartbeatUpstreamDTO{
		Configured:               u.Configured,
		LastApplyOk:              u.LastApplyOk,
		ConsecutiveApplyFailures: u.ConsecutiveApplyFailures,
	}
	if u.LastApplyError != "" {
		s := u.LastApplyError
		dto.LastApplyError = &s
	}
	if u.UpstreamNodeID != "" {
		s := u.UpstreamNodeID
		dto.UpstreamNodeID = &s
	}
	if u.UpstreamHost != "" {
		s := u.UpstreamHost
		dto.UpstreamHost = &s
	}
	if u.UpstreamAddr != "" {
		s := u.UpstreamAddr
		dto.UpstreamAddr = &s
	}
	if !u.LastAppliedAt.IsZero() {
		s := u.LastAppliedAt.UTC().Format(time.RFC3339Nano)
		dto.LastAppliedAt = &s
	}
	return dto
}
