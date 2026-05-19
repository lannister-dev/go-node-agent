package protov1

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	agentv1 "github.com/lannister-dev/go-node-agent/pkg/proto/vpn/agent/v1"
)

func MarshalHeartbeatEvent(h domain.Heartbeat, eventID, agentVersion string) ([]byte, error) {
	if eventID == "" {
		return nil, errors.New("protov1: event_id required")
	}
	if h.NodeID == "" {
		return nil, errors.New("protov1: node_id required")
	}
	if h.At.IsZero() {
		return nil, errors.New("protov1: At required")
	}
	pb := &agentv1.HeartbeatEvent{
		SchemaVersion: 1,
		EventId:       eventID,
		NodeId:        string(h.NodeID),
		EmittedAt:     timeToProto(h.At.UTC()),
		AgentVersion:  agentVersion,
		IsHealthy:     h.IsHealthy,
		Ready:         h.Ready,
		PollCount:     h.PollCount,
		Applied:       h.Applied,
		Failed:        h.Failed,
	}
	if h.LastError != "" {
		pb.LastError = proto.String(h.LastError)
	}
	if h.CPUPct != nil {
		pb.CpuPct = proto.Float64(*h.CPUPct)
	}
	if h.MemPct != nil {
		pb.MemPct = proto.Float64(*h.MemPct)
	}
	if h.BandwidthPct != nil {
		pb.BandwidthPct = proto.Float64(*h.BandwidthPct)
	}
	if h.Pool != nil {
		pb.Pool = poolToProto(h.Pool)
	}
	if h.Upstream != nil {
		pb.Upstream = upstreamHealthToProto(h.Upstream)
	}
	return proto.Marshal(pb)
}

func poolToProto(p *domain.HeartbeatPool) *agentv1.HeartbeatPoolHealth {
	out := &agentv1.HeartbeatPoolHealth{
		SlotsTotal:               p.SlotsTotal,
		SlotsActive:              p.SlotsActive,
		DesiredBackends:          p.DesiredBackends,
		DroppedOverflow:          p.DroppedOverflow,
		LastApplyOk:              p.LastApplyOk,
		ConsecutiveApplyFailures: p.ConsecutiveApplyFailures,
		LastAppliedGeneration:    p.LastAppliedGeneration,
	}
	if p.LastApplyError != "" {
		out.LastApplyError = proto.String(p.LastApplyError)
	}
	if !p.LastAppliedAt.IsZero() {
		out.LastAppliedAt = timeToProto(p.LastAppliedAt)
	}
	return out
}

func upstreamHealthToProto(u *domain.HeartbeatUpstream) *agentv1.HeartbeatUpstreamHealth {
	out := &agentv1.HeartbeatUpstreamHealth{
		Configured:               u.Configured,
		LastApplyOk:              u.LastApplyOk,
		ConsecutiveApplyFailures: u.ConsecutiveApplyFailures,
	}
	if u.LastApplyError != "" {
		out.LastApplyError = proto.String(u.LastApplyError)
	}
	if u.UpstreamNodeID != "" {
		out.UpstreamNodeId = proto.String(u.UpstreamNodeID)
	}
	if u.UpstreamHost != "" {
		out.UpstreamHost = proto.String(u.UpstreamHost)
	}
	if u.UpstreamAddr != "" {
		out.UpstreamAddr = proto.String(u.UpstreamAddr)
	}
	if !u.LastAppliedAt.IsZero() {
		out.LastAppliedAt = timeToProto(u.LastAppliedAt)
	}
	return out
}

func UnmarshalHeartbeatEvent(data []byte) (domain.Heartbeat, string, string, error) {
	var pb agentv1.HeartbeatEvent
	if err := proto.Unmarshal(data, &pb); err != nil {
		return domain.Heartbeat{}, "", "", fmt.Errorf("protov1: decode heartbeat: %w", err)
	}
	at := timeFromProto(pb.GetEmittedAt())
	if at.IsZero() {
		at = time.Now().UTC()
	}
	out := domain.Heartbeat{
		NodeID:    domain.NodeID(pb.GetNodeId()),
		At:        at,
		IsHealthy: pb.GetIsHealthy(),
		Ready:     pb.GetReady(),
		LastError: pb.GetLastError(),
		PollCount: pb.GetPollCount(),
		Applied:   pb.GetApplied(),
		Failed:    pb.GetFailed(),
	}
	if pb.CpuPct != nil {
		v := pb.GetCpuPct()
		out.CPUPct = &v
	}
	if pb.MemPct != nil {
		v := pb.GetMemPct()
		out.MemPct = &v
	}
	if pb.BandwidthPct != nil {
		v := pb.GetBandwidthPct()
		out.BandwidthPct = &v
	}
	if pb.Pool != nil {
		out.Pool = &domain.HeartbeatPool{
			SlotsTotal:               pb.Pool.GetSlotsTotal(),
			SlotsActive:              pb.Pool.GetSlotsActive(),
			DesiredBackends:          pb.Pool.GetDesiredBackends(),
			DroppedOverflow:          pb.Pool.GetDroppedOverflow(),
			LastApplyOk:              pb.Pool.GetLastApplyOk(),
			LastApplyError:           pb.Pool.GetLastApplyError(),
			ConsecutiveApplyFailures: pb.Pool.GetConsecutiveApplyFailures(),
			LastAppliedGeneration:    pb.Pool.GetLastAppliedGeneration(),
			LastAppliedAt:            timeFromProto(pb.Pool.GetLastAppliedAt()),
		}
	}
	if pb.Upstream != nil {
		out.Upstream = &domain.HeartbeatUpstream{
			Configured:               pb.Upstream.GetConfigured(),
			LastApplyOk:              pb.Upstream.GetLastApplyOk(),
			LastApplyError:           pb.Upstream.GetLastApplyError(),
			ConsecutiveApplyFailures: pb.Upstream.GetConsecutiveApplyFailures(),
			UpstreamNodeID:           pb.Upstream.GetUpstreamNodeId(),
			UpstreamHost:             pb.Upstream.GetUpstreamHost(),
			UpstreamAddr:             pb.Upstream.GetUpstreamAddr(),
			LastAppliedAt:            timeFromProto(pb.Upstream.GetLastAppliedAt()),
		}
	}
	return out, pb.GetEventId(), pb.GetAgentVersion(), nil
}
