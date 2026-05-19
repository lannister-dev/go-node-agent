package domain

import "time"

type Heartbeat struct {
	NodeID       NodeID
	At           time.Time
	IsHealthy    bool
	Ready        bool
	LastError    string
	PollCount    uint32
	Applied      uint32
	Failed       uint32
	CPUPct       *float64
	MemPct       *float64
	BandwidthPct *float64
	Pool         *HeartbeatPool
	Upstream     *HeartbeatUpstream
}

type HeartbeatPool struct {
	SlotsTotal               uint32
	SlotsActive              uint32
	DesiredBackends          uint32
	DroppedOverflow          uint32
	LastApplyOk              bool
	LastApplyError           string
	ConsecutiveApplyFailures uint32
	LastAppliedGeneration    uint32
	LastAppliedAt            time.Time
}

type HeartbeatUpstream struct {
	Configured               bool
	LastApplyOk              bool
	LastApplyError           string
	ConsecutiveApplyFailures uint32
	UpstreamNodeID           string
	UpstreamHost             string
	UpstreamAddr             string
	LastAppliedAt            time.Time
}
