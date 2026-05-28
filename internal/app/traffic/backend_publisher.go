package traffic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/adapters/xray"
	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

// XrayStatsSource exposes the per-user traffic counter delta from xray.
// Implemented by *xray.Client.QueryUserStats(reset=true).
type XrayStatsSource interface {
	QueryUserStats(ctx context.Context, reset bool) ([]xray.UserStat, error)
}

type BackendPublisherConfig struct {
	NodeID             domain.NodeID
	NodeTrafficSubject string
	UserTrafficSubject string
	Interval           time.Duration
}

type BackendPublisher struct {
	cfg   BackendPublisherConfig
	pub   ports.Publisher
	stats XrayStatsSource
	log   *slog.Logger
}

func NewBackendPublisher(cfg BackendPublisherConfig, pub ports.Publisher, stats XrayStatsSource, log *slog.Logger) (*BackendPublisher, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("backend-traffic-publisher: NodeID required")
	}
	if cfg.NodeTrafficSubject == "" {
		cfg.NodeTrafficSubject = defaultSubject
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultTickInterval
	}
	if pub == nil || stats == nil {
		return nil, errors.New("backend-traffic-publisher: pub and stats required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &BackendPublisher{
		cfg:   cfg,
		pub:   pub,
		stats: stats,
		log:   log.With("component", "backend-traffic-publisher"),
	}, nil
}

const (
	linkUplink   = "uplink"
	linkDownlink = "downlink"
)

type userTrafficDelta struct {
	ClientID      string `json:"client_id"`
	UplinkBytes   uint64 `json:"uplink_bytes"`
	DownlinkBytes uint64 `json:"downlink_bytes"`
	TotalBytes    uint64 `json:"total_bytes"`
}

func (p *BackendPublisher) Run(ctx context.Context) error {
	p.log.Info("backend traffic publisher started",
		"node_subject", p.cfg.NodeTrafficSubject,
		"user_subject", p.cfg.UserTrafficSubject,
		"interval", p.cfg.Interval)
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := p.tick(ctx); err != nil {
				p.log.Warn("backend traffic publish failed", "err", err)
			}
		}
	}
}

func (p *BackendPublisher) tick(ctx context.Context) error {
	// reset=true → each value is the delta since the previous read.
	statsList, err := p.stats.QueryUserStats(ctx, true)
	if err != nil {
		return fmt.Errorf("xray stats query: %w", err)
	}

	// Aggregate per user.
	users := map[string]*userTrafficDelta{}
	for _, s := range statsList {
		if s.Value <= 0 {
			continue
		}
		v := uint64(s.Value)
		u, ok := users[s.ClientID]
		if !ok {
			u = &userTrafficDelta{ClientID: s.ClientID}
			users[s.ClientID] = u
		}
		switch s.Link {
		case linkUplink:
			u.UplinkBytes += v
		case linkDownlink:
			u.DownlinkBytes += v
		}
		u.TotalBytes = u.UplinkBytes + u.DownlinkBytes
	}

	if len(users) == 0 {
		return nil
	}

	// Aggregate node-level totals.
	var sumUp, sumDown uint64
	deltas := make([]userTrafficDelta, 0, len(users))
	for _, u := range users {
		if u.TotalBytes == 0 {
			continue
		}
		sumUp += u.UplinkBytes
		sumDown += u.DownlinkBytes
		deltas = append(deltas, *u)
	}

	if sumUp == 0 && sumDown == 0 {
		return nil
	}

	// Publish node-level aggregate to nodes.traffic.
	nodePayload := []nodeTrafficPayload{{
		BackendNodeID: string(p.cfg.NodeID),
		BytesIn:       sumUp,
		BytesOut:      sumDown,
	}}
	nodeData, err := json.Marshal(nodePayload)
	if err != nil {
		return fmt.Errorf("marshal node: %w", err)
	}
	if err := p.pub.Publish(ctx, p.cfg.NodeTrafficSubject, nil, nodeData); err != nil {
		return fmt.Errorf("publish node: %w", err)
	}

	// Publish per-user deltas to users.traffic (for traffic_usage table).
	if p.cfg.UserTrafficSubject != "" && len(deltas) > 0 {
		userData, err := json.Marshal(deltas)
		if err != nil {
			return fmt.Errorf("marshal users: %w", err)
		}
		if err := p.pub.Publish(ctx, p.cfg.UserTrafficSubject, nil, userData); err != nil {
			return fmt.Errorf("publish users: %w", err)
		}
	}
	return nil
}
