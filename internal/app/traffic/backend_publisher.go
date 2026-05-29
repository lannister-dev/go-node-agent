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
	LiveStatsBucket    string
	Interval           time.Duration
}

type BackendPublisher struct {
	cfg   BackendPublisherConfig
	pub   ports.Publisher
	kv    KVPutter
	stats XrayStatsSource
	log   *slog.Logger
}

func NewBackendPublisher(cfg BackendPublisherConfig, pub ports.Publisher, kv KVPutter, stats XrayStatsSource, log *slog.Logger) (*BackendPublisher, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("backend-traffic-publisher: NodeID required")
	}
	if cfg.NodeTrafficSubject == "" {
		cfg.NodeTrafficSubject = defaultSubject
	}
	if cfg.LiveStatsBucket == "" {
		cfg.LiveStatsBucket = StatsKvBucket
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
		kv:    kv,
		stats: stats,
		log:   log.With("component", "backend-traffic-publisher"),
	}, nil
}

const backendStatsKeyPrefix = "backend."

type backendLivePayload struct {
	NodeID          string   `json:"node_id"`
	Ts              string   `json:"ts"`
	ActiveClientIDs []string `json:"active_client_ids"`
}

const (
	linkUplink   = "uplink"
	linkDownlink = "downlink"
)

// userTrafficDelta matches control-api's services.traffic.users.schemas.UserTrafficIn.
// Fields: identifier (client UUID), delta_bytes (uplink+downlink combined for the tick).
type userTrafficDelta struct {
	Identifier string `json:"identifier"`
	DeltaBytes uint64 `json:"delta_bytes"`
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
	p.log.Debug("xray stats fetched", "rows", len(statsList))

	// Aggregate per user: track uplink and downlink separately for node total,
	// but ship a single combined delta_bytes to control-api.
	type accum struct {
		up   uint64
		down uint64
	}
	users := map[string]*accum{}
	for _, s := range statsList {
		if s.Value <= 0 {
			continue
		}
		v := uint64(s.Value)
		u, ok := users[s.ClientID]
		if !ok {
			u = &accum{}
			users[s.ClientID] = u
		}
		switch s.Link {
		case linkUplink:
			u.up += v
		case linkDownlink:
			u.down += v
		}
	}

	if len(users) == 0 {
		p.log.Debug("backend traffic tick: no user stats", "raw_rows", len(statsList))
		p.publishLive(ctx, nil)
		return nil
	}

	// Aggregate node-level totals + build per-user list.
	var sumUp, sumDown uint64
	deltas := make([]userTrafficDelta, 0, len(users))
	activeIDs := make([]string, 0, len(users))
	for clientID, u := range users {
		total := u.up + u.down
		if total == 0 {
			continue
		}
		sumUp += u.up
		sumDown += u.down
		deltas = append(deltas, userTrafficDelta{Identifier: clientID, DeltaBytes: total})
		activeIDs = append(activeIDs, clientID)
	}
	p.publishLive(ctx, activeIDs)

	if sumUp == 0 && sumDown == 0 {
		return nil
	}
	p.log.Info("backend traffic publish", "users", len(deltas), "bytes_in", sumUp, "bytes_out", sumDown)

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

// publishLive writes the set of currently-active client_ids to KV so the admin
// can resolve them to real user_ids and show a live user count per backend.
// Best-effort: KV failures are logged but never fail the parent tick.
func (p *BackendPublisher) publishLive(ctx context.Context, activeIDs []string) {
	if p.kv == nil {
		return
	}
	payload := backendLivePayload{
		NodeID:          string(p.cfg.NodeID),
		Ts:              time.Now().UTC().Format(time.RFC3339Nano),
		ActiveClientIDs: activeIDs,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		p.log.Warn("backend live stats marshal failed", "err", err)
		return
	}
	key := backendStatsKeyPrefix + string(p.cfg.NodeID)
	if err := p.kv.KVPut(ctx, p.cfg.LiveStatsBucket, key, data); err != nil {
		p.log.Warn("backend live stats kv put failed", "err", err)
	}
}
