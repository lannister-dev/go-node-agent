package heartbeat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/wire/jsonv1"
)

type Config struct {
	NodeID       domain.NodeID
	Subject      string
	AgentVersion string
	Interval     time.Duration
}

type Heartbeat struct {
	cfg      Config
	pub      Publisher
	sampler  Sampler
	counters Counters
	ids      IDGenerator
	log      *slog.Logger
}

func New(cfg Config, pub Publisher, sampler Sampler, counters Counters, ids IDGenerator, log *slog.Logger) (*Heartbeat, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("heartbeat: NodeID required")
	}
	if cfg.Subject == "" {
		return nil, errors.New("heartbeat: Subject required")
	}
	if cfg.AgentVersion == "" {
		return nil, errors.New("heartbeat: AgentVersion required")
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("heartbeat: Interval must be > 0")
	}
	if pub == nil {
		return nil, errors.New("heartbeat: Publisher required")
	}
	if sampler == nil {
		sampler = NoopSampler{}
	}
	if counters == nil {
		counters = NoopCounters{}
	}
	if ids == nil {
		return nil, errors.New("heartbeat: IDGenerator required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Heartbeat{
		cfg:      cfg,
		pub:      pub,
		sampler:  sampler,
		counters: counters,
		ids:      ids,
		log:      log.With("component", "heartbeat"),
	}, nil
}

func (h *Heartbeat) Run(ctx context.Context) error {
	if err := h.publish(ctx); err != nil {
		h.log.Warn("initial heartbeat failed", "err", err)
	}
	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := h.publish(ctx); err != nil {
				h.log.Warn("heartbeat publish failed", "err", err)
			}
		}
	}
}

func (h *Heartbeat) publish(ctx context.Context) error {
	stats, err := h.sampler.Sample(ctx)
	if err != nil {
		h.log.Debug("sampler error; continuing with empty stats", "err", err)
		stats = Stats{}
	}
	pollCount, applied, failed := h.counters.Snapshot()
	hb := domain.Heartbeat{
		NodeID:       h.cfg.NodeID,
		At:           time.Now().UTC(),
		IsHealthy:    true,
		Ready:        true,
		PollCount:    pollCount,
		Applied:      applied,
		Failed:       failed,
		CPUPct:       stats.CPUPct,
		MemPct:       stats.MemPct,
		BandwidthPct: stats.BandwidthPct,
	}
	data, err := jsonv1.MarshalHeartbeatEvent(hb, h.ids.NewID(), h.cfg.AgentVersion)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	headers := map[string]string{
		"x-schema":        "jsonv1",
		"x-agent-version": h.cfg.AgentVersion,
		"x-node-id":       string(h.cfg.NodeID),
	}
	if err := h.pub.Publish(ctx, h.cfg.Subject, headers, data); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}
