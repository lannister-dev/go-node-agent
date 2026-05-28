package traffic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type PublisherConfig struct {
	NodeID   domain.NodeID
	NodeRole string
	Subject  string
	Interval time.Duration
}

type Publisher struct {
	cfg     PublisherConfig
	pub     ports.Publisher
	src     *Reporter
	log     *slog.Logger
	lastUp  atomic.Uint64
	lastDn  atomic.Uint64
	totalUp atomic.Uint64
	totalDn atomic.Uint64
}

func NewPublisher(cfg PublisherConfig, pub ports.Publisher, src *Reporter, log *slog.Logger) (*Publisher, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("traffic-publisher: NodeID required")
	}
	if cfg.NodeRole == "" {
		return nil, errors.New("traffic-publisher: NodeRole required")
	}
	if cfg.Subject == "" {
		cfg.Subject = "nodes.traffic"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if pub == nil || src == nil {
		return nil, errors.New("traffic-publisher: pub and src required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Publisher{cfg: cfg, pub: pub, src: src, log: log.With("component", "traffic-publisher")}, nil
}

type nodeTrafficPayload struct {
	EntryNodeID    string `json:"entry_node_id,omitempty"`
	BackendNodeID  string `json:"backend_node_id,omitempty"`
	BytesIn        uint64 `json:"bytes_in"`
	BytesOut       uint64 `json:"bytes_out"`
	ActiveSessions uint64 `json:"active_sessions"`
	TotalSessions  uint64 `json:"total_sessions"`
}

func (p *Publisher) Run(ctx context.Context) error {
	p.log.Info("traffic publisher started", "subject", p.cfg.Subject, "interval", p.cfg.Interval)
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := p.tick(ctx); err != nil {
				p.log.Warn("traffic publish failed", "err", err)
			}
		}
	}
}

func (p *Publisher) tick(ctx context.Context) error {
	upCum := p.src.UpBytes()
	dnCum := p.src.DownBytes()
	deltaUp := upCum - p.lastUp.Load()
	deltaDn := dnCum - p.lastDn.Load()
	if deltaUp == 0 && deltaDn == 0 {
		return nil
	}
	p.lastUp.Store(upCum)
	p.lastDn.Store(dnCum)
	p.totalUp.Add(deltaUp)
	p.totalDn.Add(deltaDn)

	payload := nodeTrafficPayload{
		BytesIn:  deltaDn,
		BytesOut: deltaUp,
	}
	if p.cfg.NodeRole == "entry" || p.cfg.NodeRole == "whitelist_entry" {
		payload.EntryNodeID = string(p.cfg.NodeID)
	} else {
		payload.BackendNodeID = string(p.cfg.NodeID)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("traffic-publisher: marshal: %w", err)
	}
	if err := p.pub.Publish(ctx, p.cfg.Subject, nil, data); err != nil {
		return fmt.Errorf("traffic-publisher: publish: %w", err)
	}
	return nil
}
