package traffic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

const (
	roleEntry           = "entry"
	roleWhitelistEntry  = "whitelist_entry"
	defaultSubject      = "nodes.traffic"
	defaultTickInterval = 30 * time.Second
)

type ConnectionsSource interface {
	Connections(ctx context.Context) (ports.SingBoxConnections, error)
}

type PublisherConfig struct {
	NodeID   domain.NodeID
	NodeRole string
	Subject  string
	Interval time.Duration
}

type connState struct {
	upload   uint64
	download uint64
}

type Publisher struct {
	cfg      PublisherConfig
	pub      ports.Publisher
	conns    ConnectionsSource
	log      *slog.Logger
	lastConn map[string]connState
}

func NewPublisher(cfg PublisherConfig, pub ports.Publisher, conns ConnectionsSource, log *slog.Logger) (*Publisher, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("traffic-publisher: NodeID required")
	}
	if cfg.NodeRole == "" {
		return nil, errors.New("traffic-publisher: NodeRole required")
	}
	if cfg.Subject == "" {
		cfg.Subject = defaultSubject
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultTickInterval
	}
	if pub == nil {
		return nil, errors.New("traffic-publisher: pub required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Publisher{
		cfg:      cfg,
		pub:      pub,
		conns:    conns,
		log:      log.With("component", "traffic-publisher"),
		lastConn: map[string]connState{},
	}, nil
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
	if p.conns == nil {
		return nil
	}
	snap, err := p.conns.Connections(ctx)
	if err != nil {
		return fmt.Errorf("connections fetch: %w", err)
	}

	perBackendIn := map[string]uint64{}
	perBackendOut := map[string]uint64{}
	perBackendActive := map[string]uint64{}
	cur := make(map[string]connState, len(snap.Conns))

	for _, c := range snap.Conns {
		backend := pickBackendID(c.Chains)
		if backend == "" {
			continue
		}
		var deltaUp, deltaDn uint64
		if last, ok := p.lastConn[c.ID]; ok {
			if c.Upload >= last.upload {
				deltaUp = c.Upload - last.upload
			}
			if c.Download >= last.download {
				deltaDn = c.Download - last.download
			}
		}
		if deltaUp > 0 {
			perBackendOut[backend] += deltaUp
		}
		if deltaDn > 0 {
			perBackendIn[backend] += deltaDn
		}
		perBackendActive[backend]++
		cur[c.ID] = connState{upload: c.Upload, download: c.Download}
	}
	p.lastConn = cur

	if len(perBackendActive) == 0 {
		return nil
	}

	isEntry := p.cfg.NodeRole == roleEntry || p.cfg.NodeRole == roleWhitelistEntry
	deltas := make([]nodeTrafficPayload, 0, len(perBackendActive))
	for backend, active := range perBackendActive {
		d := nodeTrafficPayload{
			BackendNodeID:  backend,
			BytesIn:        perBackendIn[backend],
			BytesOut:       perBackendOut[backend],
			ActiveSessions: active,
		}
		if isEntry {
			d.EntryNodeID = string(p.cfg.NodeID)
		}
		deltas = append(deltas, d)
	}

	data, err := json.Marshal(deltas)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := p.pub.Publish(ctx, p.cfg.Subject, nil, data); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

// pickBackendID returns the backend UUID from a sing-box clash `chains` array.
// Per-user outbounds are tagged "b-<client_uuid>-<backend_uuid>".
// With urltest groups chains can be ["b-<u>-<b>", "auto-<u>"], so we scan for the b- entry.
func pickBackendID(chains []string) string {
	for _, tag := range chains {
		if _, backend, ok := singboxgen.ParsePerUserOutboundTag(tag); ok {
			return backend
		}
	}
	return ""
}
