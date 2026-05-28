package traffic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
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
	backend  string
}

type Publisher struct {
	cfg   PublisherConfig
	pub   ports.Publisher
	conns ConnectionsSource
	log   *slog.Logger

	mu       sync.Mutex
	lastConn map[string]connState
}

func NewPublisher(cfg PublisherConfig, pub ports.Publisher, _ *Reporter, conns ConnectionsSource, log *slog.Logger) (*Publisher, error) {
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

	p.mu.Lock()
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
		cur[c.ID] = connState{upload: c.Upload, download: c.Download, backend: backend}
	}
	p.lastConn = cur
	p.mu.Unlock()

	if len(perBackendActive) == 0 {
		return nil
	}

	isEntry := p.cfg.NodeRole == "entry" || p.cfg.NodeRole == "whitelist_entry"
	deltas := make([]nodeTrafficPayload, 0, len(perBackendActive))
	for backend, active := range perBackendActive {
		d := nodeTrafficPayload{
			BytesIn:        perBackendIn[backend],
			BytesOut:       perBackendOut[backend],
			ActiveSessions: active,
			BackendNodeID:  backend,
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

// pickBackendID parses sing-box clash chain entries.
// Per-user backend outbounds are tagged "b-<client_uuid>-<backend_uuid>".
// Returns the backend UUID, or "" if no backend chain found.
func pickBackendID(chains []string) string {
	for _, tag := range chains {
		if !strings.HasPrefix(tag, "b-") {
			continue
		}
		parts := strings.Split(tag, "-")
		// b - <5 parts of client uuid> - <5 parts of backend uuid> = 11 parts
		if len(parts) < 11 {
			continue
		}
		return strings.Join(parts[6:11], "-")
	}
	return ""
}
