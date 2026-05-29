package traffic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

const (
	StatsKvBucket    = "entry-routing-stats"
	statsKeyPrefix   = "node."
	defaultStatsTick = 10 * time.Second
	tagDirect        = "direct"
)

// KVPutter is anything that can write a single key into a NATS JetStream KV bucket.
// Implemented by *nats.Transport.
type KVPutter interface {
	KVPut(ctx context.Context, bucket, key string, payload []byte) error
}

type BackendNameResolver interface {
	Get(id domain.BackendID) (singboxgen.BackendSpec, bool)
}

type StatsReporterConfig struct {
	NodeID   domain.NodeID
	Bucket   string
	Interval time.Duration
}

type StatsReporter struct {
	cfg      StatsReporterConfig
	kv       KVPutter
	conns    ConnectionsSource
	backends BackendNameResolver
	log      *slog.Logger
}

func NewStatsReporter(cfg StatsReporterConfig, kv KVPutter, conns ConnectionsSource, backends BackendNameResolver, log *slog.Logger) (*StatsReporter, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("stats-reporter: NodeID required")
	}
	if cfg.Bucket == "" {
		cfg.Bucket = StatsKvBucket
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultStatsTick
	}
	if kv == nil || conns == nil {
		return nil, errors.New("stats-reporter: kv and conns required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &StatsReporter{
		cfg:      cfg,
		kv:       kv,
		conns:    conns,
		backends: backends,
		log:      log.With("component", "stats-reporter"),
	}, nil
}

// statsPayload is the on-the-wire shape control-api expects in
// `entry-routing-stats/node.<node_id>`. Must stay in sync with
// `services.routing.entry.service.SingBoxStatsPayload` on control-api.
type statsPayload struct {
	NodeID        string         `json:"node_id"`
	Ts            string         `json:"ts"`
	Total         int            `json:"total"`
	UploadTotal   uint64         `json:"upload_total"`
	DownloadTotal uint64         `json:"download_total"`
	ByBackend     map[string]int `json:"by_backend"`
	ByClientID    map[string]int `json:"by_client_id"`
	UniqueUsers   int            `json:"unique_users"`
}

func (r *StatsReporter) Run(ctx context.Context) error {
	r.log.Info("stats reporter started", "bucket", r.cfg.Bucket, "interval", r.cfg.Interval)
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	// fire one immediately so admin UI has something within the first interval
	if err := r.tick(ctx); err != nil {
		r.log.Warn("stats publish failed", "err", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := r.tick(ctx); err != nil {
				r.log.Warn("stats publish failed", "err", err)
			}
		}
	}
}

func (r *StatsReporter) tick(ctx context.Context) error {
	snap, err := r.conns.Connections(ctx)
	if err != nil {
		return fmt.Errorf("connections: %w", err)
	}
	byBackend := map[string]int{}
	byClient := map[string]int{}
	for _, c := range snap.Conns {
		tag := r.aggregatedBackendTag(c.Chains)
		byBackend[tag]++
		// Best-effort: parsed user is the first b-<uuid>-... in chains.
		if u, _, ok := pickClientAndBackend(c.Chains); ok && u != "" {
			byClient[u]++
		}
	}

	payload := statsPayload{
		NodeID:      string(r.cfg.NodeID),
		Ts:          time.Now().UTC().Format(time.RFC3339Nano),
		Total:       len(snap.Conns),
		ByBackend:   byBackend,
		ByClientID:  byClient,
		UniqueUsers: len(byClient),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	key := statsKeyPrefix + string(r.cfg.NodeID)
	if err := r.kv.KVPut(ctx, r.cfg.Bucket, key, data); err != nil {
		return fmt.Errorf("kv put: %w", err)
	}
	return nil
}

func (r *StatsReporter) aggregatedBackendTag(chains []string) string {
	for _, c := range chains {
		if !strings.HasPrefix(c, singboxgen.PerUserOutboundTagPrefix) {
			continue
		}
		_, backend, ok := singboxgen.ParsePerUserOutboundTag(c)
		if !ok {
			continue
		}
		name := backend
		if r.backends != nil {
			if spec, found := r.backends.Get(domain.BackendID(backend)); found && spec.Name != "" {
				name = spec.Name
			}
		}
		return "backend-" + name
	}
	if len(chains) > 0 {
		return chains[len(chains)-1]
	}
	return tagDirect
}

func pickClientAndBackend(chains []string) (clientID, backendID string, ok bool) {
	for _, c := range chains {
		if cl, bk, parsed := singboxgen.ParsePerUserOutboundTag(c); parsed {
			return cl, bk, true
		}
	}
	return "", "", false
}

var _ ports.SingBoxConnections = ports.SingBoxConnections{}
