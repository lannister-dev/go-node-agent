package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("agent.reconcile")

type Rebuilder interface {
	RebuildFromStore(ctx context.Context) error
}

type NoopRebuilder struct{}

func (NoopRebuilder) RebuildFromStore(context.Context) error { return nil }

type Config struct {
	Interval time.Duration
	Jitter   time.Duration
}

type Reconciler struct {
	cfg       Config
	rebuilder Rebuilder
	log       *slog.Logger
	runs      atomic.Uint32
	failures  atomic.Uint32
	lastRunAt atomic.Int64
}

func New(cfg Config, rebuilder Rebuilder, log *slog.Logger) (*Reconciler, error) {
	if rebuilder == nil {
		return nil, errors.New("reconcile: Rebuilder required")
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("reconcile: Interval must be > 0")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{
		cfg:       cfg,
		rebuilder: rebuilder,
		log:       log.With("component", "reconcile"),
	}, nil
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	r.log.Info("reconciler started", "interval", r.cfg.Interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

func (r *Reconciler) tick(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "reconcile.tick")
	defer span.End()
	r.runs.Add(1)
	r.lastRunAt.Store(time.Now().UTC().Unix())
	if err := r.rebuilder.RebuildFromStore(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "rebuild failed")
		r.failures.Add(1)
		r.log.Warn("reconcile tick failed", "err", err)
		return
	}
	r.log.Debug("reconcile tick complete")
}

func (r *Reconciler) Snapshot() (runs, failures uint32, lastRunUnix int64) {
	return r.runs.Load(), r.failures.Load(), r.lastRunAt.Load()
}
