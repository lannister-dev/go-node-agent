package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Options struct {
	URL            string
	Name           string
	TLSConfig      *tls.Config
	ReconnectWait  time.Duration
	MaxReconnects  int
	DrainTimeout   time.Duration
	PublishTimeout time.Duration
	Logger         *slog.Logger
}

type Transport struct {
	nc             *nats.Conn
	js             jetstream.JetStream
	log            *slog.Logger
	publishTimeout time.Duration

	mu       sync.Mutex
	consumes []jetstream.ConsumeContext
}

func New(ctx context.Context, opts Options) (*Transport, error) {
	if opts.URL == "" {
		return nil, errors.New("nats: URL required")
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "nats")

	reconnectWait := opts.ReconnectWait
	if reconnectWait <= 0 {
		reconnectWait = 2 * time.Second
	}
	maxReconnects := opts.MaxReconnects
	if maxReconnects == 0 {
		maxReconnects = -1
	}
	drainTimeout := opts.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 5 * time.Second
	}
	publishTimeout := opts.PublishTimeout
	if publishTimeout <= 0 {
		publishTimeout = 3 * time.Second
	}

	name := opts.Name
	if name == "" {
		name = "go-node-agent"
	}

	natsOpts := []nats.Option{
		nats.Name(name),
		nats.ReconnectWait(reconnectWait),
		nats.MaxReconnects(maxReconnects),
		nats.DrainTimeout(drainTimeout),
		nats.RetryOnFailedConnect(true),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Info("connection closed")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			subj := ""
			if sub != nil {
				subj = sub.Subject
			}
			log.Error("async nats error", "subject", subj, "err", err)
		}),
	}
	if opts.TLSConfig != nil {
		natsOpts = append(natsOpts, nats.Secure(opts.TLSConfig))
	}

	nc, err := nats.Connect(opts.URL, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: jetstream init: %w", err)
	}

	log.Info("connected", "url", nc.ConnectedUrl(), "name", name)

	return &Transport{
		nc:             nc,
		js:             js,
		log:            log,
		publishTimeout: publishTimeout,
	}, nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	for _, cc := range t.consumes {
		cc.Stop()
	}
	t.consumes = nil
	t.mu.Unlock()

	if err := t.nc.Drain(); err != nil {
		t.nc.Close()
		return fmt.Errorf("nats: drain: %w", err)
	}
	return nil
}

func (t *Transport) trackConsume(cc jetstream.ConsumeContext) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.consumes = append(t.consumes, cc)
}
