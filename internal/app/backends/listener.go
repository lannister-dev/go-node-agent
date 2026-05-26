package backends

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/jsonv1"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type Subscriber interface {
	Subscribe(ctx context.Context, subject, durable string, handler ports.MsgHandler, opts ...ports.SubscribeOption) (ports.Unsubscribe, error)
}

type Defaults struct {
	Port      uint16
	Transport domain.TransportKind
	Reality   singboxgen.RealitySpec
}

type ListenerConfig struct {
	NodeID   domain.NodeID
	Subject  string
	Durable  string
	Defaults Defaults
}

type Listener struct {
	cfg      ListenerConfig
	sub      Subscriber
	registry *Registry
	log      *slog.Logger
}

func NewListener(cfg ListenerConfig, sub Subscriber, registry *Registry, log *slog.Logger) (*Listener, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("backends: NodeID required")
	}
	if cfg.Subject == "" || cfg.Durable == "" {
		return nil, errors.New("backends: Subject and Durable required")
	}
	if cfg.Defaults.Port == 0 {
		return nil, errors.New("backends: Defaults.Port required")
	}
	if sub == nil || registry == nil {
		return nil, errors.New("backends: sub and registry required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Listener{
		cfg:      cfg,
		sub:      sub,
		registry: registry,
		log:      log.With("component", "backend-listener"),
	}, nil
}

func (l *Listener) Run(ctx context.Context) error {
	unsub, err := l.sub.Subscribe(ctx, l.cfg.Subject, l.cfg.Durable, l.Handle)
	if err != nil {
		return fmt.Errorf("backends: subscribe %s: %w", l.cfg.Subject, err)
	}
	l.log.Info("backend listener subscribed", "subject", l.cfg.Subject, "durable", l.cfg.Durable)
	defer func() { _ = unsub() }()
	<-ctx.Done()
	return ctx.Err()
}

func (l *Listener) Handle(_ context.Context, msg ports.Msg) error {
	change, err := jsonv1.UnmarshalUpstreamChanged(msg.Data)
	if err != nil {
		l.log.Warn("decode upstream_changed failed", "err", err, "stream_seq", msg.Seq)
		return err
	}
	if change.NodeID != l.cfg.NodeID {
		l.log.Warn("upstream_changed for different node, dropping",
			"got", change.NodeID, "self", l.cfg.NodeID, "event_id", change.EventID)
		return nil
	}

	if change.Removed {
		existed := l.registry.Remove(change.BackendID)
		l.log.Info("backend removed",
			"backend_id", change.BackendID,
			"was_present", existed,
		)
		return nil
	}

	spec := l.buildSpec(change)
	added := l.registry.Upsert(spec)
	l.log.Info("backend registered",
		"backend_id", spec.ID,
		"address", spec.Address,
		"port", spec.Port,
		"new", added,
	)
	return nil
}

func (l *Listener) buildSpec(change jsonv1.UpstreamChange) singboxgen.BackendSpec {
	addr := change.InternalWgIP
	if addr == "" {
		addr = change.RealityIP
	}
	if addr == "" {
		addr = change.PublicDomain
	}
	port := l.cfg.Defaults.Port
	if change.AgentPort != 0 {
		port = change.AgentPort
	}
	return singboxgen.BackendSpec{
		ID:         change.BackendID,
		Address:    addr,
		Port:       port,
		ServerName: change.PublicDomain,
		Transport:  l.cfg.Defaults.Transport,
	}
}
