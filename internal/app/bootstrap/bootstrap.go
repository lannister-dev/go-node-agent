package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type Config struct {
	ExpectedNodeID domain.NodeID
	BootstrapToken string
	NodeKey        string
	NodeRole       string
}

type Result struct {
	Identity           domain.NodeIdentity
	FullResyncRequired bool
	WasFresh           bool
}

type Bootstrap struct {
	cfg   Config
	store IdentityStore
	ctl   Initializer
	ids   IDGenerator
	log   *slog.Logger
}

func New(cfg Config, store IdentityStore, ctl Initializer, ids IDGenerator, log *slog.Logger) *Bootstrap {
	if log == nil {
		log = slog.Default()
	}
	return &Bootstrap{cfg: cfg, store: store, ctl: ctl, ids: ids, log: log.With("component", "bootstrap")}
}

func (b *Bootstrap) Run(ctx context.Context) (Result, error) {
	existing, found, err := b.store.GetIdentity(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("load identity: %w", err)
	}
	if found {
		if err := b.assertExpectedNodeID(existing.NodeID); err != nil {
			return Result{}, err
		}
		b.log.Info("identity loaded from store",
			"node_id", existing.NodeID,
			"agent_instance_id", existing.AgentInstanceID,
			"bootstrapped_at", existing.BootstrappedAt,
		)
		return Result{Identity: existing, WasFresh: false}, nil
	}

	agentInstanceID := b.ids.NewID()
	b.log.Info("no local identity; calling control-api /initial",
		"agent_instance_id", agentInstanceID,
		"node_role", b.cfg.NodeRole,
	)

	resp, err := b.ctl.Initial(ctx, ports.InitialRequest{
		BootstrapToken:  b.cfg.BootstrapToken,
		NodeKey:         b.cfg.NodeKey,
		AgentInstanceID: agentInstanceID,
		NodeRole:        b.cfg.NodeRole,
	})
	if err != nil {
		return Result{}, fmt.Errorf("control-api initial: %w", err)
	}

	if err := b.assertExpectedNodeID(resp.Identity.NodeID); err != nil {
		return Result{}, err
	}
	if resp.Identity.AgentInstanceID != agentInstanceID {
		b.log.Warn("server returned different agent_instance_id than sent",
			"sent", agentInstanceID,
			"got", resp.Identity.AgentInstanceID,
		)
	}

	if err := b.store.PutIdentity(ctx, resp.Identity); err != nil {
		return Result{}, fmt.Errorf("persist identity: %w", err)
	}

	b.log.Info("identity bootstrapped",
		"node_id", resp.Identity.NodeID,
		"agent_instance_id", resp.Identity.AgentInstanceID,
		"full_resync_required", resp.FullResyncRequired,
	)
	return Result{
		Identity:           resp.Identity,
		FullResyncRequired: resp.FullResyncRequired,
		WasFresh:           true,
	}, nil
}

func (b *Bootstrap) assertExpectedNodeID(got domain.NodeID) error {
	if b.cfg.ExpectedNodeID == "" {
		return nil
	}
	if got != b.cfg.ExpectedNodeID {
		return fmt.Errorf("node_id mismatch: config expected %q, got %q", b.cfg.ExpectedNodeID, got)
	}
	return nil
}

var ErrIdentityNotFound = errors.New("identity not found")
