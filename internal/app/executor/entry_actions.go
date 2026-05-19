package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type EntryActions struct {
	singbox    SingBoxControl
	store      PlacementStore
	backends   BackendLookup
	inbound    singboxgen.InboundSpec
	logCfg     singboxgen.LogSpec
	clashCfg   singboxgen.ClashAPISpec
	configPath string
	log        *slog.Logger
}

type EntryActionsConfig struct {
	Inbound    singboxgen.InboundSpec
	LogCfg     singboxgen.LogSpec
	ClashCfg   singboxgen.ClashAPISpec
	ConfigPath string
}

func NewEntryActions(cfg EntryActionsConfig, sb SingBoxControl, store PlacementStore, backends BackendLookup, log *slog.Logger) (*EntryActions, error) {
	if sb == nil || store == nil || backends == nil {
		return nil, errors.New("executor: singbox/store/backends required")
	}
	if cfg.Inbound.Tag == "" {
		return nil, errors.New("executor: inbound.Tag required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &EntryActions{
		singbox:    sb,
		store:      store,
		backends:   backends,
		inbound:    cfg.Inbound,
		logCfg:     cfg.LogCfg,
		clashCfg:   cfg.ClashCfg,
		configPath: cfg.ConfigPath,
		log:        log.With("component", "entry-actions"),
	}, nil
}

func (a *EntryActions) WarmBackend(_ context.Context, plan domain.FlipPlan) error {
	if _, ok := a.backends.Get(plan.NewBackend); !ok {
		return fmt.Errorf("warm: new backend %s not in registry", plan.NewBackend)
	}
	a.log.Debug("warm verified", "new_backend", plan.NewBackend, "placement_id", plan.PlacementID)
	return nil
}

func (a *EntryActions) SwapRoute(ctx context.Context, plan domain.FlipPlan) error {
	if plan.Desired.ID == "" {
		return errors.New("swap: plan.Desired.ID required")
	}
	if plan.Desired.ID != plan.PlacementID {
		return fmt.Errorf("swap: plan.Desired.ID %q != plan.PlacementID %q", plan.Desired.ID, plan.PlacementID)
	}
	desired := plan.Desired
	desired.Applied = domain.AppliedOk
	desired.LastAppliedAt = time.Now().UTC()
	if err := a.store.PutPlacement(ctx, desired); err != nil {
		return fmt.Errorf("swap: persist placement: %w", err)
	}

	data, err := a.renderConfig(ctx)
	if err != nil {
		return fmt.Errorf("swap: render config: %w", err)
	}
	if err := a.singbox.WriteConfig(ctx, ports.SingBoxConfig{Raw: data}); err != nil {
		return fmt.Errorf("swap: write config: %w", err)
	}
	if err := a.singbox.Reload(ctx); err != nil {
		return fmt.Errorf("swap: reload sing-box: %w", err)
	}
	a.log.Info("route swapped",
		"placement_id", plan.PlacementID,
		"old", plan.OldBackend,
		"new", plan.NewBackend,
	)
	return nil
}

func (a *EntryActions) OldBackendConnections(ctx context.Context, plan domain.FlipPlan) (uint64, error) {
	conns, err := a.singbox.Connections(ctx)
	if err != nil {
		return 0, err
	}
	tag := singboxgen.OutboundTagFor(plan.OldBackend)
	return conns.PerOutbound[tag], nil
}

func (a *EntryActions) CoolOldBackend(ctx context.Context, plan domain.FlipPlan) error {
	data, err := a.renderConfig(ctx)
	if err != nil {
		return fmt.Errorf("cool: render config: %w", err)
	}
	if err := a.singbox.WriteConfig(ctx, ports.SingBoxConfig{Raw: data}); err != nil {
		return fmt.Errorf("cool: write config: %w", err)
	}
	if err := a.singbox.Reload(ctx); err != nil {
		return fmt.Errorf("cool: reload sing-box: %w", err)
	}
	a.log.Debug("cooled old backend", "placement_id", plan.PlacementID, "old", plan.OldBackend)
	return nil
}

func (a *EntryActions) SimpleApply(ctx context.Context, desired domain.Placement) error {
	if desired.ID == "" {
		return errors.New("simple-apply: placement ID required")
	}
	if desired.Desired == domain.DesiredActive {
		if _, ok := a.backends.Get(desired.BackendNodeID); !ok {
			return fmt.Errorf("simple-apply: backend %s not in registry", desired.BackendNodeID)
		}
	}
	desired.Applied = domain.AppliedOk
	desired.LastAppliedAt = time.Now().UTC()
	if err := a.store.PutPlacement(ctx, desired); err != nil {
		return fmt.Errorf("simple-apply: persist: %w", err)
	}
	data, err := a.renderConfig(ctx)
	if err != nil {
		return fmt.Errorf("simple-apply: render: %w", err)
	}
	if err := a.singbox.WriteConfig(ctx, ports.SingBoxConfig{Raw: data}); err != nil {
		return fmt.Errorf("simple-apply: write config: %w", err)
	}
	if err := a.singbox.Reload(ctx); err != nil {
		return fmt.Errorf("simple-apply: reload: %w", err)
	}
	a.log.Debug("simple-apply complete",
		"placement_id", desired.ID,
		"desired_state", desired.Desired,
		"backend", desired.BackendNodeID,
	)
	return nil
}

func (a *EntryActions) RebuildFromStore(ctx context.Context) error {
	data, err := a.renderConfig(ctx)
	if err != nil {
		return fmt.Errorf("rebuild: render: %w", err)
	}
	if err := a.singbox.WriteConfig(ctx, ports.SingBoxConfig{Raw: data}); err != nil {
		return fmt.Errorf("rebuild: write config: %w", err)
	}
	if err := a.singbox.Reload(ctx); err != nil {
		return fmt.Errorf("rebuild: reload: %w", err)
	}
	a.log.Info("rebuilt sing-box config from store")
	return nil
}

func (a *EntryActions) renderConfig(ctx context.Context) ([]byte, error) {
	placements, err := a.store.ListPlacements(ctx)
	if err != nil {
		return nil, err
	}
	state := singboxgen.NodeState{
		Log:        a.logCfg,
		ClashAPI:   a.clashCfg,
		Inbound:    a.inbound,
		Backends:   a.backends.All(),
		Placements: placements,
	}
	if a.configPath != "" {
		if base, rerr := os.ReadFile(a.configPath); rerr == nil && len(base) > 0 {
			out, merr := singboxgen.MergeBase(base, state)
			if merr == nil {
				return out, nil
			}
			a.log.Warn("merge base config failed, falling back to from-scratch build",
				"err", merr, "config_path", a.configPath)
		}
	}
	return singboxgen.Build(state)
}
