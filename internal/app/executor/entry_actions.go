package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
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

func (a *EntryActions) ValidateBackend(_ context.Context, plan domain.FlipPlan) error {
	if _, ok := a.backends.Get(plan.NewBackend); !ok {
		return fmt.Errorf("validate: new backend %s not in registry", plan.NewBackend)
	}
	a.log.Debug("backend validated", "new_backend", plan.NewBackend, "placement_id", plan.PlacementID)
	return nil
}

func (a *EntryActions) SwapRoute(ctx context.Context, plan domain.FlipPlan) error {
	if plan.Desired.ID == "" {
		return errors.New("swap: plan.Desired.ID required")
	}
	if plan.Desired.ID != plan.PlacementID {
		return fmt.Errorf("swap: plan.Desired.ID %q != plan.PlacementID %q", plan.Desired.ID, plan.PlacementID)
	}
	current, _, err := a.store.GetPlacement(ctx, plan.PlacementID)
	if err != nil {
		return fmt.Errorf("swap: load current placement: %w", err)
	}
	if plan.OpVersion != 0 && current.OpVersion >= plan.OpVersion {
		a.log.Warn("swap: stale op_version, skipping",
			"placement_id", plan.PlacementID,
			"plan_op_version", plan.OpVersion,
			"current_op_version", current.OpVersion,
		)
		return nil
	}
	desired := plan.Desired
	desired.Applied = domain.AppliedOk
	desired.LastAppliedAt = time.Now().UTC()

	data, err := a.renderConfigWith(ctx, &desired)
	if err != nil {
		return fmt.Errorf("swap: render config: %w", err)
	}
	if err := a.singbox.WriteConfig(ctx, ports.SingBoxConfig{Raw: data}); err != nil {
		return fmt.Errorf("swap: write config: %w", err)
	}
	if err := a.singbox.Reload(ctx); err != nil {
		return fmt.Errorf("swap: reload sing-box: %w", err)
	}
	if err := a.store.PutPlacement(ctx, desired); err != nil {
		return fmt.Errorf("swap: persist placement: %w", err)
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

func (a *EntryActions) OldBackendReachable(ctx context.Context, plan domain.FlipPlan) bool {
	b, ok := a.backends.Get(plan.OldBackend)
	if !ok {
		return false
	}
	dctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	addr := net.JoinHostPort(b.Address, strconv.FormatUint(uint64(b.Port), 10))
	conn, err := (&net.Dialer{}).DialContext(dctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
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
	current, _, err := a.store.GetPlacement(ctx, desired.ID)
	if err != nil {
		return fmt.Errorf("simple-apply: load current placement: %w", err)
	}
	if desired.OpVersion != 0 && current.OpVersion >= desired.OpVersion {
		a.log.Warn("simple-apply: stale op_version, skipping",
			"placement_id", desired.ID,
			"incoming_op_version", desired.OpVersion,
			"current_op_version", current.OpVersion,
		)
		return nil
	}
	desired.Applied = domain.AppliedOk
	desired.LastAppliedAt = time.Now().UTC()

	data, err := a.renderConfigWith(ctx, &desired)
	if err != nil {
		return fmt.Errorf("simple-apply: render: %w", err)
	}
	if err := a.singbox.WriteConfig(ctx, ports.SingBoxConfig{Raw: data}); err != nil {
		return fmt.Errorf("simple-apply: write config: %w", err)
	}
	if err := a.singbox.Reload(ctx); err != nil {
		return fmt.Errorf("simple-apply: reload: %w", err)
	}
	if err := a.store.PutPlacement(ctx, desired); err != nil {
		return fmt.Errorf("simple-apply: persist: %w", err)
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
	return a.renderConfigWith(ctx, nil)
}

func (a *EntryActions) renderConfigWith(ctx context.Context, overlay *domain.Placement) ([]byte, error) {
	placements, err := a.store.ListPlacements(ctx)
	if err != nil {
		return nil, err
	}
	if overlay != nil {
		placements = applyPlacementOverlay(placements, *overlay)
	}
	state := singboxgen.NodeState{
		Log:        a.logCfg,
		ClashAPI:   a.clashCfg,
		Inbound:    a.inbound,
		Backends:   a.backends.All(),
		Placements: placements,
	}
	if a.configPath != "" {
		base, rerr := os.ReadFile(a.configPath)
		if rerr != nil {
			return nil, fmt.Errorf("read base config %q: %w", a.configPath, rerr)
		}
		if len(base) > 0 {
			out, merr := singboxgen.MergeBase(base, state)
			if merr != nil {
				return nil, fmt.Errorf("merge base config %q: %w", a.configPath, merr)
			}
			return out, nil
		}
	}
	return singboxgen.Build(state)
}

func applyPlacementOverlay(placements []domain.Placement, overlay domain.Placement) []domain.Placement {
	out := make([]domain.Placement, 0, len(placements)+1)
	replaced := false
	for _, p := range placements {
		if p.ID == overlay.ID {
			out = append(out, overlay)
			replaced = true
			continue
		}
		out = append(out, p)
	}
	if !replaced {
		out = append(out, overlay)
	}
	return out
}
