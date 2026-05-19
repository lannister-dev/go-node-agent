package executor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/app/flip"
	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type SimpleApplier interface {
	SimpleApply(ctx context.Context, desired domain.Placement) error
}

type FlipExecutorOptions struct {
	DrainTimeout time.Duration
}

type FlipExecutor struct {
	simple       SimpleApplier
	orch         *flip.Orchestrator
	drainTimeout time.Duration
	log          *slog.Logger
}

func NewFlipExecutor(simple SimpleApplier, orch *flip.Orchestrator, opts FlipExecutorOptions, log *slog.Logger) (*FlipExecutor, error) {
	if simple == nil {
		return nil, errors.New("executor: SimpleApplier required")
	}
	if orch == nil {
		return nil, errors.New("executor: flip.Orchestrator required")
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = 30 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &FlipExecutor{
		simple:       simple,
		orch:         orch,
		drainTimeout: opts.DrainTimeout,
		log:          log.With("component", "executor"),
	}, nil
}

func (e *FlipExecutor) Apply(ctx context.Context, desired domain.Placement, existing domain.Placement, found bool) (bool, error) {
	if !found || !isBackendFlip(existing, desired) {
		if err := e.simple.SimpleApply(ctx, desired); err != nil {
			return false, err
		}
		return false, nil
	}
	plan := domain.FlipPlan{
		PlacementID:  desired.ID,
		OldBackend:   existing.BackendNodeID,
		NewBackend:   desired.BackendNodeID,
		DrainTimeout: e.drainTimeout,
		OpVersion:    desired.OpVersion,
		Desired:      desired,
	}
	e.log.Info("starting graceful flip",
		"placement_id", desired.ID,
		"old_backend", existing.BackendNodeID,
		"new_backend", desired.BackendNodeID,
		"op_version", desired.OpVersion,
	)
	if _, err := e.orch.Execute(ctx, plan); err != nil {
		if errors.Is(err, domain.ErrDrainTimeout) {
			e.log.Warn("flip drained on timeout, accepting force-close",
				"placement_id", desired.ID,
				"old_backend", existing.BackendNodeID,
			)
			return false, nil
		}
		return false, err
	}
	return false, nil
}

func isBackendFlip(existing, desired domain.Placement) bool {
	if existing.Applied != domain.AppliedOk {
		return false
	}
	if existing.Desired != domain.DesiredActive || desired.Desired != domain.DesiredActive {
		return false
	}
	if existing.ClientID != desired.ClientID {
		return false
	}
	return existing.BackendNodeID != desired.BackendNodeID
}
