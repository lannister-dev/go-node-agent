package flip

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

var flipTracer = otel.Tracer("agent.flip")

type Orchestrator struct {
	actions      Actions
	clock        Clock
	log          *slog.Logger
	drainPollMin time.Duration
	drainPollMax time.Duration
}

func New(actions Actions, log *slog.Logger, opts Options) (*Orchestrator, error) {
	if actions == nil {
		return nil, errors.New("flip: Actions required")
	}
	if log == nil {
		log = slog.Default()
	}
	minInt := opts.DrainPollInterval
	if minInt <= 0 {
		minInt = 50 * time.Millisecond
	}
	maxInt := opts.DrainPollMax
	if maxInt < minInt {
		maxInt = minInt
	}
	return &Orchestrator{
		actions:      actions,
		clock:        realClock{},
		log:          log.With("component", "flip"),
		drainPollMin: minInt,
		drainPollMax: maxInt,
	}, nil
}

func (o *Orchestrator) withClock(c Clock) *Orchestrator {
	o.clock = c
	return o
}

func (o *Orchestrator) Execute(ctx context.Context, plan domain.FlipPlan) (domain.FlipPlan, error) {
	if plan.PlacementID == "" {
		return plan, errors.New("flip: PlacementID required")
	}
	if plan.OldBackend == plan.NewBackend {
		return plan, errors.New("flip: OldBackend == NewBackend, no flip needed")
	}
	if plan.DrainTimeout <= 0 {
		return plan, errors.New("flip: DrainTimeout must be > 0")
	}

	ctx, span := flipTracer.Start(ctx, "flip.execute",
		trace.WithAttributes(
			attribute.String("placement_id", string(plan.PlacementID)),
			attribute.String("old_backend", string(plan.OldBackend)),
			attribute.String("new_backend", string(plan.NewBackend)),
			attribute.String("op_version", strconv.FormatUint(uint64(plan.OpVersion), 10)),
			attribute.String("drain_timeout", plan.DrainTimeout.String()),
		),
	)
	defer span.End()

	if plan.State == "" {
		plan.State = domain.FlipSteady
	}
	if plan.State == domain.FlipSteady && !plan.StartedAt.IsZero() {
		return plan, nil
	}
	if plan.StartedAt.IsZero() {
		plan.StartedAt = o.clock.Now()
	}

	for {
		if err := ctx.Err(); err != nil {
			return plan, err
		}
		next := plan.Next()
		o.log.Debug("flip transition",
			"placement_id", plan.PlacementID,
			"from", plan.State, "to", next,
			"old", plan.OldBackend, "new", plan.NewBackend,
		)
		if err := o.runPhase(ctx, plan, next); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, string(next))
			return plan, fmt.Errorf("flip phase %s: %w", next, err)
		}
		plan.State = next
		if plan.State == domain.FlipSteady {
			o.log.Info("flip complete",
				"placement_id", plan.PlacementID,
				"old", plan.OldBackend, "new", plan.NewBackend,
				"duration", o.clock.Now().Sub(plan.StartedAt),
			)
			return plan, nil
		}
	}
}

func (o *Orchestrator) runPhase(ctx context.Context, plan domain.FlipPlan, target domain.FlipState) error {
	ctx, span := flipTracer.Start(ctx, "flip.phase."+string(target),
		trace.WithAttributes(attribute.String("placement_id", string(plan.PlacementID))),
	)
	defer span.End()
	var err error
	switch target {
	case domain.FlipAnnounced:
		err = nil
	case domain.FlipWarming:
		err = o.actions.ValidateBackend(ctx, plan)
	case domain.FlipSwap:
		err = o.actions.SwapRoute(ctx, plan)
	case domain.FlipCooling:
		err = o.drain(ctx, plan)
	case domain.FlipCool:
		err = o.actions.CoolOldBackend(ctx, plan)
	case domain.FlipSteady:
		err = nil
	default:
		err = fmt.Errorf("unknown target state %q", target)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "phase failed")
	}
	return err
}

func (o *Orchestrator) drain(ctx context.Context, plan domain.FlipPlan) error {
	deadline := o.clock.Now().Add(plan.DrainTimeout)
	interval := o.drainPollMin
	var lastRemaining uint64
	first := true
	for {
		remaining, err := o.actions.OldBackendConnections(ctx, plan)
		if err != nil {
			return fmt.Errorf("query connections: %w", err)
		}
		if remaining == 0 {
			o.log.Debug("drain complete", "placement_id", plan.PlacementID)
			return nil
		}
		if !o.clock.Now().Before(deadline) {
			o.log.Warn("drain timeout, proceeding to force-close",
				"placement_id", plan.PlacementID,
				"remaining", remaining,
				"timeout", plan.DrainTimeout,
			)
			return domain.ErrDrainTimeout
		}
		if !first && remaining < lastRemaining {
			interval = o.drainPollMin
		} else if interval < o.drainPollMax {
			interval *= 2
			if interval > o.drainPollMax {
				interval = o.drainPollMax
			}
		}
		first = false
		lastRemaining = remaining
		select {
		case <-o.clock.After(interval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
