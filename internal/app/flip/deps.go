package flip

import (
	"context"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type Actions interface {
	ValidateBackend(ctx context.Context, plan domain.FlipPlan) error
	SwapRoute(ctx context.Context, plan domain.FlipPlan) error
	OldBackendConnections(ctx context.Context, plan domain.FlipPlan) (uint64, error)
	OldBackendReachable(ctx context.Context, plan domain.FlipPlan) bool
	CoolOldBackend(ctx context.Context, plan domain.FlipPlan) error
}

type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type Options struct {
	DrainPollInterval time.Duration
	DrainPollMax      time.Duration
	OverallBudgetMul  int
}
