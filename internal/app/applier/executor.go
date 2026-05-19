package applier

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type Executor interface {
	Apply(ctx context.Context, desired domain.Placement, existing domain.Placement, found bool) (retryable bool, err error)
}

type NoopExecutor struct{}

func (NoopExecutor) Apply(context.Context, domain.Placement, domain.Placement, bool) (bool, error) {
	return false, nil
}
