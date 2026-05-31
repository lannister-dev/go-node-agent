package snapshot

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/platform/idgen"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type Subscriber interface {
	Subscribe(ctx context.Context, subject, durable string, handler ports.MsgHandler, opts ...ports.SubscribeOption) (ports.Unsubscribe, error)
}

type Publisher interface {
	Publish(ctx context.Context, subject string, headers map[string]string, data []byte) error
}

type PlacementStore interface {
	GetPlacement(ctx context.Context, id domain.PlacementID) (domain.Placement, bool, error)
	PutPlacement(ctx context.Context, p domain.Placement) error
	ListPlacements(ctx context.Context) ([]domain.Placement, error)
	DeletePlacement(ctx context.Context, id domain.PlacementID) error
}

type Rebuilder interface {
	RebuildFromStore(ctx context.Context) error
}

type IDGenerator = idgen.Generator

type NoopRebuilder struct{}

func (NoopRebuilder) RebuildFromStore(context.Context) error { return nil }
