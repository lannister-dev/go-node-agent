package ports

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type Store interface {
	GetPlacement(ctx context.Context, id domain.PlacementID) (domain.Placement, bool, error)
	PutPlacement(ctx context.Context, p domain.Placement) error
	DeletePlacement(ctx context.Context, id domain.PlacementID) error
	ListPlacements(ctx context.Context) ([]domain.Placement, error)

	GetCursor(ctx context.Context, name string) (uint64, error)
	PutCursor(ctx context.Context, name string, seq uint64) error

	GetIdentity(ctx context.Context) (domain.NodeIdentity, bool, error)
	PutIdentity(ctx context.Context, id domain.NodeIdentity) error

	Snapshot(ctx context.Context) error
	Close() error
}
