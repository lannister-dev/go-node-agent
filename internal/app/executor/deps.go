package executor

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type SingBoxControl interface {
	WriteConfig(ctx context.Context, cfg ports.SingBoxConfig) error
	Reload(ctx context.Context) error
	Connections(ctx context.Context) (ports.SingBoxConnections, error)
}

type PlacementStore interface {
	GetPlacement(ctx context.Context, id domain.PlacementID) (domain.Placement, bool, error)
	PutPlacement(ctx context.Context, p domain.Placement) error
	ListPlacements(ctx context.Context) ([]domain.Placement, error)
}

type BackendLookup interface {
	All() []singboxgen.BackendSpec
	Get(id domain.BackendID) (singboxgen.BackendSpec, bool)
}

type StaticBackends struct {
	specs map[domain.BackendID]singboxgen.BackendSpec
	order []domain.BackendID
}

func NewStaticBackends(backends []singboxgen.BackendSpec) *StaticBackends {
	out := &StaticBackends{specs: map[domain.BackendID]singboxgen.BackendSpec{}}
	for _, b := range backends {
		if _, ok := out.specs[b.ID]; ok {
			continue
		}
		out.specs[b.ID] = b
		out.order = append(out.order, b.ID)
	}
	return out
}

func (s *StaticBackends) All() []singboxgen.BackendSpec {
	out := make([]singboxgen.BackendSpec, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.specs[id])
	}
	return out
}

func (s *StaticBackends) Get(id domain.BackendID) (singboxgen.BackendSpec, bool) {
	b, ok := s.specs[id]
	return b, ok
}
