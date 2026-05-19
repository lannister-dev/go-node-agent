package bootstrap

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/platform/idgen"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type IdentityStore interface {
	GetIdentity(ctx context.Context) (domain.NodeIdentity, bool, error)
	PutIdentity(ctx context.Context, id domain.NodeIdentity) error
}

type Initializer interface {
	Initial(ctx context.Context, req ports.InitialRequest) (ports.InitialResponse, error)
}

type IDGenerator = idgen.Generator
