package ports

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type InitialRequest struct {
	BootstrapToken  string
	NodeKey         string
	AgentInstanceID string
	NodeRole        string
}

type InitialResponse struct {
	Identity           domain.NodeIdentity
	FullResyncRequired bool
}

type ControlAPI interface {
	Initial(ctx context.Context, req InitialRequest) (InitialResponse, error)
}
