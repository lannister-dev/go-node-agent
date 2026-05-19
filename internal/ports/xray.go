package ports

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type XrayUser struct {
	ClientID  domain.ClientID
	Transport domain.TransportKind
}

type Xray interface {
	AddUser(ctx context.Context, user XrayUser) error
	RemoveUser(ctx context.Context, clientID domain.ClientID) error
	ListUsers(ctx context.Context) ([]XrayUser, error)
	UptimeSec(ctx context.Context) (uint64, error)
}
