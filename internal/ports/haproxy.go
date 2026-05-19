package ports

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type HAProxyServerState string

const (
	HAProxyServerReady HAProxyServerState = "ready"
	HAProxyServerDrain HAProxyServerState = "drain"
	HAProxyServerMaint HAProxyServerState = "maint"
)

type HAProxy interface {
	SetServerAddr(ctx context.Context, backend, server, addr string, port uint16) error
	SetServerState(ctx context.Context, backend, server string, state HAProxyServerState) error
	Connections(ctx context.Context, backend, server string) (uint64, error)
	ListServers(ctx context.Context, backend string) (map[string]domain.Backend, error)
}
