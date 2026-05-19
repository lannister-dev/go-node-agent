package haproxy

import (
	"context"
	"errors"
	"fmt"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

func (c *Client) SetServerAddr(ctx context.Context, backend, srv, addr string, port uint16) error {
	if backend == "" || srv == "" || addr == "" {
		return errors.New("haproxy: backend, server, addr required")
	}
	if port == 0 {
		return errors.New("haproxy: port must be > 0")
	}
	cmd := fmt.Sprintf("set server %s/%s addr %s port %d", backend, srv, addr, port)
	return c.exec(ctx, cmd)
}

func (c *Client) SetServerState(ctx context.Context, backend, srv string, state ports.HAProxyServerState) error {
	if backend == "" || srv == "" {
		return errors.New("haproxy: backend, server required")
	}
	switch state {
	case ports.HAProxyServerReady, ports.HAProxyServerDrain, ports.HAProxyServerMaint:
	default:
		return fmt.Errorf("haproxy: invalid state %q", state)
	}
	cmd := fmt.Sprintf("set server %s/%s state %s", backend, srv, state)
	return c.exec(ctx, cmd)
}
