package haproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"
)

type Options struct {
	SocketPath string
	Timeout    time.Duration
	Logger     *slog.Logger
}

type Client struct {
	socketPath string
	timeout    time.Duration
	log        *slog.Logger
}

func New(opts Options) (*Client, error) {
	if opts.SocketPath == "" {
		return nil, errors.New("haproxy: SocketPath required")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		socketPath: opts.SocketPath,
		timeout:    timeout,
		log:        log.With("component", "haproxy"),
	}, nil
}

func (c *Client) Close() error { return nil }

func (c *Client) exec(ctx context.Context, cmd string) error {
	d := &net.Dialer{Timeout: c.timeout}
	conn, err := d.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.socketPath, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(c.timeout))
	}

	if _, err := io.WriteString(conn, cmd+"\n"); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	text := strings.TrimSpace(string(resp))
	if errMsg := classifyError(text); errMsg != "" {
		return fmt.Errorf("haproxy rejected %q: %s", cmd, errMsg)
	}
	return nil
}

func classifyError(resp string) string {
	if resp == "" {
		return ""
	}
	low := strings.ToLower(resp)
	prefixes := []string{
		"unknown command",
		"no such backend",
		"no such server",
		"backend not found",
		"server not found",
		"set-server",
		"invalid",
		"need '",
		"permission denied",
	}
	first := resp
	if i := strings.Index(resp, "\n"); i >= 0 {
		first = resp[:i]
	}
	for _, p := range prefixes {
		if strings.HasPrefix(low, p) {
			return strings.TrimSpace(first)
		}
	}
	return ""
}
