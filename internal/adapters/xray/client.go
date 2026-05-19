package xray

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	cmd "github.com/lannister-dev/go-node-agent/pkg/proto/xray/app/proxyman/command"
)

type Options struct {
	Address     string
	InboundTag  string
	Timeout     time.Duration
	DialOptions []grpc.DialOption
	Logger      *slog.Logger
}

type Client struct {
	conn       *grpc.ClientConn
	handler    cmd.HandlerServiceClient
	inboundTag string
	timeout    time.Duration
	log        *slog.Logger
}

func New(opts Options) (*Client, error) {
	if opts.Address == "" {
		return nil, errors.New("xray: Address required")
	}
	if opts.InboundTag == "" {
		return nil, errors.New("xray: InboundTag required")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	dialOpts := opts.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(opts.Address, dialOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:       conn,
		handler:    cmd.NewHandlerServiceClient(conn),
		inboundTag: opts.InboundTag,
		timeout:    timeout,
		log:        log.With("component", "xray"),
	}, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) Name() string { return "xray" }

func (c *Client) Check(_ context.Context) error {
	if c.conn == nil {
		return errors.New("xray: not initialized")
	}
	state := c.conn.GetState()
	if state == connectivity.Ready || state == connectivity.Idle {
		return nil
	}
	return fmt.Errorf("xray: grpc state=%s", state.String())
}
