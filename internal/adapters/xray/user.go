package xray

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	cmd "github.com/lannister-dev/go-node-agent/pkg/proto/xray/app/proxyman/command"
	xprotocol "github.com/lannister-dev/go-node-agent/pkg/proto/xray/common/protocol"
	xserial "github.com/lannister-dev/go-node-agent/pkg/proto/xray/common/serial"
	xvless "github.com/lannister-dev/go-node-agent/pkg/proto/xray/proxy/vless"
)

const (
	typeAddUserOperation    = "xray.app.proxyman.command.AddUserOperation"
	typeRemoveUserOperation = "xray.app.proxyman.command.RemoveUserOperation"
	typeVlessAccount        = "xray.proxy.vless.Account"
)

func (c *Client) AddUser(ctx context.Context, user ports.XrayUser) error {
	if user.ClientID == "" {
		return errors.New("xray: ClientID required")
	}
	if !user.Transport.Valid() {
		return errors.New("xray: invalid transport")
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	account := &xvless.Account{
		Id:   string(user.ClientID),
		Flow: vlessFlowFor(user.Transport),
	}
	accountTM, err := wrapTyped(typeVlessAccount, account)
	if err != nil {
		return fmt.Errorf("xray: wrap account: %w", err)
	}

	op := &cmd.AddUserOperation{
		User: &xprotocol.User{
			Email:   string(user.ClientID),
			Level:   0,
			Account: accountTM,
		},
	}
	opTM, err := wrapTyped(typeAddUserOperation, op)
	if err != nil {
		return fmt.Errorf("xray: wrap add op: %w", err)
	}
	tag := c.tagFor(user.Transport)
	if tag == "" {
		return fmt.Errorf("xray: no inbound tag mapped for transport %q", user.Transport)
	}
	if _, err := c.handler.AlterInbound(ctx, &cmd.AlterInboundRequest{
		Tag:       tag,
		Operation: opTM,
	}); err != nil {
		return fmt.Errorf("xray: AlterInbound add %s tag=%s: %w", user.ClientID, tag, err)
	}
	return nil
}

func (c *Client) RemoveUser(ctx context.Context, clientID domain.ClientID) error {
	if clientID == "" {
		return errors.New("xray: ClientID required")
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	op := &cmd.RemoveUserOperation{Email: string(clientID)}
	opTM, err := wrapTyped(typeRemoveUserOperation, op)
	if err != nil {
		return fmt.Errorf("xray: wrap remove op: %w", err)
	}
	tags := c.allKnownTags()
	if len(tags) == 0 {
		return fmt.Errorf("xray: no inbound tags configured for remove %s", clientID)
	}
	var lastErr error
	removed := false
	for _, tag := range tags {
		if _, err := c.handler.AlterInbound(ctx, &cmd.AlterInboundRequest{
			Tag:       tag,
			Operation: opTM,
		}); err != nil {
			lastErr = fmt.Errorf("xray: AlterInbound remove %s tag=%s: %w", clientID, tag, err)
			continue
		}
		removed = true
	}
	if !removed && lastErr != nil {
		return lastErr
	}
	return nil
}

func (c *Client) allKnownTags() []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(c.tagByXport)+1)
	if c.inboundTag != "" {
		seen[c.inboundTag] = true
		out = append(out, c.inboundTag)
	}
	for _, tag := range c.tagByXport {
		if tag == "" || seen[tag] {
			continue
		}
		seen[tag] = true
		out = append(out, tag)
	}
	return out
}

func (c *Client) ListUsers(_ context.Context) ([]ports.XrayUser, error) {
	return nil, errors.New("xray: ListUsers not supported via HandlerService; query inbound config externally")
}

func (c *Client) UptimeSec(_ context.Context) (uint64, error) {
	return 0, errors.New("xray: UptimeSec not implemented (use StatsService)")
}

func wrapTyped(typeName string, m proto.Message) (*xserial.TypedMessage, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &xserial.TypedMessage{Type: typeName, Value: data}, nil
}

func vlessFlowFor(t domain.TransportKind) string {
	if t == domain.TransportReality {
		return "xtls-rprx-vision"
	}
	return ""
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}
