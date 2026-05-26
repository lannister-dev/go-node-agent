package xray

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	cmd "github.com/lannister-dev/go-node-agent/pkg/proto/xray/app/proxyman/command"
	xvless "github.com/lannister-dev/go-node-agent/pkg/proto/xray/proxy/vless"
)

var _ ports.Xray = (*Client)(nil)

type fakeHandlerServer struct {
	cmd.UnimplementedHandlerServiceServer
	mu          sync.Mutex
	received    []*cmd.AlterInboundRequest
	respErr     error
	respErrFunc func(*cmd.AlterInboundRequest) error
	delay       time.Duration
}

func (f *fakeHandlerServer) AlterInbound(ctx context.Context, req *cmd.AlterInboundRequest) (*cmd.AlterInboundResponse, error) {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	f.received = append(f.received, proto.Clone(req).(*cmd.AlterInboundRequest))
	err := f.respErr
	fn := f.respErrFunc
	f.mu.Unlock()
	if fn != nil {
		err = fn(req)
	}
	if err != nil {
		return nil, err
	}
	return &cmd.AlterInboundResponse{}, nil
}

func (f *fakeHandlerServer) snapshot() []*cmd.AlterInboundRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*cmd.AlterInboundRequest, len(f.received))
	copy(out, f.received)
	return out
}

func startFakeServer(t *testing.T, fake *fakeHandlerServer) *Client {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	cmd.RegisterHandlerServiceServer(srv, fake)

	done := make(chan struct{})
	go func() {
		_ = srv.Serve(lis)
		close(done)
	}()
	t.Cleanup(func() {
		srv.GracefulStop()
		<-done
	})

	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	}
	c, err := New(Options{
		Address:    "passthrough:///bufnet",
		InboundTag: "vless-in",
		Timeout:    2 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func decodeAddOp(t *testing.T, req *cmd.AlterInboundRequest) (*cmd.AddUserOperation, *xvless.Account) {
	t.Helper()
	if req.GetOperation().GetType() != typeAddUserOperation {
		t.Fatalf("op type: %s", req.GetOperation().GetType())
	}
	var op cmd.AddUserOperation
	if err := proto.Unmarshal(req.GetOperation().GetValue(), &op); err != nil {
		t.Fatalf("unmarshal add op: %v", err)
	}
	if op.GetUser().GetAccount().GetType() != typeVlessAccount {
		t.Fatalf("account type: %s", op.GetUser().GetAccount().GetType())
	}
	var acc xvless.Account
	if err := proto.Unmarshal(op.GetUser().GetAccount().GetValue(), &acc); err != nil {
		t.Fatalf("unmarshal account: %v", err)
	}
	return &op, &acc
}

func TestAddUser_VLESS_WS(t *testing.T) {
	fake := &fakeHandlerServer{}
	c := startFakeServer(t, fake)
	if err := c.AddUser(t.Context(), ports.XrayUser{
		ClientID:  "01234567-89ab-cdef-0123-456789abcdef",
		Transport: domain.TransportWS,
	}); err != nil {
		t.Fatalf("add: %v", err)
	}
	reqs := fake.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 req, got %d", len(reqs))
	}
	if reqs[0].GetTag() != "vless-in" {
		t.Errorf("tag: %s", reqs[0].GetTag())
	}
	op, acc := decodeAddOp(t, reqs[0])
	if op.GetUser().GetEmail() != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Errorf("email: %s", op.GetUser().GetEmail())
	}
	if acc.GetId() != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Errorf("account id: %s", acc.GetId())
	}
	if acc.GetFlow() != "" {
		t.Errorf("ws should have empty flow, got %q", acc.GetFlow())
	}
}

func TestAddUser_RealityHasVisionFlow(t *testing.T) {
	fake := &fakeHandlerServer{}
	c := startFakeServer(t, fake)
	if err := c.AddUser(t.Context(), ports.XrayUser{
		ClientID:  "01234567-89ab-cdef-0123-456789abcdef",
		Transport: domain.TransportReality,
	}); err != nil {
		t.Fatal(err)
	}
	_, acc := decodeAddOp(t, fake.snapshot()[0])
	if acc.GetFlow() != "xtls-rprx-vision" {
		t.Errorf("reality flow: %q", acc.GetFlow())
	}
}

func TestRemoveUser(t *testing.T) {
	fake := &fakeHandlerServer{}
	c := startFakeServer(t, fake)
	if err := c.RemoveUser(t.Context(), "01234567-89ab-cdef-0123-456789abcdef"); err != nil {
		t.Fatal(err)
	}
	reqs := fake.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("requests: %d", len(reqs))
	}
	if reqs[0].GetOperation().GetType() != typeRemoveUserOperation {
		t.Errorf("type: %s", reqs[0].GetOperation().GetType())
	}
	var op cmd.RemoveUserOperation
	if err := proto.Unmarshal(reqs[0].GetOperation().GetValue(), &op); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if op.GetEmail() != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Errorf("email: %s", op.GetEmail())
	}
}

func TestAddUser_ServerError(t *testing.T) {
	fake := &fakeHandlerServer{respErr: errors.New("xray rejected user (already exists)")}
	c := startFakeServer(t, fake)
	err := c.AddUser(t.Context(), ports.XrayUser{
		ClientID:  "01234567-89ab-cdef-0123-456789abcdef",
		Transport: domain.TransportWS,
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAddUser_RemovesAndRetriesOnAlreadyExists(t *testing.T) {
	fake := &fakeHandlerServer{}
	fake.respErrFunc = func(req *cmd.AlterInboundRequest) error {
		if len(fake.received) == 1 && req.GetOperation().GetType() == typeAddUserOperation {
			return errors.New("proxy/vless: user already exists")
		}
		return nil
	}
	c := startFakeServer(t, fake)
	if err := c.AddUser(t.Context(), ports.XrayUser{
		ClientID:  "01234567-89ab-cdef-0123-456789abcdef",
		Transport: domain.TransportReality,
	}); err != nil {
		t.Fatalf("expected self-heal to succeed, got: %v", err)
	}
	reqs := fake.snapshot()
	var addOps, removeOps int
	for _, r := range reqs {
		switch r.GetOperation().GetType() {
		case typeAddUserOperation:
			addOps++
		case typeRemoveUserOperation:
			removeOps++
		}
	}
	if addOps != 2 {
		t.Errorf("expected 2 add ops (initial + retry), got %d", addOps)
	}
	if removeOps == 0 {
		t.Errorf("expected at least 1 remove op for self-heal, got 0")
	}
}

func TestAddUser_RejectsInvalidInputs(t *testing.T) {
	c := startFakeServer(t, &fakeHandlerServer{})
	cases := []ports.XrayUser{
		{Transport: domain.TransportWS},
		{ClientID: "x", Transport: "bogus"},
	}
	for _, u := range cases {
		if err := c.AddUser(t.Context(), u); err == nil {
			t.Errorf("expected error for %+v", u)
		}
	}
}

func TestContextCancel(t *testing.T) {
	fake := &fakeHandlerServer{delay: 500 * time.Millisecond}
	c := startFakeServer(t, fake)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := c.AddUser(ctx, ports.XrayUser{
		ClientID:  "01234567-89ab-cdef-0123-456789abcdef",
		Transport: domain.TransportWS,
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestListUsers_NotImplemented(t *testing.T) {
	c := startFakeServer(t, &fakeHandlerServer{})
	if _, err := c.ListUsers(t.Context()); err == nil {
		t.Fatal("expected not-implemented error")
	}
}

func TestUptimeSec_NotImplemented(t *testing.T) {
	c := startFakeServer(t, &fakeHandlerServer{})
	if _, err := c.UptimeSec(t.Context()); err == nil {
		t.Fatal("expected not-implemented error")
	}
}

func TestNew_Validates(t *testing.T) {
	cases := map[string]Options{
		"missing Address":    {InboundTag: "t"},
		"missing InboundTag": {Address: "127.0.0.1:1"},
	}
	for name, opts := range cases {
		if _, err := New(opts); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
