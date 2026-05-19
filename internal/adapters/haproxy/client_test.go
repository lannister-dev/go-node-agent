package haproxy

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type fakeAdminServer struct {
	mu       sync.Mutex
	received []string
	respond  func(cmd string) string
}

func newFakeAdminServer(t *testing.T, respond func(string) string) (string, *fakeAdminServer) {
	t.Helper()
	if respond == nil {
		respond = func(string) string { return "" }
	}
	dir, err := os.MkdirTemp("/tmp", "hp")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	path := filepath.Join(dir, "s")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen unix %s: %v", path, err)
	}
	f := &fakeAdminServer{respond: respond}
	t.Cleanup(func() { _ = ln.Close(); _ = os.RemoveAll(dir) })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.handle(conn)
		}
	}()
	return path, f
}

func (f *fakeAdminServer) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return
	}
	cmd := strings.TrimRight(line, "\r\n")
	f.mu.Lock()
	f.received = append(f.received, cmd)
	resp := f.respond(cmd)
	f.mu.Unlock()
	if resp != "" {
		_, _ = conn.Write([]byte(resp))
	}
}

func (f *fakeAdminServer) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.received))
	copy(out, f.received)
	return out
}

func newTestClient(t *testing.T, path string) *Client {
	t.Helper()
	c, err := New(Options{SocketPath: path, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestClient_SetServerAddr_SendsCorrectCommand(t *testing.T) {
	path, srv := newFakeAdminServer(t, nil)
	c := newTestClient(t, path)
	if err := c.SetServerAddr(t.Context(), "be_backend", "srv-praha-02", "10.0.0.42", 9000); err != nil {
		t.Fatalf("set addr: %v", err)
	}
	got := srv.snapshot()
	if len(got) != 1 || got[0] != "set server be_backend/srv-praha-02 addr 10.0.0.42 port 9000" {
		t.Errorf("commands: %v", got)
	}
}

func TestClient_SetServerState_SendsCorrectCommand(t *testing.T) {
	path, srv := newFakeAdminServer(t, nil)
	c := newTestClient(t, path)
	for _, state := range []ports.HAProxyServerState{ports.HAProxyServerReady, ports.HAProxyServerDrain, ports.HAProxyServerMaint} {
		if err := c.SetServerState(t.Context(), "be_x", "srv-1", state); err != nil {
			t.Errorf("state %s: %v", state, err)
		}
	}
	got := srv.snapshot()
	want := []string{
		"set server be_x/srv-1 state ready",
		"set server be_x/srv-1 state drain",
		"set server be_x/srv-1 state maint",
	}
	if len(got) != len(want) {
		t.Fatalf("expected 3 commands, got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("cmd[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestClient_ErrorResponseClassified(t *testing.T) {
	path, _ := newFakeAdminServer(t, func(string) string {
		return "No such server.\n"
	})
	c := newTestClient(t, path)
	err := c.SetServerAddr(t.Context(), "be_x", "missing", "1.2.3.4", 80)
	if err == nil {
		t.Fatal("expected error for No such server")
	}
}

func TestClient_UnknownCommandClassified(t *testing.T) {
	path, _ := newFakeAdminServer(t, func(string) string {
		return "Unknown command.\n"
	})
	c := newTestClient(t, path)
	err := c.SetServerState(t.Context(), "be_x", "srv-1", ports.HAProxyServerReady)
	if err == nil {
		t.Fatal("expected error for Unknown command")
	}
}

func TestClient_ContextCancel(t *testing.T) {
	path, _ := newFakeAdminServer(t, func(string) string {
		time.Sleep(500 * time.Millisecond)
		return ""
	})
	c := newTestClient(t, path)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := c.SetServerAddr(ctx, "be_x", "srv-1", "1.2.3.4", 80)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestClient_DialError(t *testing.T) {
	c, err := New(Options{SocketPath: "/non/existent/socket.sock", Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	err = c.SetServerAddr(t.Context(), "be_x", "srv-1", "1.2.3.4", 80)
	if err == nil {
		t.Fatal("expected dial error")
	}
}

func TestClient_RejectsInvalidInputs(t *testing.T) {
	path, _ := newFakeAdminServer(t, nil)
	c := newTestClient(t, path)

	cases := []struct {
		name         string
		backend, srv string
		addr         string
		port         uint16
	}{
		{"empty backend", "", "s", "1.2.3.4", 80},
		{"empty server", "b", "", "1.2.3.4", 80},
		{"empty addr", "b", "s", "", 80},
		{"zero port", "b", "s", "1.2.3.4", 0},
	}
	for _, c2 := range cases {
		if err := c.SetServerAddr(t.Context(), c2.backend, c2.srv, c2.addr, c2.port); err == nil {
			t.Errorf("%s: expected error", c2.name)
		}
	}

	if err := c.SetServerState(t.Context(), "b", "s", "weird"); err == nil {
		t.Error("invalid state should error")
	}
	if err := c.SetServerState(t.Context(), "", "s", ports.HAProxyServerReady); err == nil {
		t.Error("empty backend should error")
	}
}

func TestNew_RejectsEmptySocketPath(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestClient_PortConformance(t *testing.T) {
	var _ interface {
		SetServerAddr(context.Context, string, string, string, uint16) error
		SetServerState(context.Context, string, string, ports.HAProxyServerState) error
	} = (*Client)(nil)
	if errors.Is(nil, nil) {
		t.Log("ok")
	}
}
