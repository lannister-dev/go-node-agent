package nats

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natssrv "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

var _ ports.Transport = (*Transport)(nil)

func startServer(t *testing.T) *natssrv.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server did not become ready")
	}
	return srv
}

func newTransport(t *testing.T, srv *natssrv.Server) *Transport {
	t.Helper()
	tr, err := New(t.Context(), Options{
		URL:    srv.ClientURL(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func mustCreateStream(t *testing.T, srv *natssrv.Server, name string, subjects []string) {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("admin jetstream: %v", err)
	}
	if _, err := js.CreateOrUpdateStream(t.Context(), jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
}

func TestTransport_PublishSubscribe_RoundTrip(t *testing.T) {
	srv := startServer(t)
	mustCreateStream(t, srv, "AGENT_CMDS", []string{"agent.placements.>"})

	tr := newTransport(t, srv)
	const subject = "agent.placements.lv-01.commands"

	received := make(chan ports.Msg, 1)
	unsub, err := tr.Subscribe(t.Context(), subject, "agent_lv01_cmds", func(_ context.Context, msg ports.Msg) error {
		received <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = unsub() })

	if err := tr.Publish(t.Context(), subject, map[string]string{"x-schema": "v1", "x-trace": "abc"}, []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-received:
		if string(got.Data) != "hello" {
			t.Errorf("data = %q", got.Data)
		}
		if got.Subject != subject {
			t.Errorf("subject = %q", got.Subject)
		}
		if got.Headers["x-schema"] != "v1" || got.Headers["x-trace"] != "abc" {
			t.Errorf("headers = %+v", got.Headers)
		}
		if got.Seq == 0 {
			t.Error("expected non-zero stream seq")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestTransport_Subscribe_DurableReplaysOnReconnect(t *testing.T) {
	srv := startServer(t)
	mustCreateStream(t, srv, "AGENT_CMDS", []string{"agent.placements.>"})

	const subject = "agent.placements.lv-01.commands"
	const durable = "agent_lv01_replay"

	tr1 := newTransport(t, srv)
	if err := tr1.Publish(t.Context(), subject, nil, []byte("m1")); err != nil {
		t.Fatalf("publish m1: %v", err)
	}
	if err := tr1.Publish(t.Context(), subject, nil, []byte("m2")); err != nil {
		t.Fatalf("publish m2: %v", err)
	}

	received := make([]string, 0, 2)
	var mu sync.Mutex
	done := make(chan struct{}, 1)
	unsub, err := tr1.Subscribe(t.Context(), subject, durable, func(_ context.Context, msg ports.Msg) error {
		mu.Lock()
		received = append(received, string(msg.Data))
		if len(received) == 2 {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		mu.Lock()
		t.Fatalf("timeout. received so far: %v", received)
	}
	if err := unsub(); err != nil {
		t.Errorf("unsub: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 || received[0] != "m1" || received[1] != "m2" {
		t.Errorf("expected [m1, m2], got %v", received)
	}
}

func TestTransport_Subscribe_NakOnHandlerError(t *testing.T) {
	srv := startServer(t)
	mustCreateStream(t, srv, "AGENT_CMDS", []string{"agent.placements.>"})

	const subject = "agent.placements.lv-01.commands"
	tr := newTransport(t, srv)

	var attempts atomic.Int32
	got := make(chan struct{}, 1)
	unsub, err := tr.Subscribe(t.Context(), subject, "nak_test", func(_ context.Context, msg ports.Msg) error {
		n := attempts.Add(1)
		if n >= 2 {
			select {
			case got <- struct{}{}:
			default:
			}
			return nil
		}
		return errors.New("transient fail")
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = unsub() })

	if err := tr.Publish(t.Context(), subject, nil, []byte("retry-me")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatalf("did not receive redelivery; attempts=%d", attempts.Load())
	}
	if attempts.Load() < 2 {
		t.Errorf("expected >= 2 delivery attempts, got %d", attempts.Load())
	}
}

func TestTransport_Subscribe_RejectsEmptyArgs(t *testing.T) {
	srv := startServer(t)
	mustCreateStream(t, srv, "AGENT_CMDS", []string{"agent.placements.>"})
	tr := newTransport(t, srv)

	noopHandler := func(context.Context, ports.Msg) error { return nil }
	cases := []struct {
		subject, durable string
		handler          ports.MsgHandler
	}{
		{"", "d", noopHandler},
		{"agent.placements.x.commands", "", noopHandler},
		{"agent.placements.x.commands", "d", nil},
	}
	for _, c := range cases {
		if _, err := tr.Subscribe(t.Context(), c.subject, c.durable, c.handler); err == nil {
			t.Errorf("expected error for case %+v", c)
		}
	}
}

func TestNew_RejectsEmptyURL(t *testing.T) {
	if _, err := New(t.Context(), Options{}); err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestTransport_Close_DrainsAndStopsConsumers(t *testing.T) {
	srv := startServer(t)
	mustCreateStream(t, srv, "AGENT_CMDS", []string{"agent.placements.>"})
	const subject = "agent.placements.lv-01.commands"

	tr, err := New(t.Context(), Options{
		URL:    srv.ClientURL(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = tr.Subscribe(t.Context(), subject, "close_test", func(context.Context, ports.Msg) error { return nil })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
