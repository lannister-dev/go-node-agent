package backends

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type fakeSub struct {
	mu      sync.Mutex
	handler ports.MsgHandler
	subject string
	durable string
	ready   chan struct{}
}

func newFakeSub() *fakeSub { return &fakeSub{ready: make(chan struct{})} }

func (f *fakeSub) Subscribe(_ context.Context, subject, durable string, h ports.MsgHandler, _ ...ports.SubscribeOption) (ports.Unsubscribe, error) {
	f.mu.Lock()
	f.handler = h
	f.subject = subject
	f.durable = durable
	f.mu.Unlock()
	if f.ready != nil {
		close(f.ready)
	}
	return func() error { return nil }, nil
}

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newListener(t *testing.T, sub Subscriber, reg *Registry) *Listener {
	t.Helper()
	l, err := NewListener(ListenerConfig{
		NodeID:  "lv-01",
		Subject: "agent.placements.lv-01.upstream",
		Durable: "agent_lv01_upstream",
		Defaults: Defaults{
			Port:      9000,
			Transport: domain.TransportWS,
		},
	}, sub, reg, silent())
	if err != nil {
		t.Fatalf("new listener: %v", err)
	}
	return l
}

func TestListener_RegistersBackendOnEvent(t *testing.T) {
	reg := NewRegistry()
	l := newListener(t, &fakeSub{}, reg)

	body := []byte(`{
		"schema_version": 1,
		"event_id": "evt-1",
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"upstream_node_id": "praha-02",
		"upstream_public_domain": "praha-02.vpn.example.com"
	}`)
	if err := l.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	spec, ok := reg.Get("praha-02")
	if !ok {
		t.Fatal("backend not registered")
	}
	if spec.Address != "praha-02.vpn.example.com" {
		t.Errorf("address: %s", spec.Address)
	}
	if spec.Port != 9000 {
		t.Errorf("port should fall back to default, got %d", spec.Port)
	}
	if spec.Transport != domain.TransportWS {
		t.Errorf("transport: %s", spec.Transport)
	}
	if spec.Reality.Enabled {
		t.Error("Reality should be disabled when reality_ip absent")
	}
}

func TestListener_PrefersInternalWgIP(t *testing.T) {
	reg := NewRegistry()
	l := newListener(t, &fakeSub{}, reg)
	body := []byte(`{
		"event_id": "e",
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"upstream_node_id": "praha-02",
		"upstream_public_domain": "praha-02.vpn.example.com",
		"upstream_reality_ip": "1.2.3.4",
		"upstream_internal_wg_ip": "10.10.0.5",
		"upstream_agent_port": 10100
	}`)
	if err := l.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatal(err)
	}
	spec, _ := reg.Get("praha-02")
	if spec.Address != "10.10.0.5" {
		t.Errorf("must prefer internal_wg_ip; got %s", spec.Address)
	}
	if spec.Port != 10100 {
		t.Errorf("must use upstream_agent_port; got %d", spec.Port)
	}
	if spec.Reality.Enabled {
		t.Error("backend outbound must never enable Reality (wg-mesh internal traffic is plain)")
	}
}

func TestListener_FallsBackToRealityIPThenPublicDomain(t *testing.T) {
	reg := NewRegistry()
	l := newListener(t, &fakeSub{}, reg)
	body := []byte(`{
		"event_id": "e2",
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"upstream_node_id": "legacy",
		"upstream_public_domain": "x.example.com",
		"upstream_reality_ip": "1.2.3.4"
	}`)
	_ = l.Handle(t.Context(), ports.Msg{Data: body})
	spec, _ := reg.Get("legacy")
	if spec.Address != "1.2.3.4" {
		t.Errorf("without internal_wg_ip should fall back to reality_ip; got %s", spec.Address)
	}
	if spec.Reality.Enabled {
		t.Error("Reality must remain disabled in backend outbound")
	}
}

func TestListener_DifferentNodeDropped(t *testing.T) {
	reg := NewRegistry()
	l := newListener(t, &fakeSub{}, reg)
	body := []byte(`{
		"schema_version": 1,
		"event_id": "e",
		"node_id": "SOME-OTHER",
		"emitted_at": "2026-05-19T10:00:00Z",
		"upstream_node_id": "praha-02",
		"upstream_public_domain": "x"
	}`)
	if err := l.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatalf("different-node should ack silently, got: %v", err)
	}
	if reg.Len() != 0 {
		t.Error("should not register backend for other-node event")
	}
}

func TestListener_RemovedFlagRemovesBackend(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(sample("praha-02", "10.0.0.2", 9000))
	l := newListener(t, &fakeSub{}, reg)

	body := []byte(`{
		"schema_version": 1,
		"event_id": "evt-rm",
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"upstream_node_id": "praha-02",
		"upstream_public_domain": "praha-02.vpn.example.com",
		"removed": true
	}`)
	if err := l.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("praha-02"); ok {
		t.Error("backend should be removed from registry")
	}
}

func TestListener_RemovedNoOpForMissingBackend(t *testing.T) {
	reg := NewRegistry()
	l := newListener(t, &fakeSub{}, reg)
	body := []byte(`{
		"schema_version": 1,
		"event_id": "evt-rm",
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"upstream_node_id": "ghost",
		"upstream_public_domain": "x",
		"removed": true
	}`)
	if err := l.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatalf("missing-backend removal should ack silently: %v", err)
	}
}

func TestListener_DecodeFailureNaksMsg(t *testing.T) {
	reg := NewRegistry()
	l := newListener(t, &fakeSub{}, reg)
	if err := l.Handle(t.Context(), ports.Msg{Data: []byte("not json")}); err == nil {
		t.Fatal("decode failure should return err so msg is NAK'd")
	}
}

func TestListener_RunSubscribesAndStops(t *testing.T) {
	sub := newFakeSub()
	l := newListener(t, sub, NewRegistry())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	<-sub.ready
	sub.mu.Lock()
	subject := sub.subject
	durable := sub.durable
	sub.mu.Unlock()
	if subject != "agent.placements.lv-01.upstream" {
		t.Errorf("subject: %s", subject)
	}
	if durable != "agent_lv01_upstream" {
		t.Errorf("durable: %s", durable)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", err)
	}
}

func TestNewListener_Validates(t *testing.T) {
	good := ListenerConfig{NodeID: "n", Subject: "s", Durable: "d", Defaults: Defaults{Port: 9000}}
	cases := map[string]ListenerConfig{
		"missing NodeID":    {Subject: "s", Durable: "d", Defaults: Defaults{Port: 9000}},
		"missing Subject":   {NodeID: "n", Durable: "d", Defaults: Defaults{Port: 9000}},
		"missing Durable":   {NodeID: "n", Subject: "s", Defaults: Defaults{Port: 9000}},
		"zero default port": {NodeID: "n", Subject: "s", Durable: "d"},
	}
	for name, cfg := range cases {
		if _, err := NewListener(cfg, &fakeSub{}, NewRegistry(), silent()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := NewListener(good, nil, NewRegistry(), silent()); err == nil {
		t.Error("nil sub should error")
	}
	if _, err := NewListener(good, &fakeSub{}, nil, silent()); err == nil {
		t.Error("nil registry should error")
	}
}
