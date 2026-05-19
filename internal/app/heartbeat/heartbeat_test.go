package heartbeat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakePublisher struct {
	mu   sync.Mutex
	msgs []capturedMsg
	err  error
}

type capturedMsg struct {
	Subject string
	Headers map[string]string
	Data    []byte
}

func (f *fakePublisher) Publish(_ context.Context, subject string, headers map[string]string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	hcopy := make(map[string]string, len(headers))
	for k, v := range headers {
		hcopy[k] = v
	}
	dcopy := make([]byte, len(data))
	copy(dcopy, data)
	f.msgs = append(f.msgs, capturedMsg{Subject: subject, Headers: hcopy, Data: dcopy})
	return nil
}

func (f *fakePublisher) snapshot() []capturedMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]capturedMsg, len(f.msgs))
	copy(out, f.msgs)
	return out
}

type fakeCounters struct{ poll, applied, failed uint32 }

func (f fakeCounters) Snapshot() (uint32, uint32, uint32) { return f.poll, f.applied, f.failed }

type fakeIDs struct{ n atomic.Int64 }

func (f *fakeIDs) NewID() string {
	n := f.n.Add(1)
	return "evt-" + string(rune('0'+n))
}

func silentLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestHeartbeat(t *testing.T, pub Publisher, ctr Counters, interval time.Duration) *Heartbeat {
	t.Helper()
	h, err := New(Config{
		NodeID:       "lv-01",
		Subject:      "agent.heartbeats.lv-01.events",
		AgentVersion: "1.2.3",
		Interval:     interval,
	}, pub, NoopSampler{}, ctr, &fakeIDs{}, silentLogger())
	if err != nil {
		t.Fatalf("new heartbeat: %v", err)
	}
	return h
}

func TestHeartbeat_PublishesOnTickAndExitsOnCancel(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHeartbeat(t, pub, NoopCounters{}, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()

	time.Sleep(120 * time.Millisecond)
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	msgs := pub.snapshot()
	if len(msgs) < 3 {
		t.Fatalf("expected >= 3 heartbeats in 120ms @ 20ms, got %d", len(msgs))
	}
	first := msgs[0]
	if first.Subject != "agent.heartbeats.lv-01.events" {
		t.Errorf("subject: %s", first.Subject)
	}
	if first.Headers["x-schema"] != "jsonv1" {
		t.Errorf("x-schema: %s", first.Headers["x-schema"])
	}
	if first.Headers["x-node-id"] != "lv-01" {
		t.Errorf("x-node-id: %s", first.Headers["x-node-id"])
	}
	var payload map[string]any
	if err := json.Unmarshal(first.Data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["node_id"] != "lv-01" || payload["agent_version"] != "1.2.3" || payload["schema_version"].(float64) != 1 {
		t.Errorf("payload mismatch: %+v", payload)
	}
}

func TestHeartbeat_IncludesCounters(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHeartbeat(t, pub, fakeCounters{poll: 7, applied: 6, failed: 1}, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = h.Run(ctx) }()

	time.Sleep(40 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)

	msgs := pub.snapshot()
	if len(msgs) == 0 {
		t.Fatal("no messages")
	}
	var payload map[string]any
	if err := json.Unmarshal(msgs[0].Data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["poll_count"].(float64) != 7 || payload["applied"].(float64) != 6 || payload["failed"].(float64) != 1 {
		t.Errorf("counters mismatch: %+v", payload)
	}
}

func TestHeartbeat_ContinuesAfterPublishError(t *testing.T) {
	pub := &fakePublisher{err: errors.New("nats down")}
	h := newTestHeartbeat(t, pub, NoopCounters{}, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = h.Run(ctx) }()

	time.Sleep(80 * time.Millisecond)
	pub.mu.Lock()
	pub.err = nil
	pub.mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)

	msgs := pub.snapshot()
	if len(msgs) == 0 {
		t.Fatal("expected messages after error cleared")
	}
}

func TestNew_RejectsInvalidConfig(t *testing.T) {
	pub := &fakePublisher{}
	ids := &fakeIDs{}
	good := Config{NodeID: "n", Subject: "s", AgentVersion: "v", Interval: time.Second}

	cases := map[string]Config{
		"missing NodeID":       {Subject: "s", AgentVersion: "v", Interval: time.Second},
		"missing Subject":      {NodeID: "n", AgentVersion: "v", Interval: time.Second},
		"missing AgentVersion": {NodeID: "n", Subject: "s", Interval: time.Second},
		"zero Interval":        {NodeID: "n", Subject: "s", AgentVersion: "v"},
	}
	for name, cfg := range cases {
		if _, err := New(cfg, pub, NoopSampler{}, NoopCounters{}, ids, silentLogger()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := New(good, nil, NoopSampler{}, NoopCounters{}, ids, silentLogger()); err == nil {
		t.Error("expected error for nil publisher")
	}
	if _, err := New(good, pub, NoopSampler{}, NoopCounters{}, nil, silentLogger()); err == nil {
		t.Error("expected error for nil ids")
	}
}
