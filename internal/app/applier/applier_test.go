package applier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type fakeSubscriber struct {
	mu      sync.Mutex
	handler ports.MsgHandler
	subject string
	durable string
	ready   chan struct{}
}

func newFakeSubscriber() *fakeSubscriber { return &fakeSubscriber{ready: make(chan struct{})} }

func (f *fakeSubscriber) Subscribe(_ context.Context, subject, durable string, h ports.MsgHandler) (ports.Unsubscribe, error) {
	f.mu.Lock()
	f.handler = h
	f.subject = subject
	f.durable = durable
	f.mu.Unlock()
	close(f.ready)
	return func() error { return nil }, nil
}

func (f *fakeSubscriber) snapshot() (subject, durable string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subject, f.durable
}

type fakePublisher struct {
	mu   sync.Mutex
	msgs []capturedMsg
}

type capturedMsg struct {
	Subject string
	Data    []byte
}

func (f *fakePublisher) Publish(_ context.Context, subject string, _ map[string]string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.msgs = append(f.msgs, capturedMsg{Subject: subject, Data: cp})
	return nil
}

func (f *fakePublisher) snapshot() []capturedMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]capturedMsg, len(f.msgs))
	copy(out, f.msgs)
	return out
}

type memStore struct {
	mu   sync.Mutex
	data map[domain.PlacementID]domain.Placement
}

func newMemStore() *memStore { return &memStore{data: map[domain.PlacementID]domain.Placement{}} }

func (m *memStore) GetPlacement(_ context.Context, id domain.PlacementID) (domain.Placement, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.data[id]
	return p, ok, nil
}

func (m *memStore) PutPlacement(_ context.Context, p domain.Placement) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[p.ID] = p
	return nil
}

type fakeExecutor struct {
	called    atomic.Int32
	retryable bool
	err       error
	gotState  domain.DesiredState
}

func (f *fakeExecutor) Apply(_ context.Context, desired domain.Placement, _ domain.Placement, _ bool) (bool, error) {
	f.called.Add(1)
	f.gotState = desired.Desired
	return f.retryable, f.err
}

type fakeIDs struct{ n atomic.Int64 }

func (f *fakeIDs) NewID() string { return fmt.Sprintf("evt-%d", f.n.Add(1)) }

func silentLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestApplier(t *testing.T, executor Executor) (*Applier, *fakePublisher, *memStore) {
	t.Helper()
	pub := &fakePublisher{}
	store := newMemStore()
	a, err := New(Config{
		NodeID:         "lv-01",
		CommandSubject: "agent.placements.lv-01.commands",
		ResultSubject:  "agent.placement_results.lv-01.results",
		Durable:        "agent_lv01_commands",
	}, newFakeSubscriber(), pub, store, executor, &fakeIDs{}, silentLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return a, pub, store
}

func makeCommand(t *testing.T, placementID string, opVersion uint64, desired string, backend string) []byte {
	t.Helper()
	body := fmt.Sprintf(`{
		"schema_version": 1,
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"event_id": "evt-cmd-%s-%d",
		"placement_id": %q,
		"key_id": "k-%s",
		"op_version": %d,
		"desired_state": %q,
		"backend_node_id": %q,
		"protocol": "vless",
		"transport": "ws",
		"client_id": "01234567-89ab-cdef-0123-456789abcdef",
		"is_revoked": false,
		"snapshot_complete": false
	}`, placementID, opVersion, placementID, placementID, opVersion, desired, backend)
	return []byte(body)
}

func extractReportStatus(t *testing.T, raw []byte) (state, status string, errMsg string) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	state, _ = got["applied_state"].(string)
	if s, ok := got["report_status"].(string); ok {
		status = s
	}
	if e, ok := got["error"].(string); ok {
		errMsg = e
	}
	return
}

func TestApplier_FreshSucceeds(t *testing.T) {
	exec := &fakeExecutor{}
	a, pub, store := newTestApplier(t, exec)
	err := a.Handle(t.Context(), ports.Msg{Data: makeCommand(t, "p-1", 1, "active", "praha-02")})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.called.Load() != 1 {
		t.Errorf("expected executor called once, got %d", exec.called.Load())
	}
	stored, ok, _ := store.GetPlacement(t.Context(), "p-1")
	if !ok || stored.Applied != domain.AppliedOk || stored.OpVersion != 1 {
		t.Errorf("stored: %+v", stored)
	}
	if stored.LastAppliedAt.IsZero() {
		t.Error("LastAppliedAt should be set")
	}

	msgs := pub.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 result published, got %d", len(msgs))
	}
	state, status, errStr := extractReportStatus(t, msgs[0].Data)
	if state != "applied" || status != "applied" || errStr != "" {
		t.Errorf("report mismatch: state=%s status=%s err=%s", state, status, errStr)
	}
}

func TestApplier_StaleOpVersionSkipped(t *testing.T) {
	exec := &fakeExecutor{}
	a, pub, store := newTestApplier(t, exec)
	_ = store.PutPlacement(t.Context(), domain.Placement{
		ID:        "p-1",
		NodeID:    "lv-01",
		OpVersion: 10,
		Applied:   domain.AppliedOk,
		Desired:   domain.DesiredActive,
	})
	err := a.Handle(t.Context(), ports.Msg{Data: makeCommand(t, "p-1", 5, "active", "x")})
	if err != nil {
		t.Fatal(err)
	}
	if exec.called.Load() != 0 {
		t.Errorf("executor should not be called for stale op")
	}
	msgs := pub.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 result, got %d", len(msgs))
	}
	_, status, _ := extractReportStatus(t, msgs[0].Data)
	if status != "skipped_stale" {
		t.Errorf("status: %s", status)
	}
}

func TestApplier_IdempotentSkipped(t *testing.T) {
	exec := &fakeExecutor{}
	a, pub, store := newTestApplier(t, exec)
	_ = store.PutPlacement(t.Context(), domain.Placement{
		ID:            "p-1",
		NodeID:        "lv-01",
		OpVersion:     5,
		Applied:       domain.AppliedOk,
		Desired:       domain.DesiredActive,
		BackendNodeID: "praha-02",
	})
	err := a.Handle(t.Context(), ports.Msg{Data: makeCommand(t, "p-1", 5, "active", "praha-02")})
	if err != nil {
		t.Fatal(err)
	}
	if exec.called.Load() != 0 {
		t.Errorf("executor should not be called for idempotent op")
	}
	msgs := pub.snapshot()
	_, status, _ := extractReportStatus(t, msgs[0].Data)
	if status != "skipped_idempotent" {
		t.Errorf("status: %s", status)
	}
}

func TestApplier_SameOpVersionDifferentBackendApplies(t *testing.T) {
	exec := &fakeExecutor{}
	a, _, store := newTestApplier(t, exec)
	_ = store.PutPlacement(t.Context(), domain.Placement{
		ID: "p-1", NodeID: "lv-01", OpVersion: 5, Applied: domain.AppliedOk,
		Desired: domain.DesiredActive, BackendNodeID: "old-backend",
	})
	err := a.Handle(t.Context(), ports.Msg{Data: makeCommand(t, "p-1", 5, "active", "new-backend")})
	if err != nil {
		t.Fatal(err)
	}
	if exec.called.Load() != 1 {
		t.Errorf("expected executor called for backend change at same op_version")
	}
}

func TestApplier_NonRetryableErrorReported(t *testing.T) {
	exec := &fakeExecutor{err: errors.New("xray bad config"), retryable: false}
	a, pub, store := newTestApplier(t, exec)
	err := a.Handle(t.Context(), ports.Msg{Data: makeCommand(t, "p-1", 1, "active", "praha-02")})
	if err != nil {
		t.Fatalf("non-retryable should be reported (handler returns nil); got: %v", err)
	}
	stored, _, _ := store.GetPlacement(t.Context(), "p-1")
	if stored.Applied != domain.AppliedError {
		t.Errorf("stored Applied: %s", stored.Applied)
	}
	msgs := pub.snapshot()
	state, status, errStr := extractReportStatus(t, msgs[0].Data)
	if state != "error" || status != "error" || errStr != "xray bad config" {
		t.Errorf("report: state=%s status=%s err=%s", state, status, errStr)
	}
}

func TestApplier_RetryableErrorReturnsHandlerError(t *testing.T) {
	exec := &fakeExecutor{err: errors.New("xray transient"), retryable: true}
	a, pub, _ := newTestApplier(t, exec)
	err := a.Handle(t.Context(), ports.Msg{Data: makeCommand(t, "p-1", 1, "active", "praha-02")})
	if err == nil {
		t.Fatal("retryable error should make handler return error (triggers Nak)")
	}
	if len(pub.snapshot()) != 0 {
		t.Error("retryable should not publish result")
	}
}

func TestApplier_DecodeFailureReturnsErr(t *testing.T) {
	exec := &fakeExecutor{}
	a, _, _ := newTestApplier(t, exec)
	err := a.Handle(t.Context(), ports.Msg{Data: []byte("not json")})
	if err == nil {
		t.Fatal("decode failure should return err to nak")
	}
}

func TestApplier_DifferentNodeAcked(t *testing.T) {
	exec := &fakeExecutor{}
	a, pub, _ := newTestApplier(t, exec)
	body := []byte(`{
		"schema_version":1,"node_id":"OTHER","emitted_at":"2026-01-01T00:00:00Z",
		"event_id":"e","placement_id":"p","key_id":"k","op_version":1,
		"desired_state":"active","backend_node_id":"b","protocol":"vless","transport":"ws",
		"client_id":"c","is_revoked":false,"snapshot_complete":false
	}`)
	err := a.Handle(t.Context(), ports.Msg{Data: body})
	if err != nil {
		t.Errorf("different-node should ack (nil err): %v", err)
	}
	if exec.called.Load() != 0 {
		t.Error("executor should not be called for other-node command")
	}
	if len(pub.snapshot()) != 0 {
		t.Error("no result should be published for other-node command")
	}
}

func TestApplier_RunSubscribesAndStopsOnCancel(t *testing.T) {
	sub := newFakeSubscriber()
	pub := &fakePublisher{}
	store := newMemStore()
	a, err := New(Config{
		NodeID:         "lv-01",
		CommandSubject: "agent.placements.lv-01.commands",
		ResultSubject:  "agent.placement_results.lv-01.results",
		Durable:        "test_durable",
	}, sub, pub, store, NoopExecutor{}, &fakeIDs{}, silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	<-sub.ready
	subj, dur := sub.snapshot()
	if subj != "agent.placements.lv-01.commands" {
		t.Errorf("subject: %s", subj)
	}
	if dur != "test_durable" {
		t.Errorf("durable: %s", dur)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestApplier_Counters(t *testing.T) {
	exec := &fakeExecutor{}
	a, _, _ := newTestApplier(t, exec)
	mustHandle := func(p string, op uint64, desired, backend string) {
		t.Helper()
		if err := a.Handle(t.Context(), ports.Msg{Data: makeCommand(t, p, op, desired, backend)}); err != nil {
			t.Fatalf("handle %s/%d: %v", p, op, err)
		}
	}
	mustHandle("p-1", 1, "active", "praha-02")
	mustHandle("p-2", 1, "active", "praha-02")
	mustHandle("p-1", 1, "active", "praha-02")

	exec.err = errors.New("permanent fail")
	exec.retryable = false
	if err := a.Handle(t.Context(), ports.Msg{Data: makeCommand(t, "p-3", 1, "active", "praha-02")}); err != nil {
		t.Fatalf("handle p-3: %v", err)
	}

	received, applied, failed := a.Snapshot()
	if received != 4 {
		t.Errorf("received = %d, want 4", received)
	}
	if applied != 2 {
		t.Errorf("applied = %d, want 2 (p-1 fresh + p-2 fresh; p-1 second was idempotent; p-3 failed)", applied)
	}
	if failed != 1 {
		t.Errorf("failed = %d, want 1", failed)
	}
}

func TestApplier_Counters_DifferentNodeNotCounted(t *testing.T) {
	a, _, _ := newTestApplier(t, &fakeExecutor{})
	body := []byte(`{
		"schema_version":1,"node_id":"OTHER","emitted_at":"2026-01-01T00:00:00Z",
		"event_id":"e","placement_id":"p","key_id":"k","op_version":1,
		"desired_state":"active","backend_node_id":"b","protocol":"vless","transport":"ws",
		"client_id":"c","is_revoked":false,"snapshot_complete":false
	}`)
	_ = a.Handle(t.Context(), ports.Msg{Data: body})
	received, _, _ := a.Snapshot()
	if received != 0 {
		t.Errorf("different-node commands should not count; received=%d", received)
	}
}

func TestNew_ValidatesArgs(t *testing.T) {
	good := Config{NodeID: "n", CommandSubject: "c", ResultSubject: "r", Durable: "d"}
	cases := map[string]Config{
		"missing NodeID":  {CommandSubject: "c", ResultSubject: "r", Durable: "d"},
		"missing Command": {NodeID: "n", ResultSubject: "r", Durable: "d"},
		"missing Result":  {NodeID: "n", CommandSubject: "c", Durable: "d"},
		"missing Durable": {NodeID: "n", CommandSubject: "c", ResultSubject: "r"},
	}
	for name, cfg := range cases {
		if _, err := New(cfg, newFakeSubscriber(), &fakePublisher{}, newMemStore(), NoopExecutor{}, &fakeIDs{}, silentLogger()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := New(good, nil, &fakePublisher{}, newMemStore(), NoopExecutor{}, &fakeIDs{}, silentLogger()); err == nil {
		t.Error("expected error for nil sub")
	}
}
