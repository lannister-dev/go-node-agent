package snapshot

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

type fakeSub struct {
	mu      sync.Mutex
	handler ports.MsgHandler
}

func (f *fakeSub) Subscribe(_ context.Context, _, _ string, h ports.MsgHandler) (ports.Unsubscribe, error) {
	f.mu.Lock()
	f.handler = h
	f.mu.Unlock()
	return func() error { return nil }, nil
}

type fakePub struct {
	mu   sync.Mutex
	msgs [][]byte
	err  error
}

func (f *fakePub) Publish(_ context.Context, _ string, _ map[string]string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.msgs = append(f.msgs, cp)
	return nil
}

func (f *fakePub) snapshot() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.msgs))
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

func (m *memStore) all() map[domain.PlacementID]domain.Placement {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[domain.PlacementID]domain.Placement, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out
}

type fakeRebuilder struct {
	calls atomic.Int32
	err   error
}

func (r *fakeRebuilder) RebuildFromStore(context.Context) error {
	r.calls.Add(1)
	return r.err
}

type fakeIDs struct{ n atomic.Int64 }

func (f *fakeIDs) NewID() string { return fmt.Sprintf("e-%d", f.n.Add(1)) }

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newConsumer(t *testing.T, store PlacementStore, rebuilder Rebuilder, pub Publisher) *Consumer {
	t.Helper()
	c, err := NewConsumer(ConsumerConfig{
		NodeID:            "lv-01",
		ChunkSubject:      "agent.snapshots.lv-01.chunks",
		SyncReportSubject: "agent.sync_reports.lv-01.events",
		Durable:           "test_snap_consumer",
	}, &fakeSub{}, pub, store, rebuilder, &fakeIDs{}, silent())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func chunkJSON(items []map[string]any, chunkIndex int, isLast bool, snapID string) []byte {
	chunk := map[string]any{
		"schema_version": 1,
		"node_id":        "lv-01",
		"emitted_at":     "2026-05-19T10:00:00Z",
		"chunk_index":    chunkIndex,
		"is_last_chunk":  isLast,
		"items":          items,
	}
	if snapID != "" {
		chunk["snapshot_id"] = snapID
	}
	data, _ := json.Marshal(chunk)
	return data
}

func itemJSON(placementID string, op int, desired, backend string) map[string]any {
	return map[string]any{
		"schema_version":    1,
		"node_id":           "lv-01",
		"emitted_at":        "2026-05-19T10:00:00Z",
		"event_id":          "evt-" + placementID,
		"placement_id":      placementID,
		"key_id":            "k-" + placementID,
		"op_version":        op,
		"desired_state":     desired,
		"backend_node_id":   backend,
		"protocol":          "vless",
		"transport":         "ws",
		"client_id":         "uuid-" + placementID,
		"is_revoked":        false,
		"snapshot_complete": false,
	}
}

func TestConsumer_NonLastChunkOnlyPersists(t *testing.T) {
	store := newMemStore()
	rebuilder := &fakeRebuilder{}
	pub := &fakePub{}
	c := newConsumer(t, store, rebuilder, pub)

	body := chunkJSON([]map[string]any{
		itemJSON("p-1", 1, "active", "praha-02"),
		itemJSON("p-2", 1, "active", "praha-02"),
	}, 0, false, "snap-7")

	if err := c.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatal(err)
	}
	if rebuilder.calls.Load() != 0 {
		t.Error("rebuild should NOT be called on non-last chunk")
	}
	if len(pub.snapshot()) != 0 {
		t.Error("sync report should NOT be published yet")
	}
	if len(store.all()) != 2 {
		t.Errorf("store size: %d", len(store.all()))
	}
}

func TestConsumer_LastChunkRebuildsAndPublishesSyncReport(t *testing.T) {
	store := newMemStore()
	rebuilder := &fakeRebuilder{}
	pub := &fakePub{}
	c := newConsumer(t, store, rebuilder, pub)

	if err := c.Handle(t.Context(), ports.Msg{Data: chunkJSON([]map[string]any{
		itemJSON("p-1", 1, "active", "praha-02"),
	}, 0, false, "snap-7")}); err != nil {
		t.Fatal(err)
	}
	if err := c.Handle(t.Context(), ports.Msg{Data: chunkJSON([]map[string]any{
		itemJSON("p-2", 1, "active", "latvia-01"),
	}, 1, true, "snap-7")}); err != nil {
		t.Fatal(err)
	}

	if rebuilder.calls.Load() != 1 {
		t.Errorf("rebuild calls: %d", rebuilder.calls.Load())
	}
	msgs := pub.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 sync_report, got %d", len(msgs))
	}
	var got map[string]any
	_ = json.Unmarshal(msgs[0], &got)
	if got["full_resync_completed"] != true {
		t.Errorf("full_resync_completed: %v", got["full_resync_completed"])
	}
	if got["synced_count"].(float64) != 2 {
		t.Errorf("synced_count: %v", got["synced_count"])
	}
	if got["inventory_hash"] != "snap-7" {
		t.Errorf("inventory_hash: %v", got["inventory_hash"])
	}

	chunks, items, completed := c.Stats()
	if chunks != 2 || items != 2 || !completed {
		t.Errorf("stats: chunks=%d items=%d completed=%v", chunks, items, completed)
	}
}

func TestConsumer_StaleOpVersionSkipped(t *testing.T) {
	store := newMemStore()
	_ = store.PutPlacement(t.Context(), domain.Placement{
		ID:            "p-1",
		OpVersion:     10,
		Applied:       domain.AppliedOk,
		Desired:       domain.DesiredActive,
		BackendNodeID: "old",
	})
	rebuilder := &fakeRebuilder{}
	c := newConsumer(t, store, rebuilder, &fakePub{})

	body := chunkJSON([]map[string]any{
		itemJSON("p-1", 5, "active", "new"),
	}, 0, true, "snap-x")
	if err := c.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatal(err)
	}
	stored, _, _ := store.GetPlacement(t.Context(), "p-1")
	if stored.BackendNodeID != "old" || stored.OpVersion != 10 {
		t.Errorf("stale should not overwrite: %+v", stored)
	}
	_, items, _ := c.Stats()
	if items != 0 {
		t.Errorf("stale item should not count: %d", items)
	}
}

func TestConsumer_DropsDifferentNode(t *testing.T) {
	store := newMemStore()
	rebuilder := &fakeRebuilder{}
	c := newConsumer(t, store, rebuilder, &fakePub{})

	body := []byte(`{
		"schema_version": 1,
		"node_id": "OTHER",
		"emitted_at": "2026-05-19T10:00:00Z",
		"chunk_index": 0,
		"is_last_chunk": true,
		"items": []
	}`)
	if err := c.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatalf("different-node should ack silently: %v", err)
	}
	if rebuilder.calls.Load() != 0 {
		t.Error("rebuild should not run for other node")
	}
}

func TestConsumer_DecodeFailureNaks(t *testing.T) {
	c := newConsumer(t, newMemStore(), &fakeRebuilder{}, &fakePub{})
	if err := c.Handle(t.Context(), ports.Msg{Data: []byte("not json")}); err == nil {
		t.Fatal("decode failure should return error")
	}
}

func TestConsumer_RebuildErrorPropagates(t *testing.T) {
	c := newConsumer(t, newMemStore(), &fakeRebuilder{err: errors.New("singbox down")}, &fakePub{})
	body := chunkJSON([]map[string]any{itemJSON("p-1", 1, "active", "praha-02")}, 0, true, "snap")
	if err := c.Handle(t.Context(), ports.Msg{Data: body}); err == nil {
		t.Fatal("rebuild error should propagate")
	}
}

func TestConsumer_NoopRebuilderAllowsObserverMode(t *testing.T) {
	pub := &fakePub{}
	c, err := NewConsumer(ConsumerConfig{
		NodeID:            "lv-01",
		ChunkSubject:      "s",
		SyncReportSubject: "sr",
		Durable:           "d",
	}, &fakeSub{}, pub, newMemStore(), nil, &fakeIDs{}, silent())
	if err != nil {
		t.Fatal(err)
	}
	body := chunkJSON([]map[string]any{itemJSON("p-1", 1, "active", "praha-02")}, 0, true, "snap")
	if err := c.Handle(t.Context(), ports.Msg{Data: body}); err != nil {
		t.Fatal(err)
	}
	if len(pub.snapshot()) != 1 {
		t.Error("noop rebuilder should still allow sync report publish")
	}
}

func TestNewConsumer_Validates(t *testing.T) {
	good := ConsumerConfig{NodeID: "n", ChunkSubject: "c", SyncReportSubject: "r", Durable: "d"}
	cases := map[string]ConsumerConfig{
		"missing NodeID":  {ChunkSubject: "c", SyncReportSubject: "r", Durable: "d"},
		"missing Chunk":   {NodeID: "n", SyncReportSubject: "r", Durable: "d"},
		"missing SyncRep": {NodeID: "n", ChunkSubject: "c", Durable: "d"},
		"missing Durable": {NodeID: "n", ChunkSubject: "c", SyncReportSubject: "r"},
	}
	for name, cfg := range cases {
		if _, err := NewConsumer(cfg, &fakeSub{}, &fakePub{}, newMemStore(), NoopRebuilder{}, &fakeIDs{}, silent()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := NewConsumer(good, nil, &fakePub{}, newMemStore(), NoopRebuilder{}, &fakeIDs{}, silent()); err == nil {
		t.Error("nil sub should error")
	}
}
