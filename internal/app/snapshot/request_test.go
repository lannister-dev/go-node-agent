package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/wire/jsonv1"
)

func TestRequester_PublishesValidRequest(t *testing.T) {
	pub := &fakePub{}
	r, err := NewRequester(RequesterConfig{
		NodeID:         "lv-01",
		RequestSubject: "agent.snapshots.lv-01.request",
	}, pub, silent())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Request(t.Context(), jsonv1.SnapshotReasonStartup); err != nil {
		t.Fatal(err)
	}
	msgs := pub.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	var got map[string]any
	_ = json.Unmarshal(msgs[0], &got)
	if got["node_id"] != "lv-01" || got["reason"] != "startup" {
		t.Errorf("payload: %+v", got)
	}
	if got["schema_version"].(float64) != 1 {
		t.Errorf("schema_version: %v", got["schema_version"])
	}
}

func TestRequester_PublishErrorPropagates(t *testing.T) {
	pub := &fakePub{err: errors.New("nats down")}
	r, _ := NewRequester(RequesterConfig{NodeID: "n", RequestSubject: "s"}, pub, silent())
	if err := r.Request(t.Context(), jsonv1.SnapshotReasonStartup); err == nil {
		t.Fatal("expected publish error")
	}
}

func TestRequester_RejectsBadReason(t *testing.T) {
	pub := &fakePub{}
	r, _ := NewRequester(RequesterConfig{NodeID: "n", RequestSubject: "s"}, pub, silent())
	if err := r.Request(t.Context(), "garbage"); err == nil {
		t.Fatal("expected error for bad reason")
	}
}

func TestNewRequester_Validates(t *testing.T) {
	cases := map[string]RequesterConfig{
		"missing NodeID":  {RequestSubject: "s"},
		"missing Subject": {NodeID: "n"},
	}
	for name, cfg := range cases {
		if _, err := NewRequester(cfg, &fakePub{}, silent()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := NewRequester(RequesterConfig{NodeID: "n", RequestSubject: "s"}, nil, silent()); err == nil {
		t.Error("nil pub should error")
	}
}

var _ = context.TODO
