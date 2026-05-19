package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/lannister-dev/go-node-agent/internal/adapters/badger"
	"github.com/lannister-dev/go-node-agent/internal/adapters/controlapi"
	natsa "github.com/lannister-dev/go-node-agent/internal/adapters/nats"
	"github.com/lannister-dev/go-node-agent/internal/app/applier"
	"github.com/lannister-dev/go-node-agent/internal/app/bootstrap"
	"github.com/lannister-dev/go-node-agent/internal/app/heartbeat"
	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/platform/idgen"
	"github.com/lannister-dev/go-node-agent/internal/wire"
)

func TestFullAgentFlow(t *testing.T) {
	natsServer := startNATSServer(t)
	provisionStreams(t, natsServer)

	ctlState := &controlAPIState{}
	ctlURL := startMockControlAPI(t, ctlState)

	store, err := badger.Open(badger.Options{Path: t.TempDir(), Logger: silentLogger()})
	if err != nil {
		t.Fatalf("badger: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctl, err := controlapi.New(controlapi.Options{BaseURL: ctlURL, Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("controlapi: %v", err)
	}
	t.Cleanup(func() { _ = ctl.Close() })

	bs := bootstrap.New(bootstrap.Config{
		BootstrapToken: "test-boot",
		NodeKey:        "test-key",
		NodeRole:       "entry",
	}, store, ctl, idgen.UUID{}, silentLogger())

	bsRes, err := bs.Run(t.Context())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if bsRes.Identity.NodeID != "test-node-01" {
		t.Fatalf("identity: %+v", bsRes.Identity)
	}
	if !bsRes.WasFresh {
		t.Error("expected fresh bootstrap")
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	natsTr, err := natsa.New(ctx, natsa.Options{
		URL:    natsServer.ClientURL(),
		Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(func() { _ = natsTr.Close() })

	subjects := wire.NewSubjects(wire.SubjectPrefixes{
		Command:    "agent.placements",
		Result:     "agent.placement_results",
		Snapshot:   "agent.snapshots",
		Heartbeat:  "agent.heartbeats",
		SyncReport: "agent.sync_reports",
	})

	nodeID := bsRes.Identity.NodeID
	app, err := applier.New(applier.Config{
		NodeID:         nodeID,
		CommandSubject: subjects.PlacementCommand(nodeID),
		ResultSubject:  subjects.PlacementResult(nodeID),
		Durable:        "agent_test_commands",
	}, natsTr, natsTr, store, applier.NoopExecutor{}, idgen.UUID{}, silentLogger())
	if err != nil {
		t.Fatalf("applier: %v", err)
	}

	hb, err := heartbeat.New(heartbeat.Config{
		NodeID:       nodeID,
		Subject:      subjects.Heartbeat(nodeID),
		AgentVersion: "test",
		Interval:     150 * time.Millisecond,
	}, natsTr, heartbeat.NoopSampler{}, app, idgen.UUID{}, silentLogger())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	resultCh := make(chan []byte, 4)
	stopResult := subscribeAndDrain(t, natsServer, subjects.PlacementResult(nodeID), "test_results_consumer", resultCh)
	t.Cleanup(stopResult)

	heartbeatCh := make(chan []byte, 4)
	stopHB := subscribeAndDrain(t, natsServer, subjects.Heartbeat(nodeID), "test_heartbeats_consumer", heartbeatCh)
	t.Cleanup(stopHB)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return hb.Run(gctx) })
	g.Go(func() error { return app.Run(gctx) })

	t.Cleanup(func() {
		cancel()
		if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("group exit: %v", err)
		}
	})

	cmdJSON := []byte(`{
		"schema_version": 1,
		"node_id": "test-node-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"event_id": "evt-cmd-1",
		"placement_id": "p-1",
		"key_id": "k-1",
		"op_version": 1,
		"desired_state": "active",
		"backend_node_id": "praha-02",
		"protocol": "vless",
		"transport": "ws",
		"client_id": "01234567-89ab-cdef-0123-456789abcdef",
		"is_revoked": false,
		"snapshot_complete": false
	}`)
	publishPlacementCommand(t, natsServer, subjects.PlacementCommand(nodeID), cmdJSON)

	select {
	case data := <-resultCh:
		var got map[string]any
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("decode result: %v", err)
		}
		if got["placement_id"] != "p-1" || got["applied_state"] != "applied" || got["report_status"] != "applied" {
			t.Errorf("unexpected result: %+v", got)
		}
		if got["op_version"].(float64) != 1 {
			t.Errorf("op_version: %v", got["op_version"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for apply result")
	}

	stored, found, err := store.GetPlacement(t.Context(), "p-1")
	if err != nil {
		t.Fatalf("get placement: %v", err)
	}
	if !found {
		t.Fatal("placement not persisted")
	}
	if stored.Applied != domain.AppliedOk || stored.OpVersion != 1 {
		t.Errorf("stored: %+v", stored)
	}

	select {
	case data := <-heartbeatCh:
		var got map[string]any
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("decode heartbeat: %v", err)
		}
		if got["node_id"] != "test-node-01" {
			t.Errorf("node_id: %v", got["node_id"])
		}
		if got["agent_version"] != "test" {
			t.Errorf("agent_version: %v", got["agent_version"])
		}
		if got["schema_version"].(float64) != 1 {
			t.Errorf("schema_version: %v", got["schema_version"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for heartbeat")
	}

	received, applied, failed := app.Snapshot()
	if received != 1 || applied != 1 || failed != 0 {
		t.Errorf("counters: received=%d applied=%d failed=%d", received, applied, failed)
	}

	if ctlState.initialCalls != 1 {
		t.Errorf("control-api initial called %d times, want 1", ctlState.initialCalls)
	}

	select {
	case <-heartbeatCh:
	case <-time.After(500 * time.Millisecond):
		t.Error("ticker did not fire a follow-up heartbeat")
	}
}

func TestSecondStart_ReusesIdentity(t *testing.T) {
	natsServer := startNATSServer(t)
	provisionStreams(t, natsServer)
	ctlState := &controlAPIState{}
	ctlURL := startMockControlAPI(t, ctlState)

	storeDir := t.TempDir()

	for i := range 2 {
		store, err := badger.Open(badger.Options{Path: storeDir, Logger: silentLogger()})
		if err != nil {
			t.Fatalf("badger %d: %v", i, err)
		}
		ctl, err := controlapi.New(controlapi.Options{BaseURL: ctlURL, Timeout: 3 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		bs := bootstrap.New(bootstrap.Config{
			BootstrapToken: "boot", NodeKey: "key", NodeRole: "entry",
		}, store, ctl, idgen.UUID{}, silentLogger())
		_, err = bs.Run(t.Context())
		if err != nil {
			t.Fatalf("bootstrap iter %d: %v", i, err)
		}
		_ = ctl.Close()
		_ = store.Close()
	}

	if ctlState.initialCalls != 1 {
		t.Errorf("control-api /initial should be called only on first start; got %d calls", ctlState.initialCalls)
	}
}
