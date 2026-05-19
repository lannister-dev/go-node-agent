package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/lannister-dev/go-node-agent/internal/adapters/badger"
	natsa "github.com/lannister-dev/go-node-agent/internal/adapters/nats"
	"github.com/lannister-dev/go-node-agent/internal/adapters/singbox"
	"github.com/lannister-dev/go-node-agent/internal/app/applier"
	"github.com/lannister-dev/go-node-agent/internal/app/backends"
	"github.com/lannister-dev/go-node-agent/internal/app/executor"
	"github.com/lannister-dev/go-node-agent/internal/app/flip"
	"github.com/lannister-dev/go-node-agent/internal/app/heartbeat"
	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/platform/idgen"
	"github.com/lannister-dev/go-node-agent/internal/wire"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type fakeSingBoxAPI struct {
	mu             sync.Mutex
	configs        [][]byte
	connsRemaining atomic.Uint64
	server         *httptest.Server
}

func newFakeSingBoxAPI(t *testing.T, initialConns uint64) *fakeSingBoxAPI {
	t.Helper()
	api := &fakeSingBoxAPI{}
	api.connsRemaining.Store(initialConns)
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/version":
			_, _ = w.Write([]byte(`{"version":"1.10.0"}`))
		case r.Method == http.MethodPut && r.URL.Path == "/configs":
			var body struct {
				Path string `json:"path"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			data, err := os.ReadFile(body.Path)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			api.mu.Lock()
			api.configs = append(api.configs, data)
			api.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/connections":
			remaining := api.connsRemaining.Load()
			resp := map[string]any{
				"downloadTotal": 0,
				"uploadTotal":   0,
				"connections":   []any{},
			}
			if remaining > 0 {
				connList := make([]any, 0, remaining)
				for i := range remaining {
					connList = append(connList, map[string]any{
						"id":     "conn-" + string(rune('a'+i)),
						"chains": []string{"in", "backend-praha-02"},
					})
				}
				resp["connections"] = connList
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.server.Close)
	return api
}

func (a *fakeSingBoxAPI) URL() string { return a.server.URL }

func (a *fakeSingBoxAPI) configCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.configs)
}

func (a *fakeSingBoxAPI) lastConfig() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.configs) == 0 {
		return nil
	}
	return a.configs[len(a.configs)-1]
}

func writeBaseSingBoxConfig(t *testing.T, dir string) string {
	t.Helper()
	base := map[string]any{
		"log": map[string]any{"level": "info"},
		"inbounds": []any{
			map[string]any{
				"type":        "vless",
				"tag":         "vless-in",
				"listen":      "::",
				"listen_port": 443,
				"users":       []any{},
				"tls": map[string]any{
					"enabled":     true,
					"server_name": "www.cloudflare.com",
					"reality": map[string]any{
						"enabled":     true,
						"private_key": "OPERATOR-PROVIDED-REALITY-KEY",
						"short_id":    []string{"abc123"},
					},
				},
			},
		},
		"outbounds": []any{
			map[string]any{"type": "direct", "tag": "direct"},
			map[string]any{"type": "block", "tag": "block"},
		},
		"route": map[string]any{
			"rules": []any{},
			"final": "direct",
		},
	}
	data, _ := json.MarshalIndent(base, "", "  ")
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func parseLastConfig(t *testing.T, api *fakeSingBoxAPI) map[string]any {
	t.Helper()
	raw := api.lastConfig()
	if raw == nil {
		t.Fatal("no config written")
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

func ruleForOutbound(rules []any, outbound string) (map[string]any, bool) {
	for _, r := range rules {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		if m["outbound"] == outbound {
			return m, true
		}
	}
	return nil, false
}

func TestFullFlipFlow_EndToEnd(t *testing.T) {
	natsServer := startNATSServer(t)
	provisionStreams(t, natsServer)
	ctlURL := startMockControlAPI(t, &controlAPIState{})

	tmpDir := t.TempDir()
	configPath := writeBaseSingBoxConfig(t, tmpDir)
	sbAPI := newFakeSingBoxAPI(t, 0)

	store, err := badger.Open(badger.Options{Path: t.TempDir(), Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	_ = ctlURL
	natsTr, err := natsa.New(t.Context(), natsa.Options{URL: natsServer.ClientURL(), Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = natsTr.Close() })

	sb, err := singbox.New(singbox.Options{
		APIURL:     sbAPI.URL(),
		ConfigPath: configPath,
		Logger:     silentLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sb.Close() })

	reg := backends.NewRegistry()
	reg.Upsert(singboxgen.BackendSpec{ID: "praha-02", Address: "10.0.0.2", Port: 9000, Transport: domain.TransportWS})
	reg.Upsert(singboxgen.BackendSpec{ID: "latvia-01", Address: "10.0.0.1", Port: 9000, Transport: domain.TransportWS})

	actions, err := executor.NewEntryActions(executor.EntryActionsConfig{
		Inbound:    singboxgen.InboundSpec{Tag: "vless-in", Listen: singboxgen.ListenSpec{Address: "::", Port: 443}},
		LogCfg:     singboxgen.LogSpec{Level: "info"},
		ConfigPath: configPath,
	}, sb, store, reg, silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	orch, err := flip.New(actions, silentLogger(), flip.Options{DrainPollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	flipExec, err := executor.NewFlipExecutor(actions, orch, executor.FlipExecutorOptions{
		DrainTimeout: 200 * time.Millisecond,
	}, silentLogger())
	if err != nil {
		t.Fatal(err)
	}

	const nodeID domain.NodeID = "lv-01"
	subjects := wire.NewSubjects(wire.SubjectPrefixes{
		Command:    "agent.placements",
		Result:     "agent.placement_results",
		Snapshot:   "agent.snapshots",
		Heartbeat:  "agent.heartbeats",
		SyncReport: "agent.sync_reports",
	})

	app, err := applier.New(applier.Config{
		NodeID:         nodeID,
		CommandSubject: subjects.PlacementCommand(nodeID),
		ResultSubject:  subjects.PlacementResult(nodeID),
		Durable:        "flip_test_commands",
	}, natsTr, natsTr, store, flipExec, idgen.UUID{}, silentLogger())
	if err != nil {
		t.Fatal(err)
	}

	hb, err := heartbeat.New(heartbeat.Config{
		NodeID:       nodeID,
		Subject:      subjects.Heartbeat(nodeID),
		AgentVersion: "test",
		Interval:     500 * time.Millisecond,
	}, natsTr, heartbeat.NoopSampler{}, app, idgen.UUID{}, silentLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return hb.Run(gctx) })
	g.Go(func() error { return app.Run(gctx) })

	resultCh := make(chan []byte, 8)
	stopResult := subscribeAndDrain(t, natsServer, subjects.PlacementResult(nodeID), "flip_test_results", resultCh)
	t.Cleanup(stopResult)

	cmd1 := []byte(`{
		"schema_version": 1,
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"event_id": "evt-1",
		"placement_id": "p-1",
		"key_id": "k-1",
		"op_version": 1,
		"desired_state": "active",
		"backend_node_id": "praha-02",
		"protocol": "vless",
		"transport": "ws",
		"client_id": "uuid-a",
		"is_revoked": false,
		"snapshot_complete": false
	}`)
	publishPlacementCommand(t, natsServer, subjects.PlacementCommand(nodeID), cmd1)
	waitForResult(t, resultCh, "applied")

	first := parseLastConfig(t, sbAPI)
	if !hasUserUUID(first, "uuid-a") {
		t.Fatal("uuid-a should be in inbound users after first apply")
	}
	if !hasOutbound(first, "backend-praha-02") {
		t.Fatal("praha-02 outbound should be present")
	}
	if !hasRoute(first, "uuid-a", "backend-praha-02") {
		t.Fatal("user should be routed to praha-02")
	}
	if !hasRealityKey(first, "OPERATOR-PROVIDED-REALITY-KEY") {
		t.Fatal("REALITY private_key should survive merge")
	}

	sbAPI.connsRemaining.Store(2)

	configCountBefore := sbAPI.configCount()
	cmd2 := []byte(`{
		"schema_version": 1,
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"event_id": "evt-2",
		"placement_id": "p-1",
		"key_id": "k-1",
		"op_version": 2,
		"desired_state": "active",
		"backend_node_id": "latvia-01",
		"protocol": "vless",
		"transport": "ws",
		"client_id": "uuid-a",
		"is_revoked": false,
		"snapshot_complete": false
	}`)

	flipDone := make(chan struct{})
	go func() {
		waitForResultBackground(t, resultCh)
		close(flipDone)
	}()

	publishPlacementCommand(t, natsServer, subjects.PlacementCommand(nodeID), cmd2)

	go func() {
		time.Sleep(40 * time.Millisecond)
		sbAPI.connsRemaining.Store(0)
	}()

	select {
	case <-flipDone:
	case <-time.After(3 * time.Second):
		t.Fatal("flip did not complete in time")
	}

	final := parseLastConfig(t, sbAPI)
	if !hasUserUUID(final, "uuid-a") {
		t.Error("uuid-a should still be present after flip")
	}
	if !hasRoute(final, "uuid-a", "backend-latvia-01") {
		t.Errorf("user should be routed to latvia-01 after flip")
	}
	if hasRoute(final, "uuid-a", "backend-praha-02") {
		t.Errorf("old praha-02 route should be gone")
	}
	if !hasRealityKey(final, "OPERATOR-PROVIDED-REALITY-KEY") {
		t.Error("REALITY private_key should still survive after flip")
	}
	if sbAPI.configCount() < configCountBefore+2 {
		t.Errorf("expected at least 2 reloads during flip (swap + cool), got %d new", sbAPI.configCount()-configCountBefore)
	}

	received, applied, failed := app.Snapshot()
	if received != 2 || applied != 2 || failed != 0 {
		t.Errorf("counters: received=%d applied=%d failed=%d (want 2/2/0)", received, applied, failed)
	}

	stored, ok, _ := store.GetPlacement(t.Context(), "p-1")
	if !ok {
		t.Fatal("placement should be persisted")
	}
	if stored.BackendNodeID != "latvia-01" || stored.OpVersion != 2 {
		t.Errorf("stored state: %+v", stored)
	}
}

func waitForResult(t *testing.T, ch <-chan []byte, wantStatus string) {
	t.Helper()
	select {
	case data := <-ch:
		var got map[string]any
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("decode result: %v", err)
		}
		if got["report_status"] != wantStatus {
			t.Fatalf("report_status: got %v, want %s", got["report_status"], wantStatus)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}
}

func waitForResultBackground(t *testing.T, ch <-chan []byte) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Error("background wait timed out")
	}
}

func hasUserUUID(cfg map[string]any, uuid string) bool {
	for _, inb := range cfg["inbounds"].([]any) {
		for _, u := range inb.(map[string]any)["users"].([]any) {
			if u.(map[string]any)["uuid"] == uuid {
				return true
			}
		}
	}
	return false
}

func hasOutbound(cfg map[string]any, tag string) bool {
	for _, o := range cfg["outbounds"].([]any) {
		if o.(map[string]any)["tag"] == tag {
			return true
		}
	}
	return false
}

func hasRoute(cfg map[string]any, userUUID, outbound string) bool {
	rules := cfg["route"].(map[string]any)["rules"].([]any)
	rule, ok := ruleForOutbound(rules, outbound)
	if !ok {
		return false
	}
	users, ok := rule["user"].([]any)
	if !ok {
		return false
	}
	for _, u := range users {
		if u == userUUID {
			return true
		}
	}
	return false
}

func hasRealityKey(cfg map[string]any, key string) bool {
	inb := cfg["inbounds"].([]any)[0].(map[string]any)
	tls, ok := inb["tls"].(map[string]any)
	if !ok {
		return false
	}
	reality, ok := tls["reality"].(map[string]any)
	if !ok {
		return false
	}
	return reality["private_key"] == key
}

var _ = errors.New
