package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	natssrv "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func silentLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func startNATSServer(t *testing.T) *natssrv.Server {
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

func provisionStreams(t *testing.T, srv *natssrv.Server) {
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
		Name:     "AGENT",
		Subjects: []string{"agent.>"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
}

type controlAPIState struct {
	initialCalls int
}

func startMockControlAPI(t *testing.T, st *controlAPIState) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agent/initial":
			st.initialCalls++
			agentID := r.Header.Get("X-Agent-Instance-ID")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node_id":              "test-node-01",
				"node_auth_token":      "tok-test",
				"agent_instance_id":    agentID,
				"full_resync_required": true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func subscribeAndDrain(t *testing.T, srv *natssrv.Server, subject, durable string, ch chan<- []byte) func() {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "AGENT")
	if err != nil {
		t.Fatal(err)
	}
	cons, err := stream.CreateOrUpdateConsumer(t.Context(), jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	cc, err := cons.Consume(func(m jetstream.Msg) {
		data := make([]byte, len(m.Data()))
		copy(data, m.Data())
		select {
		case ch <- data:
		default:
		}
		_ = m.Ack()
	})
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		cc.Stop()
		nc.Close()
	}
}

func publishPlacementCommand(t *testing.T, srv *natssrv.Server, subject string, body []byte) {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, subject, body); err != nil {
		t.Fatalf("publish cmd: %v", err)
	}
}
