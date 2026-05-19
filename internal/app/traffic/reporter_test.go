package traffic

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func sseServer(t *testing.T, events []trafficEvent, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/traffic" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, ev := range events {
			if r.Context().Err() != nil {
				return
			}
			fmt.Fprintf(w, "data: {\"up\":%d,\"down\":%d}\n\n", ev.Up, ev.Down)
			if flusher != nil {
				flusher.Flush()
			}
			if delay > 0 {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(delay):
				}
			}
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestReporter_AccumulatesEvents(t *testing.T) {
	events := []trafficEvent{
		{Up: 100, Down: 200},
		{Up: 250, Down: 500},
		{Up: 400, Down: 800},
	}
	srv := sseServer(t, events, 10*time.Millisecond)

	r, err := New(Config{SingBoxAPIURL: srv.URL, ReconnectDelay: 50 * time.Millisecond}, silent())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = r.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("did not accumulate events; got up=%d down=%d events=%d",
				r.UpBytes(), r.DownBytes(), r.Events())
		default:
			if r.Events() >= 3 {
				cancel()
				if r.UpBytes() != 400 {
					t.Errorf("UpBytes: got %d, want 400 (latest)", r.UpBytes())
				}
				if r.DownBytes() != 800 {
					t.Errorf("DownBytes: got %d, want 800", r.DownBytes())
				}
				if r.LastEventUnix() == 0 {
					t.Error("LastEventUnix should be set")
				}
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func TestReporter_ReconnectsOnStreamEnd(t *testing.T) {
	var (
		mu  sync.Mutex
		hit int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/traffic" {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		hit++
		isFirst := hit == 1
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if isFirst {
			fmt.Fprint(w, "data: {\"up\":50,\"down\":100}\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		fmt.Fprint(w, "data: {\"up\":999,\"down\":1999}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	r, err := New(Config{SingBoxAPIURL: srv.URL, ReconnectDelay: 20 * time.Millisecond}, silent())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = r.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("did not reconnect; up=%d disconnects=%d", r.UpBytes(), r.Disconnects())
		default:
			if r.UpBytes() >= 999 {
				cancel()
				if r.Disconnects() == 0 {
					t.Error("expected at least 1 disconnect logged")
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestReporter_500SurfacesAsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	r, err := New(Config{SingBoxAPIURL: srv.URL, ReconnectDelay: 30 * time.Millisecond}, silent())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = r.Run(ctx) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	if r.Disconnects() == 0 {
		t.Error("expected disconnects on 500")
	}
}

func TestNew_Validates(t *testing.T) {
	if _, err := New(Config{}, silent()); err == nil {
		t.Fatal("expected error for missing SingBoxAPIURL")
	}
}
