// Command entry-proxy is the embedded VLESS+REALITY entry proxy (ADR 0005).
// It replaces the external sing-box container on entry nodes; the agent drives
// it over a local control API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/entryproxy"
)

func main() {
	if err := run(); err != nil {
		slog.Error("entry-proxy", "err", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	p, err := entryproxy.New(entryproxy.Config{
		ListenAddr:      env("ENTRY_LISTEN_ADDR", ":443"),
		PlainListenAddr: os.Getenv("ENTRY_PLAIN_LISTEN_ADDR"),
		RealityKey:      os.Getenv("REALITY_PRIVATE_KEY"),
		ShortID:         os.Getenv("REALITY_SHORT_ID"),
		ServerName:      env("REALITY_SERVER_NAME", "www.cloudflare.com"),
		HandshakeServer: os.Getenv("REALITY_HANDSHAKE_SERVER"),
		HandshakePort:   parsePort(env("REALITY_HANDSHAKE_PORT", "443")),
		MuxIdleTimeout:  parseDuration(env("ENTRY_MUX_IDLE_TIMEOUT", "5m")),
	}, log)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := p.Start(ctx); err != nil {
		return err
	}

	control := entryproxy.NewControlServer(p, log)
	go func() {
		if err := control.Serve(ctx, env("ENTRY_PROXY_SOCKET", "/var/run/entry-proxy/control.sock")); err != nil {
			log.Error("control api", "err", err)
		}
	}()

	if addr := os.Getenv("ENTRY_DEBUG_ADDR"); addr != "" {
		go serveDebug(ctx, addr, log)
	}

	<-ctx.Done()
	return p.Close()
}

// serveDebug exposes pprof and a memstats snapshot for on-demand profiling.
// Off unless ENTRY_DEBUG_ADDR is set; reach it with a port-forward, e.g.
// `go tool pprof http://localhost:6060/debug/pprof/heap`.
func serveDebug(ctx context.Context, addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/memstats", func(w http.ResponseWriter, _ *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"heap_alloc":    m.HeapAlloc,
			"heap_inuse":    m.HeapInuse,
			"heap_idle":     m.HeapIdle,
			"heap_released": m.HeapReleased,
			"stack_inuse":   m.StackInuse,
			"sys":           m.Sys,
			"num_gc":        m.NumGC,
			"goroutines":    runtime.NumGoroutine(),
			"gomemlimit":    debug.SetMemoryLimit(-1),
		})
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	log.Info("debug server listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("debug server", "err", err)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parsePort(s string) uint16 {
	n, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 443
	}
	return uint16(n)
}

func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 5 * time.Minute
	}
	return d
}
