// Command entry-proxy is the embedded VLESS+REALITY entry proxy (ADR 0005).
// It replaces the external sing-box container on entry nodes; the agent drives
// it over a local control API (added in a follow-up slice).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/lannister-dev/go-node-agent/internal/entryproxy"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port, _ := strconv.Atoi(env("REALITY_HANDSHAKE_PORT", "443"))
	cfg := entryproxy.Config{
		ListenAddr:      env("ENTRY_LISTEN_ADDR", ":443"),
		RealityKey:      os.Getenv("REALITY_PRIVATE_KEY"),
		ShortID:         os.Getenv("REALITY_SHORT_ID"),
		ServerName:      env("REALITY_SERVER_NAME", "www.cloudflare.com"),
		HandshakeServer: os.Getenv("REALITY_HANDSHAKE_SERVER"),
		HandshakePort:   uint16(port),
	}

	p, err := entryproxy.New(cfg, log)
	if err != nil {
		log.Error("init", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := p.Start(ctx); err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}

	control := entryproxy.NewControlServer(p, log)
	go func() {
		if err := control.Serve(ctx, env("ENTRY_PROXY_SOCKET", "/var/run/entry-proxy/control.sock")); err != nil {
			log.Error("control api", "err", err)
		}
	}()

	<-ctx.Done()
	_ = p.Close()
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
