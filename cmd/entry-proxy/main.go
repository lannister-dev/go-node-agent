// Command entry-proxy is the embedded VLESS+REALITY entry proxy (ADR 0005).
// It replaces the external sing-box container on entry nodes; the agent drives
// it over a local control API.
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
	if err := run(); err != nil {
		slog.Error("entry-proxy", "err", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	p, err := entryproxy.New(entryproxy.Config{
		ListenAddr:      env("ENTRY_LISTEN_ADDR", ":443"),
		RealityKey:      os.Getenv("REALITY_PRIVATE_KEY"),
		ShortID:         os.Getenv("REALITY_SHORT_ID"),
		ServerName:      env("REALITY_SERVER_NAME", "www.cloudflare.com"),
		HandshakeServer: os.Getenv("REALITY_HANDSHAKE_SERVER"),
		HandshakePort:   parsePort(env("REALITY_HANDSHAKE_PORT", "443")),
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

	<-ctx.Done()
	return p.Close()
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
