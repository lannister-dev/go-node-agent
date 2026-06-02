//go:build with_utls

package entryproxy_test

import (
	"net"
	"testing"
	"time"

	vmess "github.com/sagernet/sing-vmess"

	"github.com/lannister-dev/go-node-agent/internal/entryproxy"
)

func TestMuxIdleTimeoutWired(t *testing.T) {
	priv, _ := genRealityKeys(t)
	target := tlsTarget(t)
	host, portStr, _ := net.SplitHostPort(target)
	port, _ := net.LookupPort("tcp", portStr)
	_, err := entryproxy.New(entryproxy.Config{
		ListenAddr:      "127.0.0.1:0",
		RealityKey:      priv,
		ShortID:         smokeShortID,
		ServerName:      smokeServerName,
		HandshakeServer: host,
		HandshakePort:   uint16(port),
		MuxIdleTimeout:  7 * time.Second,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if vmess.ServerReadTimeout != 7*time.Second {
		t.Fatalf("ServerReadTimeout=%v want 7s", vmess.ServerReadTimeout)
	}
}
