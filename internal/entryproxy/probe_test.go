//go:build with_utls

package entryproxy_test

import (
	"context"
	"crypto/ecdh"
	"encoding/base64"
	"net"
	"os"
	"testing"
	"time"

	singtls "github.com/sagernet/sing-box/common/tls"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

// TestProbeRealEntry connects to a live entry as a given VLESS user and reports
// whether the tunnel actually carries traffic. Gated on PROBE_ENTRY_ADDR.
//
//	PROBE_ENTRY_ADDR=217.60.252.176:443 \
//	PROBE_REALITY_PRIV=<base64url priv> PROBE_SHORT_ID=<hex> PROBE_SNI=www.cloudflare.com \
//	PROBE_UUID=<client-id> PROBE_FLOW=xtls-rprx-vision PROBE_DST=1.1.1.1:80 \
//	go test -tags with_utls -run TestProbeRealEntry ./internal/entryproxy/ -v
func TestProbeRealEntry(t *testing.T) {
	addr := os.Getenv("PROBE_ENTRY_ADDR")
	if addr == "" {
		t.Skip("set PROBE_ENTRY_ADDR to probe a live entry")
	}
	priv := os.Getenv("PROBE_REALITY_PRIV")
	sid := os.Getenv("PROBE_SHORT_ID")
	sni := os.Getenv("PROBE_SNI")
	uuid := os.Getenv("PROBE_UUID")
	flow := os.Getenv("PROBE_FLOW")
	dst := os.Getenv("PROBE_DST")
	if dst == "" {
		dst = "1.1.1.1:80"
	}

	pkBytes, err := base64.RawURLEncoding.DecodeString(priv)
	if err != nil {
		t.Fatalf("decode priv: %v", err)
	}
	k, err := ecdh.X25519().NewPrivateKey(pkBytes)
	if err != nil {
		t.Fatalf("x25519: %v", err)
	}
	pub := base64.RawURLEncoding.EncodeToString(k.PublicKey().Bytes())
	t.Logf("probe: entry=%s sni=%s uuid=%s flow=%q dst=%s pub=%s", addr, sni, uuid, flow, dst, pub)

	tcp, err := net.DialTimeout("tcp", addr, 8*time.Second)
	if err != nil {
		t.Fatalf("dial entry: %v", err)
	}
	defer func() { _ = tcp.Close() }()

	cfg, err := singtls.NewRealityClient(context.Background(), singlog.NewNOPFactory().Logger(), sni, option.OutboundTLSOptions{
		Enabled:    true,
		ServerName: sni,
		UTLS:       &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "chrome"},
		Reality:    &option.OutboundRealityOptions{Enabled: true, PublicKey: pub, ShortID: sid},
	})
	if err != nil {
		t.Fatalf("reality cfg: %v", err)
	}
	hctx, hcancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer hcancel()
	tlsConn, err := singtls.ClientHandshake(hctx, tcp, cfg)
	if err != nil {
		t.Fatalf("REALITY handshake FAILED (wrong pub/short_id/sni, or entry down): %v", err)
	}
	t.Log("REALITY handshake OK")

	vc, err := vless.NewClient(uuid, flow, logger.NOP())
	if err != nil {
		t.Fatalf("vless client: %v", err)
	}
	conn, err := vc.DialEarlyConn(tlsConn, M.ParseSocksaddr(dst))
	if err != nil {
		t.Fatalf("vless dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Send an HTTP/1.0 request through the tunnel; a working backend returns bytes.
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	req := "GET / HTTP/1.0\r\nHost: " + dst + "\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("tunnel write FAILED (user not authenticated / no route): %v", err)
	}
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if n > 0 {
		t.Logf("TUNNEL WORKS — got %d bytes: %q", n, string(buf[:n]))
		return
	}
	t.Fatalf("TUNNEL DEAD — no bytes back (read err=%v). a7be85c6 not served / routed to a backend that rejects it.", err)
}
