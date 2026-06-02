//go:build with_utls

package entryproxy_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	singtls "github.com/sagernet/sing-box/common/tls"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	vmess "github.com/sagernet/sing-vmess"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/lannister-dev/go-node-agent/internal/entryproxy"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

func udpEchoBackend(t *testing.T, userIDs ...string) string {
	t.Helper()
	packetHandler := func(ctx context.Context, conn N.PacketConn, _ adapter.InboundContext, _ N.CloseHandlerFunc) {
		_ = bufio.CopyPacketConn(ctx, conn, conn)
	}
	svc := vless.NewService[string](logger.NOP(), adapter.NewUpstreamContextHandlerEx(nil, packetHandler))
	svc.UpdateUsers(userIDs, userIDs, make([]string, len(userIDs)))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				src := M.SocksaddrFromNet(c.RemoteAddr())
				md := adapter.InboundContext{Source: src}
				_ = svc.NewConnection(adapter.WithContext(context.Background(), &md), c, src, func(error) {})
			}()
		}
	}()
	return ln.Addr().String()
}

func realityXUDPClient(t *testing.T, proxyAddr, pubKey, clientID, dst string) vmess.PacketConn {
	t.Helper()
	tcp, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	cfg, err := singtls.NewRealityClient(context.Background(), singlog.NewNOPFactory().Logger(), smokeServerName, option.OutboundTLSOptions{
		Enabled:    true,
		ServerName: smokeServerName,
		UTLS:       &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "chrome"},
		Reality:    &option.OutboundRealityOptions{Enabled: true, PublicKey: pubKey, ShortID: smokeShortID},
	})
	if err != nil {
		t.Fatalf("reality client cfg: %v", err)
	}
	tlsConn, err := singtls.ClientHandshake(context.Background(), tcp, cfg)
	if err != nil {
		t.Fatalf("reality handshake: %v", err)
	}
	vc, err := vless.NewClient(clientID, userFlow, logger.NOP())
	if err != nil {
		t.Fatalf("vless client: %v", err)
	}
	pc, err := vc.DialEarlyXUDPPacketConn(tlsConn, M.ParseSocksaddr(dst))
	if err != nil {
		t.Fatalf("xudp dial: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

func TestUDPRelayThroughProxy(t *testing.T) {
	user := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	be := udpEchoBackend(t, user)
	priv, pub := genRealityKeys(t)
	target := tlsTarget(t)
	host, portStr, _ := net.SplitHostPort(target)
	port, _ := net.LookupPort("tcp", portStr)
	p, err := entryproxy.New(entryproxy.Config{
		ListenAddr:      "127.0.0.1:0",
		RealityKey:      priv,
		ShortID:         smokeShortID,
		ServerName:      smokeServerName,
		HandshakeServer: host,
		HandshakePort:   uint16(port),
	}, nil)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := p.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.SetBackends(ctx, []ports.EntryBackend{entryBackend(t, "udp-be", be)}); err != nil {
		t.Fatal(err)
	}
	if err := p.AddUser(ctx, user, userFlow); err != nil {
		t.Fatal(err)
	}
	if err := p.SelectBackend(ctx, user, "udp-be"); err != nil {
		t.Fatal(err)
	}

	pc := realityXUDPClient(t, p.Addr(), pub, user, "1.1.1.1:53")
	dest := M.ParseSocksaddr("1.1.1.1:53").UDPAddr()

	if _, err := pc.WriteTo([]byte("ping-udp"), dest); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	_ = pc.SetReadDeadline(time.Now().Add(3 * time.Second))
	recv := make([]byte, 1500)
	n, _, err := pc.ReadFrom(recv)
	if err != nil {
		t.Fatalf("read packet (udp dropped?): %v", err)
	}
	if string(recv[:n]) != "ping-udp" {
		t.Fatalf("udp echo mismatch: got %q", recv[:n])
	}
}
