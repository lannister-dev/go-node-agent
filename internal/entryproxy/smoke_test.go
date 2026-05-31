//go:build with_utls

package entryproxy_test

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	singtls "github.com/sagernet/sing-box/common/tls"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/lannister-dev/go-node-agent/internal/entryproxy"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

const (
	smokeServerName = "example.com"
	smokeShortID    = "0123456789abcdef"
	userFlow        = "" // no vision: backend leg is plain TCP
)

func genRealityKeys(t *testing.T) (priv, pub string) {
	t.Helper()
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("x25519: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(k.Bytes()),
		base64.RawURLEncoding.EncodeToString(k.PublicKey().Bytes())
}

// tlsTarget is the local TLS server REALITY relays the handshake to.
func tlsTarget(t *testing.T) string {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: smokeServerName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{smokeServerName},
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatalf("tls target: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, c); _ = c.Close() }()
		}
	}()
	return ln.Addr().String()
}

// echoServer returns bytes it receives.
func echoServer(t *testing.T) string {
	t.Helper()
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
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	return ln.Addr().String()
}

// fakeBackend is a plain-TCP VLESS server that dials the requested destination.
// It registers the entry's users (the entry authenticates as the user itself).
func fakeBackend(t *testing.T, userIDs ...string) string {
	t.Helper()
	handler := func(ctx context.Context, conn net.Conn, md adapter.InboundContext, _ N.CloseHandlerFunc) {
		up, err := net.Dial("tcp", md.Destination.String())
		if err != nil {
			_ = conn.Close()
			return
		}
		go func() { _, _ = io.Copy(up, conn); _ = up.Close() }()
		_, _ = io.Copy(conn, up)
		_ = conn.Close()
	}
	svc := vless.NewService[string](logger.NOP(), adapter.NewUpstreamContextHandlerEx(handler, nil))
	flows := make([]string, len(userIDs))
	svc.UpdateUsers(userIDs, userIDs, flows)

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

func taggedBackend(t *testing.T, tag string, userIDs ...string) string {
	t.Helper()
	handler := func(_ context.Context, conn net.Conn, _ adapter.InboundContext, _ N.CloseHandlerFunc) {
		_, _ = conn.Write([]byte(tag))
		_ = conn.Close()
	}
	svc := vless.NewService[string](logger.NOP(), adapter.NewUpstreamContextHandlerEx(handler, nil))
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

func readTag(t *testing.T, conn net.Conn) string {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 8)
	n, _ := io.ReadFull(conn, buf[:3])
	return string(buf[:n])
}

func entryBackend(t *testing.T, id, addr string) ports.EntryBackend {
	t.Helper()
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := net.LookupPort("tcp", portStr)
	return ports.EntryBackend{ID: id, Address: host, Port: uint16(port)}
}

// realityVlessClient connects through the entry proxy as clientID to dst.
func realityVlessClient(t *testing.T, proxyAddr, pubKey, clientID, dst string) net.Conn {
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
	conn, err := vc.DialEarlyConn(tlsConn, M.ParseSocksaddr(dst))
	if err != nil {
		t.Fatalf("vless dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func roundtrip(t *testing.T, conn net.Conn, msg string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf, []byte(msg)) {
		t.Fatalf("echo mismatch: got %q want %q", buf, msg)
	}
}

func TestSmokeRoutingSelectsBackendAndSwitches(t *testing.T) {
	priv, pub := genRealityKeys(t)
	target := tlsTarget(t)
	user := "cccccccc-cccc-cccc-cccc-cccccccccccc"

	be1 := taggedBackend(t, "BE1", user)
	be2 := taggedBackend(t, "BE2", user)

	targetHost, targetPortStr, _ := net.SplitHostPort(target)
	targetPort, _ := net.LookupPort("tcp", targetPortStr)
	p, err := entryproxy.New(entryproxy.Config{
		ListenAddr:      "127.0.0.1:0",
		RealityKey:      priv,
		ShortID:         smokeShortID,
		ServerName:      smokeServerName,
		HandshakeServer: targetHost,
		HandshakePort:   uint16(targetPort),
	}, nil)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Close() }()

	if err := p.SetBackends(ctx, []ports.EntryBackend{entryBackend(t, "be1", be1), entryBackend(t, "be2", be2)}); err != nil {
		t.Fatal(err)
	}
	if err := p.AddUser(ctx, user, userFlow); err != nil {
		t.Fatal(err)
	}

	// Route to be2 → traffic must land on BE2.
	if err := p.SelectBackend(ctx, user, "be2"); err != nil {
		t.Fatal(err)
	}
	if tag := readTag(t, realityVlessClient(t, p.Addr(), pub, user, "1.1.1.1:80")); tag != "BE2" {
		t.Fatalf("routed to wrong backend: got %q want BE2", tag)
	}

	// Switch route to be1 → a new connection must land on BE1.
	if err := p.SelectBackend(ctx, user, "be1"); err != nil {
		t.Fatal(err)
	}
	if tag := readTag(t, realityVlessClient(t, p.Addr(), pub, user, "1.1.1.1:80")); tag != "BE1" {
		t.Fatalf("route switch failed: got %q want BE1", tag)
	}
}

func TestNewResolvesDomainHandshakeServer(t *testing.T) {
	priv, _ := genRealityKeys(t)
	// A domain handshake server must be resolved up front — passing it through
	// makes sing-box's REALITY dialer nil-deref (the bug that broke the deploy).
	p, err := entryproxy.New(entryproxy.Config{
		ListenAddr:      "127.0.0.1:0",
		RealityKey:      priv,
		ShortID:         smokeShortID,
		ServerName:      smokeServerName,
		HandshakeServer: "localhost",
		HandshakePort:   443,
	}, nil)
	if err != nil {
		t.Fatalf("New with domain handshake server: %v", err)
	}
	_ = p
}

func TestHandshakeTimeoutClosesIdleConn(t *testing.T) {
	priv, _ := genRealityKeys(t)
	target := tlsTarget(t)
	targetHost, targetPortStr, _ := net.SplitHostPort(target)
	targetPort, _ := net.LookupPort("tcp", targetPortStr)

	p, err := entryproxy.New(entryproxy.Config{
		ListenAddr:       "127.0.0.1:0",
		RealityKey:       priv,
		ShortID:          smokeShortID,
		ServerName:       smokeServerName,
		HandshakeServer:  targetHost,
		HandshakePort:    uint16(targetPort),
		HandshakeTimeout: 200 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Close() }()

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Send nothing: the server must drop us after HandshakeTimeout.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	start := time.Now()
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the server to close an idle connection")
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("idle connection not dropped promptly: %v", elapsed)
	}
}

func TestSmokeEndToEndAndLiveAddNoDrop(t *testing.T) {
	priv, pub := genRealityKeys(t)
	target := tlsTarget(t)
	echo := echoServer(t)

	userA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	userB := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	backendAddr := fakeBackend(t, userA, userB)

	targetHost, targetPortStr, _ := net.SplitHostPort(target)
	targetPort, _ := net.LookupPort("tcp", targetPortStr)
	beHost, bePortStr, _ := net.SplitHostPort(backendAddr)
	bePort, _ := net.LookupPort("tcp", bePortStr)

	p, err := entryproxy.New(entryproxy.Config{
		ListenAddr:      "127.0.0.1:0",
		RealityKey:      priv,
		ShortID:         smokeShortID,
		ServerName:      smokeServerName,
		HandshakeServer: targetHost,
		HandshakePort:   uint16(targetPort),
	}, nil)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Close() }()

	if err := p.SetBackends(ctx, []ports.EntryBackend{{ID: "be1", Address: beHost, Port: uint16(bePort)}}); err != nil {
		t.Fatal(err)
	}
	if err := p.AddUser(ctx, userA, userFlow); err != nil {
		t.Fatal(err)
	}
	if err := p.SelectBackend(ctx, userA, "be1"); err != nil {
		t.Fatal(err)
	}

	proxyAddr := p.Addr()
	connA := realityVlessClient(t, proxyAddr, pub, userA, echo)
	roundtrip(t, connA, "hello-before-add")

	// Live-add a second user while A is connected — must not drop A.
	if err := p.AddUser(ctx, userB, userFlow); err != nil {
		t.Fatal(err)
	}
	roundtrip(t, connA, "hello-after-add")

	if n, _ := p.BackendConnections(ctx, "be1"); n == 0 {
		t.Fatal("expected at least one active backend connection")
	}

	conns, _ := p.ActiveConnections(ctx)
	if len(conns) == 0 {
		t.Fatal("expected an active connection")
	}
	var up, down uint64
	for _, c := range conns {
		up += c.Upload
		down += c.Download
	}
	if up == 0 || down == 0 {
		t.Fatalf("expected non-zero byte counters, got up=%d down=%d", up, down)
	}
}
