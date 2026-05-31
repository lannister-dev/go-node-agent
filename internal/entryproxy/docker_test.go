//go:build with_utls

package entryproxy_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/lannister-dev/go-node-agent/internal/entryproxy/api"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

// TestDockerEntryProxyE2E runs the packaged entry-proxy image as a container,
// drives its control API over the mounted unix socket, and routes a real
// REALITY+VLESS client through it to a host backend. Gated on ENTRY_PROXY_DOCKER
// since it needs Docker + the entry-proxy:local image (docker build -f
// Dockerfile.entry-proxy -t entry-proxy:local .).
func TestDockerEntryProxyE2E(t *testing.T) {
	if os.Getenv("ENTRY_PROXY_DOCKER") == "" {
		t.Skip("set ENTRY_PROXY_DOCKER=1 to run the containerized e2e")
	}
	priv, pub := genRealityKeys(t)
	handshake := tlsTargetAny(t)
	backend := taggedBackendAny(t, "BE1", "cccccccc-cccc-cccc-cccc-cccccccccccc")
	_, hsPort, _ := net.SplitHostPort(handshake)
	user := "cccccccc-cccc-cccc-cccc-cccccccccccc"

	// A named volume (ext4 inside the VM) carries the control socket: a host
	// bind-mounted unix socket can't be connect()ed across the macOS↔VM boundary,
	// so the control API is driven from a curl sidecar sharing this volume.
	vol := "entry-proxy-e2e-sock"
	_ = exec.Command("docker", "volume", "rm", "-f", vol).Run()
	if out, err := exec.Command("docker", "volume", "create", vol).CombinedOutput(); err != nil {
		t.Fatalf("docker volume create: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "volume", "rm", "-f", vol).Run() })

	name := "entry-proxy-e2e-test"
	_ = exec.Command("docker", "rm", "-f", name).Run()
	runArgs := []string{
		"run", "-d", "--rm", "--name", name,
		"--user", "0:0", // entry-proxy runs as root in prod (binds :443, owns the control socket)
		"-p", "127.0.0.1:0:443",
		"-v", vol + ":/var/run/entry-proxy",
		"-e", "REALITY_PRIVATE_KEY=" + priv,
		"-e", "REALITY_SHORT_ID=" + smokeShortID,
		"-e", "REALITY_SERVER_NAME=" + smokeServerName,
		"-e", "REALITY_HANDSHAKE_SERVER=host.docker.internal",
		"-e", "REALITY_HANDSHAKE_PORT=" + hsPort,
		"entry-proxy:local",
	}
	if out, err := exec.Command("docker", runArgs...).CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	hostPort := dockerPublishedPort(t, name)
	waitControlReady(t, vol)

	// Drive the real binary's control plane via the sidecar.
	_, bPortStr, _ := net.SplitHostPort(backend)
	bPort, _ := net.LookupPort("tcp", bPortStr)
	ctlPost(t, vol, api.PathSetBackends, api.SetBackendsRequest{
		Backends: []ports.EntryBackend{{ID: "be1", Address: "host.docker.internal", Port: uint16(bPort)}},
	})
	ctlPost(t, vol, api.PathAddUser, api.AddUserRequest{ClientID: user, Flow: userFlow})
	ctlPost(t, vol, api.PathSelectBackend, api.SelectBackendRequest{ClientID: user, BackendID: "be1"})

	// Drive the real binary's data plane: REALITY+VLESS client → container :443 → host backend.
	conn := realityVlessClient(t, "127.0.0.1:"+hostPort, pub, user, "1.1.1.1:80")
	if tag := readTag(t, conn); tag != "BE1" {
		t.Fatalf("containerized routing failed: got %q want BE1", tag)
	}
}

func dockerPublishedPort(t *testing.T, name string) string {
	t.Helper()
	out, err := exec.Command("docker", "port", name, "443/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port: %v: %s", err, out)
	}
	// e.g. "127.0.0.1:54321"
	line := strings.TrimSpace(strings.Split(string(out), "\n")[0])
	_, port, err := net.SplitHostPort(line)
	if err != nil {
		t.Fatalf("parse docker port %q: %v", line, err)
	}
	return port
}

func curlSidecar(vol, path, jsonBody string) (int, string, error) {
	args := []string{
		"run", "--rm", "--user", "0:0", "-v", vol + ":/s", "curlimages/curl:8.11.1",
		"-s", "-m", "5", "-w", "\n%{http_code}",
		"-X", "POST", "--unix-socket", "/s/control.sock",
	}
	if jsonBody != "" {
		args = append(args, "-H", "Content-Type: application/json", "--data", jsonBody)
	}
	args = append(args, "http://unix"+path)
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return 0, string(out), err
	}
	s := strings.TrimSpace(string(out))
	nl := strings.LastIndex(s, "\n")
	codeStr := s
	body := ""
	if nl >= 0 {
		body = s[:nl]
		codeStr = strings.TrimSpace(s[nl+1:])
	}
	code, _ := strconv.Atoi(codeStr)
	return code, body, nil
}

func waitControlReady(t *testing.T, vol string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if code, _, err := curlSidecar(vol, api.PathStatus, ""); err == nil && code == http.StatusOK {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("control socket never became ready")
}

func ctlPost(t *testing.T, vol, path string, body any) {
	t.Helper()
	buf, _ := json.Marshal(body)
	code, respBody, err := curlSidecar(vol, path, string(buf))
	if err != nil {
		t.Fatalf("control POST %s: %v", path, err)
	}
	if code < 200 || code >= 300 {
		t.Fatalf("control POST %s: status %d: %s", path, code, respBody)
	}
}

func tlsTargetAny(t *testing.T) string {
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
	ln, err := tls.Listen("tcp", "0.0.0.0:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
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

func taggedBackendAny(t *testing.T, tag string, userIDs ...string) string {
	t.Helper()
	handler := func(_ context.Context, conn net.Conn, _ adapter.InboundContext, _ N.CloseHandlerFunc) {
		_, _ = conn.Write([]byte(tag))
		_ = conn.Close()
	}
	svc := vless.NewService[string](logger.NOP(), adapter.NewUpstreamContextHandlerEx(handler, nil))
	svc.UpdateUsers(userIDs, userIDs, make([]string, len(userIDs)))
	ln, err := net.Listen("tcp", "0.0.0.0:0")
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
