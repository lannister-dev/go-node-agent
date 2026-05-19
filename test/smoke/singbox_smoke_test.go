//go:build smoke

package smoke_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/lannister-dev/go-node-agent/internal/adapters/singbox"
	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

const baseConfigJSON = `{
  "log": {"level": "info"},
  "experimental": {
    "clash_api": {"external_controller": "0.0.0.0:9090"}
  },
  "inbounds": [
    {
      "type": "vless",
      "tag": "vless-in",
      "listen": "0.0.0.0",
      "listen_port": 8443,
      "users": []
    }
  ],
  "outbounds": [
    {"type": "direct", "tag": "direct"},
    {"type": "block", "tag": "block"}
  ],
  "route": {"rules": [], "final": "direct"}
}`

func writeHostConfig(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "singbox-smoke-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	cfg := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfg, []byte(baseConfigJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestRealSingBox_ReloadViaClashAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	configPath := writeHostConfig(t)
	containerCfgPath := "/etc/sing-box/config.json"

	req := testcontainers.ContainerRequest{
		Image:        "ghcr.io/sagernet/sing-box:latest",
		ExposedPorts: []string{"9090/tcp"},
		Cmd:          []string{"run", "-c", containerCfgPath},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      configPath,
				ContainerFilePath: containerCfgPath,
				FileMode:          0o644,
			},
		},
		WaitingFor: wait.ForLog("router: created").WithStartupTimeout(30 * time.Second),
	}

	cont, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("docker / sing-box image unavailable: %v", err)
	}
	t.Cleanup(func() {
		shutCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = cont.Terminate(shutCtx)
	})

	host, err := cont.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := cont.MappedPort(ctx, "9090")
	if err != nil {
		t.Fatal(err)
	}
	apiURL := "http://" + net.JoinHostPort(host, port.Port())

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cli, err := singbox.New(singbox.Options{
		APIURL:     apiURL,
		ConfigPath: configPath,
		Logger:     logger,
		Timeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	if err := cli.Healthy(ctx); err != nil {
		t.Fatalf("real sing-box /version: %v", err)
	}

	state := singboxgen.NodeState{
		Log:      singboxgen.LogSpec{Level: "info"},
		ClashAPI: singboxgen.ClashAPISpec{Enabled: true, ExternalCt: "0.0.0.0:9090"},
		Inbound: singboxgen.InboundSpec{
			Tag:    "vless-in",
			Listen: singboxgen.ListenSpec{Address: "0.0.0.0", Port: 8443},
		},
		Backends: []singboxgen.BackendSpec{
			{ID: "praha-02", Address: "10.0.0.2", Port: 9000, Transport: domain.TransportWS},
		},
		Placements: []domain.Placement{
			{
				ID:            "p-1",
				ClientID:      "01234567-89ab-cdef-0123-456789abcdef",
				BackendNodeID: "praha-02",
				NodeID:        "lv-01",
				Desired:       domain.DesiredActive,
				Transport:     domain.TransportWS,
				OpVersion:     1,
			},
		},
	}

	base, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := singboxgen.MergeBase(base, state)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if err := cli.WriteConfig(ctx, ports.SingBoxConfig{Raw: merged}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := cli.Reload(ctx); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if err := cli.Healthy(ctx); err != nil {
		t.Fatalf("health after reload: %v", err)
	}

	written, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(written, &got); err != nil {
		t.Fatal(err)
	}
	inb := got["inbounds"].([]any)[0].(map[string]any)
	users := inb["users"].([]any)
	if len(users) != 1 || users[0].(map[string]any)["uuid"] != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Errorf("user list not injected into running sing-box config: %+v", users)
	}

	conns, err := cli.Connections(ctx)
	if err != nil {
		t.Fatalf("connections query failed: %v", err)
	}
	t.Logf("real sing-box reports %d active connections", conns.Total)
}
