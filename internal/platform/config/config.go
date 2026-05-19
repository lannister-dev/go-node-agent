package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	NodeID         string
	BootstrapToken string
	NodeKey        string
	NodeRole       string

	ControlAPIURL string

	NATSURL      string
	NATSCertPath string
	NATSKeyPath  string
	NATSCAPath   string
	NATSName     string

	NATSCommandPrefix    string
	NATSResultPrefix     string
	NATSSnapshotPrefix   string
	NATSHeartbeatPrefix  string
	NATSSyncReportPrefix string

	SingBoxAPIURL     string
	SingBoxConfigPath string

	XrayGRPCAddr string

	HAProxySocket string

	StorePath string

	HTTPAddr string

	LogLevel  string
	LogFormat string

	HeartbeatInterval time.Duration
	TrafficInterval   time.Duration
	DrainTimeout      time.Duration
	SnapshotInterval  time.Duration
	ReconcileInterval time.Duration

	EnableExecutor          bool
	SingBoxInboundTag       string
	SingBoxListenAddress    string
	SingBoxListenPort       uint16
	SingBoxLogLevel         string
	BackendDefaultPort      uint16
	BackendDefaultTransport string
	XrayInboundTag          string
	BandwidthNIC            string
	BandwidthCapacityMbps   uint16
	OTLPEndpoint            string
	OTLPInsecure            bool
}

func Load() (Config, error) {
	var err error
	cfg := Config{
		NodeID:               env("NODE_ID", ""),
		BootstrapToken:       env("BOOTSTRAP_TOKEN", ""),
		NodeKey:              env("NODE_KEY", ""),
		NodeRole:             env("NODE_ROLE", ""),
		ControlAPIURL:        env("CONTROL_API_URL", ""),
		NATSURL:              env("NATS_URL", "nats://nats.nats.svc.cluster.local:4222"),
		NATSCertPath:         env("NATS_CERT_PATH", ""),
		NATSKeyPath:          env("NATS_KEY_PATH", ""),
		NATSCAPath:           env("NATS_CA_PATH", ""),
		NATSName:             env("NATS_NAME", "go-node-agent"),
		NATSCommandPrefix:    env("NATS_COMMAND_PREFIX", "agent.placements"),
		NATSResultPrefix:     env("NATS_RESULT_PREFIX", "agent.placement_results"),
		NATSSnapshotPrefix:   env("NATS_SNAPSHOT_PREFIX", "agent.snapshots"),
		NATSHeartbeatPrefix:  env("NATS_HEARTBEAT_PREFIX", "agent.heartbeats"),
		NATSSyncReportPrefix: env("NATS_SYNC_REPORT_PREFIX", "agent.sync_reports"),
		SingBoxAPIURL:        env("SINGBOX_API_URL", "http://127.0.0.1:9090"),
		SingBoxConfigPath:    env("SINGBOX_CONFIG_PATH", "/var/lib/sing-box-shared/sing-box/config.json"),
		XrayGRPCAddr:         env("XRAY_GRPC_ADDR", "127.0.0.1:10085"),
		HAProxySocket:        env("HAPROXY_SOCKET", "/var/run/haproxy/admin.sock"),
		StorePath:            env("STORE_PATH", "/var/lib/go-node-agent"),
		HTTPAddr:             env("HTTP_ADDR", ":8080"),
		LogLevel:             env("LOG_LEVEL", "info"),
		LogFormat:            env("LOG_FORMAT", "json"),

		SingBoxInboundTag:       env("SINGBOX_INBOUND_TAG", "vless-in"),
		SingBoxListenAddress:    env("SINGBOX_LISTEN_ADDRESS", "::"),
		SingBoxLogLevel:         env("SINGBOX_LOG_LEVEL", "info"),
		BackendDefaultTransport: env("BACKEND_DEFAULT_TRANSPORT", "ws"),
		XrayInboundTag:          env("XRAY_INBOUND_TAG", "vless-in"),
		BandwidthNIC:            env("BANDWIDTH_NIC", ""),
		OTLPEndpoint:            env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	}
	cfg.OTLPInsecure = envBool("OTEL_EXPORTER_OTLP_INSECURE", true)

	if cfg.SingBoxListenPort, err = envU16("SINGBOX_LISTEN_PORT", 443); err != nil {
		return cfg, err
	}
	if cfg.BackendDefaultPort, err = envU16("BACKEND_DEFAULT_PORT", 9000); err != nil {
		return cfg, err
	}
	if cfg.BandwidthCapacityMbps, err = envU16("BANDWIDTH_CAPACITY_MBPS", 0); err != nil {
		return cfg, err
	}
	cfg.EnableExecutor = envBool("ENABLE_EXECUTOR", false)

	if cfg.HeartbeatInterval, err = envDur("HEARTBEAT_INTERVAL", 10*time.Second); err != nil {
		return cfg, err
	}
	if cfg.TrafficInterval, err = envDur("TRAFFIC_INTERVAL", 30*time.Second); err != nil {
		return cfg, err
	}
	if cfg.DrainTimeout, err = envDur("DRAIN_TIMEOUT", 30*time.Second); err != nil {
		return cfg, err
	}
	if cfg.SnapshotInterval, err = envDur("SNAPSHOT_INTERVAL", 60*time.Second); err != nil {
		return cfg, err
	}
	if cfg.ReconcileInterval, err = envDur("RECONCILE_INTERVAL", 5*time.Minute); err != nil {
		return cfg, err
	}

	return cfg, cfg.validate()
}

func (c Config) validate() error {
	var missing []string
	if c.BootstrapToken == "" {
		missing = append(missing, "BOOTSTRAP_TOKEN")
	}
	if c.NodeKey == "" {
		missing = append(missing, "NODE_KEY")
	}
	if c.ControlAPIURL == "" {
		missing = append(missing, "CONTROL_API_URL")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("LOG_LEVEL must be debug|info|warn|error")
	}
	switch strings.ToLower(c.LogFormat) {
	case "json", "text":
	default:
		return errors.New("LOG_FORMAT must be json|text")
	}
	return nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envDur(key string, fallback time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback, nil
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second, nil
	}
	return time.ParseDuration(v)
}

func envU16(key string, fallback uint16) (uint16, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseUint(v, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return uint16(n), nil
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on", "y":
		return true
	case "0", "false", "no", "off", "n":
		return false
	}
	return fallback
}
