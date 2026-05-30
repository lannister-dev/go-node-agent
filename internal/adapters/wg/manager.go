package wg

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

const (
	DefaultListenPort   = 4500
	DefaultKeepaliveSec = 25
	DefaultInterface    = "wg0"
	privateKeyFile      = "wg-private.key"
)

type Peer struct {
	PublicKey  string
	Endpoint   string
	ListenPort int
	Address    string
}

type ApplyState struct {
	Address    string
	ListenPort int
	Peers      []Peer
}

type Manager struct {
	iface  string
	keyDir string
	priv   wgtypes.Key
}

func New(iface, keyDir string) (*Manager, error) {
	if iface == "" {
		iface = DefaultInterface
	}
	if keyDir == "" {
		return nil, errors.New("wg: keyDir required")
	}
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return nil, fmt.Errorf("wg: mkdir keyDir: %w", err)
	}
	priv, err := loadOrGenerateKey(filepath.Join(keyDir, privateKeyFile))
	if err != nil {
		return nil, err
	}
	return &Manager{iface: iface, keyDir: keyDir, priv: priv}, nil
}

func (m *Manager) PublicKey() string {
	return m.priv.PublicKey().String()
}

func (m *Manager) Apply(s ApplyState) error {
	addr, mask, err := parseAddress(s.Address)
	if err != nil {
		return err
	}
	listenPort := s.ListenPort
	if listenPort == 0 {
		listenPort = DefaultListenPort
	}
	client, err := wgctrl.New()
	if err != nil {
		return fmt.Errorf("wg: new client: %w", err)
	}
	defer func() { _ = client.Close() }()
	if dev, derr := client.Device(m.iface); derr == nil && dev.ListenPort != 0 && dev.ListenPort != listenPort {
		if err := recreateInterface(m.iface); err != nil {
			return fmt.Errorf("wg: recreate %s for port change %d→%d: %w", m.iface, dev.ListenPort, listenPort, err)
		}
	}
	if err := ensureInterface(m.iface, addr, mask); err != nil {
		return err
	}

	peers, err := buildPeers(s.Peers)
	if err != nil {
		return err
	}
	cfg := wgtypes.Config{
		PrivateKey:   &m.priv,
		ListenPort:   &listenPort,
		ReplacePeers: true,
		Peers:        peers,
	}
	if err := client.ConfigureDevice(m.iface, cfg); err != nil {
		return fmt.Errorf("wg: configure %s: %w", m.iface, err)
	}
	allowed := make([]*net.IPNet, 0, len(peers))
	for _, p := range peers {
		for i := range p.AllowedIPs {
			allowed = append(allowed, &p.AllowedIPs[i])
		}
	}
	if err := syncRoutes(m.iface, allowed); err != nil {
		return err
	}
	return nil
}

func loadOrGenerateKey(path string) (wgtypes.Key, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		key, perr := wgtypes.ParseKey(string(trimNewline(data)))
		if perr == nil {
			return key, nil
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return wgtypes.Key{}, fmt.Errorf("wg: read key: %w", err)
	}
	key, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		return wgtypes.Key{}, fmt.Errorf("wg: generate key: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(key.String()), 0o600); err != nil {
		return wgtypes.Key{}, fmt.Errorf("wg: write key: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return wgtypes.Key{}, fmt.Errorf("wg: rename key: %w", err)
	}
	return key, nil
}

func trimNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r' || b[len(b)-1] == ' ') {
		b = b[:len(b)-1]
	}
	return b
}

func parseAddress(addr string) (net.IP, net.IPMask, error) {
	if addr == "" {
		return nil, nil, errors.New("wg: empty address")
	}
	ip, ipnet, err := net.ParseCIDR(addr)
	if err != nil {
		ip = net.ParseIP(addr)
		if ip == nil {
			return nil, nil, fmt.Errorf("wg: invalid address %q", addr)
		}
		return ip.To4(), net.CIDRMask(32, 32), nil
	}
	return ip.To4(), ipnet.Mask, nil
}

func buildPeers(in []Peer) ([]wgtypes.PeerConfig, error) {
	keepalive := DefaultKeepaliveSec * time.Second
	out := make([]wgtypes.PeerConfig, 0, len(in))
	for _, p := range in {
		key, err := wgtypes.ParseKey(p.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("wg: parse peer key %q: %w", p.PublicKey, err)
		}
		listenPort := p.ListenPort
		if listenPort == 0 {
			listenPort = DefaultListenPort
		}
		endpoint, err := resolveUDP(p.Endpoint, listenPort)
		if err != nil {
			return nil, err
		}
		ip := net.ParseIP(p.Address)
		if ip == nil || ip.To4() == nil {
			return nil, fmt.Errorf("wg: invalid peer address %q", p.Address)
		}
		allowedIPs := []net.IPNet{{IP: ip.To4(), Mask: net.CIDRMask(32, 32)}}
		out = append(out, wgtypes.PeerConfig{
			PublicKey:                   key,
			Endpoint:                    endpoint,
			AllowedIPs:                  allowedIPs,
			PersistentKeepaliveInterval: &keepalive,
			ReplaceAllowedIPs:           true,
		})
	}
	return out, nil
}

func resolveUDP(host string, port int) (*net.UDPAddr, error) {
	if host == "" {
		return nil, errors.New("wg: empty peer endpoint")
	}
	ip := net.ParseIP(host)
	if ip != nil {
		return &net.UDPAddr{IP: ip, Port: port}, nil
	}
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, fmt.Errorf("wg: resolve %q: %w", host, err)
	}
	return addr, nil
}
