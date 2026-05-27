package wg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

func TestLoadOrGenerate_PersistsAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "k")
	a, err := loadOrGenerateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := loadOrGenerateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != b.String() {
		t.Fatalf("key rotated on reload: %s vs %s", a.String(), b.String())
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("key file perms %v, want 0600", info.Mode().Perm())
	}
}

func TestParseAddress_PlainIPv4(t *testing.T) {
	ip, mask, err := parseAddress("10.10.0.3")
	if err != nil {
		t.Fatal(err)
	}
	if ip.String() != "10.10.0.3" {
		t.Errorf("ip: %s", ip)
	}
	if ones, _ := mask.Size(); ones != 32 {
		t.Errorf("mask /%d, want /32", ones)
	}
}

func TestParseAddress_CIDR(t *testing.T) {
	ip, mask, err := parseAddress("10.10.0.3/24")
	if err != nil {
		t.Fatal(err)
	}
	if ip.String() != "10.10.0.3" {
		t.Errorf("ip: %s", ip)
	}
	if ones, _ := mask.Size(); ones != 24 {
		t.Errorf("mask /%d, want /24", ones)
	}
}

func TestParseAddress_Empty(t *testing.T) {
	if _, _, err := parseAddress(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestBuildPeers_HappyPath(t *testing.T) {
	peerKey, _ := wgtypes.GenerateKey()
	peers, err := buildPeers([]Peer{{
		PublicKey:  peerKey.String(),
		Endpoint:   "203.0.113.5",
		ListenPort: 51820,
		Address:    "10.10.0.4",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 {
		t.Fatalf("peers: %d", len(peers))
	}
	p := peers[0]
	if p.Endpoint.IP.String() != "203.0.113.5" || p.Endpoint.Port != 51820 {
		t.Errorf("endpoint: %v", p.Endpoint)
	}
	if len(p.AllowedIPs) != 1 || p.AllowedIPs[0].IP.String() != "10.10.0.4" {
		t.Errorf("allowed: %+v", p.AllowedIPs)
	}
	if p.PersistentKeepaliveInterval == nil || p.PersistentKeepaliveInterval.Seconds() != float64(DefaultKeepaliveSec) {
		t.Errorf("keepalive: %v", p.PersistentKeepaliveInterval)
	}
}

func TestBuildPeers_RejectsBadKey(t *testing.T) {
	_, err := buildPeers([]Peer{{PublicKey: "not-a-key", Endpoint: "1.2.3.4", Address: "10.10.0.4"}})
	if err == nil || !strings.Contains(err.Error(), "parse peer key") {
		t.Fatalf("expected parse error, got %v", err)
	}
}

func TestBuildPeers_RejectsBadAddress(t *testing.T) {
	peerKey, _ := wgtypes.GenerateKey()
	_, err := buildPeers([]Peer{{PublicKey: peerKey.String(), Endpoint: "1.2.3.4", Address: "not-an-ip"}})
	if err == nil || !strings.Contains(err.Error(), "invalid peer address") {
		t.Fatalf("expected invalid address, got %v", err)
	}
}

func TestBuildPeers_RejectsEmptyEndpoint(t *testing.T) {
	peerKey, _ := wgtypes.GenerateKey()
	_, err := buildPeers([]Peer{{PublicKey: peerKey.String(), Address: "10.10.0.4"}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewManager_PublicKeyMatchesGenerated(t *testing.T) {
	mgr, err := New("wg0", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if mgr.PublicKey() == "" {
		t.Fatal("empty public key")
	}
	// Calling again must give the same key.
	mgr2, _ := New("wg0", mgr.keyDir)
	if mgr.PublicKey() != mgr2.PublicKey() {
		t.Errorf("public key changed across reloads")
	}
}
