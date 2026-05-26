package jsonv1

import (
	"testing"
)

func TestUnmarshalUpstreamChanged_Full(t *testing.T) {
	data := []byte(`{
		"schema_version": 1,
		"event_id": "evt-up-1",
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"upstream_node_id": "praha-02",
		"upstream_public_domain": "praha-02.vpn.example.com",
		"upstream_reality_ip": "10.0.0.42"
	}`)
	change, err := UnmarshalUpstreamChanged(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if change.EventID != "evt-up-1" {
		t.Errorf("event_id: %s", change.EventID)
	}
	if change.NodeID != "lv-01" {
		t.Errorf("node_id: %s", change.NodeID)
	}
	if change.BackendID != "praha-02" {
		t.Errorf("backend_id: %s", change.BackendID)
	}
	if change.PublicDomain != "praha-02.vpn.example.com" {
		t.Errorf("public_domain: %s", change.PublicDomain)
	}
	if change.RealityIP != "10.0.0.42" {
		t.Errorf("reality_ip: %s", change.RealityIP)
	}
}

func TestUnmarshalUpstreamChanged_WithWgIPAndPort(t *testing.T) {
	data := []byte(`{
		"event_id": "evt-3",
		"node_id": "entry-1",
		"emitted_at": "2026-05-26T10:00:00Z",
		"upstream_node_id": "backend-1",
		"upstream_public_domain": "",
		"upstream_internal_wg_ip": "10.10.0.5",
		"upstream_agent_port": 10100
	}`)
	change, err := UnmarshalUpstreamChanged(data)
	if err != nil {
		t.Fatal(err)
	}
	if change.InternalWgIP != "10.10.0.5" {
		t.Errorf("internal_wg_ip: %q", change.InternalWgIP)
	}
	if change.AgentPort != 10100 {
		t.Errorf("agent_port: %d", change.AgentPort)
	}
}

func TestUnmarshalUpstreamChanged_OmitsRealityIP(t *testing.T) {
	data := []byte(`{
		"schema_version": 1,
		"event_id": "evt-up-2",
		"node_id": "lv-01",
		"emitted_at": "2026-05-19T10:00:00Z",
		"upstream_node_id": "praha-02",
		"upstream_public_domain": "praha-02.vpn.example.com"
	}`)
	change, err := UnmarshalUpstreamChanged(data)
	if err != nil {
		t.Fatal(err)
	}
	if change.RealityIP != "" {
		t.Errorf("reality_ip should be empty when omitted: %q", change.RealityIP)
	}
}

func TestUnmarshalUpstreamChanged_Rejects(t *testing.T) {
	cases := map[string]string{
		"bad json":                 `{`,
		"missing upstream_node_id": `{"event_id":"e","node_id":"n","emitted_at":"2026-01-01T00:00:00Z","upstream_public_domain":"x"}`,
	}
	for name, body := range cases {
		if _, err := UnmarshalUpstreamChanged([]byte(body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
