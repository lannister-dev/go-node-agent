package wgmesh

import (
	"encoding/json"
	"testing"
)

func TestPeerPayload_DecodesControlAPIShape(t *testing.T) {
	// Matches the exact JSON published by services/wg/publisher.py.
	raw := []byte(`{
		"node_id": "14ed9ba9-0b3c-4b52-8ea4-8c69c638063e",
		"address": "10.10.0.3",
		"listen_port": 51820,
		"peers": [
			{
				"node_id": "3780ba57-c318-4b9b-9c27-142f2de60d84",
				"name": "par-backend-01",
				"public_key": "JAhYG2mwU4TTlNmOPUHsmUZqptN0CjfutpRQj-MfvCM",
				"endpoint": "217.60.252.176",
				"listen_port": 51820,
				"address": "10.10.0.2"
			}
		]
	}`)
	var p peerPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if p.Address != "10.10.0.3" {
		t.Errorf("address: %s", p.Address)
	}
	if len(p.Peers) != 1 {
		t.Fatalf("peers: %d", len(p.Peers))
	}
	if p.Peers[0].Endpoint != "217.60.252.176" {
		t.Errorf("peer endpoint: %s", p.Peers[0].Endpoint)
	}
}

func TestPubkeyPayload_MatchesControlAPIShape(t *testing.T) {
	// Validates: control-api Pydantic model expects extra="forbid", so unknown
	// fields would break it. Keep the wire format minimal.
	p := pubkeyPayload{
		NodeID:     "abc",
		PublicKey:  "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		ListenPort: 51820,
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	for _, k := range []string{"node_id", "public_key", "listen_port"} {
		if _, ok := got[k]; !ok {
			t.Errorf("missing field %q", k)
		}
	}
	if len(got) != 3 {
		t.Errorf("unexpected fields, got %d: %v", len(got), got)
	}
}
