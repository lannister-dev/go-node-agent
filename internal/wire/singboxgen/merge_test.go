package singboxgen

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func baseConfigJSON() []byte {
	return []byte(`{
		"log": {"level": "info"},
		"inbounds": [
			{
				"type": "vless",
				"tag": "vless-in",
				"listen": "::",
				"listen_port": 443,
				"users": [],
				"tls": {
					"enabled": true,
					"server_name": "www.cloudflare.com",
					"reality": {
						"enabled": true,
						"handshake": {"server": "www.cloudflare.com", "server_port": 443},
						"private_key": "ABCD-SUPER-SECRET-PRIVATE-KEY",
						"short_id": ["abc123"]
					}
				}
			}
		],
		"outbounds": [
			{"type": "direct", "tag": "direct"},
			{"type": "block", "tag": "block"}
		],
		"route": {
			"rules": [
				{"protocol": ["dns"], "outbound": "direct"}
			],
			"final": "direct"
		}
	}`)
}

func mergeState() NodeState {
	return NodeState{
		Inbound: InboundSpec{Tag: "vless-in", Listen: ListenSpec{Address: "::", Port: 443}},
		Backends: []BackendSpec{
			{ID: "praha-02", Address: "10.0.0.2", Port: 9000, Transport: domain.TransportWS},
			{ID: "latvia-01", Address: "10.0.0.1", Port: 9000, Transport: domain.TransportReality, Reality: RealitySpec{Enabled: true, ServerName: "latvia.example.com"}},
		},
		Placements: []domain.Placement{
			{ID: "p-1", ClientID: "uuid-a", BackendNodeID: "praha-02", Desired: domain.DesiredActive, Transport: domain.TransportWS},
			{ID: "p-2", ClientID: "uuid-b", BackendNodeID: "latvia-01", Desired: domain.DesiredActive, Transport: domain.TransportReality},
		},
	}
}

func TestMergeBase_PreservesRealityKeys(t *testing.T) {
	merged, err := MergeBase(baseConfigJSON(), mergeState())
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(merged, &got); err != nil {
		t.Fatal(err)
	}
	inb := got["inbounds"].([]any)[0].(map[string]any)
	tls := inb["tls"].(map[string]any)
	reality := tls["reality"].(map[string]any)
	if reality["private_key"] != "ABCD-SUPER-SECRET-PRIVATE-KEY" {
		t.Errorf("private_key lost: %v", reality["private_key"])
	}
	shortIDs := reality["short_id"].([]any)
	if len(shortIDs) != 1 || shortIDs[0] != "abc123" {
		t.Errorf("short_id lost: %+v", shortIDs)
	}
}

func TestMergeBase_InjectsUsersIntoMatchingInbound(t *testing.T) {
	merged, err := MergeBase(baseConfigJSON(), mergeState())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(merged, &got)
	users := got["inbounds"].([]any)[0].(map[string]any)["users"].([]any)
	if len(users) != 2 {
		t.Fatalf("users: %d", len(users))
	}
	uuids := map[string]bool{}
	for _, u := range users {
		uuids[u.(map[string]any)["uuid"].(string)] = true
	}
	if !uuids["uuid-a"] || !uuids["uuid-b"] {
		t.Errorf("users uuids: %+v", uuids)
	}
}

func TestMergeBase_AppendsBackendOutbounds(t *testing.T) {
	merged, _ := MergeBase(baseConfigJSON(), mergeState())
	var got map[string]any
	_ = json.Unmarshal(merged, &got)
	outs := got["outbounds"].([]any)
	tags := make([]string, 0, len(outs))
	for _, o := range outs {
		tags = append(tags, o.(map[string]any)["tag"].(string))
	}
	if len(tags) != 4 {
		t.Fatalf("outbounds: %v", tags)
	}
	if tags[0] != "direct" || tags[1] != "block" {
		t.Errorf("base outbounds reorder broken: %v", tags)
	}
	if tags[2] != "backend-latvia-01" || tags[3] != "backend-praha-02" {
		t.Errorf("dynamic outbounds not appended in sorted order: %v", tags)
	}
}

func TestMergeBase_PrependsDynamicRoutePrependsKeepsBaseRules(t *testing.T) {
	merged, _ := MergeBase(baseConfigJSON(), mergeState())
	var got map[string]any
	_ = json.Unmarshal(merged, &got)
	rules := got["route"].(map[string]any)["rules"].([]any)
	if len(rules) != 3 {
		t.Fatalf("rules: %d", len(rules))
	}
	if rules[0].(map[string]any)["outbound"] != "backend-latvia-01" {
		t.Errorf("first dynamic rule: %+v", rules[0])
	}
	last := rules[len(rules)-1].(map[string]any)
	if protos, ok := last["protocol"].([]any); !ok || len(protos) == 0 || protos[0] != "dns" {
		t.Errorf("base dns rule should be kept at end: %+v", last)
	}
}

func TestMergeBase_BackendOutboundsAreplainVlessWithoutTLS(t *testing.T) {
	// Backend outbounds carry entry→backend traffic over wg-mesh.
	// Wg-mesh already provides authenticated encryption; layering Reality on top
	// adds no security but breaks sing-box when utls.fingerprint is missing.
	state := mergeState()
	for i := range state.Backends {
		state.Backends[i].Reality = RealitySpec{}
	}
	merged, err := MergeBase(baseConfigJSON(), state)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(merged, &got)
	outs := got["outbounds"].([]any)
	for _, o := range outs {
		m := o.(map[string]any)
		tag, _ := m["tag"].(string)
		if !strings.HasPrefix(tag, "backend-") {
			continue
		}
		if _, has := m["tls"]; has {
			t.Errorf("backend outbound %q must not carry tls when Reality is disabled: %+v", tag, m)
		}
	}
}

func TestMergeBase_IdempotentAcrossRebuilds(t *testing.T) {
	state := mergeState()
	once, _ := MergeBase(baseConfigJSON(), state)
	twice, _ := MergeBase(once, state)

	var a, b map[string]any
	_ = json.Unmarshal(once, &a)
	_ = json.Unmarshal(twice, &b)
	if len(a["outbounds"].([]any)) != len(b["outbounds"].([]any)) {
		t.Errorf("outbounds duplicated on re-merge: %d → %d", len(a["outbounds"].([]any)), len(b["outbounds"].([]any)))
	}
	if len(a["route"].(map[string]any)["rules"].([]any)) != len(b["route"].(map[string]any)["rules"].([]any)) {
		t.Errorf("route rules duplicated on re-merge")
	}
}

func TestMergeBase_RemovesStalePlacements(t *testing.T) {
	state := mergeState()
	merged, _ := MergeBase(baseConfigJSON(), state)

	state.Placements = state.Placements[:1]
	again, _ := MergeBase(merged, state)
	var got map[string]any
	_ = json.Unmarshal(again, &got)
	users := got["inbounds"].([]any)[0].(map[string]any)["users"].([]any)
	if len(users) != 1 {
		t.Errorf("user removal not reflected: %d users left", len(users))
	}
	rules := got["route"].(map[string]any)["rules"].([]any)
	for _, r := range rules {
		if out, ok := r.(map[string]any)["outbound"].(string); ok && out == "backend-latvia-01" {
			t.Error("stale rule for removed placement not cleaned")
		}
	}
}

func TestMergeBase_EmptyBaseUsesBuilder(t *testing.T) {
	out, err := MergeBase(nil, mergeState())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "backend-praha-02") {
		t.Error("nil base should fall through to Build()")
	}
}

func TestMergeBase_RejectsMissingInboundTag(t *testing.T) {
	state := mergeState()
	state.Inbound.Tag = "missing-tag"
	if _, err := MergeBase(baseConfigJSON(), state); err == nil {
		t.Fatal("expected error when base lacks matching inbound tag")
	}
}

func TestMergeBase_RejectsMalformedBase(t *testing.T) {
	if _, err := MergeBase([]byte("not json"), mergeState()); err == nil {
		t.Fatal("expected parse error")
	}
}
