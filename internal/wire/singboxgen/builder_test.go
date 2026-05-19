package singboxgen

import (
	"encoding/json"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func sampleState() NodeState {
	return NodeState{
		Log: LogSpec{Level: "info"},
		ClashAPI: ClashAPISpec{
			Enabled:    true,
			ExternalCt: "127.0.0.1:9090",
			Secret:     "s3cret",
		},
		Inbound: InboundSpec{
			Tag:    "vless-in",
			Listen: ListenSpec{Address: "::", Port: 443, Sniff: true},
			Reality: RealitySpec{
				Enabled:    true,
				ServerName: "www.cloudflare.com",
				ShortIDs:   []string{"abc123"},
				Handshake:  "www.cloudflare.com",
			},
		},
		Backends: []BackendSpec{
			{ID: "praha-02", Address: "10.0.0.2", Port: 9000, Transport: domain.TransportReality},
			{ID: "latvia-01", Address: "10.0.0.1", Port: 9000, Transport: domain.TransportWS},
		},
		Placements: []domain.Placement{
			{ID: "p-1", ClientID: "uuid-a", BackendNodeID: "praha-02", Desired: domain.DesiredActive, Transport: domain.TransportReality},
			{ID: "p-2", ClientID: "uuid-b", BackendNodeID: "latvia-01", Desired: domain.DesiredActive, Transport: domain.TransportWS},
			{ID: "p-3", ClientID: "uuid-c", BackendNodeID: "praha-02", Desired: domain.DesiredInactive},
		},
	}
}

func parseConfig(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, data)
	}
	return got
}

func TestBuild_HappyPath(t *testing.T) {
	data, err := Build(sampleState())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	got := parseConfig(t, data)

	inbounds := got["inbounds"].([]any)
	if len(inbounds) != 1 {
		t.Fatalf("inbounds: %d", len(inbounds))
	}
	in := inbounds[0].(map[string]any)
	if in["tag"] != "vless-in" || in["type"] != "vless" {
		t.Errorf("inbound shape: %+v", in)
	}
	if in["listen_port"].(float64) != 443 {
		t.Errorf("port: %v", in["listen_port"])
	}
	users := in["users"].([]any)
	if len(users) != 2 {
		t.Fatalf("users: %d (inactive should be excluded)", len(users))
	}
	if users[0].(map[string]any)["uuid"] != "uuid-a" {
		t.Errorf("users sort order broken: %+v", users)
	}
	if users[0].(map[string]any)["flow"] != "xtls-rprx-vision" {
		t.Errorf("reality user should have vision flow")
	}
	if _, has := users[1].(map[string]any)["flow"]; has {
		t.Errorf("ws user should have no flow key (omitempty)")
	}
}

func TestBuild_OutboundsIncludeDirectBlockAndBackends(t *testing.T) {
	data, _ := Build(sampleState())
	got := parseConfig(t, data)
	outs := got["outbounds"].([]any)

	tags := make([]string, 0, len(outs))
	for _, o := range outs {
		tags = append(tags, o.(map[string]any)["tag"].(string))
	}
	want := []string{"direct", "block", "backend-latvia-01", "backend-praha-02"}
	if len(tags) != len(want) {
		t.Fatalf("outbounds: got %v, want %v", tags, want)
	}
	for i := range want {
		if tags[i] != want[i] {
			t.Errorf("outbound %d: got %q, want %q", i, tags[i], want[i])
		}
	}
}

func TestBuild_RouteRulesGroupActiveUsersByBackend(t *testing.T) {
	data, _ := Build(sampleState())
	got := parseConfig(t, data)
	route := got["route"].(map[string]any)
	rules := route["rules"].([]any)
	if len(rules) != 2 {
		t.Fatalf("rules: %d", len(rules))
	}
	want := map[string][]string{
		"backend-latvia-01": {"uuid-b"},
		"backend-praha-02":  {"uuid-a"},
	}
	for _, r := range rules {
		m := r.(map[string]any)
		tag := m["outbound"].(string)
		usersAny := m["user"].([]any)
		gotUsers := make([]string, len(usersAny))
		for i, u := range usersAny {
			gotUsers[i] = u.(string)
		}
		expect, ok := want[tag]
		if !ok {
			t.Errorf("unexpected route outbound: %s", tag)
			continue
		}
		if len(expect) != len(gotUsers) {
			t.Errorf("%s: users %v != %v", tag, gotUsers, expect)
			continue
		}
		for i := range expect {
			if expect[i] != gotUsers[i] {
				t.Errorf("%s: user[%d]=%s, want %s", tag, i, gotUsers[i], expect[i])
			}
		}
	}
	if route["final"] != "direct" {
		t.Errorf("final: %v", route["final"])
	}
}

func TestBuild_ClashAPIIncluded(t *testing.T) {
	data, _ := Build(sampleState())
	got := parseConfig(t, data)
	exp, ok := got["experimental"].(map[string]any)
	if !ok {
		t.Fatal("experimental missing")
	}
	clash := exp["clash_api"].(map[string]any)
	if clash["external_controller"] != "127.0.0.1:9090" {
		t.Errorf("clash addr: %v", clash["external_controller"])
	}
	if clash["secret"] != "s3cret" {
		t.Errorf("clash secret: %v", clash["secret"])
	}
}

func TestBuild_ClashAPIOmittedWhenDisabled(t *testing.T) {
	state := sampleState()
	state.ClashAPI.Enabled = false
	data, _ := Build(state)
	got := parseConfig(t, data)
	if _, has := got["experimental"]; has {
		t.Error("experimental should be omitted when ClashAPI disabled")
	}
}

func TestBuild_InactivePlacementsExcluded(t *testing.T) {
	state := sampleState()
	for i := range state.Placements {
		state.Placements[i].Desired = domain.DesiredInactive
	}
	data, _ := Build(state)
	got := parseConfig(t, data)
	users := got["inbounds"].([]any)[0].(map[string]any)["users"].([]any)
	if len(users) != 0 {
		t.Errorf("expected empty users (all inactive), got %d", len(users))
	}
	rules := got["route"].(map[string]any)["rules"].([]any)
	if len(rules) != 0 {
		t.Errorf("expected no rules (all inactive), got %d", len(rules))
	}
}

func TestBuild_DedupClientIDs(t *testing.T) {
	state := sampleState()
	state.Placements = append(state.Placements, domain.Placement{
		ID: "p-dup", ClientID: "uuid-a", BackendNodeID: "praha-02",
		Desired: domain.DesiredActive, Transport: domain.TransportReality,
	})
	data, _ := Build(state)
	got := parseConfig(t, data)
	users := got["inbounds"].([]any)[0].(map[string]any)["users"].([]any)
	uuids := map[string]int{}
	for _, u := range users {
		uuids[u.(map[string]any)["uuid"].(string)]++
	}
	for uuid, n := range uuids {
		if n > 1 {
			t.Errorf("uuid %s appears %d times", uuid, n)
		}
	}
}

func TestBuild_RejectsPlacementReferencingUnknownBackend(t *testing.T) {
	state := sampleState()
	state.Placements = append(state.Placements, domain.Placement{
		ID: "p-x", ClientID: "uuid-x", BackendNodeID: "ghost", Desired: domain.DesiredActive,
	})
	if _, err := Build(state); err == nil {
		t.Fatal("expected error for unknown backend reference")
	}
}

func TestBuild_RejectsDuplicateBackendIDs(t *testing.T) {
	state := sampleState()
	state.Backends = append(state.Backends, BackendSpec{ID: "praha-02", Address: "1.1.1.1", Port: 9000})
	if _, err := Build(state); err == nil {
		t.Fatal("expected duplicate backend error")
	}
}

func TestBuild_RejectsEmptyTag(t *testing.T) {
	state := sampleState()
	state.Inbound.Tag = ""
	if _, err := Build(state); err == nil {
		t.Fatal("expected error for missing tag")
	}
}

func TestBuild_RejectsZeroPort(t *testing.T) {
	state := sampleState()
	state.Inbound.Listen.Port = 0
	if _, err := Build(state); err == nil {
		t.Fatal("expected error for zero port")
	}
}

func TestBuild_RealityTLSStructPresent(t *testing.T) {
	data, _ := Build(sampleState())
	got := parseConfig(t, data)
	in := got["inbounds"].([]any)[0].(map[string]any)
	tls, ok := in["tls"].(map[string]any)
	if !ok {
		t.Fatal("inbound.tls missing")
	}
	if tls["enabled"] != true || tls["server_name"] != "www.cloudflare.com" {
		t.Errorf("tls shape: %+v", tls)
	}
	reality, ok := tls["reality"].(map[string]any)
	if !ok {
		t.Fatal("reality missing")
	}
	if reality["enabled"] != true {
		t.Errorf("reality not enabled: %+v", reality)
	}
}

func TestBuild_DeterministicOutput(t *testing.T) {
	state := sampleState()
	a, _ := Build(state)
	b, _ := Build(state)
	if string(a) != string(b) {
		t.Error("output not deterministic across two builds")
	}
}
