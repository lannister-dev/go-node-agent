package singboxgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

const (
	directOutboundTag = "direct"
	blockOutboundTag  = "block"
	vlessType         = "vless"
	defaultClashAddr  = "127.0.0.1:9090"
)

func Build(state NodeState) ([]byte, error) {
	if err := validate(state); err != nil {
		return nil, err
	}
	cfg := singBoxConfig{
		Log:       buildLog(state.Log),
		Inbounds:  []inbound{buildInbound(state.Inbound, state.Placements)},
		Outbounds: buildOutbounds(state.Placements, state.Backends),
		Route:     buildRoute(state.Placements, state.Backends),
	}
	if state.ClashAPI.Enabled {
		cfg.Experimental = &experimental{ClashAPI: buildClashAPI(state.ClashAPI)}
	}

	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("singboxgen: marshal: %w", err)
	}
	return out, nil
}

func validate(state NodeState) error {
	if state.Inbound.Tag == "" {
		return errors.New("singboxgen: inbound.tag required")
	}
	if state.Inbound.Listen.Port == 0 {
		return errors.New("singboxgen: inbound.listen.port required")
	}
	seen := map[domain.BackendID]bool{}
	for _, b := range state.Backends {
		if b.ID == "" {
			return errors.New("singboxgen: backend id required")
		}
		if b.Address == "" || b.Port == 0 {
			return fmt.Errorf("singboxgen: backend %s requires address+port", b.ID)
		}
		if seen[b.ID] {
			return fmt.Errorf("singboxgen: duplicate backend id %q", b.ID)
		}
		seen[b.ID] = true
	}
	for _, p := range state.Placements {
		if p.Desired != domain.DesiredActive {
			continue
		}
		if p.ClientID == "" {
			return fmt.Errorf("singboxgen: placement %s missing client_id", p.ID)
		}
		if !seen[p.BackendNodeID] {
			return fmt.Errorf("singboxgen: placement %s references unknown backend %q", p.ID, p.BackendNodeID)
		}
	}
	return nil
}

func buildLog(l LogSpec) *logConfig {
	if l.Level == "" && !l.Disabled {
		return nil
	}
	return &logConfig{Level: l.Level, Disabled: l.Disabled}
}

func buildClashAPI(c ClashAPISpec) clashAPIConfig {
	addr := c.ExternalCt
	if addr == "" {
		addr = defaultClashAddr
	}
	return clashAPIConfig{
		ExternalController: addr,
		Secret:             c.Secret,
	}
}

func buildInbound(spec InboundSpec, placements []domain.Placement) inbound {
	in := inbound{
		Type:       vlessType,
		Tag:        spec.Tag,
		Listen:     coalesce(spec.Listen.Address, "::"),
		ListenPort: spec.Listen.Port,
		Sniff:      spec.Listen.Sniff,
		Users:      collectActiveUsers(placements),
	}
	if spec.Reality.Enabled {
		in.TLS = buildTLS(spec.Reality)
	}
	return in
}

func collectActiveUsers(placements []domain.Placement) []vlessUser {
	seen := map[domain.ClientID]bool{}
	out := make([]vlessUser, 0, len(placements))
	for _, p := range placements {
		if p.Desired != domain.DesiredActive {
			continue
		}
		if p.ClientID == "" || seen[p.ClientID] {
			continue
		}
		seen[p.ClientID] = true
		out = append(out, vlessUser{
			Name: string(p.ClientID),
			UUID: string(p.ClientID),
			Flow: flowForTransport(p.Transport),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UUID < out[j].UUID })
	return out
}

func flowForTransport(t domain.TransportKind) string {
	if t == domain.TransportReality {
		return "xtls-rprx-vision"
	}
	return ""
}

func buildTLS(r RealitySpec) *tlsConfig {
	return &tlsConfig{
		Enabled:    true,
		ServerName: r.ServerName,
		Reality: &realityConfig{
			Enabled:    true,
			Handshake:  handshake{Server: r.Handshake, ServerPort: 443},
			PrivateKey: "",
			ShortID:    append([]string{}, r.ShortIDs...),
		},
	}
}

func buildOutbounds(placements []domain.Placement, backends []BackendSpec) []outbound {
	out := make([]outbound, 0, 2+len(placements))
	out = append(out,
		outbound{Type: directOutboundTag, Tag: directOutboundTag},
		outbound{Type: blockOutboundTag, Tag: blockOutboundTag},
	)
	backendByID := map[domain.BackendID]BackendSpec{}
	for _, b := range backends {
		backendByID[b.ID] = b
	}
	type pair struct {
		ClientID  domain.ClientID
		BackendID domain.BackendID
	}
	seen := map[pair]bool{}
	pairs := make([]pair, 0, len(placements))
	for _, p := range placements {
		if p.Desired != domain.DesiredActive {
			continue
		}
		if _, ok := backendByID[p.BackendNodeID]; !ok {
			continue
		}
		if p.ClientID == "" {
			continue
		}
		k := pair{ClientID: p.ClientID, BackendID: p.BackendNodeID}
		if seen[k] {
			continue
		}
		seen[k] = true
		pairs = append(pairs, k)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].BackendID != pairs[j].BackendID {
			return pairs[i].BackendID < pairs[j].BackendID
		}
		return pairs[i].ClientID < pairs[j].ClientID
	})
	for _, p := range pairs {
		b := backendByID[p.BackendID]
		out = append(out, outbound{
			Type:       vlessType,
			Tag:        perUserOutboundTagFor(p.ClientID, p.BackendID),
			Server:     b.Address,
			ServerPort: b.Port,
			UUID:       string(p.ClientID),
			Flow:       "",
		})
	}
	return out
}

func buildRoute(placements []domain.Placement, backends []BackendSpec) routeConfig {
	backendByID := map[domain.BackendID]bool{}
	for _, b := range backends {
		backendByID[b.ID] = true
	}

	type pair struct {
		ClientID  domain.ClientID
		BackendID domain.BackendID
	}
	seen := map[pair]bool{}
	pairs := make([]pair, 0, len(placements))
	for _, p := range placements {
		if p.Desired != domain.DesiredActive {
			continue
		}
		if !backendByID[p.BackendNodeID] {
			continue
		}
		if p.ClientID == "" {
			continue
		}
		k := pair{ClientID: p.ClientID, BackendID: p.BackendNodeID}
		if seen[k] {
			continue
		}
		seen[k] = true
		pairs = append(pairs, k)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].BackendID != pairs[j].BackendID {
			return pairs[i].BackendID < pairs[j].BackendID
		}
		return pairs[i].ClientID < pairs[j].ClientID
	})

	rules := make([]routeRule, 0, len(pairs))
	for _, p := range pairs {
		rules = append(rules, routeRule{
			AuthUser: []string{string(p.ClientID)},
			Outbound: perUserOutboundTagFor(p.ClientID, p.BackendID),
		})
	}

	return routeConfig{
		Rules:               rules,
		AutoDetectInterface: true,
		Final:               directOutboundTag,
	}
}

func outboundTagFor(id domain.BackendID) string {
	return OutboundTagFor(id)
}

func OutboundTagFor(id domain.BackendID) string {
	return "backend-" + strings.ToLower(string(id))
}

func perUserOutboundTagFor(clientID domain.ClientID, backendID domain.BackendID) string {
	return "b-" + strings.ToLower(string(clientID)) + "-" + strings.ToLower(string(backendID))
}

func coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func dedupSorted(s []string) []string {
	if len(s) <= 1 {
		return s
	}
	out := s[:1]
	for _, v := range s[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}
