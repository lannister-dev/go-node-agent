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
	directOutboundTag        = "direct"
	blockOutboundTag         = "block"
	vlessType                = "vless"
	selectorType             = "selector"
	defaultClashAddr         = "127.0.0.1:9090"
	PerUserSelectorTagPrefix = "u-"
)

func perUserSelectorTagFor(clientID domain.ClientID) string {
	return PerUserSelectorTagPrefix + strings.ToLower(string(clientID))
}

func PerUserSelectorTagFor(clientID domain.ClientID) string {
	return perUserSelectorTagFor(clientID)
}

func PerUserOutboundTagFor(clientID domain.ClientID, backendID domain.BackendID) string {
	return perUserOutboundTagFor(clientID, backendID)
}

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
	backendByID := map[domain.BackendID]BackendSpec{}
	sortedBackends := make([]BackendSpec, 0, len(backends))
	for _, b := range backends {
		backendByID[b.ID] = b
		sortedBackends = append(sortedBackends, b)
	}
	sort.Slice(sortedBackends, func(i, j int) bool {
		return sortedBackends[i].ID < sortedBackends[j].ID
	})

	desiredBackendByUser := map[domain.ClientID]domain.BackendID{}
	users := make([]domain.ClientID, 0, len(placements))
	seenUser := map[domain.ClientID]bool{}
	for _, p := range placements {
		if p.Desired != domain.DesiredActive {
			continue
		}
		if p.ClientID == "" {
			continue
		}
		if _, ok := backendByID[p.BackendNodeID]; !ok {
			continue
		}
		if !seenUser[p.ClientID] {
			seenUser[p.ClientID] = true
			users = append(users, p.ClientID)
		}
		desiredBackendByUser[p.ClientID] = p.BackendNodeID
	}
	sort.Slice(users, func(i, j int) bool { return users[i] < users[j] })

	out := make([]outbound, 0, 2+len(users)*(len(sortedBackends)+1))
	out = append(out,
		outbound{Type: directOutboundTag, Tag: directOutboundTag},
		outbound{Type: blockOutboundTag, Tag: blockOutboundTag},
	)

	for _, uid := range users {
		for _, b := range sortedBackends {
			out = append(out, outbound{
				Type:       vlessType,
				Tag:        perUserOutboundTagFor(uid, b.ID),
				Server:     b.Address,
				ServerPort: b.Port,
				UUID:       string(uid),
				Flow:       "",
			})
		}
		options := make([]string, 0, len(sortedBackends)+1)
		for _, b := range sortedBackends {
			options = append(options, perUserOutboundTagFor(uid, b.ID))
		}
		options = append(options, blockOutboundTag)
		out = append(out, outbound{
			Type:                      selectorType,
			Tag:                       perUserSelectorTagFor(uid),
			Outbounds:                 options,
			Default:                   perUserOutboundTagFor(uid, desiredBackendByUser[uid]),
			InterruptExistConnections: false,
		})
	}
	return out
}

func buildRoute(placements []domain.Placement, backends []BackendSpec) routeConfig {
	backendByID := map[domain.BackendID]bool{}
	for _, b := range backends {
		backendByID[b.ID] = true
	}

	seen := map[domain.ClientID]bool{}
	users := make([]domain.ClientID, 0, len(placements))
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
		if seen[p.ClientID] {
			continue
		}
		seen[p.ClientID] = true
		users = append(users, p.ClientID)
	}
	sort.Slice(users, func(i, j int) bool { return users[i] < users[j] })

	rules := make([]routeRule, 0, len(users))
	for _, uid := range users {
		rules = append(rules, routeRule{
			AuthUser: []string{string(uid)},
			Outbound: perUserSelectorTagFor(uid),
		})
	}

	return routeConfig{
		Rules:               rules,
		AutoDetectInterface: true,
		Final:               directOutboundTag,
	}
}

// PerUserOutboundTagPrefix marks per-user backend outbounds rendered by the entry agent.
// Traffic publisher and drain reader rely on this prefix to attribute connections to a backend.
const PerUserOutboundTagPrefix = "b-"

// ParsePerUserOutboundTag extracts (clientID, backendID) from a per-user outbound tag.
// Returns ok=false if tag is not in the expected b-<client_uuid>-<backend_uuid> format.
func ParsePerUserOutboundTag(tag string) (clientID, backendID string, ok bool) {
	if !strings.HasPrefix(tag, PerUserOutboundTagPrefix) {
		return "", "", false
	}
	parts := strings.Split(tag, "-")
	if len(parts) != 11 {
		return "", "", false
	}
	return strings.Join(parts[1:6], "-"), strings.Join(parts[6:11], "-"), true
}

func perUserOutboundTagFor(clientID domain.ClientID, backendID domain.BackendID) string {
	return PerUserOutboundTagPrefix + strings.ToLower(string(clientID)) + "-" + strings.ToLower(string(backendID))
}

func coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
