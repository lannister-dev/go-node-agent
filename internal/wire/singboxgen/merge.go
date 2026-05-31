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
	dynamicTagPrefix       = "b-"
	legacyDynamicTagPrefix = "backend-"
)

func isDynamicTag(tag string) bool {
	return strings.HasPrefix(tag, dynamicTagPrefix) || strings.HasPrefix(tag, legacyDynamicTagPrefix)
}

func MergeBase(base []byte, state NodeState) ([]byte, error) {
	if len(base) == 0 {
		return Build(state)
	}
	if err := validate(state); err != nil {
		return nil, err
	}

	var root map[string]any
	if err := json.Unmarshal(base, &root); err != nil {
		return nil, fmt.Errorf("singboxgen: parse base: %w", err)
	}

	if err := mergeInbounds(root, state); err != nil {
		return nil, err
	}
	mergeOutbounds(root, state)
	mergeRoute(root, state)

	return json.MarshalIndent(root, "", "  ")
}

func mergeInbounds(root map[string]any, state NodeState) error {
	raw, ok := root["inbounds"]
	if !ok {
		return errors.New("singboxgen: base missing inbounds[]")
	}
	inbounds, ok := raw.([]any)
	if !ok {
		return errors.New("singboxgen: base inbounds must be array")
	}

	users := collectActiveUsers(state.Placements)
	jsonUsers := make([]any, 0, len(users))
	for _, u := range users {
		entry := map[string]any{"name": u.UUID, "uuid": u.UUID}
		if u.Flow != "" {
			entry["flow"] = u.Flow
		}
		jsonUsers = append(jsonUsers, entry)
	}

	matched := false
	for i, inb := range inbounds {
		m, ok := inb.(map[string]any)
		if !ok {
			continue
		}
		if m["tag"] == state.Inbound.Tag {
			if len(jsonUsers) > 0 {
				m["users"] = jsonUsers
			}
			inbounds[i] = m
			matched = true
		}
	}
	if !matched {
		return fmt.Errorf("singboxgen: base has no inbound with tag %q", state.Inbound.Tag)
	}
	root["inbounds"] = inbounds
	return nil
}

func mergeOutbounds(root map[string]any, state NodeState) {
	rawOut, _ := root["outbounds"].([]any)
	filtered := make([]any, 0, len(rawOut))
	for _, o := range rawOut {
		om, ok := o.(map[string]any)
		if !ok {
			filtered = append(filtered, o)
			continue
		}
		tag, _ := om["tag"].(string)
		if isDynamicTag(tag) {
			continue
		}
		filtered = append(filtered, o)
	}

	backendByID := map[domain.BackendID]BackendSpec{}
	for _, b := range state.Backends {
		backendByID[b.ID] = b
	}
	type pair struct {
		ClientID  domain.ClientID
		BackendID domain.BackendID
	}
	seen := map[pair]bool{}
	pairs := make([]pair, 0, len(state.Placements))
	for _, p := range state.Placements {
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
		filtered = append(filtered, map[string]any{
			"type":        vlessType,
			"tag":         perUserOutboundTagFor(p.ClientID, p.BackendID),
			"server":      b.Address,
			"server_port": float64(b.Port),
			"uuid":        string(p.ClientID),
			"flow":        "",
		})
	}
	root["outbounds"] = filtered
}

func mergeRoute(root map[string]any, state NodeState) {
	route, _ := root["route"].(map[string]any)
	if route == nil {
		route = map[string]any{}
	}
	rawRules, _ := route["rules"].([]any)

	kept := make([]any, 0, len(rawRules))
	for _, r := range rawRules {
		rm, ok := r.(map[string]any)
		if !ok {
			kept = append(kept, r)
			continue
		}
		if outbound, ok := rm["outbound"].(string); ok && isDynamicTag(outbound) {
			continue
		}
		kept = append(kept, r)
	}

	dynamic := buildDynamicRouteRules(state)
	final := make([]any, 0, len(dynamic)+len(kept))
	for _, d := range dynamic {
		final = append(final, d)
	}
	final = append(final, kept...)
	route["rules"] = final
	root["route"] = route
}

func buildDynamicRouteRules(state NodeState) []map[string]any {
	backendByID := map[domain.BackendID]bool{}
	for _, b := range state.Backends {
		backendByID[b.ID] = true
	}

	type pair struct {
		ClientID  domain.ClientID
		BackendID domain.BackendID
	}
	seen := map[pair]bool{}
	pairs := make([]pair, 0, len(state.Placements))
	for _, p := range state.Placements {
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

	rules := make([]map[string]any, 0, len(pairs))
	for _, p := range pairs {
		rules = append(rules, map[string]any{
			"auth_user": []any{string(p.ClientID)},
			"outbound":  perUserOutboundTagFor(p.ClientID, p.BackendID),
		})
	}
	return rules
}
