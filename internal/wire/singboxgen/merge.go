package singboxgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

const dynamicTagPrefix = "backend-"

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
		entry := map[string]any{"uuid": u.UUID}
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
			m["users"] = jsonUsers
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
		if strings.HasPrefix(tag, dynamicTagPrefix) {
			continue
		}
		filtered = append(filtered, o)
	}

	sorted := append([]BackendSpec{}, state.Backends...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	for _, b := range sorted {
		entry := map[string]any{
			"type":        vlessType,
			"tag":         outboundTagFor(b.ID),
			"server":      b.Address,
			"server_port": float64(b.Port),
		}
		if b.Reality.Enabled {
			entry["tls"] = map[string]any{
				"enabled":     true,
				"server_name": b.Reality.ServerName,
				"reality": map[string]any{
					"enabled": true,
				},
			}
		}
		filtered = append(filtered, entry)
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
		if outbound, ok := rm["outbound"].(string); ok && strings.HasPrefix(outbound, dynamicTagPrefix) {
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

	usersByBackend := map[domain.BackendID][]string{}
	for _, p := range state.Placements {
		if p.Desired != domain.DesiredActive {
			continue
		}
		if !backendByID[p.BackendNodeID] {
			continue
		}
		usersByBackend[p.BackendNodeID] = append(usersByBackend[p.BackendNodeID], string(p.ClientID))
	}

	bids := make([]domain.BackendID, 0, len(usersByBackend))
	for id := range usersByBackend {
		bids = append(bids, id)
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i] < bids[j] })

	rules := make([]map[string]any, 0, len(bids))
	for _, id := range bids {
		users := usersByBackend[id]
		sort.Strings(users)
		users = dedupSorted(users)
		userVals := make([]any, len(users))
		for i, u := range users {
			userVals[i] = u
		}
		rules = append(rules, map[string]any{
			"user":     userVals,
			"outbound": outboundTagFor(id),
		})
	}
	return rules
}
