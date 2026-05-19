package controlapi

type initialResponseDTO struct {
	NodeID             string `json:"node_id"`
	NodeAuthToken      string `json:"node_auth_token"`
	AgentInstanceID    string `json:"agent_instance_id"`
	FullResyncRequired bool   `json:"full_resync_required"`
}
