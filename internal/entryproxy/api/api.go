// Package api is the sing-box-free wire contract for the entry-proxy control
// plane (agent ↔ entry-proxy over a unix socket). Both the server (entry-proxy)
// and the client adapter (agent) depend on it; it must not import the embedded
// proxy, so the agent stays free of the sing-box dependency tree.
package api

import "github.com/lannister-dev/go-node-agent/internal/ports"

const (
	PathAddUser            = "/v1/add-user"
	PathRemoveUser         = "/v1/remove-user"
	PathSelectBackend      = "/v1/select-backend"
	PathSetUserBackends    = "/v1/set-user-backends"
	PathSetBackends        = "/v1/set-backends"
	PathBackendConnections = "/v1/backend-connections"
	PathActiveConnections  = "/v1/active-connections"
	PathStatus             = "/v1/status"
)

type AddUserRequest struct {
	ClientID string `json:"client_id"`
	Flow     string `json:"flow"`
}

type RemoveUserRequest struct {
	ClientID string `json:"client_id"`
}

type SelectBackendRequest struct {
	ClientID  string `json:"client_id"`
	BackendID string `json:"backend_id"`
}

type SetUserBackendsRequest struct {
	ClientID   string   `json:"client_id"`
	BackendIDs []string `json:"backend_ids"`
}

type SetBackendsRequest struct {
	Backends []ports.EntryBackend `json:"backends"`
}

type BackendConnectionsRequest struct {
	BackendID string `json:"backend_id"`
}

type BackendConnectionsResponse struct {
	Count uint64 `json:"count"`
}

type ActiveConnectionsResponse struct {
	Conns []ports.EntryConnection `json:"conns"`
}

type StatusResponse struct {
	Epoch int64 `json:"epoch"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
