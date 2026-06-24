package ports

import "context"

// EntryProxy is the runtime control surface of the entry VLESS+REALITY proxy.
// Users and per-user backend routing are mutated live, with no config reload.
type EntryProxy interface {
	AddUser(ctx context.Context, clientID, flow string) error
	RemoveUser(ctx context.Context, clientID string) error
	SelectBackend(ctx context.Context, clientID, backendID string) error
	SetUserBackends(ctx context.Context, clientID string, backendIDs []string) error
	SetBackends(ctx context.Context, backends []EntryBackend) error
	BackendConnections(ctx context.Context, backendID string) (uint64, error)
	ActiveConnections(ctx context.Context) ([]EntryConnection, error)
}

// EntryConnection is a single live connection with its cumulative byte counters.
type EntryConnection struct {
	ID        string `json:"id"`
	ClientID  string `json:"client_id"`
	BackendID string `json:"backend_id"`
	Upload    uint64 `json:"upload"`
	Download  uint64 `json:"download"`
}

// EntryBackend is a VLESS upstream reachable over the wg-mesh. The entry
// presents each user's own client_id to the backend (the backend has that user
// added), so a backend carries no credentials of its own — only where to dial.
type EntryBackend struct {
	ID      string `json:"id"`      // backend node id
	Address string `json:"address"` // wg-mesh host
	Port    uint16 `json:"port"`
}
