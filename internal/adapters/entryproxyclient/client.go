// Package entryproxyclient is the agent-side adapter to the embedded entry
// proxy. It implements ports.EntryProxy over the unix-socket control API and
// deliberately imports neither the proxy nor sing-box, keeping the agent binary
// free of that dependency tree.
package entryproxyclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/entryproxy/api"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type Client struct {
	http *http.Client
}

func New(socketPath string) *Client {
	return &Client{
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
				},
			},
		},
	}
}

func (c *Client) AddUser(ctx context.Context, clientID, flow string) error {
	return c.post(ctx, api.PathAddUser, api.AddUserRequest{ClientID: clientID, Flow: flow}, nil)
}

func (c *Client) RemoveUser(ctx context.Context, clientID string) error {
	return c.post(ctx, api.PathRemoveUser, api.RemoveUserRequest{ClientID: clientID}, nil)
}

func (c *Client) SelectBackend(ctx context.Context, clientID, backendID string) error {
	return c.post(ctx, api.PathSelectBackend, api.SelectBackendRequest{ClientID: clientID, BackendID: backendID}, nil)
}

func (c *Client) SetBackends(ctx context.Context, backends []ports.EntryBackend) error {
	return c.post(ctx, api.PathSetBackends, api.SetBackendsRequest{Backends: backends}, nil)
}

func (c *Client) BackendConnections(ctx context.Context, backendID string) (uint64, error) {
	var resp api.BackendConnectionsResponse
	if err := c.post(ctx, api.PathBackendConnections, api.BackendConnectionsRequest{BackendID: backendID}, &resp); err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (c *Client) ActiveConnections(ctx context.Context) ([]ports.EntryConnection, error) {
	var resp api.ActiveConnectionsResponse
	if err := c.post(ctx, api.PathActiveConnections, struct{}{}, &resp); err != nil {
		return nil, err
	}
	return resp.Conns, nil
}

// Connections implements traffic.ConnectionsSource: each live connection maps to
// one sing-box connection with a canonical per-user backend chain, so the
// existing StatsReporter (counts) and Publisher (byte deltas keyed by ID) both
// work unchanged.
func (c *Client) Connections(ctx context.Context) (ports.SingBoxConnections, error) {
	conns, err := c.ActiveConnections(ctx)
	if err != nil {
		return ports.SingBoxConnections{}, err
	}
	snap := ports.SingBoxConnections{Conns: make([]ports.SingBoxConn, 0, len(conns))}
	for _, ec := range conns {
		snap.Conns = append(snap.Conns, ports.SingBoxConn{
			ID:       ec.ID,
			Chains:   []string{singboxgen.PerUserOutboundTagFor(domain.ClientID(ec.ClientID), domain.BackendID(ec.BackendID))},
			Upload:   ec.Upload,
			Download: ec.Download,
		})
	}
	snap.Total = uint64(len(snap.Conns))
	return snap, nil
}

func (c *Client) Epoch(ctx context.Context) (int64, error) {
	var resp api.StatusResponse
	if err := c.post(ctx, api.PathStatus, struct{}{}, &resp); err != nil {
		return 0, err
	}
	return resp.Epoch, nil
}

func (c *Client) Name() string { return "entry-proxy" }

func (c *Client) Check(ctx context.Context) error {
	_, err := c.Epoch(ctx)
	return err
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix"+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		var e api.ErrorResponse
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Error == "" {
			e.Error = resp.Status
		}
		return fmt.Errorf("entryproxy control %s: %s", path, e.Error)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

var _ ports.EntryProxy = (*Client)(nil)
