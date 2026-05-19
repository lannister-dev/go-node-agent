package singbox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type connectionsResponse struct {
	DownloadTotal uint64       `json:"downloadTotal"`
	UploadTotal   uint64       `json:"uploadTotal"`
	Connections   []connection `json:"connections"`
}

type connection struct {
	ID     string   `json:"id"`
	Chains []string `json:"chains"`
}

func (c *Client) Connections(ctx context.Context) (ports.SingBoxConnections, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/connections"), http.NoBody)
	if err != nil {
		return ports.SingBoxConnections{}, fmt.Errorf("singbox: build connections request: %w", err)
	}
	c.authorize(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return ports.SingBoxConnections{}, fmt.Errorf("singbox: connections request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return ports.SingBoxConnections{}, fmt.Errorf("singbox: connections status=%d body=%s", resp.StatusCode, string(body))
	}
	var dto connectionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&dto); err != nil {
		return ports.SingBoxConnections{}, fmt.Errorf("singbox: decode connections: %w", err)
	}
	out := ports.SingBoxConnections{
		Total:       uint64(len(dto.Connections)),
		PerOutbound: map[string]uint64{},
	}
	for _, conn := range dto.Connections {
		outbound := lastOutbound(conn.Chains)
		if outbound == "" {
			outbound = "_unknown"
		}
		out.PerOutbound[outbound]++
	}
	return out, nil
}

func lastOutbound(chain []string) string {
	if len(chain) == 0 {
		return ""
	}
	return chain[len(chain)-1]
}
