package controlapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

const (
	pathInitial    = "/api/agent/initial"
	maxBodyBytes   = 1 << 20
	headerNodeKey  = "X-Node-Key"
	headerAgentID  = "X-Agent-Instance-ID"
	headerNodeRole = "X-Node-Role"
)

func (c *Client) Initial(ctx context.Context, req ports.InitialRequest) (ports.InitialResponse, error) {
	if req.BootstrapToken == "" {
		return ports.InitialResponse{}, &NonRetryableError{Underlying: errors.New("BootstrapToken required")}
	}
	if req.NodeKey == "" {
		return ports.InitialResponse{}, &NonRetryableError{Underlying: errors.New("NodeKey required")}
	}
	if req.AgentInstanceID == "" {
		return ports.InitialResponse{}, &NonRetryableError{Underlying: errors.New("AgentInstanceID required")}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(pathInitial), http.NoBody)
	if err != nil {
		return ports.InitialResponse{}, &NonRetryableError{Underlying: fmt.Errorf("build request: %w", err)}
	}
	httpReq.Header.Set("Authorization", "Bearer "+req.BootstrapToken)
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", c.userAgent)
	httpReq.Header.Set(headerNodeKey, req.NodeKey)
	httpReq.Header.Set(headerAgentID, req.AgentInstanceID)
	if req.NodeRole != "" {
		httpReq.Header.Set(headerNodeRole, req.NodeRole)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return ports.InitialResponse{}, ctx.Err()
		}
		return ports.InitialResponse{}, &RetryableError{Underlying: fmt.Errorf("http do: %w", err)}
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if readErr != nil {
		return ports.InitialResponse{}, &RetryableError{Status: resp.StatusCode, Underlying: fmt.Errorf("read body: %w", readErr)}
	}

	switch {
	case resp.StatusCode >= 500:
		return ports.InitialResponse{}, &RetryableError{Status: resp.StatusCode, Underlying: errors.New("server error"), Body: string(body)}
	case resp.StatusCode >= 400:
		return ports.InitialResponse{}, &NonRetryableError{Status: resp.StatusCode, Underlying: errors.New("client error"), Body: string(body)}
	case resp.StatusCode >= 300:
		return ports.InitialResponse{}, &NonRetryableError{Status: resp.StatusCode, Underlying: errors.New("unexpected redirect"), Body: string(body)}
	}

	var dto initialResponseDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return ports.InitialResponse{}, &NonRetryableError{Status: resp.StatusCode, Underlying: fmt.Errorf("decode body: %w", err), Body: string(body)}
	}
	if dto.NodeID == "" || dto.NodeAuthToken == "" || dto.AgentInstanceID == "" {
		return ports.InitialResponse{}, &NonRetryableError{Status: resp.StatusCode, Underlying: errors.New("response missing required fields"), Body: string(body)}
	}

	return ports.InitialResponse{
		Identity: domain.NodeIdentity{
			NodeID:          domain.NodeID(dto.NodeID),
			AgentInstanceID: dto.AgentInstanceID,
			AuthToken:       dto.NodeAuthToken,
			BootstrappedAt:  time.Now().UTC(),
		},
		FullResyncRequired: dto.FullResyncRequired,
	}, nil
}
