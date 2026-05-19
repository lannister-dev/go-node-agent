package singbox

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

func (c *Client) Healthy(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/version"), http.NoBody)
	if err != nil {
		return fmt.Errorf("singbox: build version request: %w", err)
	}
	c.authorize(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("singbox: version request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("singbox: version status=%d", resp.StatusCode)
	}
	return nil
}

func (c *Client) Check(ctx context.Context) error { return c.Healthy(ctx) }
