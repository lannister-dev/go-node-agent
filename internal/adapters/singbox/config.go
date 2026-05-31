package singbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

func (c *Client) WriteConfig(ctx context.Context, cfg ports.SingBoxConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(cfg.Raw) == 0 {
		return errors.New("singbox: empty config")
	}
	dir := filepath.Dir(c.configPath)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("singbox: mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".sing-box-cfg-*.tmp")
	if err != nil {
		return fmt.Errorf("singbox: create tmp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, err := tmp.Write(cfg.Raw); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("singbox: write tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("singbox: fsync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("singbox: close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, c.configPath); err != nil {
		cleanup()
		return fmt.Errorf("singbox: rename to %s: %w", c.configPath, err)
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (c *Client) Reload(ctx context.Context) error {
	body, err := json.Marshal(struct {
		Path string `json:"path"`
	}{Path: c.configPath})
	if err != nil {
		return fmt.Errorf("singbox: marshal reload body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url("/configs")+"?force=true", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("singbox: build reload request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.authorize(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("singbox: reload request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("singbox: reload status=%d body=%s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (c *Client) SelectOutbound(ctx context.Context, selectorTag, target string) error {
	if selectorTag == "" || target == "" {
		return errors.New("singbox: selector and target required")
	}
	body, err := json.Marshal(struct {
		Name string `json:"name"`
	}{Name: target})
	if err != nil {
		return fmt.Errorf("singbox: marshal selector body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url("/proxies/"+selectorTag), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("singbox: build selector request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.authorize(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("singbox: select outbound request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("singbox: select outbound status=%d body=%s", resp.StatusCode, string(respBody))
	}
	return nil
}
