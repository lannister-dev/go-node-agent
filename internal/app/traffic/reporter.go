package traffic

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	SingBoxAPIURL  string
	ReconnectDelay time.Duration
	HTTPTimeout    time.Duration
}

type Reporter struct {
	cfg  Config
	http *http.Client
	log  *slog.Logger

	upBytes     atomic.Uint64
	downBytes   atomic.Uint64
	events      atomic.Uint64
	disconnects atomic.Uint32
	lastAt      atomic.Int64
}

func New(cfg Config, log *slog.Logger) (*Reporter, error) {
	if cfg.SingBoxAPIURL == "" {
		return nil, errors.New("traffic: SingBoxAPIURL required")
	}
	if cfg.ReconnectDelay <= 0 {
		cfg.ReconnectDelay = 2 * time.Second
	}
	timeout := cfg.HTTPTimeout
	if timeout <= 0 {
		timeout = 0
	}
	if log == nil {
		log = slog.Default()
	}
	return &Reporter{
		cfg: cfg,
		http: &http.Client{
			Timeout: timeout,
		},
		log: log.With("component", "traffic"),
	}, nil
}

func (r *Reporter) UpBytes() uint64      { return r.upBytes.Load() }
func (r *Reporter) DownBytes() uint64    { return r.downBytes.Load() }
func (r *Reporter) Events() uint64       { return r.events.Load() }
func (r *Reporter) Disconnects() uint32  { return r.disconnects.Load() }
func (r *Reporter) LastEventUnix() int64 { return r.lastAt.Load() }

func (r *Reporter) Run(ctx context.Context) error {
	r.log.Info("traffic reporter started", "url", r.cfg.SingBoxAPIURL)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := r.stream(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.disconnects.Add(1)
			r.log.Warn("traffic stream closed, reconnecting", "err", err, "delay", r.cfg.ReconnectDelay)
			select {
			case <-time.After(r.cfg.ReconnectDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		return err
	}
}

type trafficEvent struct {
	Up   uint64 `json:"up"`
	Down uint64 `json:"down"`
}

func (r *Reporter) stream(ctx context.Context) error {
	url := strings.TrimRight(r.cfg.SingBoxAPIURL, "/") + "/traffic"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return fmt.Errorf("traffic: build request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := r.http.Do(req)
	if err != nil {
		return fmt.Errorf("traffic: GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("traffic: GET %s status=%d", url, resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("traffic: stream EOF")
			}
			return fmt.Errorf("traffic: read: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		payload := strings.TrimPrefix(line, "data:")
		payload = strings.TrimSpace(payload)
		if payload == "" {
			continue
		}
		var ev trafficEvent
		if jerr := json.Unmarshal([]byte(payload), &ev); jerr != nil {
			r.log.Debug("traffic: parse event failed", "line", line, "err", jerr)
			continue
		}
		r.upBytes.Store(ev.Up)
		r.downBytes.Store(ev.Down)
		r.events.Add(1)
		r.lastAt.Store(time.Now().UTC().Unix())
	}
}
