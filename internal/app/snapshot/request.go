package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/wire/jsonv1"
)

type RequesterConfig struct {
	NodeID         domain.NodeID
	RequestSubject string
}

type Requester struct {
	cfg RequesterConfig
	pub Publisher
	log *slog.Logger
}

func NewRequester(cfg RequesterConfig, pub Publisher, log *slog.Logger) (*Requester, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("snapshot: NodeID required")
	}
	if cfg.RequestSubject == "" {
		return nil, errors.New("snapshot: RequestSubject required")
	}
	if pub == nil {
		return nil, errors.New("snapshot: Publisher required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Requester{cfg: cfg, pub: pub, log: log.With("component", "snapshot-requester")}, nil
}

func (r *Requester) Request(ctx context.Context, reason string) error {
	req := jsonv1.SnapshotRequest{
		NodeID:      r.cfg.NodeID,
		RequestedAt: time.Now().UTC(),
		Reason:      reason,
	}
	data, err := jsonv1.MarshalSnapshotRequestEvent(req)
	if err != nil {
		return fmt.Errorf("snapshot: marshal request: %w", err)
	}
	headers := map[string]string{
		"x-schema":  "jsonv1",
		"x-node-id": string(r.cfg.NodeID),
	}
	if err := r.pub.Publish(ctx, r.cfg.RequestSubject, headers, data); err != nil {
		return fmt.Errorf("snapshot: publish request: %w", err)
	}
	r.log.Info("snapshot requested", "reason", reason)
	return nil
}
