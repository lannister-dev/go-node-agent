package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type XrayUserManager interface {
	AddUser(ctx context.Context, user ports.XrayUser) error
	RemoveUser(ctx context.Context, clientID domain.ClientID) error
}

type BackendExecutor struct {
	xray  XrayUserManager
	store PlacementStore
	log   *slog.Logger
}

func NewBackendExecutor(xray XrayUserManager, store PlacementStore, log *slog.Logger) (*BackendExecutor, error) {
	if xray == nil || store == nil {
		return nil, errors.New("executor: xray and store required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &BackendExecutor{
		xray:  xray,
		store: store,
		log:   log.With("component", "backend-executor"),
	}, nil
}

func (e *BackendExecutor) Apply(ctx context.Context, desired domain.Placement, existing domain.Placement, found bool) (bool, error) {
	if desired.ID == "" || desired.ClientID == "" {
		return false, errors.New("backend-executor: placement ID and ClientID required")
	}

	nowActive := desired.Desired == domain.DesiredActive && !desired.IsRevoked
	wasActive := found &&
		existing.Desired == domain.DesiredActive &&
		!existing.IsRevoked &&
		existing.Applied == domain.AppliedOk

	switch {
	case nowActive && !wasActive:
		if err := e.xray.AddUser(ctx, ports.XrayUser{
			ClientID:  desired.ClientID,
			Transport: desired.Transport,
		}); err != nil {
			return classifyXrayError(err)
		}
		e.log.Info("xray user added",
			"placement_id", desired.ID,
			"client_id", desired.ClientID,
			"transport", desired.Transport,
		)
	case !nowActive && wasActive:
		if err := e.xray.RemoveUser(ctx, existing.ClientID); err != nil {
			return classifyXrayError(err)
		}
		e.log.Info("xray user removed",
			"placement_id", desired.ID,
			"client_id", existing.ClientID,
		)
	default:
		e.log.Debug("backend-executor no-op",
			"placement_id", desired.ID,
			"now_active", nowActive,
			"was_active", wasActive,
		)
	}

	toStore := desired
	toStore.Applied = domain.AppliedOk
	toStore.LastAppliedAt = time.Now().UTC()
	if err := e.store.PutPlacement(ctx, toStore); err != nil {
		return false, fmt.Errorf("backend-executor: persist: %w", err)
	}
	return false, nil
}

func classifyXrayError(err error) (retryable bool, _ error) {
	return false, err
}
