package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

// EntryProxyActions drives the embedded entry proxy (ports.EntryProxy) instead
// of rendering + reloading sing-box. Users and per-user routing are applied as
// live API calls, so a new user never triggers a reload. It satisfies the same
// flip.Actions + SimpleApplier + Rebuilder surface as EntryActions.
type EntryProxyActions struct {
	proxy    ports.EntryProxy
	store    PlacementStore
	backends BackendLookup
	log      *slog.Logger
	pending  atomic.Bool
	users    atomic.Int64
}

func (a *EntryProxyActions) HasPending() bool { return a.pending.Load() }

func (a *EntryProxyActions) Users() int { return int(a.users.Load()) }

func NewEntryProxyActions(proxy ports.EntryProxy, store PlacementStore, backends BackendLookup, log *slog.Logger) (*EntryProxyActions, error) {
	if proxy == nil || store == nil || backends == nil {
		return nil, errors.New("executor: proxy/store/backends required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &EntryProxyActions{
		proxy:    proxy,
		store:    store,
		backends: backends,
		log:      log.With("component", "entry-proxy-actions"),
	}, nil
}

func (a *EntryProxyActions) ValidateBackend(_ context.Context, plan domain.FlipPlan) error {
	if _, ok := a.backends.Get(plan.NewBackend); !ok {
		return fmt.Errorf("validate: new backend %s not in registry", plan.NewBackend)
	}
	return nil
}

func (a *EntryProxyActions) SwapRoute(ctx context.Context, plan domain.FlipPlan) error {
	if plan.Desired.ID == "" || plan.Desired.ID != plan.PlacementID {
		return fmt.Errorf("swap: bad plan placement id %q/%q", plan.Desired.ID, plan.PlacementID)
	}
	current, _, err := a.store.GetPlacement(ctx, plan.PlacementID)
	if err != nil {
		return fmt.Errorf("swap: load current placement: %w", err)
	}
	if plan.OpVersion != 0 && current.OpVersion >= plan.OpVersion {
		a.log.Warn("swap: stale op_version, skipping", "placement_id", plan.PlacementID)
		return nil
	}
	desired := plan.Desired
	desired.Applied = domain.AppliedOk
	desired.LastAppliedAt = time.Now().UTC()
	if err := a.SyncBackends(ctx); err != nil {
		return err
	}
	if err := a.proxy.SelectBackend(ctx, string(desired.ClientID), string(desired.BackendNodeID)); err != nil {
		return fmt.Errorf("swap: select backend: %w", err)
	}
	if err := a.store.PutPlacement(ctx, desired); err != nil {
		return fmt.Errorf("swap: persist placement: %w", err)
	}
	a.log.Info("route swapped", "placement_id", plan.PlacementID, "old", plan.OldBackend, "new", plan.NewBackend)
	return nil
}

func (a *EntryProxyActions) OldBackendConnections(ctx context.Context, plan domain.FlipPlan) (uint64, error) {
	return a.proxy.BackendConnections(ctx, string(plan.OldBackend))
}

func (a *EntryProxyActions) OldBackendReachable(ctx context.Context, plan domain.FlipPlan) bool {
	b, ok := a.backends.Get(plan.OldBackend)
	if !ok {
		return false
	}
	dctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dctx, "tcp", net.JoinHostPort(b.Address, strconv.Itoa(int(b.Port))))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// CoolOldBackend is a no-op: there are no per-user outbounds to drop. The
// backend pool is pruned when the registry next syncs via SetBackends.
func (a *EntryProxyActions) CoolOldBackend(context.Context, domain.FlipPlan) error {
	return nil
}

func (a *EntryProxyActions) SimpleApply(ctx context.Context, desired domain.Placement) error {
	if desired.ID == "" {
		return errors.New("simple-apply: placement ID required")
	}
	current, _, err := a.store.GetPlacement(ctx, desired.ID)
	if err != nil {
		return fmt.Errorf("simple-apply: load current placement: %w", err)
	}
	if desired.OpVersion != 0 && current.OpVersion >= desired.OpVersion {
		a.log.Warn("simple-apply: stale op_version, skipping", "placement_id", desired.ID)
		return nil
	}
	desired.Applied = domain.AppliedOk
	desired.LastAppliedAt = time.Now().UTC()

	if desired.Desired == domain.DesiredActive && !desired.IsRevoked {
		if err := a.SyncBackends(ctx); err != nil {
			return err
		}
		if err := a.proxy.AddUser(ctx, string(desired.ClientID), singboxgen.FlowForTransport(desired.Transport)); err != nil {
			return fmt.Errorf("simple-apply: add user: %w", err)
		}
		if _, ok := a.backends.Get(desired.BackendNodeID); ok {
			if err := a.proxy.SelectBackend(ctx, string(desired.ClientID), string(desired.BackendNodeID)); err != nil {
				return fmt.Errorf("simple-apply: select backend: %w", err)
			}
		} else {
			a.pending.Store(true)
			a.log.Warn("simple-apply: backend not in registry, route deferred", "client_id", desired.ClientID, "backend", desired.BackendNodeID)
		}
	} else {
		if err := a.proxy.RemoveUser(ctx, string(desired.ClientID)); err != nil {
			return fmt.Errorf("simple-apply: remove user: %w", err)
		}
	}
	if err := a.store.PutPlacement(ctx, desired); err != nil {
		return fmt.Errorf("simple-apply: persist placement: %w", err)
	}
	a.log.Debug("simple-apply complete", "placement_id", desired.ID, "desired_state", desired.Desired, "backend", desired.BackendNodeID)
	return nil
}

// RebuildFromStore pushes the full desired state to the proxy. The proxy holds
// no state across restarts, so this re-establishes the backend pool, the user
// set, and per-user routing from the store + registry.
func (a *EntryProxyActions) RebuildFromStore(ctx context.Context) error {
	if err := a.SyncBackends(ctx); err != nil {
		return err
	}
	placements, err := a.store.ListPlacements(ctx)
	if err != nil {
		return fmt.Errorf("rebuild: list placements: %w", err)
	}
	byUser := map[domain.ClientID][]domain.Placement{}
	for _, p := range placements {
		if p.Desired != domain.DesiredActive || p.IsRevoked || p.ClientID == "" {
			continue
		}
		byUser[p.ClientID] = append(byUser[p.ClientID], p)
	}
	pending := false
	for clientID, cands := range byUser {
		if err := a.proxy.AddUser(ctx, string(clientID), singboxgen.FlowForTransport(cands[0].Transport)); err != nil {
			return fmt.Errorf("rebuild: add user %s: %w", clientID, err)
		}
		ids := a.eligibleBackendIDs(cands)
		if len(ids) == 0 {
			pending = true
			a.log.Warn("rebuild: no known backend for user yet, route deferred", "client_id", clientID)
			continue
		}
		if err := a.proxy.SetUserBackends(ctx, string(clientID), ids); err != nil {
			return fmt.Errorf("rebuild: set user backends for %s: %w", clientID, err)
		}
	}
	a.pending.Store(pending)
	a.users.Store(int64(len(byUser)))
	if len(byUser) == 0 {
		a.log.Warn("entry proxy rebuilt with zero users — store empty or all placements filtered; node will keep requesting snapshots")
	} else {
		a.log.Info("entry proxy rebuilt from store", "users", len(byUser), "pending", pending)
	}
	return nil
}

func (a *EntryProxyActions) eligibleBackendIDs(cands []domain.Placement) []string {
	ids := make([]string, 0, len(cands))
	seen := make(map[domain.BackendID]struct{}, len(cands))
	for i := range cands {
		bid := cands[i].BackendNodeID
		if _, dup := seen[bid]; dup {
			continue
		}
		if _, ok := a.backends.Get(bid); !ok {
			continue
		}
		seen[bid] = struct{}{}
		ids = append(ids, string(bid))
	}
	return ids
}

// SyncBackends pushes the current backend registry to the proxy pool.
func (a *EntryProxyActions) SyncBackends(ctx context.Context) error {
	specs := a.backends.All()
	out := make([]ports.EntryBackend, 0, len(specs))
	for _, b := range specs {
		out = append(out, ports.EntryBackend{ID: string(b.ID), Address: b.Address, Port: b.Port})
	}
	if err := a.proxy.SetBackends(ctx, out); err != nil {
		return fmt.Errorf("sync backends: %w", err)
	}
	return nil
}
