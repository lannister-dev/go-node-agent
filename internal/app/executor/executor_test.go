package executor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/app/flip"
	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type fakeSimpleApplier struct {
	mu         sync.Mutex
	calls      int
	gotDesired domain.Placement
	err        error
}

func (f *fakeSimpleApplier) SimpleApply(_ context.Context, desired domain.Placement) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.gotDesired = desired
	return f.err
}

type fakeFlipActions struct {
	mu            sync.Mutex
	calls         []string
	connRemaining uint64
	connErr       error
	warmErr       error
	swapErr       error
	coolErr       error
}

func (f *fakeFlipActions) ValidateBackend(_ context.Context, _ domain.FlipPlan) error {
	f.mu.Lock()
	f.calls = append(f.calls, "validate")
	f.mu.Unlock()
	return f.warmErr
}

func (f *fakeFlipActions) SwapRoute(_ context.Context, _ domain.FlipPlan) error {
	f.mu.Lock()
	f.calls = append(f.calls, "swap")
	f.mu.Unlock()
	return f.swapErr
}

func (f *fakeFlipActions) OldBackendConnections(_ context.Context, _ domain.FlipPlan) (uint64, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "drain")
	defer f.mu.Unlock()
	return f.connRemaining, f.connErr
}

func (f *fakeFlipActions) CoolOldBackend(_ context.Context, _ domain.FlipPlan) error {
	f.mu.Lock()
	f.calls = append(f.calls, "cool")
	f.mu.Unlock()
	return f.coolErr
}

func (f *fakeFlipActions) callOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

func silentLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func makePlacement(id, backend, client string, op uint64, applied domain.AppliedState, desired domain.DesiredState) domain.Placement {
	return domain.Placement{
		ID:            domain.PlacementID(id),
		ClientID:      domain.ClientID(client),
		BackendNodeID: domain.BackendID(backend),
		OpVersion:     domain.OpVersion(op),
		Applied:       applied,
		Desired:       desired,
		NodeID:        "lv-01",
	}
}

func newExecutor(t *testing.T, simple SimpleApplier, actions flip.Actions) *FlipExecutor {
	t.Helper()
	orch, err := flip.New(actions, silentLogger(), flip.Options{DrainPollInterval: 1 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	exec, err := NewFlipExecutor(simple, orch, FlipExecutorOptions{DrainTimeout: 50 * time.Millisecond}, silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	return exec
}

func TestFlipExecutor_FreshPlacementUsesSimple(t *testing.T) {
	simple := &fakeSimpleApplier{}
	actions := &fakeFlipActions{}
	exec := newExecutor(t, simple, actions)

	desired := makePlacement("p-1", "praha-02", "uuid-a", 1, domain.AppliedPending, domain.DesiredActive)
	_, err := exec.Apply(t.Context(), desired, domain.Placement{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if simple.calls != 1 {
		t.Errorf("simple calls: %d", simple.calls)
	}
	if len(actions.callOrder()) != 0 {
		t.Errorf("flip actions should not run for fresh: %v", actions.callOrder())
	}
}

func TestFlipExecutor_BackendChangeRunsFlip(t *testing.T) {
	simple := &fakeSimpleApplier{}
	actions := &fakeFlipActions{connRemaining: 0}
	exec := newExecutor(t, simple, actions)

	existing := makePlacement("p-1", "praha-02", "uuid-a", 1, domain.AppliedOk, domain.DesiredActive)
	desired := makePlacement("p-1", "latvia-01", "uuid-a", 2, domain.AppliedPending, domain.DesiredActive)

	_, err := exec.Apply(t.Context(), desired, existing, true)
	if err != nil {
		t.Fatal(err)
	}
	if simple.calls != 0 {
		t.Errorf("SimpleApply should not be called when flipping; got %d calls", simple.calls)
	}
	order := actions.callOrder()
	expected := []string{"validate", "swap", "drain", "cool"}
	if !sliceStartsWith(order, expected) {
		t.Errorf("flip order: %v, expected prefix %v", order, expected)
	}
}

func TestFlipExecutor_SameBackendActiveUsesSimple(t *testing.T) {
	simple := &fakeSimpleApplier{}
	actions := &fakeFlipActions{}
	exec := newExecutor(t, simple, actions)

	existing := makePlacement("p-1", "praha-02", "uuid-a", 1, domain.AppliedOk, domain.DesiredActive)
	desired := makePlacement("p-1", "praha-02", "uuid-a", 2, domain.AppliedPending, domain.DesiredActive)

	if _, err := exec.Apply(t.Context(), desired, existing, true); err != nil {
		t.Fatal(err)
	}
	if simple.calls != 1 {
		t.Errorf("expected simple apply for same-backend, got %d", simple.calls)
	}
	if len(actions.callOrder()) != 0 {
		t.Error("flip should not run for same backend")
	}
}

func TestFlipExecutor_DeactivateUsesSimple(t *testing.T) {
	simple := &fakeSimpleApplier{}
	actions := &fakeFlipActions{}
	exec := newExecutor(t, simple, actions)

	existing := makePlacement("p-1", "praha-02", "uuid-a", 1, domain.AppliedOk, domain.DesiredActive)
	desired := makePlacement("p-1", "praha-02", "uuid-a", 2, domain.AppliedPending, domain.DesiredInactive)

	if _, err := exec.Apply(t.Context(), desired, existing, true); err != nil {
		t.Fatal(err)
	}
	if simple.calls != 1 {
		t.Errorf("deactivate should use simple, got %d simple calls", simple.calls)
	}
	if len(actions.callOrder()) != 0 {
		t.Error("flip should not run for deactivate")
	}
}

func TestFlipExecutor_PreviousAppliedErrorBacksToSimple(t *testing.T) {
	simple := &fakeSimpleApplier{}
	actions := &fakeFlipActions{}
	exec := newExecutor(t, simple, actions)

	existing := makePlacement("p-1", "praha-02", "uuid-a", 1, domain.AppliedError, domain.DesiredActive)
	desired := makePlacement("p-1", "latvia-01", "uuid-a", 2, domain.AppliedPending, domain.DesiredActive)

	if _, err := exec.Apply(t.Context(), desired, existing, true); err != nil {
		t.Fatal(err)
	}
	if simple.calls != 1 {
		t.Error("if previous apply failed, should retry via simple path (no graceful flip)")
	}
}

func TestFlipExecutor_DrainTimeoutSwallowed(t *testing.T) {
	simple := &fakeSimpleApplier{}
	actions := &fakeFlipActions{connRemaining: 5}
	exec := newExecutor(t, simple, actions)

	existing := makePlacement("p-1", "praha-02", "uuid-a", 1, domain.AppliedOk, domain.DesiredActive)
	desired := makePlacement("p-1", "latvia-01", "uuid-a", 2, domain.AppliedPending, domain.DesiredActive)

	_, err := exec.Apply(t.Context(), desired, existing, true)
	if err != nil {
		t.Fatalf("drain timeout should be swallowed by FlipExecutor, got: %v", err)
	}
	order := actions.callOrder()
	if len(order) < 2 || order[0] != "validate" || order[1] != "swap" {
		t.Errorf("validate+swap should still run before drain: %v", order)
	}
}

func TestFlipExecutor_ValidateFailureSurfaces(t *testing.T) {
	simple := &fakeSimpleApplier{}
	actions := &fakeFlipActions{warmErr: errors.New("backend offline")}
	exec := newExecutor(t, simple, actions)

	existing := makePlacement("p-1", "praha-02", "uuid-a", 1, domain.AppliedOk, domain.DesiredActive)
	desired := makePlacement("p-1", "latvia-01", "uuid-a", 2, domain.AppliedPending, domain.DesiredActive)

	if _, err := exec.Apply(t.Context(), desired, existing, true); err == nil {
		t.Fatal("expected error from validate failure")
	}
}

func TestFlipExecutor_SimpleErrorSurfaces(t *testing.T) {
	simple := &fakeSimpleApplier{err: errors.New("singbox unreachable")}
	exec := newExecutor(t, simple, &fakeFlipActions{})

	desired := makePlacement("p-1", "praha-02", "uuid-a", 1, domain.AppliedPending, domain.DesiredActive)
	if _, err := exec.Apply(t.Context(), desired, domain.Placement{}, false); err == nil {
		t.Fatal("expected simple error to surface")
	}
}

func TestNewFlipExecutor_Validates(t *testing.T) {
	orch, _ := flip.New(&fakeFlipActions{}, silentLogger(), flip.Options{})
	if _, err := NewFlipExecutor(nil, orch, FlipExecutorOptions{}, silentLogger()); err == nil {
		t.Error("nil simple should error")
	}
	if _, err := NewFlipExecutor(&fakeSimpleApplier{}, nil, FlipExecutorOptions{}, silentLogger()); err == nil {
		t.Error("nil orch should error")
	}
}

func sliceStartsWith(s, prefix []string) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i, v := range prefix {
		if s[i] != v {
			return false
		}
	}
	return true
}
