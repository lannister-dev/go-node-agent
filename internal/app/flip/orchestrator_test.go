package flip

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

type fakeClock struct {
	mu   sync.Mutex
	now  time.Time
	hops int
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.hops++
	ch := make(chan time.Time, 1)
	ch <- c.now
	c.mu.Unlock()
	return ch
}

type fakeActions struct {
	mu sync.Mutex

	order []string

	warmErr        error
	swapErr        error
	coolErr        error
	connectionsErr error

	connectionsScript []uint64
	connectionsIndex  int
	defaultRemaining  uint64

	connQueries atomic.Int32
}

func (f *fakeActions) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.order = append(f.order, name)
}

func (f *fakeActions) WarmBackend(_ context.Context, _ domain.FlipPlan) error {
	f.record("warm")
	return f.warmErr
}

func (f *fakeActions) SwapRoute(_ context.Context, _ domain.FlipPlan) error {
	f.record("swap")
	return f.swapErr
}

func (f *fakeActions) OldBackendConnections(_ context.Context, _ domain.FlipPlan) (uint64, error) {
	f.connQueries.Add(1)
	if f.connectionsErr != nil {
		return 0, f.connectionsErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.connectionsIndex < len(f.connectionsScript) {
		v := f.connectionsScript[f.connectionsIndex]
		f.connectionsIndex++
		return v, nil
	}
	return f.defaultRemaining, nil
}

func (f *fakeActions) CoolOldBackend(_ context.Context, _ domain.FlipPlan) error {
	f.record("cool")
	return f.coolErr
}

func (f *fakeActions) callOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.order))
	copy(out, f.order)
	return out
}

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newPlan() domain.FlipPlan {
	return domain.FlipPlan{
		PlacementID:  "p-1",
		OldBackend:   "praha-02",
		NewBackend:   "latvia-01",
		DrainTimeout: 30 * time.Second,
	}
}

func newOrch(t *testing.T, actions *fakeActions) (*Orchestrator, *fakeClock) {
	t.Helper()
	clk := newFakeClock(time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC))
	o, err := New(actions, silent(), Options{DrainPollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return o.withClock(clk), clk
}

func TestOrchestrator_HappyPath(t *testing.T) {
	actions := &fakeActions{connectionsScript: []uint64{3, 1, 0}}
	o, _ := newOrch(t, actions)
	plan, err := o.Execute(t.Context(), newPlan())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if plan.State != domain.FlipSteady {
		t.Errorf("final state: %s", plan.State)
	}
	want := []string{"warm", "swap", "cool"}
	if got := actions.callOrder(); !equalSlices(got, want) {
		t.Errorf("order: got %v, want %v", got, want)
	}
	if actions.connQueries.Load() < 3 {
		t.Errorf("expected >= 3 connection queries, got %d", actions.connQueries.Load())
	}
}

func TestOrchestrator_DrainTimeoutForceCloses(t *testing.T) {
	actions := &fakeActions{defaultRemaining: 5}
	o, _ := newOrch(t, actions)
	plan := newPlan()
	plan.DrainTimeout = 50 * time.Millisecond
	_, err := o.Execute(t.Context(), plan)
	if !errors.Is(err, domain.ErrDrainTimeout) {
		t.Fatalf("expected ErrDrainTimeout, got %v", err)
	}
	got := actions.callOrder()
	if len(got) != 2 || got[0] != "warm" || got[1] != "swap" {
		t.Errorf("only warm+swap should run before timeout, got %v", got)
	}
	if containsStr(got, "cool") {
		t.Error("cool should not run after drain timeout")
	}
}

func TestOrchestrator_WarmFailureAborts(t *testing.T) {
	actions := &fakeActions{warmErr: errors.New("xray add user failed")}
	o, _ := newOrch(t, actions)
	_, err := o.Execute(t.Context(), newPlan())
	if err == nil {
		t.Fatal("expected error")
	}
	got := actions.callOrder()
	if len(got) != 1 || got[0] != "warm" {
		t.Errorf("expected warm only, got %v", got)
	}
}

func TestOrchestrator_SwapFailureAbortsBeforeDrain(t *testing.T) {
	actions := &fakeActions{swapErr: errors.New("sing-box reload failed")}
	o, _ := newOrch(t, actions)
	_, err := o.Execute(t.Context(), newPlan())
	if err == nil {
		t.Fatal("expected error")
	}
	got := actions.callOrder()
	if len(got) != 2 || got[1] != "swap" {
		t.Errorf("expected warm+swap, got %v", got)
	}
	if actions.connQueries.Load() != 0 {
		t.Error("drain should not poll on swap failure")
	}
}

func TestOrchestrator_CoolFailureSurfacesError(t *testing.T) {
	actions := &fakeActions{
		connectionsScript: []uint64{0},
		coolErr:           errors.New("xray remove user failed"),
	}
	o, _ := newOrch(t, actions)
	_, err := o.Execute(t.Context(), newPlan())
	if err == nil {
		t.Fatal("expected error")
	}
	got := actions.callOrder()
	want := []string{"warm", "swap", "cool"}
	if !equalSlices(got, want) {
		t.Errorf("order: %v", got)
	}
}

func TestOrchestrator_ConnectionsQueryErrorAborts(t *testing.T) {
	actions := &fakeActions{connectionsErr: errors.New("sing-box api down")}
	o, _ := newOrch(t, actions)
	_, err := o.Execute(t.Context(), newPlan())
	if err == nil {
		t.Fatal("expected error")
	}
	if containsStr(actions.callOrder(), "cool") {
		t.Error("cool should not run after drain query failure")
	}
}

func TestOrchestrator_ContextCancelDuringDrain(t *testing.T) {
	actions := &fakeActions{defaultRemaining: 5}
	o, err := New(actions, silent(), Options{DrainPollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := o.Execute(ctx, newPlan())
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("execute did not return after cancel")
	}
}

func TestOrchestrator_RejectsBadPlans(t *testing.T) {
	o, _ := newOrch(t, &fakeActions{})
	cases := map[string]domain.FlipPlan{
		"missing placement_id": {OldBackend: "a", NewBackend: "b", DrainTimeout: time.Second},
		"same backends":        {PlacementID: "p", OldBackend: "a", NewBackend: "a", DrainTimeout: time.Second},
		"zero drain timeout":   {PlacementID: "p", OldBackend: "a", NewBackend: "b"},
	}
	for name, plan := range cases {
		if _, err := o.Execute(t.Context(), plan); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestNew_RejectsNilActions(t *testing.T) {
	if _, err := New(nil, silent(), Options{}); err == nil {
		t.Fatal("expected error")
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsStr(s []string, want string) bool {
	for _, v := range s {
		if v == want {
			return true
		}
	}
	return false
}
