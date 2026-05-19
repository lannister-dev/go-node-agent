package reconcile

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

type fakeRebuilder struct {
	calls atomic.Int32
	err   error
}

func (f *fakeRebuilder) RebuildFromStore(context.Context) error {
	f.calls.Add(1)
	return f.err
}

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestReconciler_RunsOnInterval(t *testing.T) {
	rb := &fakeRebuilder{}
	r, err := New(Config{Interval: 20 * time.Millisecond}, rb, silent())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(120 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", err)
	}
	if rb.calls.Load() < 3 {
		t.Errorf("expected >= 3 reconcile ticks in 120ms @ 20ms, got %d", rb.calls.Load())
	}
	runs, failures, lastAt := r.Snapshot()
	if runs == 0 || lastAt == 0 {
		t.Errorf("stats: runs=%d lastAt=%d", runs, lastAt)
	}
	if failures != 0 {
		t.Errorf("unexpected failures: %d", failures)
	}
}

func TestReconciler_ContinuesAfterFailure(t *testing.T) {
	rb := &fakeRebuilder{err: errors.New("singbox down")}
	r, err := New(Config{Interval: 15 * time.Millisecond}, rb, silent())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = r.Run(ctx) }()

	time.Sleep(60 * time.Millisecond)
	cancel()
	time.Sleep(5 * time.Millisecond)

	runs, failures, _ := r.Snapshot()
	if runs < 2 {
		t.Errorf("expected >= 2 runs, got %d", runs)
	}
	if failures < 2 {
		t.Errorf("expected >= 2 failures, got %d", failures)
	}
}

func TestNew_Validates(t *testing.T) {
	if _, err := New(Config{}, &fakeRebuilder{}, silent()); err == nil {
		t.Error("zero Interval should error")
	}
	if _, err := New(Config{Interval: time.Second}, nil, silent()); err == nil {
		t.Error("nil Rebuilder should error")
	}
}
