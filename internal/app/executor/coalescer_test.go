package executor

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func TestCoalescer_CollapsesBurstIntoOneFlush(t *testing.T) {
	sb := &fakeSingBox{}
	store := newMemStore(samplePlacement())
	a := newActions(t, sb, store, backendsRegistry())
	coal, err := NewRenderCoalescer(a, CoalescerOptions{Debounce: 30 * time.Millisecond, MaxBatch: 256}, silent())
	if err != nil {
		t.Fatal(err)
	}
	a.AttachCoalescer(coal)
	ctx := t.Context()
	go func() { _ = coal.Run(ctx) }()

	var wg sync.WaitGroup
	for i := range 50 {
		p := samplePlacement()
		p.ID = domain.PlacementID("p-" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
		p.ClientID = domain.ClientID("uuid-" + string(rune('a'+i%26)))
		p.BackendNodeID = "latvia-01"
		p.OpVersion = 99
		wg.Add(1)
		go func(pl domain.Placement) {
			defer wg.Done()
			_ = coal.Apply(ctx, &pl)
		}(p)
	}
	wg.Wait()

	reloads := sb.reloadCount()
	if reloads >= 50 {
		t.Errorf("expected coalesced reloads, got %d (=submit count, no batching)", reloads)
	}
	if reloads == 0 {
		t.Error("expected at least one reload")
	}
}

func TestCoalescer_SurfacesFlushError(t *testing.T) {
	sb := &fakeSingBox{reloadErr: errors.New("singbox down")}
	store := newMemStore(samplePlacement())
	a := newActions(t, sb, store, backendsRegistry())
	coal, err := NewRenderCoalescer(a, CoalescerOptions{Debounce: 10 * time.Millisecond}, silent())
	if err != nil {
		t.Fatal(err)
	}
	a.AttachCoalescer(coal)
	ctx := t.Context()
	go func() { _ = coal.Run(ctx) }()

	p := samplePlacement()
	p.BackendNodeID = "latvia-01"
	p.OpVersion = 99
	if err := coal.Apply(ctx, &p); err == nil {
		t.Fatal("expected reload error to surface to submitter")
	}
}

func TestCoalescer_NilOverlayTriggersRender(t *testing.T) {
	sb := &fakeSingBox{}
	store := newMemStore(samplePlacement())
	a := newActions(t, sb, store, backendsRegistry())
	coal, err := NewRenderCoalescer(a, CoalescerOptions{Debounce: 5 * time.Millisecond}, silent())
	if err != nil {
		t.Fatal(err)
	}
	a.AttachCoalescer(coal)
	ctx := t.Context()
	go func() { _ = coal.Run(ctx) }()

	if err := coal.Apply(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if sb.reloadCount() != 1 {
		t.Errorf("expected 1 reload, got %d", sb.reloadCount())
	}
}
