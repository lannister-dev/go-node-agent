package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type renderReq struct {
	overlay *domain.Placement
	done    chan error
}

type CoalescerOptions struct {
	Debounce time.Duration
	MaxBatch int
	Buffer   int
}

type RenderCoalescer struct {
	a        *EntryActions
	submit   chan renderReq
	debounce time.Duration
	maxBatch int
	log      *slog.Logger
}

func NewRenderCoalescer(a *EntryActions, opts CoalescerOptions, log *slog.Logger) (*RenderCoalescer, error) {
	if a == nil {
		return nil, errors.New("coalescer: EntryActions required")
	}
	debounce := opts.Debounce
	if debounce <= 0 {
		debounce = 20 * time.Millisecond
	}
	maxBatch := opts.MaxBatch
	if maxBatch <= 0 {
		maxBatch = 256
	}
	buf := opts.Buffer
	if buf <= 0 {
		buf = 256
	}
	if log == nil {
		log = slog.Default()
	}
	return &RenderCoalescer{
		a:        a,
		submit:   make(chan renderReq, buf),
		debounce: debounce,
		maxBatch: maxBatch,
		log:      log.With("component", "render-coalescer"),
	}, nil
}

func (c *RenderCoalescer) Apply(ctx context.Context, overlay *domain.Placement) error {
	done := make(chan error, 1)
	req := renderReq{overlay: overlay, done: done}
	select {
	case c.submit <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *RenderCoalescer) Run(ctx context.Context) error {
	pending := make([]renderReq, 0, c.maxBatch)
	overlays := map[domain.PlacementID]domain.Placement{}
	var flushChan <-chan time.Time
	var timer *time.Timer

	resetBatch := func() {
		pending = pending[:0]
		overlays = map[domain.PlacementID]domain.Placement{}
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		flushChan = nil
	}

	doFlush := func() {
		if len(pending) == 0 {
			return
		}
		err := c.flushBatch(ctx, overlays)
		if err != nil {
			c.log.Warn("flush failed", "pending", len(pending), "err", err)
		} else {
			c.log.Debug("flush ok", "batch", len(pending), "overlays", len(overlays))
		}
		for _, req := range pending {
			req.done <- err
			close(req.done)
		}
		resetBatch()
	}

	for {
		select {
		case req := <-c.submit:
			pending = append(pending, req)
			if req.overlay != nil {
				overlays[req.overlay.ID] = *req.overlay
			}
			if len(pending) >= c.maxBatch {
				doFlush()
			} else if timer == nil {
				timer = time.NewTimer(c.debounce)
				flushChan = timer.C
			}
		case <-flushChan:
			doFlush()
		case <-ctx.Done():
			for _, req := range pending {
				req.done <- ctx.Err()
				close(req.done)
			}
			return ctx.Err()
		}
	}
}

func (c *RenderCoalescer) flushBatch(ctx context.Context, overlays map[domain.PlacementID]domain.Placement) error {
	a := c.a
	placements, err := a.store.ListPlacements(ctx)
	if err != nil {
		return fmt.Errorf("coalescer: list placements: %w", err)
	}
	for _, p := range overlays {
		placements = applyPlacementOverlay(placements, p)
	}
	state := singboxgen.NodeState{
		Log:        a.logCfg,
		ClashAPI:   a.clashCfg,
		Inbound:    a.inbound,
		Backends:   a.backends.All(),
		Placements: placements,
	}
	data, err := a.renderFromState(state)
	if err != nil {
		return fmt.Errorf("coalescer: render: %w", err)
	}
	if err := a.singbox.WriteConfig(ctx, ports.SingBoxConfig{Raw: data}); err != nil {
		return fmt.Errorf("coalescer: write config: %w", err)
	}
	if err := a.singbox.Reload(ctx); err != nil {
		return fmt.Errorf("coalescer: reload: %w", err)
	}
	for _, p := range overlays {
		if perr := a.store.PutPlacement(ctx, p); perr != nil {
			return fmt.Errorf("coalescer: persist %s: %w", p.ID, perr)
		}
	}
	return nil
}
