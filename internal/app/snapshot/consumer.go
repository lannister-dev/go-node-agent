package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/jsonv1"
)

const snapshotApplyConcurrency = 8

type ConsumerConfig struct {
	NodeID            domain.NodeID
	ChunkSubject      string
	SyncReportSubject string
	Durable           string
}

type Consumer struct {
	cfg       ConsumerConfig
	sub       Subscriber
	pub       Publisher
	store     PlacementStore
	rebuilder Rebuilder
	ids       IDGenerator
	log       *slog.Logger

	receivedChunks atomic.Uint32
	syncedItems    atomic.Uint32
	completed      atomic.Bool

	seenMu   sync.Mutex
	seenIDs  map[domain.PlacementID]struct{}
	seenSnap string
}

func NewConsumer(cfg ConsumerConfig, sub Subscriber, pub Publisher, store PlacementStore, rebuilder Rebuilder, ids IDGenerator, log *slog.Logger) (*Consumer, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("snapshot: NodeID required")
	}
	if cfg.ChunkSubject == "" || cfg.SyncReportSubject == "" || cfg.Durable == "" {
		return nil, errors.New("snapshot: ChunkSubject, SyncReportSubject, Durable required")
	}
	if sub == nil || pub == nil || store == nil || ids == nil {
		return nil, errors.New("snapshot: sub, pub, store, ids required")
	}
	if rebuilder == nil {
		rebuilder = NoopRebuilder{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		cfg:       cfg,
		sub:       sub,
		pub:       pub,
		store:     store,
		rebuilder: rebuilder,
		ids:       ids,
		log:       log.With("component", "snapshot"),
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	unsub, err := c.sub.Subscribe(ctx, c.cfg.ChunkSubject, c.cfg.Durable, c.Handle)
	if err != nil {
		return fmt.Errorf("snapshot: subscribe %s: %w", c.cfg.ChunkSubject, err)
	}
	c.log.Info("snapshot consumer subscribed",
		"subject", c.cfg.ChunkSubject,
		"durable", c.cfg.Durable)
	defer func() { _ = unsub() }()
	<-ctx.Done()
	return ctx.Err()
}

func (c *Consumer) Stats() (chunks, items uint32, completed bool) {
	return c.receivedChunks.Load(), c.syncedItems.Load(), c.completed.Load()
}

func (c *Consumer) Handle(ctx context.Context, msg ports.Msg) error {
	chunk, err := jsonv1.UnmarshalSnapshotChunkEvent(msg.Data)
	if err != nil {
		c.log.Warn("decode snapshot chunk failed", "err", err, "stream_seq", msg.Seq)
		return err
	}
	if chunk.NodeID != c.cfg.NodeID {
		c.log.Warn("snapshot chunk for different node, dropping",
			"got", chunk.NodeID, "self", c.cfg.NodeID)
		return nil
	}
	c.receivedChunks.Add(1)
	c.trackSeen(chunk.SnapshotID, chunk.Items)

	var persisted atomic.Uint32
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(snapshotApplyConcurrency)
	for _, cmd := range chunk.Items {
		g.Go(func() error {
			applied, err := c.applyItem(gctx, cmd)
			if err != nil {
				return fmt.Errorf("snapshot chunk %d item %s: %w",
					chunk.ChunkIndex, cmd.Placement.ID, err)
			}
			if applied {
				persisted.Add(1)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	c.syncedItems.Add(persisted.Load())

	c.log.Info("snapshot chunk processed",
		"chunk_index", chunk.ChunkIndex,
		"items", len(chunk.Items),
		"persisted", persisted.Load(),
		"is_last", chunk.IsLastChunk,
		"snapshot_id", chunk.SnapshotID,
	)

	if !chunk.IsLastChunk {
		return nil
	}

	pruned, err := c.pruneAbsent(ctx, chunk.SnapshotID)
	if err != nil {
		return fmt.Errorf("prune after last chunk: %w", err)
	}
	if pruned > 0 {
		c.log.Info("snapshot pruned stale placements",
			"snapshot_id", chunk.SnapshotID,
			"pruned", pruned,
		)
	}

	if err := c.rebuilder.RebuildFromStore(ctx); err != nil {
		return fmt.Errorf("rebuild after last chunk: %w", err)
	}
	if err := c.publishSyncReport(ctx, chunk.SnapshotID); err != nil {
		return fmt.Errorf("publish sync report: %w", err)
	}
	c.completed.Store(true)
	return nil
}

func (c *Consumer) trackSeen(snapshotID string, items []domain.PlacementCommand) {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()
	if c.seenSnap != snapshotID {
		c.seenIDs = map[domain.PlacementID]struct{}{}
		c.seenSnap = snapshotID
	}
	for _, cmd := range items {
		c.seenIDs[cmd.Placement.ID] = struct{}{}
	}
}

func (c *Consumer) pruneAbsent(ctx context.Context, snapshotID string) (int, error) {
	c.seenMu.Lock()
	seen := c.seenIDs
	currentSnap := c.seenSnap
	c.seenIDs = nil
	c.seenSnap = ""
	c.seenMu.Unlock()
	if currentSnap != snapshotID {
		return 0, nil
	}
	if len(seen) == 0 {
		c.log.Warn("snapshot brought zero items; skipping prune to preserve last-known-good state",
			"snapshot_id", snapshotID)
		return 0, nil
	}
	all, err := c.store.ListPlacements(ctx)
	if err != nil {
		return 0, err
	}
	pruned := 0
	for _, p := range all {
		if _, ok := seen[p.ID]; ok {
			continue
		}
		if err := c.store.DeletePlacement(ctx, p.ID); err != nil {
			return pruned, fmt.Errorf("delete absent %s: %w", p.ID, err)
		}
		pruned++
	}
	return pruned, nil
}

func (c *Consumer) applyItem(ctx context.Context, cmd domain.PlacementCommand) (bool, error) {
	existing, found, err := c.store.GetPlacement(ctx, cmd.Placement.ID)
	if err != nil {
		return false, err
	}
	if found && existing.OpVersion >= cmd.Placement.OpVersion {
		return false, nil
	}
	toStore := cmd.Placement
	toStore.Applied = domain.AppliedOk
	toStore.LastAppliedAt = time.Now().UTC()
	if err := c.store.PutPlacement(ctx, toStore); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Consumer) publishSyncReport(ctx context.Context, snapshotID string) error {
	report := jsonv1.SyncReport{
		EventID:             c.ids.NewID(),
		NodeID:              c.cfg.NodeID,
		EmittedAt:           time.Now().UTC(),
		SyncedCount:         c.syncedItems.Load(),
		FullResyncCompleted: true,
		InventoryHash:       snapshotID,
	}
	data, err := jsonv1.MarshalSyncReportEvent(report)
	if err != nil {
		return err
	}
	headers := map[string]string{
		"x-schema":  "jsonv1",
		"x-node-id": string(c.cfg.NodeID),
	}
	return c.pub.Publish(ctx, c.cfg.SyncReportSubject, headers, data)
}
