package applier

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/wire/jsonv1"
)

var applierTracer = otel.Tracer("agent.applier")

const applierWorkers = 8

type Config struct {
	NodeID         domain.NodeID
	CommandSubject string
	ResultSubject  string
	Durable        string
}

type Applier struct {
	cfg      Config
	sub      Subscriber
	pub      Publisher
	store    PlacementStore
	executor Executor
	ids      IDGenerator
	log      *slog.Logger

	receivedTotal atomic.Uint32
	appliedTotal  atomic.Uint32
	failedTotal   atomic.Uint32
}

func (a *Applier) Snapshot() (received, applied, failed uint32) {
	return a.receivedTotal.Load(), a.appliedTotal.Load(), a.failedTotal.Load()
}

func New(cfg Config, sub Subscriber, pub Publisher, store PlacementStore, executor Executor, ids IDGenerator, log *slog.Logger) (*Applier, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("applier: NodeID required")
	}
	if cfg.CommandSubject == "" || cfg.ResultSubject == "" || cfg.Durable == "" {
		return nil, errors.New("applier: CommandSubject, ResultSubject, Durable required")
	}
	if sub == nil || pub == nil || store == nil || ids == nil {
		return nil, errors.New("applier: sub, pub, store, ids required")
	}
	if executor == nil {
		executor = NoopExecutor{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Applier{
		cfg:      cfg,
		sub:      sub,
		pub:      pub,
		store:    store,
		executor: executor,
		ids:      ids,
		log:      log.With("component", "applier"),
	}, nil
}

func (a *Applier) Run(ctx context.Context) error {
	unsub, err := a.sub.Subscribe(ctx, a.cfg.CommandSubject, a.cfg.Durable, a.Handle,
		ports.WithConcurrency(applierWorkers, placementShardKey),
	)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	a.log.Info("applier subscribed",
		"subject", a.cfg.CommandSubject,
		"durable", a.cfg.Durable,
		"workers", applierWorkers,
	)
	defer func() { _ = unsub() }()
	<-ctx.Done()
	return ctx.Err()
}

func placementShardKey(msg ports.Msg) uint64 {
	id, err := jsonv1.PlacementIDFromCommandEvent(msg.Data)
	if err != nil || id == "" {
		return msg.Seq
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return h.Sum64()
}

func (a *Applier) Handle(ctx context.Context, msg ports.Msg) error {
	ctx, span := applierTracer.Start(ctx, "applier.handle")
	defer span.End()

	cmd, err := jsonv1.UnmarshalPlacementCommandEvent(msg.Data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode")
		a.log.Warn("decode command failed", "err", err, "stream_seq", msg.Seq)
		return err
	}
	span.SetAttributes(
		attribute.String("placement_id", string(cmd.Placement.ID)),
		attribute.String("event_id", cmd.EventID),
		attribute.String("op_version", strconv.FormatUint(uint64(cmd.Placement.OpVersion), 10)),
		attribute.String("desired_state", string(cmd.Placement.Desired)),
		attribute.String("backend_node_id", string(cmd.Placement.BackendNodeID)),
	)
	if cmd.NodeID != a.cfg.NodeID {
		span.SetAttributes(attribute.Bool("other_node", true))
		a.log.Warn("command for different node, dropping",
			"got", cmd.NodeID, "self", a.cfg.NodeID, "event_id", cmd.EventID)
		return nil
	}
	a.receivedTotal.Add(1)

	existing, found, err := a.store.GetPlacement(ctx, cmd.Placement.ID)
	if err != nil {
		return fmt.Errorf("load placement: %w", err)
	}

	if found && existing.OpVersion > cmd.Placement.OpVersion {
		a.log.Debug("skipped stale",
			"placement_id", cmd.Placement.ID,
			"current_op", existing.OpVersion,
			"event_op", cmd.Placement.OpVersion)
		return a.publishReport(ctx, cmd, existing.Applied, domain.ReportSkippedStale, "")
	}
	if found && existing.OpVersion == cmd.Placement.OpVersion &&
		existing.Applied == domain.AppliedOk &&
		existing.Desired == cmd.Placement.Desired &&
		existing.BackendNodeID == cmd.Placement.BackendNodeID {
		a.log.Debug("skipped idempotent",
			"placement_id", cmd.Placement.ID, "op_version", cmd.Placement.OpVersion)
		return a.publishReport(ctx, cmd, existing.Applied, domain.ReportSkippedIdempotent, "")
	}

	retryable, applyErr := a.executor.Apply(ctx, cmd.Placement, existing, found)
	if applyErr != nil {
		if retryable {
			a.log.Warn("apply retryable error",
				"placement_id", cmd.Placement.ID, "err", applyErr)
			return applyErr
		}
		toStore := cmd.Placement
		toStore.Applied = domain.AppliedError
		toStore.LastAppliedAt = time.Now().UTC()
		if perr := a.store.PutPlacement(ctx, toStore); perr != nil {
			a.log.Error("persist error-state failed",
				"placement_id", cmd.Placement.ID, "err", perr)
		}
		a.failedTotal.Add(1)
		return a.publishReport(ctx, cmd, domain.AppliedError, domain.ReportError, applyErr.Error())
	}

	toStore := cmd.Placement
	toStore.Applied = domain.AppliedOk
	toStore.LastAppliedAt = time.Now().UTC()
	if err := a.store.PutPlacement(ctx, toStore); err != nil {
		return fmt.Errorf("persist: %w", err)
	}
	a.appliedTotal.Add(1)
	return a.publishReport(ctx, cmd, domain.AppliedOk, domain.ReportApplied, "")
}

func (a *Applier) publishReport(ctx context.Context, cmd domain.PlacementCommand, state domain.AppliedState, status domain.ReportStatus, errStr string) error {
	report := domain.PlacementReport{
		PlacementID:  cmd.Placement.ID,
		NodeID:       a.cfg.NodeID,
		OpVersion:    cmd.Placement.OpVersion,
		AppliedState: state,
		Status:       status,
		Err:          errStr,
		AppliedAt:    time.Now().UTC(),
	}
	data, err := jsonv1.MarshalApplyResultEvent(report, a.ids.NewID(), report.AppliedAt)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	headers := map[string]string{
		"x-schema":  "jsonv1",
		"x-node-id": string(a.cfg.NodeID),
	}
	if err := a.pub.Publish(ctx, a.cfg.ResultSubject, headers, data); err != nil {
		return fmt.Errorf("publish result: %w", err)
	}
	return nil
}
