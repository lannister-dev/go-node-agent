package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/lannister-dev/go-node-agent/internal/adapters/badger"
	"github.com/lannister-dev/go-node-agent/internal/adapters/controlapi"
	"github.com/lannister-dev/go-node-agent/internal/adapters/entryproxyclient"
	natsa "github.com/lannister-dev/go-node-agent/internal/adapters/nats"
	"github.com/lannister-dev/go-node-agent/internal/adapters/singbox"
	"github.com/lannister-dev/go-node-agent/internal/adapters/wg"
	"github.com/lannister-dev/go-node-agent/internal/adapters/xray"
	"github.com/lannister-dev/go-node-agent/internal/app/applier"
	"github.com/lannister-dev/go-node-agent/internal/app/backends"
	"github.com/lannister-dev/go-node-agent/internal/app/bootstrap"
	"github.com/lannister-dev/go-node-agent/internal/app/executor"
	"github.com/lannister-dev/go-node-agent/internal/app/flip"
	"github.com/lannister-dev/go-node-agent/internal/app/heartbeat"
	"github.com/lannister-dev/go-node-agent/internal/app/reconcile"
	"github.com/lannister-dev/go-node-agent/internal/app/snapshot"
	"github.com/lannister-dev/go-node-agent/internal/app/traffic"
	"github.com/lannister-dev/go-node-agent/internal/app/wgmesh"
	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/platform/config"
	"github.com/lannister-dev/go-node-agent/internal/platform/idgen"
	"github.com/lannister-dev/go-node-agent/internal/platform/logger"
	"github.com/lannister-dev/go-node-agent/internal/platform/telemetry"
	"github.com/lannister-dev/go-node-agent/internal/ports"
	"github.com/lannister-dev/go-node-agent/internal/server"
	"github.com/lannister-dev/go-node-agent/internal/wire"
	"github.com/lannister-dev/go-node-agent/internal/wire/jsonv1"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

var (
	version   = "dev"
	commit    = "none"
	buildTime = "unknown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("agent exited with error", "err", err)
		os.Exit(1)
	}
}

type entryStack struct {
	executor   applier.Executor
	listener   *backends.Listener
	actions    reconcile.Rebuilder
	singbox    *singbox.Client
	registry   *backends.Registry
	coalescer  *executor.RenderCoalescer
	entryProxy *entryproxyclient.Client
}

type backendStack struct {
	executor  applier.Executor
	xray      *xray.Client
	rebuilder *backendRebuilder
}

type backendRebuilder struct {
	xray  *xray.Client
	store *badger.Store
	log   *slog.Logger
}

const backendRebuildConcurrency = 10

func (r *backendRebuilder) RebuildFromStore(ctx context.Context) error {
	placements, err := r.store.ListPlacements(ctx)
	if err != nil {
		return fmt.Errorf("backend-rebuild: list placements: %w", err)
	}
	var added atomic.Int32
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(backendRebuildConcurrency)
	for _, p := range placements {
		if p.Desired != domain.DesiredActive || p.IsRevoked || p.ClientID == "" {
			continue
		}
		g.Go(func() error {
			if err := r.xray.AddUser(gctx, ports.XrayUser{
				ClientID:  p.ClientID,
				Transport: p.Transport,
			}); err != nil {
				r.log.Warn("backend-rebuild: AddUser failed",
					"placement_id", p.ID,
					"client_id", p.ClientID,
					"err", err,
				)
				return nil
			}
			added.Add(1)
			return nil
		})
	}
	_ = g.Wait()
	r.log.Info("backend-rebuild complete",
		"placements_total", len(placements),
		"users_added", added.Load(),
	)
	return nil
}

func pickRebuilder(es *entryStack, bs *backendStack) reconcile.Rebuilder {
	if es != nil && es.actions != nil {
		return es.actions
	}
	if bs != nil && bs.rebuilder != nil {
		return bs.rebuilder
	}
	return nil
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := logger.New(cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(log)

	log.Info("starting",
		"version", version,
		"commit", commit,
		"build_time", buildTime,
		"control_api", cfg.ControlAPIURL,
		"nats", cfg.NATSURL,
		"store_path", cfg.StorePath,
		"node_role", cfg.NodeRole,
		"executor_enabled", cfg.EnableExecutor,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	otelShutdown, err := telemetry.Setup(ctx, telemetry.Options{
		Endpoint:       cfg.OTLPEndpoint,
		Insecure:       cfg.OTLPInsecure,
		ServiceName:    "go-node-agent",
		ServiceVersion: version,
		NodeID:         cfg.NodeID,
	})
	if err != nil {
		return err
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if serr := otelShutdown(shutCtx); serr != nil {
			log.Warn("otel shutdown", "err", serr)
		}
	}()

	store, err := badger.Open(badger.Options{Path: cfg.StorePath, Logger: log})
	if err != nil {
		return err
	}
	defer func() {
		if cerr := store.Close(); cerr != nil {
			log.Error("store close", "err", cerr)
		}
	}()

	ctl, err := controlapi.New(controlapi.Options{
		BaseURL:   cfg.ControlAPIURL,
		UserAgent: "go-node-agent/" + version,
	})
	if err != nil {
		return err
	}
	defer func() { _ = ctl.Close() }()

	bs := bootstrap.New(bootstrap.Config{
		ExpectedNodeID: domain.NodeID(cfg.NodeID),
		BootstrapToken: cfg.BootstrapToken,
		NodeKey:        cfg.NodeKey,
		NodeRole:       cfg.NodeRole,
	}, store, ctl, idgen.UUID{}, log)

	bsRes, err := runBootstrapWithRetry(ctx, log, bs)
	if err != nil {
		return err
	}
	nodeID := bsRes.Identity.NodeID

	log.Info("bootstrap complete",
		"node_id", nodeID,
		"agent_instance_id", bsRes.Identity.AgentInstanceID,
		"full_resync_required", bsRes.FullResyncRequired,
		"was_fresh", bsRes.WasFresh,
	)

	natsTr, err := natsa.New(ctx, natsa.Options{
		URL:            cfg.NATSURL,
		Name:           cfg.NATSName + "-" + string(nodeID),
		PublishTimeout: cfg.NATSPublishTimeout,
		ReconnectWait:  cfg.NATSReconnectWait,
		Logger:         log,
	})
	if err != nil {
		return err
	}
	defer func() { _ = natsTr.Close() }()

	subjects := wire.NewSubjects(wire.SubjectPrefixes{
		Command:    cfg.NATSCommandPrefix,
		Result:     cfg.NATSResultPrefix,
		Snapshot:   cfg.NATSSnapshotPrefix,
		Heartbeat:  cfg.NATSHeartbeatPrefix,
		SyncReport: cfg.NATSSyncReportPrefix,
	})

	healthChecks := []server.HealthCheck{natsTr}
	var stack *entryStack
	var backendStack *backendStack
	var appExec applier.Executor = applier.NoopExecutor{}

	switch {
	case cfg.EnableExecutor && strings.EqualFold(cfg.NodeRole, "entry"):
		stack, err = buildEntryStack(cfg, nodeID, store, natsTr, subjects, log)
		if err != nil {
			return err
		}
		defer func() {
			if stack != nil && stack.singbox != nil {
				_ = stack.singbox.Close()
			}
		}()
		appExec = stack.executor
		if stack.singbox != nil {
			healthChecks = append(healthChecks, stack.singbox)
		}
		if stack.entryProxy != nil {
			healthChecks = append(healthChecks, stack.entryProxy)
		}
		log.Info("real executor enabled (entry role)", "embedded_singbox", cfg.SingBoxEmbedded)
	case cfg.EnableExecutor && strings.EqualFold(cfg.NodeRole, "backend"):
		backendStack, err = buildBackendStack(cfg, store, log)
		if err != nil {
			return err
		}
		defer func() {
			if backendStack != nil && backendStack.xray != nil {
				_ = backendStack.xray.Close()
			}
		}()
		appExec = backendStack.executor
		healthChecks = append(healthChecks, backendStack.xray)
		log.Info("real executor enabled (backend role)")
	case cfg.EnableExecutor:
		log.Warn("ENABLE_EXECUTOR=true but NODE_ROLE not entry/backend; falling back to NoopExecutor",
			"node_role", cfg.NodeRole)
	}

	app, err := applier.New(applier.Config{
		NodeID:         nodeID,
		CommandSubject: subjects.PlacementCommand(nodeID),
		ResultSubject:  subjects.PlacementResult(nodeID),
		Durable:        "agent_" + string(nodeID) + "_commands",
	}, natsTr, natsTr, store, appExec, idgen.UUID{}, log)
	if err != nil {
		return err
	}

	sampler := heartbeat.NewSystemSamplerWith(heartbeat.SystemSamplerOptions{
		NIC:                 cfg.BandwidthNIC,
		CapacityBytesPerSec: uint64(cfg.BandwidthCapacityMbps) * 1_000_000 / 8,
	})
	hb, err := heartbeat.New(heartbeat.Config{
		NodeID:       nodeID,
		Subject:      subjects.Heartbeat(nodeID),
		AgentVersion: version,
		Interval:     cfg.HeartbeatInterval,
	}, natsTr, sampler, app, idgen.UUID{}, log)
	if err != nil {
		return err
	}

	var snapRebuilder snapshot.Rebuilder = snapshot.NoopRebuilder{}
	if stack != nil {
		snapRebuilder = stack.actions
	}
	if backendStack != nil && backendStack.rebuilder != nil {
		snapRebuilder = backendStack.rebuilder
		if err := backendStack.rebuilder.RebuildFromStore(ctx); err != nil {
			log.Warn("initial backend rebuild failed", "err", err)
		}
	}
	snapConsumer, err := snapshot.NewConsumer(snapshot.ConsumerConfig{
		NodeID:            nodeID,
		ChunkSubject:      subjects.SnapshotChunk(nodeID),
		SyncReportSubject: subjects.SyncReport(nodeID),
		Durable:           "agent_" + string(nodeID) + "_snapshots",
	}, natsTr, natsTr, store, snapRebuilder, idgen.UUID{}, log)
	if err != nil {
		return err
	}

	snapRequester, err := snapshot.NewRequester(snapshot.RequesterConfig{
		NodeID:         nodeID,
		RequestSubject: subjects.SnapshotRequest(nodeID),
	}, natsTr, log)
	if err != nil {
		return err
	}

	if bsRes.FullResyncRequired || cfg.NodeRole == "entry" || cfg.NodeRole == "whitelist_entry" {
		if rerr := snapRequester.Request(ctx, jsonv1.SnapshotReasonStartup); rerr != nil {
			log.Warn("snapshot request failed", "err", rerr)
		}
	}

	if cfg.WgEnabled {
		wgMgr, err := wg.New(cfg.WgInterface, cfg.WgKeyDir)
		if err != nil {
			return fmt.Errorf("wg manager: %w", err)
		}
		wgSvc, err := wgmesh.New(wgmesh.Config{
			NodeID:     nodeID,
			ListenPort: int(cfg.WgListenPort),
		}, wgMgr, natsTr, log)
		if err != nil {
			return fmt.Errorf("wgmesh service: %w", err)
		}
		go func() {
			if err := wgSvc.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("wgmesh exited", "err", err)
			}
		}()
	}

	var trafficReporter *traffic.Reporter
	if stack != nil {
		trafficReporter, err = traffic.New(traffic.Config{
			SingBoxAPIURL: cfg.SingBoxAPIURL,
		}, log)
		if err != nil {
			return err
		}
	}

	var trafficPub *traffic.Publisher
	if trafficReporter != nil {
		var connsSrc traffic.ConnectionsSource
		if stack != nil {
			switch {
			case stack.singbox != nil:
				connsSrc = stack.singbox
			case stack.entryProxy != nil:
				connsSrc = stack.entryProxy
			}
		}
		trafficPub, err = traffic.NewPublisher(traffic.PublisherConfig{
			NodeID:   nodeID,
			NodeRole: cfg.NodeRole,
			Subject:  cfg.NATSNodesTrafficSubject,
			Interval: cfg.TrafficInterval,
		}, natsTr, connsSrc, log)
		if err != nil {
			return err
		}
	}

	var statsRep *traffic.StatsReporter
	if stack != nil && stack.registry != nil {
		var statsSrc traffic.ConnectionsSource
		switch {
		case stack.singbox != nil:
			statsSrc = stack.singbox
		case stack.entryProxy != nil:
			statsSrc = stack.entryProxy
		}
		if statsSrc != nil {
			statsRep, err = traffic.NewStatsReporter(traffic.StatsReporterConfig{
				NodeID: nodeID,
			}, natsTr, statsSrc, stack.registry, log)
			if err != nil {
				return err
			}
		}
	}

	var backendTrafficPub *traffic.BackendPublisher
	if backendStack != nil && backendStack.xray != nil {
		backendTrafficPub, err = traffic.NewBackendPublisher(traffic.BackendPublisherConfig{
			NodeID:             nodeID,
			NodeTrafficSubject: cfg.NATSNodesTrafficSubject,
			UserTrafficSubject: cfg.NATSUsersTrafficSubject,
			Interval:           cfg.TrafficInterval,
		}, natsTr, natsTr, backendStack.xray, log)
		if err != nil {
			return err
		}
	}

	var trafficSrc server.TrafficSource
	if trafficReporter != nil {
		trafficSrc = trafficReporter
	}
	adminSrv, err := server.New(server.Options{
		Addr:        cfg.HTTPAddr,
		Stats:       app,
		Traffic:     trafficSrc,
		Checks:      healthChecks,
		Logger:      log,
		EnablePprof: true,
	})
	if err != nil {
		return err
	}
	defer func() { _ = adminSrv.Close() }()

	var reconciler *reconcile.Reconciler
	if rebuilder := pickRebuilder(stack, backendStack); rebuilder != nil {
		reconciler, err = reconcile.New(reconcile.Config{
			Interval: cfg.ReconcileInterval,
		}, rebuilder, log)
		if err != nil {
			return err
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return hb.Run(gctx) })
	g.Go(func() error { return app.Run(gctx) })
	g.Go(func() error { return snapConsumer.Run(gctx) })
	g.Go(func() error { return adminSrv.Run(gctx) })
	if reconciler != nil {
		g.Go(func() error { return reconciler.Run(gctx) })
	}
	if trafficReporter != nil {
		g.Go(func() error { return trafficReporter.Run(gctx) })
	}
	if trafficPub != nil {
		g.Go(func() error { return trafficPub.Run(gctx) })
	}
	if backendTrafficPub != nil {
		g.Go(func() error { return backendTrafficPub.Run(gctx) })
	}
	if statsRep != nil {
		g.Go(func() error { return statsRep.Run(gctx) })
	}
	if stack != nil && stack.listener != nil {
		g.Go(func() error { return stack.listener.Run(gctx) })
	}
	if stack != nil && stack.coalescer != nil {
		g.Go(func() error { return stack.coalescer.Run(gctx) })
	}
	if stack != nil && stack.entryProxy != nil && stack.actions != nil {
		g.Go(func() error { return runEntryProxyResync(gctx, stack.entryProxy, stack.actions, log) })
	}

	log.Info("agent running",
		"heartbeat_subject", subjects.Heartbeat(nodeID),
		"heartbeat_interval", cfg.HeartbeatInterval,
		"command_subject", subjects.PlacementCommand(nodeID),
		"result_subject", subjects.PlacementResult(nodeID),
		"upstream_subject", subjects.UpstreamChanged(nodeID),
		"snapshot_chunk_subject", subjects.SnapshotChunk(nodeID),
		"http_addr", adminSrv.Addr(),
	)

	werr := g.Wait()
	log.Info("shutdown signal received, snapshotting store")
	snapshotCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if serr := store.Snapshot(snapshotCtx); serr != nil {
		log.Warn("snapshot on shutdown", "err", serr)
	}
	if werr != nil && !errors.Is(werr, context.Canceled) {
		return werr
	}
	return nil
}

func buildEntryStack(
	cfg config.Config,
	nodeID domain.NodeID,
	store *badger.Store,
	natsTr *natsa.Transport,
	subjects wire.Subjects,
	log *slog.Logger,
) (*entryStack, error) {
	if cfg.SingBoxEmbedded {
		return buildEmbeddedEntryStack(cfg, nodeID, store, natsTr, subjects, log)
	}
	sb, err := singbox.New(singbox.Options{
		APIURL:     cfg.SingBoxAPIURL,
		ConfigPath: cfg.SingBoxConfigPath,
		Logger:     log,
	})
	if err != nil {
		return nil, err
	}
	reg := backends.NewRegistry()

	actions, err := executor.NewEntryActions(executor.EntryActionsConfig{
		Inbound: singboxgen.InboundSpec{
			Tag: cfg.SingBoxInboundTag,
			Listen: singboxgen.ListenSpec{
				Address: cfg.SingBoxListenAddress,
				Port:    cfg.SingBoxListenPort,
				Sniff:   true,
			},
		},
		LogCfg: singboxgen.LogSpec{Level: cfg.SingBoxLogLevel},
		ClashCfg: singboxgen.ClashAPISpec{
			Enabled:    true,
			ExternalCt: strings.TrimPrefix(strings.TrimPrefix(cfg.SingBoxAPIURL, "http://"), "https://"),
		},
		ConfigPath: cfg.SingBoxConfigPath,
	}, sb, store, reg, log)
	if err != nil {
		_ = sb.Close()
		return nil, err
	}

	orch, err := flip.New(actions, log, flip.Options{})
	if err != nil {
		_ = sb.Close()
		return nil, err
	}

	flipExec, err := executor.NewFlipExecutor(actions, orch, executor.FlipExecutorOptions{
		DrainTimeout: cfg.DrainTimeout,
	}, log)
	if err != nil {
		_ = sb.Close()
		return nil, err
	}

	listener, err := backends.NewListener(backends.ListenerConfig{
		NodeID:  nodeID,
		Subject: subjects.UpstreamChanged(nodeID),
		Durable: "agent_" + string(nodeID) + "_upstream",
		Defaults: backends.Defaults{
			Port:      cfg.BackendDefaultPort,
			Transport: domain.TransportKind(cfg.BackendDefaultTransport),
		},
	}, natsTr, reg, log)
	if err != nil {
		_ = sb.Close()
		return nil, err
	}

	coalescer, err := executor.NewRenderCoalescer(actions, executor.CoalescerOptions{}, log)
	if err != nil {
		_ = sb.Close()
		return nil, err
	}
	actions.AttachCoalescer(coalescer)

	return &entryStack{
		executor:  flipExec,
		listener:  listener,
		actions:   actions,
		singbox:   sb,
		registry:  reg,
		coalescer: coalescer,
	}, nil
}

// buildEmbeddedEntryStack drives the embedded entry proxy (ADR 0005) over its
// control socket instead of rendering + reloading an external sing-box.
func buildEmbeddedEntryStack(
	cfg config.Config,
	nodeID domain.NodeID,
	store *badger.Store,
	natsTr *natsa.Transport,
	subjects wire.Subjects,
	log *slog.Logger,
) (*entryStack, error) {
	reg := backends.NewRegistry()
	proxy := entryproxyclient.New(cfg.EntryProxySocket)

	actions, err := executor.NewEntryProxyActions(proxy, store, reg, log)
	if err != nil {
		return nil, err
	}
	orch, err := flip.New(actions, log, flip.Options{})
	if err != nil {
		return nil, err
	}
	flipExec, err := executor.NewFlipExecutor(actions, orch, executor.FlipExecutorOptions{
		DrainTimeout: cfg.DrainTimeout,
	}, log)
	if err != nil {
		return nil, err
	}
	listener, err := backends.NewListener(backends.ListenerConfig{
		NodeID:  nodeID,
		Subject: subjects.UpstreamChanged(nodeID),
		Durable: "agent_" + string(nodeID) + "_upstream",
		Defaults: backends.Defaults{
			Port:      cfg.BackendDefaultPort,
			Transport: domain.TransportKind(cfg.BackendDefaultTransport),
		},
	}, natsTr, reg, log)
	if err != nil {
		return nil, err
	}

	return &entryStack{
		executor:   flipExec,
		listener:   listener,
		actions:    actions,
		registry:   reg,
		entryProxy: proxy,
	}, nil
}

// runEntryProxyResync pushes store state to the embedded proxy on startup and
// whenever the proxy restarts (its epoch changes), since the proxy holds no
// state across restarts.
func runEntryProxyResync(ctx context.Context, proxy *entryproxyclient.Client, rebuilder reconcile.Rebuilder, log *slog.Logger) error {
	const interval = 5 * time.Second
	t := time.NewTicker(interval)
	defer t.Stop()
	var lastEpoch int64
	resync := func() {
		epoch, err := proxy.Epoch(ctx)
		if err != nil {
			log.Warn("entry proxy epoch check failed", "err", err)
			return
		}
		if epoch == lastEpoch {
			return
		}
		if err := rebuilder.RebuildFromStore(ctx); err != nil {
			log.Warn("entry proxy resync failed", "err", err)
			return
		}
		lastEpoch = epoch
		log.Info("entry proxy resynced from store", "epoch", epoch)
	}
	resync()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			resync()
		}
	}
}

func buildBackendStack(cfg config.Config, store *badger.Store, log *slog.Logger) (*backendStack, error) {
	tagByXport := map[domain.TransportKind]string{}
	if cfg.XrayInboundTagWS != "" {
		tagByXport[domain.TransportWS] = cfg.XrayInboundTagWS
	}
	if cfg.XrayInboundTagReality != "" {
		tagByXport[domain.TransportReality] = cfg.XrayInboundTagReality
	}
	if cfg.XrayInboundTagXHTTP != "" {
		tagByXport[domain.TransportXHTTP] = cfg.XrayInboundTagXHTTP
	}
	if cfg.XrayInboundTagTCP != "" {
		tagByXport[domain.TransportTCP] = cfg.XrayInboundTagTCP
	}
	xc, err := xray.New(xray.Options{
		Address:           cfg.XrayGRPCAddr,
		InboundTag:        cfg.XrayInboundTag,
		InboundTagByXport: tagByXport,
		MirrorTag:         cfg.XrayInboundTagWgInternal,
		Timeout:           3 * time.Second,
		Logger:            log,
	})
	if err != nil {
		return nil, err
	}
	be, err := executor.NewBackendExecutor(xc, store, log)
	if err != nil {
		_ = xc.Close()
		return nil, err
	}
	rb := &backendRebuilder{xray: xc, store: store, log: log}
	return &backendStack{executor: be, xray: xc, rebuilder: rb}, nil
}

func runBootstrapWithRetry(ctx context.Context, log *slog.Logger, bs *bootstrap.Bootstrap) (bootstrap.Result, error) {
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 30 * time.Second
	)
	backoff := initialBackoff
	for {
		res, err := bs.Run(ctx)
		if err == nil {
			return res, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return bootstrap.Result{}, err
		}
		var nonRetry *controlapi.NonRetryableError
		if errors.As(err, &nonRetry) {
			return bootstrap.Result{}, err
		}
		log.Warn("bootstrap failed, retrying", "err", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return bootstrap.Result{}, ctx.Err()
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
