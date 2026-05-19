# Changelog

All notable changes to this project. Format based on Keep a Changelog.

## [Unreleased]

### Added — Phase 2: graceful flip + real executor

- Flip state machine (`app/flip`) — `STEADY → ANNOUNCED → WARMING → SWAP → COOLING → STEADY` with drain-poll loop and `ErrDrainTimeout` force-close path
- Sing-box config builder (`wire/singboxgen`) — pure transform `NodeState → JSON`, deterministic output, validation
- `EntryActions` (`app/executor`) — implements `flip.Actions` via sing-box adapter + config builder
- `FlipExecutor` (`app/executor`) — high-level executor that routes between flip and simple paths based on backend change detection
- `Applier.Executor` interface refactor — takes `desired + existing + found` so executors can decide flip vs simple
- Backend `Registry` (`app/backends`) — thread-safe dynamic registry, satisfies `executor.BackendLookup`
- Backend `Listener` (`app/backends`) — consumes `UpstreamChangedPayload` events from NATS, upserts registry
- Snapshot `Consumer` (`app/snapshot`) — processes `SnapshotChunkEvent`, idempotency by `op_version`, rebuilds sing-box on last chunk, publishes `SyncReportEvent`
- Snapshot `Requester` (`app/snapshot`) — publishes `SnapshotRequestEvent` on startup when `full_resync_required=true`
- `wire/jsonv1` codecs for snapshot request/chunk, sync report, upstream changed
- Main.go: optional entry-stack wiring when `ENABLE_EXECUTOR=true && NODE_ROLE=entry`, snapshot pipeline always active

### Added — Phase 1: observer mode

- Bootstrap use case (`app/bootstrap`) — `/api/agent/initial` handshake with retry loop, identity persistence in Badger
- Heartbeat use case (`app/heartbeat`) — periodic publish, gopsutil CPU/mem sampler, counter bridge from applier
- Applier use case (`app/applier`) — subscribe + decode + dedup (stale/idempotent) + dispatch + report
- NATS Transport adapter (`adapters/nats`) — JetStream durable consumers, mTLS-ready, reconnect handling
- Sing-box Clash API adapter (`adapters/singbox`) — atomic config write, `PUT /configs?force=true` reload, connections query
- Xray gRPC adapter (`adapters/xray`) — minimal proto subset + AlterInbound for VLESS user add/remove
- HAProxy admin socket adapter (`adapters/haproxy`) — `set server addr/state` via Unix socket
- Badger store adapter (`adapters/badger`) — placement, cursor, identity persistence
- Control-API HTTP client (`adapters/controlapi`) — typed retryable/non-retryable errors
- Admin HTTP server (`server/`) — `/healthz`, `/readyz`, `/livez`, `/metrics`, `/debug/pprof/*`
- Pydantic-compatible `wire/jsonv1` codecs — heartbeat, placement command/result/upstream
- NATS subjects helper (`wire/subjects.go`)
- Helm chart for DaemonSet deployment (`deploy/helm`)
- 4 ADRs documenting architectural decisions
- Integration test harness with embedded NATS + httptest control-api
- CI workflows: test+race, lint, govulncheck, helm lint, buf lint, multi-arch image build+push

### Architecture

- Hexagonal (ports & adapters) layout
- `domain/` pure types, no I/O
- `ports/` interfaces, one per external system
- `adapters/` concrete impls
- `app/` use cases compose ports
- Compile-time port-conformance checks across all adapters
