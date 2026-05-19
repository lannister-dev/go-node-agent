# go-node-agent

Go agent running as DaemonSet on VPN edge nodes. Drop-in replacement for the Python `node-agent`, with **graceful TCP flip** added — drain old backend connections instead of dropping them when traffic migrates.

## What it does

- Bootstraps identity against `vpn-control-api` via `POST /api/agent/initial`, persists locally
- Subscribes to NATS JetStream for placement commands, backend changes, and snapshot chunks
- Applies user routing changes by re-rendering sing-box config + Clash API reload (no process restart)
- Drains old backend on flip via sing-box `/connections` polling before removing it
- Publishes heartbeats, apply results, sync reports back to control-plane
- Exposes `/healthz`, `/readyz`, `/livez`, `/metrics`, `/debug/pprof/*` on HTTP
- Pure Go binary in scratch image, non-root, RO rootfs, ~16MB

## Goals (SLO)

| Metric | Target |
|---|---|
| p50 hot-patch latency (NATS msg → applied) | ≤ 150ms |
| p99 hot-patch latency | ≤ 400ms |
| TCP drops on graceful flip | 0% |
| RSS @ 10k users | ≤ 100MB |
| Image size | ≤ 25MB |
| GC pause p99 | ≤ 1ms |
| 24h soak (memory / FD leaks) | clean |

## Architecture

Hexagonal layout (ports & adapters):

```
cmd/agent/                            binary entry, DI wiring
internal/
  domain/                             pure types — Placement, Backend, FlipPlan, ...
  ports/                              interfaces (seams)
  app/                                use cases (orchestrators, no I/O)
    applier/                            handle placement command (decode + dedup + dispatch)
    bootstrap/                          /initial handshake
    executor/                           FlipExecutor + EntryActions (drives sing-box)
    flip/                               drain state machine (STEADY→WARMING→SWAP→COOLING)
    heartbeat/                          periodic HeartbeatEvent publish
    backends/                           dynamic backend registry + NATS listener
    snapshot/                           full-resync consumer + request publisher
  adapters/                           concrete impls
    nats/  controlapi/  badger/  singbox/  xray/  haproxy/
  platform/                           config, logger, idgen
  server/                             admin HTTP (health, metrics, pprof)
  wire/                               serialization formats
    jsonv1/                             Pydantic-compatible JSON (Phase-1 wire)
    singboxgen/                         sing-box config builder
api/proto/                            .proto sources (future proto wire)
pkg/proto/                            generated Go protobuf
deploy/helm/                          DaemonSet chart
test/integration/                     end-to-end tests (embedded NATS, no Docker)
docs/adr/                             architecture decision records
```

See `docs/adr/0001-hexagonal-architecture.md` for the why.

## Quickstart

```bash
make tools        install buf, golangci-lint, protoc-gen-go, mockery, govulncheck
make proto        regenerate protobuf via buf
make build        bin/agent (static, ~16MB)
make test         go test -race ./...
make lint         golangci-lint
make docker       multi-arch image → harbor.lannister-dev.ru/vpn/node-agent
make ci           full CI suite (vet + lint + test + govulncheck)
```

## Configuration (env)

### Required
- `NODE_KEY` — pre-shared node identity key
- `BOOTSTRAP_TOKEN` — control-api bootstrap token
- `CONTROL_API_URL` — base URL (e.g. `https://dev.lannister-dev.ru`)

### NATS
- `NATS_URL` (default `nats://nats.nats.svc.cluster.local:4222`)
- `NATS_CERT_PATH`, `NATS_KEY_PATH`, `NATS_CA_PATH` — mTLS (optional)
- `NATS_COMMAND_PREFIX` / `NATS_RESULT_PREFIX` / `NATS_SNAPSHOT_PREFIX` / `NATS_HEARTBEAT_PREFIX` / `NATS_SYNC_REPORT_PREFIX` — subject prefix overrides

### Identity
- `NODE_ID` — optional, asserts server-returned identity matches
- `NODE_ROLE` — `entry` for entry nodes; required for `ENABLE_EXECUTOR=true`

### Local services
- `SINGBOX_API_URL` (default `http://127.0.0.1:9090`) — Clash API endpoint
- `SINGBOX_CONFIG_PATH` (default `/var/lib/sing-box-shared/sing-box/config.json`)
- `XRAY_GRPC_ADDR` (default `127.0.0.1:10085`)
- `HAPROXY_SOCKET` (default `/var/run/haproxy/admin.sock`)
- `STORE_PATH` (default `/var/lib/go-node-agent`) — Badger directory

### Executor (drop-in mode)
- `ENABLE_EXECUTOR` (default `false`) — when `true` AND `NODE_ROLE=entry`, agent renders sing-box config & drives flip. Otherwise `NoopExecutor` (observer mode)
- `SINGBOX_INBOUND_TAG` (default `vless-in`)
- `SINGBOX_LISTEN_ADDRESS` (default `::`)
- `SINGBOX_LISTEN_PORT` (default `443`)
- `BACKEND_DEFAULT_PORT` (default `9000`) — applied to backends from `UpstreamChanged` events
- `BACKEND_DEFAULT_TRANSPORT` (default `ws`)

### Timing
- `HEARTBEAT_INTERVAL` (default `10s`)
- `TRAFFIC_INTERVAL` (default `30s`)
- `DRAIN_TIMEOUT` (default `30s`) — max wait for old backend connections to die

### Logging / HTTP
- `LOG_LEVEL` = `debug|info|warn|error`
- `LOG_FORMAT` = `json|text`
- `HTTP_ADDR` (default `:8080`)

## Deployment

### Helm

```bash
helm install go-node-agent ./deploy/helm \
  --namespace vpn-dev \
  --values ./deploy/helm/values.dev.yaml \
  --set secrets.bootstrapTokenSecretName=go-node-agent-bootstrap-dev \
  --set secrets.nodeKeySecretName=go-node-agent-nodekey-dev
```

The chart deploys a DaemonSet selecting nodes by label (default `role: vpn, agent: go`). Pods mount hostPath volumes for state, HAProxy socket, and shared sing-box config dir.

### Observer mode (default — safe for canary)

`executor.enabled=false`. Agent:
- bootstraps identity
- streams heartbeats
- receives commands but **doesn't apply them** (NoopExecutor) — only acknowledges
- proves NATS connectivity + control-api auth + metrics

### Real executor mode — entry nodes

`executor.enabled=true` AND `env.nodeRole=entry`. Agent additionally:
- reads operator-provided base sing-box config from `SINGBOX_CONFIG_PATH` (with REALITY private_key + short_id)
- **merges** dynamic user list + per-user route rules + per-backend outbounds into base, preserving REALITY block
- runs full graceful flip when backend changes (drain old connections via `/connections` polling before removing route)
- listens to backend upstream changes via NATS (`agent.placements.<node>.upstream`)
- handles snapshot full-resync (`agent.snapshots.<node>.chunks`)

Operator workflow:
1. Provision `SINGBOX_CONFIG_PATH` with a base config containing inbound `vless-in` with REALITY block (private_key, short_id, server_name). Empty `users: []`, empty `outbounds` beyond `direct`/`block`, empty `route.rules`.
2. Deploy agent with `ENABLE_EXECUTOR=true`, `NODE_ROLE=entry`.
3. Agent loads base, merges dynamic state on each placement command. REALITY keys survive every re-render.

### Real executor mode — backend nodes

`executor.enabled=true` AND `env.nodeRole=backend`. Agent:
- connects to local xray via gRPC (`XRAY_GRPC_ADDR`)
- on `desired=active` placement command → `AlterInbound AddUser` (idempotent on repeat)
- on `desired=inactive` or `is_revoked=true` → `AlterInbound RemoveUser`
- no graceful flip at backend layer (user add/remove is atomic; flip happens at entry layer between backends)
- xray health check at `/readyz`

## NATS subjects

Default subject layout (configurable via env):
- `agent.placements.<node_id>.commands` — inbound placement commands
- `agent.placements.<node_id>.upstream` — inbound backend registry updates
- `agent.placement_results.<node_id>.results` — outbound apply results
- `agent.snapshots.<node_id>.request` — outbound snapshot requests
- `agent.snapshots.<node_id>.chunks` — inbound snapshot chunks
- `agent.heartbeats.<node_id>.events` — outbound heartbeats
- `agent.sync_reports.<node_id>.events` — outbound sync reports

All wire format: Pydantic-compatible JSON (matches `vpn-control-api/services/nodes/agent/schemas.py`). Migration to protobuf is planned (see `docs/adr/0002-protobuf-over-nats.md`).

## HTTP endpoints

| Path | Returns |
|---|---|
| `GET /healthz` | always 200 if process alive |
| `GET /livez` | always 200 if process alive |
| `GET /readyz` | 200 if NATS connected (+ sing-box reachable when executor enabled), 503 otherwise |
| `GET /metrics` | Prometheus format: `agent_placement_received_total`, `_applied_total`, `_failed_total`, `go_*`, `process_*` |
| `GET /debug/pprof/*` | Standard Go pprof (gated by `EnablePprof` flag) |

## Migration phases

- **Phase 0** ✓ Repo scaffold, hexagonal layout, ADRs
- **Phase 1** ✓ Bootstrap, heartbeat, applier with NoopExecutor, NATS adapter, badger persistence, HTTP admin, helm chart, integration tests
- **Phase 2** ✓ Flip state machine, sing-box config builder, EntryActions, FlipExecutor, backend registry + listener, snapshot consumer
- **Phase 3** — TODO: OTel traces, protobuf wire migration, sing-box base config template merging, bandwidth_pct sampler

## Testing

```bash
make test                                # unit + integration (no Docker)
make cover                               # HTML coverage report
make fuzz                                # fuzz protobuf parsers + state machines
make vuln                                # govulncheck
```

Integration tests under `test/integration/` use embedded NATS server + httptest control-api — exercise the full agent flow (bootstrap → heartbeat → applier → snapshot) without containers.

## License

(TBD by org policy)
