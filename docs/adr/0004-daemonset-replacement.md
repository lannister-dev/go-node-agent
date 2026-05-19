# 0004 — DaemonSet replaces Python node-agent (not sidecar)

Status: accepted
Date: 2026-05-18

## Context

Two deployment shapes were on the table:
1. **Sidecar** — Go agent runs alongside the Python `node-agent` on every VPN edge node during migration.
2. **Replacement** — Go agent replaces Python `node-agent` entirely on a per-node basis, gated by a label.

## Decision

**Replacement.** On any given node, exactly one agent runs — either the Python one (legacy) or the Go one (new). Selection by DaemonSet pod-selector against a node label (`agent=go|py`).

## Why not sidecar

- **Sing-box config file ownership.** Both agents would write to the same `/var/lib/sing-box-shared/sing-box/config.json` with independent state machines — a guaranteed race.
- **NATS consumer duplication.** Two durable consumers on `agent.placements.<node_id>.commands` → either duplicate applies or split-brain.
- **Heartbeat / traffic confusion.** Control-api sees two heartbeats per node with conflicting metrics.
- **Operational burden.** Feature flags for "who owns what right now" become unmaintainable.

A *single*-host observer (Go agent on a dev node with NO Python agent, read-only NATS consumer, no applies) is a fine Phase-1 validation step — that is **not** the same as "sidecar in prod".

## Migration

- Phase 0: this repo bootstrap.
- Phase 1: deploy as observer on a dedicated dev node (no Python agent on that node). Validate schemas, contracts, metrics in real prod traffic.
- Phase 2: canary one prod node — flip label `agent=go`, Python pod evicted, Go pod scheduled.
- Phase 3: widen canary one region at a time (Latvia → Praha → ...).
- Phase 4: 100% nodes on Go. Archive Python `node-agent` repo (read-only).

## Consequences

- Helm chart for `vpn-sing-box` DaemonSet gets a second variant (`agent: go|py`) selected by node label.
- The bash file-watch wrapper in `infra/k8s/charts/vpn-sing-box/templates/daemonset.yaml` is removed in the Go variant — Go agent owns sing-box config writes and reload via Clash API directly.
- Rollback is fast: relabel the node back to `agent=py`.
