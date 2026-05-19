# 0003 — Badger as local WAL / snapshot store

Status: proposed
Date: 2026-05-18

## Context

The agent must persist applied-placement state locally so that on process restart we don't re-apply every op from scratch (which would mean either replaying NATS from start or asking control-api for a full snapshot). Two reasons:
1. **Faster cold start** — restore from local snapshot in <100ms, then catch up only the gap from NATS.
2. **Idempotency** — `op_version` of last applied state per placement, to drop stale/duplicate commands.

## Decision

Use **Badger v4** (`github.com/dgraph-io/badger/v4`) for the local KV store.

Layout:
- `placements/<placement_id>` → last applied `op_version` + state proto.
- `snapshot/cursor` → highest NATS msg seq we've acked.
- `audit/<ts>` → recent op history for debugging / `pprof`-style inspection.

Snapshot on graceful shutdown, periodic snapshot every 60s during normal operation.

## Alternatives considered

- **SQLite** — robust, SQL is nice for ad-hoc inspection. But CGo dependency (or pure-Go modernc.org/sqlite, slower). Overkill — we don't need joins.
- **Raw files (JSON / protobuf on disk)** — too easy to corrupt on partial writes; no LSM compaction means file grows unbounded.
- **BoltDB / bbolt** — read-optimized; our workload is write-heavy (every applied op is a write). Rejected.
- **No persistence, only NATS replay** — cold start time scales with backlog; control-api can't always serve a full snapshot quickly. Rejected.

## Consequences

- One extra dep (badger has zero CGo, pure-Go — fine for scratch image).
- Disk footprint per node: ~100MB at 50k placements (estimate). DaemonSet uses `emptyDir` or `hostPath` for `/var/lib/go-node-agent`.
- Adapter lives in `internal/adapters/badger/` and implements `ports.Store`. Replacable later if needed.
