# 0002 — Protobuf over NATS as wire format

Status: proposed
Date: 2026-05-18

## Context

The Python `node-agent` + `vpn-control-api` pair currently uses Pydantic models serialized to JSON on every NATS subject. With a Go agent joining the picture, two languages need to agree on wire shape.

## Decision

Migrate all NATS payloads (placement commands, heartbeats, traffic deltas, sync reports, upstream/pool events) to **protobuf**.

- Source of truth: `.proto` files in `api/proto/v1/` of this repo. Mirror in `vpn-control-api` (later: extract into a dedicated `vpn-proto` repo).
- Generated Go in `pkg/proto/v1/`, generated Python committed in the control-api repo.
- Schema version pinned in NATS headers (`x-msg-schema=v1`). Consumers reject unknown versions.
- HTTP boundaries (admin UI, `/initial` bootstrap) keep JSON / Pydantic — protobuf brings no value at HTTP edge and complicates browser tooling.

## Why

- Compact wire format (≈30–60% smaller than JSON) → lower NATS bandwidth on hot subjects (traffic deltas, heartbeats).
- Strict typing across Go/Python without manual sync of two schema sources.
- Forward/backward compatibility built into the encoding (field tags, optional fields).
- Smaller GC footprint on the Go side (no `map[string]any` parsing).

## Alternatives considered

- **Keep JSON + Pydantic, mirror types manually in Go.** Cheap to start, but every schema change breaks one side silently. Rejected.
- **MessagePack / CBOR.** Similar wins to protobuf without the schema discipline. Rejected — schema is the point.
- **Avro / FlatBuffers.** Overkill for our message sizes, less Go-native tooling.

## Migration

- Phase 1: control-api dual-encodes (publishes JSON on existing subjects, publishes protobuf on parallel `v2.*` subjects).
- Phase 2: Go agent subscribes only to `v2.*`. Python agent stays on JSON.
- Phase 3: Python agent migrated; JSON subjects retired.

## Consequences

- One more codegen step in CI on both repos (`make proto`).
- Local dev needs `protoc` + `protoc-gen-go` installed (`make tools`).
- Wire payloads no longer human-readable in NATS CLI — operators use a small `protop` debug tool (deferred).
