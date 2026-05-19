# 0001 — Hexagonal architecture (ports & adapters)

Status: accepted
Date: 2026-05-18

## Context

This service talks to six distinct external systems:
1. NATS JetStream (commands, heartbeats, traffic, KV)
2. Sing-box Clash API (HTTP)
3. Xray gRPC (per-user adds/removes)
4. HAProxy admin socket (`set server` for upstream/pool)
5. Control-api HTTP (`/initial` bootstrap only)
6. Local persistent store (WAL + snapshot, recover after crash)

The crown jewel is the **flip state machine** (`STEADY → ANNOUNCED → WARMING → SWAP → COOLING → STEADY`). It must be unit-testable without spinning up real sing-box / Xray / HAProxy.

## Decision

**Hexagonal (ports & adapters) layout**, adapted to Go idiom:
- `internal/domain/` — pure types, no I/O, no dependencies on `app/` or `adapters/`.
- `internal/app/` — use cases (orchestrators). Depend only on `domain/` and `ports/` interfaces. No transport, no SQL, no HTTP.
- `internal/ports/` — small interfaces that adapters implement. Defined where there's cross-cutting reuse; otherwise each `app/<usecase>/` package may declare its own narrow interface (true "accept interfaces" Go style).
- `internal/adapters/` — concrete implementations of ports. The only place that imports nats.go, gRPC stubs, HTTP clients, badger, etc.
- `internal/platform/` — cross-cutting infra not tied to any domain concern: config, logger, telemetry, health.
- `cmd/agent/` — DI wiring, signal handling.

## Alternatives considered

**Mirror Python `node-agent` layout (`services/<X>/{service,schemas,manager}.py`).**
Rejected — the Python tree grew organically and conflates transport, business logic, and persistence in single packages. Mirroring it would inherit the same boundaries problem.

**Layered (controller / service / repository).**
Rejected — three-tier layering hides the multi-adapter reality. With six adapters, "service" becomes a god-package.

**Flat `pkg/<feature>/`.**
Rejected — fine for tiny services, but here we want clear seams for testing and for swapping NATS → gRPC streaming later.

## Consequences

- Slightly more upfront boilerplate (port interfaces + adapter constructors).
- Domain & app layers can be unit-tested with cheap fakes — no docker, no network.
- Adding a new adapter (e.g., NATS → gRPC streaming) is a one-package change.
- A new contributor can find anything via the layout: "where does it talk to X?" → `adapters/X/`.
- Discipline required: nothing in `domain/` or `app/` may import an adapter package. Enforced by `golangci-lint` `depguard` rule (to be configured).
