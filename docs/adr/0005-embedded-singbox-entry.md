# 0005 — Embedded sing-box on entry nodes (dynamic users without reload)

Status: proposed
Date: 2026-05-30

## Context

Entry nodes terminate VLESS+REALITY in a separate `vpn-sing-box` container
(`ghcr.io/sagernet/sing-box:v1.13.11`, infra chart `k8s/charts/vpn-sing-box`).
The agent drives it over the Clash/sing-box HTTP API.

Two runtime mutations happen on an entry:

- **Backend flip** (move a user's egress to another backend) — already cheap:
  per-user selector outbound + Clash `PUT /proxies`, no reload, connections
  survive (`entry_actions.go:trySelectorFlip`).
- **New user arrives** — expensive: a brand-new VLESS user means new
  `inbound.users[]` *and* new per-user outbounds + route rule, none of which
  sing-box can add at runtime via API. So the agent re-renders the whole config,
  writes the file, and `PUT /configs?force=true`. **A sing-box reload drops all
  live connections on the node** (the reason for the `skip-reload-if-identical`
  hash guard, commit `5aaa25f`). One new user disconnects everyone.

The backend tier does not have this problem: it runs xray and adds users live
via gRPC `AlterInbound AddUser` — no reload, connections survive.

Moving the entry to xray was rejected: xray cannot do per-user backend routing
mutably at runtime the way the sing-box selector does. The selector-based
per-user routing is the entry's core capability and must be kept.

## Decision

Embed sing-box as a **Go library inside the agent** on entry nodes. sing-box
stays the engine for the hard, security-critical part (REALITY + VLESS); all
runtime dynamics move into agent-owned Go code. Retire the `vpn-sing-box`
daemonset on entries — the agent listens on `:443` itself.

Key design move — **collapse per-user routing into one dispatcher**:

```
internet ──VLESS+REALITY──▶ [VLESS service]   auth UUID, sets ctx.User
                                  │ ctx.User = <clientID>, dst
                                  ▼
                          [dispatcher (agent Go code)]
                                  │  map[clientID]backendID  (RWMutex)
                                  ▼
                          VLESS client pool, one per backend, over wg-mesh
```

- sing-box config/state is **static**: one REALITY VLESS inbound, nothing else
  mutated at runtime — except the inbound user set.
- **Flip** = update `map[clientID]backendID` (pure Go, in-memory, instant). No
  selector, no Clash API.
- **New backend** = add a VLESS client to the pool (pure Go).
- **New / removed user** = the single live call into sing-box code:
  `vless.Service.UpdateUsers(...)` + a map entry. No file, no reload, no dropped
  connections.
- **Drain** = exact per-backend connection count from the dispatcher (replaces
  parsing `/connections`).

This makes the entry as dynamic as the backend, and deletes the whole
render → write → reload → coalescer → skip-hash path and the per-user
outbound/selector generation in `singboxgen`.

## Go/no-go — verified against sing-box v1.13.11

| Need | Status |
|---|---|
| REALITY TLS server, public ctor | `common/tls.NewServer(ctx, log, option.InboundTLSOptions)` → `NewRealityServer` |
| Live VLESS user update | `vless.Service.UpdateUsers(users, uuids, flows)` (sing-vmess), exported |
| Authenticated user reaches dispatch | `adapter.InboundContext.User` + `adapter.ContextFrom(ctx)` |
| Custom outbound (box flavor) | `outbound.Register[Opts](reg, type, ctor)`, iface `Outbound{Type,Tag,Network,Dependencies,N.Dialer}` |
| Embed entrypoint (box flavor) | `box.New` + `box.Context(ctx, inboundRegistry, outboundRegistry, …)` |

Gap: sing-box's `protocol/vless.Inbound.service` is **unexported**, so its
`UpdateUsers` is not reachable through the stock inbound. Resolved fork-free by
constructing the VLESS service ourselves (see Implementation).

## Implementation — fork-free, lean

The entry profile is exactly VLESS + REALITY + TCP (no multiplex / ws), so we do
not need sing-box's full inbound/router/`box`. We own a small inbound:

1. TCP listener on `:443`.
2. `tls.NewServer` with the REALITY options (private_key, short_id,
   server_name, handshake server/port — from agent env, same values the chart
   feeds today).
3. `vless.NewService[K](logger, handler)` + `service.UpdateUsers(...)` for the
   live user set.
4. `handler.NewConnectionEx(ctx, conn, metadata)` reads `metadata.User` +
   `metadata.Destination`, looks up the backend, dials it through a per-backend
   `vless` **client** over the wg-mesh, and splices. This handler *is* the
   dispatcher — no `box`, no custom outbound type, no router rules.

Everything sing-box-specific lives behind one port:

```go
type EntryProxy interface {
    Start(ctx) error; Close() error
    AddUser(ctx, clientID, flow string) error            // live
    RemoveUser(ctx, clientID string) error               // live
    SelectBackend(ctx, clientID, backendID string) error // flip, live
    SetBackends(ctx, []BackendSpec) error                // dialer pool, live
    BackendConnections(ctx, backendID) (uint64, error)   // drain
}
```

One adapter `internal/adapters/singboxembed` implements it; the rest of the app
(`flip.Orchestrator` stays; `EntryActions` retargets to `EntryProxy`) is
proxy-agnostic. `box`-based flavor (custom inbound + dispatcher outbound types,
register, run `box`) is the fallback if we later need sing-box's broader
transport/router features.

## Handling sing-box updates

The explicit worry. Strategy is **ride upstream, never patch the protocol**:

- sing-box is a pinned `go.mod` dependency. We touch **only exported APIs**, so
  REALITY/VLESS fixes arrive on a version bump — an advantage of library-embed
  over a binary fork.
- Anti-corruption layer: all sing-box code is confined to `singboxembed` behind
  `EntryProxy`. An update can break at most that one package.
- Typed config: build REALITY/inbound options as `option.*` structs, not JSON —
  upstream field drift becomes a **compile error** in one place, not silent
  runtime breakage.
- Zero internal patches: by constructing the VLESS service ourselves we avoid
  the one unexported seam, so there is **no fork to rebase**.
- CI gate on bump: a smoke test boots the embedded proxy, connects a real
  VLESS+REALITY client, live-adds a user, flips a backend, and asserts no
  existing connection drops. Runs on every sing-box version bump.
- Bump deliberately (not auto), reading the changelog for inbound/REALITY
  changes.

## Build

sing-box's REALITY (server and client) is gated behind the `with_utls` build
tag and pulls `github.com/metacubex/utls`. The entry-proxy binary **must** be
built with `-tags with_utls` — without it `tls.NewServer` returns a stub and
REALITY fails at runtime. Static binary is ~9 MB. The `cmd/agent` binary is
unaffected (it does not import the proxy). The runtime smoke
(`internal/entryproxy/smoke_test.go`) carries `//go:build with_utls` and boots a
real REALITY+VLESS client through the proxy to a backend, then live-adds a
second user and asserts the first connection is not dropped.

## Migration / rollout

- Flag `SINGBOX_EMBEDDED=true` (entry, executor enabled). Old external-sing-box
  path stays as fallback.
- Canary one dev entry: run probes, manual flip, verify a new user does **not**
  drop existing connections.
- Retire the `vpn-sing-box` daemonset on entries; the agent pod takes `:443`,
  the REALITY keys, and the api port is gone.
- Widen once proven; remove the render/reload/coalescer code paths.

## Consequences

- Crash isolation lost: a sing-box panic now takes the agent with it (today it
  is a separate container). Mitigate with a supervised goroutine + `recover`;
  k8s restarts the pod. Accepted trade — full runtime control for one process.
- Agent binary grows (sing-box dep tree) and now holds the REALITY private key
  and the public `:443` socket. Keep RO rootfs / minimal caps.
- The entry and backend tiers converge on one model: authenticated user set is
  mutated live, routing is an in-memory map — symmetric, no reloads.
