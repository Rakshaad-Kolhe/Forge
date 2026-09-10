# PR 22 — Realtime Event Transport & WebSocket Gateway

Objective: cross-process realtime event delivery. Outbox -> EventPublisher -> Redis Pub/Sub
-> WebSocket gateway -> clients. PostgreSQL stays authoritative; Redis + WS are transient.

Base: feat/pr-22-realtime-gateway  (worktree .claude/worktrees/pr-22-realtime-gateway,
branched from feat/pr-21-outbox @ 0263b68).

Decisions (user-approved):
- Minimal shared-secret handshake + pluggable SubscriptionAuthorizer seam (placeholder impl).
- Dedicated apps/realtime-gateway service.
- New dep: ws + @types/ws (gateway only).
- Redis Pub/Sub via ioredis (already a dep) — no new transport dep.
- REALTIME_PUBLISH_ENABLED gates all Redis-publisher wiring; default off => zero behaviour change.

## Workstreams
- [ ] WS-A  transport + @forge/realtime publisher/subscriber + config + scheduler/worker wiring
- [ ] WS-B  apps/realtime-gateway (protocol, auth, authz, origin, registry, backpressure, heartbeat, shutdown)
- [ ] WS-C  benchmark:websocket + failure experiments + docs

## Current: WS-A

## Files changed
(see git diff)

## Tests
(pending)

## Known risks / escalation watch
- ioredis subscriber-mode connection semantics
- ws upgrade Authorization header access -> may fall back to Sec-WebSocket-Protocol
- config tests may be exhaustive -> wider edit in WS-A
- backpressure: prefer explicit pending counter over ws.bufferedAmount
