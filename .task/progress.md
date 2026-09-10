# PR 22 — Realtime Event Transport & WebSocket Gateway  (DONE — verifying)

Objective: cross-process realtime event delivery. Outbox -> EventPublisher -> Redis Pub/Sub
-> WebSocket gateway -> clients. PostgreSQL stays authoritative; Redis + WS are transient.

Base: feat/pr-22-realtime-gateway (worktree .claude/worktrees/pr-22-realtime-gateway,
branched from feat/pr-21-outbox @ 0263b68).

## Workstreams
- [x] WS-A  @forge/redis pubsub + @forge/realtime publisher/subscriber + config + scheduler/worker wiring  (commit 6243d10)
- [x] WS-B  apps/realtime-gateway (protocol, auth, authz, origin, registry, backpressure, heartbeat, shutdown)  (commit fc7e89c)
- [x] WS-C  benchmark:websocket + docs (events.md §19, invariants.md §15, overview.md, README.md)

## Verification
- tsc -b: green.  eslint .: green.  prettier --check .: green.
- vitest apps/realtime-gateway: 60/60 (unit + live-Redis integration + experiments).
- benchmark:websocket: runs green; report at benchmarks/reports/websocket-benchmark-report.json.
  lifecycle first-receipt p50 ~0.5-2.1ms / p99 ~1-4.8ms (fan-out 1..100);
  JobLogChunk 64KiB first-receipt p50 ~44-78ms / p99 ~67-106ms.
- Full `npm test`: Redis + Postgres reachable (WSL); Docker daemon NOT available =>
  executor/worker/scheduler *live-container* tests SKIPPED-BY-INFRA (pre-existing; PR 22
  touches no executor/Docker code).

## Decisions (user-approved)
- Minimal shared-secret handshake + pluggable SubscriptionAuthorizer seam (placeholder impl).
- Dedicated apps/realtime-gateway service.
- New runtime dep: ws + @types/ws (gateway only). New devDep: tsx (benchmark runnability).
- REALTIME_PUBLISH_ENABLED gates all publisher wiring; default off => zero behaviour change.
