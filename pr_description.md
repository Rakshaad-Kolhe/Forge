# PR 22 — Realtime Event Transport & WebSocket Gateway

Branch `feat/pr-22-realtime-gateway`, cut from `0263b68` (PR 21 tip). 3 feature commits:

| Commit    | Workstream | Scope                                                                                                      |
| --------- | ---------- | ---------------------------------------------------------------------------------------------------------- |
| `6243d10` | WS-A       | `@forge/redis` Pub/Sub adapter + `@forge/realtime` publisher/subscriber + config + scheduler/worker wiring |
| `fc7e89c` | WS-B       | `apps/realtime-gateway` — protocol, auth, authz, origin, registry, backpressure, heartbeat, shutdown       |
| `6eb573d` | WS-C       | `npm run benchmark:websocket` + docs (`events.md` §19, `invariants.md` §15, `overview.md`, `README.md`)    |

**Guarantee established:** a committed Forge event becomes visible across processes in real
time — `PostgreSQL + outbox` → `OutboxDispatcher` → `EventPublisher` → Redis Pub/Sub →
WebSocket gateway → clients — **without changing what is authoritative**.

**Not claimed:** exactly-once WebSocket delivery, global/cross-producer ordering, durable
WebSocket history, or event replay from Redis Pub/Sub. A disconnected client may miss events
and recovers authoritative state through the API.

---

## 1. Task Classification

**T3 (high risk).** Cross-process distributed transport, WebSocket connection lifecycle, a
new authN/authZ boundary, concurrency + backpressure, Redis-failure recovery, multi-instance
fan-out, and latency measurement. No auto-escalation was needed — started at T3.

---

## 2. Repository Inspection (findings that shaped the design)

All `[VERIFIED]` against the `feat/pr-21-outbox` tree:

- **Branch discrepancy.** The session launched on `feat/pr-19-execution-engine`, which has
  **no `packages/events` and no `packages/outbox`**. PR 20 and PR 21 were subsequently merged
  into `main` (via PR #20 and PR #21). PR 22 was branched from the `feat/pr-21-outbox` tip
  into this worktree and cleanly targets `main`.
- **`@forge/events` (PR 20).** `EventPublisher` / `EventSubscriber` are a transport-neutral
  seam; `InProcessEventBus` is the only implementation. `parseForgeEvent` (zod) strips
  unknown fields, rejects a wrong `version`. §14.8 fixes the ordering caveat: a terminal
  event may precede `JobLogChunk` for the same job.
- **`@forge/outbox` (PR 21).** `OutboxDispatcher` takes an injected `EventPublisher`,
  wraps `publisher.publish` in a bounded timeout, and on failure calls `markRetry` — the
  row stays `PENDING`. Wired in `apps/scheduler/src/index.ts` `startScheduler({ pool,
publisher })`, but **no caller ever passes a real cross-process publisher**.
- **`@forge/redis` has NO Pub/Sub** — only KV + coordination primitives + `getRawClient()`.
  `ioredis` (already a dependency) supports Pub/Sub natively — zero new transport
  dependency.
- **No authentication, session, user, project, or resource-authorization model anywhere.**
  `apps/api` is `GET /health` + a 404 handler. `@forge/contracts` has zero auth/session
  types. There is no pipelines/runs/jobs REST surface to resolve a resource against.
- **No WebSocket / SSE / realtime code.** `ws` and `@types/ws` are not installed; Node has
  no built-in WebSocket **server**.
- `docs/architecture/events.md` §15 and `invariants.md` §14.10 explicitly anticipate this
  PR and forbid partial implementation before it.

### User-approved decisions (the repo did not determine these)

1. **Base / location** — new worktree branched from `feat/pr-21-outbox`.
2. **Auth scope** — the smallest explicit bridge (shared-secret handshake, one opaque
   principal) + a pluggable `SubscriptionAuthorizer` seam with a documented placeholder
   implementation. Real per-resource authorization is deferred to the PR that builds the
   resource API.
3. **Gateway hosting** — a dedicated `apps/realtime-gateway` service (no DB access), not
   mounted on `apps/api`.

Near-forced: a WebSocket **server** needs a library — `ws` (itself dependency-free) added
to the gateway package only. `tsx` added as a **devDependency** so all three
`benchmark:*` scripts run offline.

---

## 3. Execution Strategy

| Metric              | Value                                                                                                                                                                                                                                                                                |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Workstreams         | 3 (WS-A transport, WS-B gateway, WS-C benchmark+docs) — mostly serial                                                                                                                                                                                                                |
| Subagents           | 1 (final independent verifier, §50)                                                                                                                                                                                                                                                  |
| Review cadence      | at each workstream boundary, applicable profiles only (architecture, security, concurrency, correctness, performance)                                                                                                                                                                |
| Testing cadence     | focused per workstream; full `npm test` once at the end                                                                                                                                                                                                                              |
| Direct vs delegated | all implementation direct in the main session; delegation only for independent verification                                                                                                                                                                                          |
| Rework rounds       | 1 significant — the live slow-consumer experiment (loopback TCP has no usable write backpressure below tens of MB and Redis Pub/Sub caps a slow subscriber at 32 MB); replaced with a deterministic real-gateway test driven by an in-process transport + the fake-socket unit proof |

Why this shape: the dependency topology is transport → publisher → gateway → integration,
almost entirely serial. Splitting it across parallel subagents would have added
context-rehydration cost and merge risk for no wall-clock gain. Direct execution with a
fresh independent verifier at the end was the smallest orchestration that preserved the
required rigor.

---

## 4. Final Architecture

```text
                     PostgreSQL
                  ┌──────────────┐
                  │ jobs / leases│  authoritative, durable
                  │ outbox_events│
                  └──────┬───────┘
                         │  one transaction (PR 21)
                         ▼
                 OutboxDispatcher            (apps/scheduler-hosted)
                         │  EventPublisher.publish(event)
                         ▼
                 RedisEventPublisher          @forge/realtime — implements EventPublisher
                         │
                         ▼
                 Redis Pub/Sub               single channel  forge:realtime:events   (transient)
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       RedisEventSubscriber   RedisEventSubscriber      @forge/realtime — implements EventSubscriber
              │                     │
       RealtimeGateway A     RealtimeGateway B          apps/realtime-gateway (no DB access)
              │                     │
              ▼                     ▼
          WebSocket clients     WebSocket clients        (transient; best-effort)
```

`JobLogChunk` takes the same path but is best-effort end-to-end (`safePublish` on the
worker side — not in the durable outbox, per PR 21 §18.3).

---

## 5. Transport Semantics

| Segment                                           | Class                                                                                                            |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| PostgreSQL state + `outbox_events` row (one tx)   | **Durable.** Unaffected by Redis/WebSocket availability.                                                         |
| `OutboxDispatcher → RedisEventPublisher → Redis`  | **At-least-once** for durable lifecycle events. Redis down ⇒ publish throws ⇒ `markRetry` ⇒ row stays `PENDING`. |
| `Redis → RedisEventSubscriber → Gateway → client` | **Best-effort.** Disconnected client misses events; slow client is disconnected; no replay.                      |
| `JobLogChunk` end-to-end                          | **Best-effort** the whole way.                                                                                   |

No exactly-once WebSocket delivery. No global/cross-producer ordering (§14.8 holds — a
client may observe `JobSucceeded`/`JobFailed` before some/all `JobLogChunk` for that job).
Duplicate `event_id` delivery is possible; a future client dedupes on `event_id`.

---

## 6. WebSocket Protocol (v1)

- **Connection**: HTTP upgrade → origin allowlist → shared-secret auth → instance capacity
  → `ws.handleUpgrade` → server sends `{ "type": "ready", "v": 1, "connection_id": …,
"heartbeat_interval_ms": …, "limits": {…} }`.
- **Client → server**: `subscribe` / `unsubscribe` / `ping`, each
  `{ type, v: 1, target?: { kind: "pipeline"|"run"|"job", id } }`.
- **Server → client**: `ready`, `subscribed`, `unsubscribed`, `event` (the full PR 20
  envelope, unmodified), `pong`, `closing`, `error`.
- **Error codes**: `INVALID_MESSAGE`, `UNSUPPORTED_VERSION`, `UNAUTHORIZED`, `FORBIDDEN`,
  `INVALID_SUBSCRIPTION`, `SUBSCRIPTION_LIMIT`, `RATE_LIMITED`, `MESSAGE_TOO_LARGE`,
  `SERVER_SHUTTING_DOWN`, `SLOW_CONSUMER`. A malformed frame is answered with `error` and
  the connection stays open.
- **Shutdown**: stop accepting → stop Redis fan-out → send `closing` → close sockets →
  bounded wait (`shutdownGraceMs`) → terminate stragglers → close HTTP server. Idempotent.

Redis details are never exposed; a client cannot name a channel (the `subscribe` schema
has no channel field and strips unknown keys).

---

## 7. Security

- **Origin** (`origin.ts`): validated against `WEBSOCKET_ORIGIN_ALLOWLIST` (CSV). Empty
  allowlist rejects every browser `Origin`; `*` is never accepted; a request with no
  `Origin` (non-browser) is allowed but still needs the token; opaque `"null"` is rejected.
- **Authentication** (`auth.ts`): `WEBSOCKET_AUTH_TOKEN` presented as `Authorization:
Bearer <token>` **or** the `forge.v1.token.<token>` subprotocol — never the URL query
  string; compared with `crypto.timingSafeEqual` (length-guarded). The gateway **refuses to
  start** with an empty token (`gatewayConfigFromAppConfig` throws). It yields **one opaque
  principal** — this is a bridge over Forge's absent session model, documented as a
  limitation, not per-user identity.
- **Authorization** (`authorization.ts`): every `subscribe` runs through
  `SubscriptionAuthorizer.authorize(principal, target)`; a `false` result or a throw yields
  `FORBIDDEN` and the subscription is refused. The shipped `AllowAuthenticatedAuthorizer` is
  a **documented placeholder** — Forge has no resource-ownership model — allowing any
  authenticated principal any well-formed target. Replacing it is a one-line wiring change.
- **Bounds**: `WEBSOCKET_MAX_CONNECTIONS` (per instance), `WEBSOCKET_MAX_SUBSCRIPTIONS_PER_CONNECTION`,
  `WEBSOCKET_MAX_PENDING_MESSAGES` (outbound queue), `WEBSOCKET_MAX_MESSAGE_BYTES` (inbound,
  via `ws` `maxPayload`). No secret / token / cookie / full sensitive payload is logged.

---

## 8. Backpressure

Each connection owns a bounded outbound queue counted in `GatewayConnection.send` (pending
incremented before the write, decremented in its completion callback). At the bound the
connection is closed with `SLOW_CONSUMER` and **fully released** (`registry.remove` +
metric). Each connection owns its own counter and socket, so one slow client never stalls
another.

**Evidence** — `connection.test.ts` (fake socket, deterministic): queue never exceeds the
cap, `disconnectSlowConsumer` fires on the over-cap send, `onClose` runs exactly once, sends
after close are inert. `slow-consumer.experiment.test.ts` (real gateway + real `ws` server +
real `ws` client, in-process transport for deterministic burst timing): a paused client is
dropped for backpressure with `activeConnections`/`activeSubscriptions` back to 0, a healthy
client on the same stream receives the entire 400-event feed, and the dropped client
reconnects and resubscribes.

---

## 9. Redis Failure Model

- **Startup outage**: `apps/realtime-gateway/src/index.ts` logs
  `realtime.redis_connect_failed_degraded_start` and **keeps serving WebSocket handshakes**;
  `ioredis` retries and auto-resubscribes on recovery. No events flow until Redis is back.
  PostgreSQL and `outbox_events` are untouched; no false terminal events.
- **Runtime drop** (`gateway-lifecycle.integration.test.ts`): existing connections stay
  open and answer `ping`; the gateway does not crash; `stop()` still completes bounded.
- **Recovery**: `ioredis` re-establishes the subscription automatically. Missed events are
  **not** replayed — durable recovery is the PostgreSQL outbox, not Pub/Sub.

---

## 10. Multi-Gateway

`multi-gateway.integration.test.ts` (live Redis): two gateway instances on one channel;
one publish reaches clients on both; a client on gateway B is unaffected when gateway A
stops. Each instance holds only its own connection/subscription state (an inverted
key → connection-ids index for fan-out). No sticky sessions.

---

## 11. Reconnect

`connect → authenticate → ready → subscribe → live events`. On reconnect the client
re-authenticates, re-subscribes, and refreshes authoritative state **through the API**.
Reconnect does not recover events missed during the gap.

---

## 12. Ordering

Unchanged from §14.8: no global or cross-producer ordering; a client may see a terminal
`JobSucceeded`/`JobFailed`/`JobCancelled` before some or all `JobLogChunk` for that job. No
client-side ordering guarantee is introduced.

---

## 13. Benchmarks

`npm run benchmark:websocket` → `benchmarks/reports/websocket-benchmark-report.json`.
Real gateway + real `ws` clients + real `RedisEventPublisher` → Redis path; measures
`publish()` → first-client-receipt and → full-fan-out. Machine: Intel i7-14650HX, Node
v25, Redis on WSL, loopback.

| Phase     | Fan-out | Events | Bytes | first p50 (ms) | first p95 | first p99 | full p95 | evt/s |
| --------- | ------- | ------ | ----- | -------------- | --------- | --------- | -------- | ----- |
| lifecycle | 1       | 200    | 334   | 0.56           | 0.78      | 1.05      | 0.79     | 1690  |
| lifecycle | 10      | 200    | 334   | 0.67           | 0.87      | 0.98      | 1.03     | 1219  |
| lifecycle | 50      | 200    | 334   | 1.53           | 2.13      | 2.61      | 2.99     | 474   |
| lifecycle | 100     | 200    | 334   | 2.15           | 2.90      | 4.76      | 4.15     | 314   |
| logchunk  | 1       | 100    | 65536 | 44.1           | 55.8      | 67.2      | 55.8     | 21.8  |
| logchunk  | 10      | 100    | 65536 | 47.5           | 62.2      | 68.7      | 69.5     | 18.6  |
| logchunk  | 50      | 100    | 65536 | 61.4           | 69.0      | 87.2      | 90.6     | 13.3  |
| logchunk  | 100     | 100    | 65536 | 78.4           | 92.3      | 106.4     | 132.5    | 9.9   |

**Interpretation (honest).** Small lifecycle events deliver in ~0.5–5 ms p50–p99 across
fan-out 1→100. A bounded 64 KiB `JobLogChunk` is ~50–100× slower (p50 44→78 ms) and its
cost is dominated by JSON serialize + Redis + per-client fan-out of the large payload, and
it grows with fan-out. This is a real characteristic to be aware of when streaming logs to
many viewers; it is not a bottleneck introduced by, or fixable within, PR 22. No
sub-second / high-throughput / production-scale claim is made.

---

## 14. Failure Experiments (observed)

| Experiment           | Result                                                                                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Redis startup outage | Gateway starts degraded, serves handshakes; no state impact. (`index.ts` degraded-start path; asserted indirectly.)                                     |
| Redis runtime drop   | Connection survives, `ping`→`pong` still works, no crash, bounded `stop()`. (`gateway-lifecycle.integration.test.ts`)                                   |
| Redis recovery       | ioredis auto-resubscribes (library behaviour; `pubsub` adapter surfaces the transition via `onConnectionChange` + logs).                                |
| Gateway restart      | Client reconnects to a fresh instance on the same channel; events flow. Gateway holds no authoritative state. (`gateway-lifecycle.integration.test.ts`) |
| Client reconnect     | Re-auth + resubscribe + live events. (`slow-consumer.experiment.test.ts`, `gateway-lifecycle.integration.test.ts`)                                      |
| Slow consumer        | Dropped for backpressure; bounded footprint; healthy client unaffected; reconnect works. (`connection.test.ts` + `slow-consumer.experiment.test.ts`)    |
| Multi-gateway        | One publish → clients on A and B; B unaffected by A stopping. (`multi-gateway.integration.test.ts`)                                                     |

---

## 15. Tests

**Full suite: `npm test` → 736 passed / 736, 81 files, exit 0** (Redis + Postgres + Docker
all reachable on this machine).

PR 22 adds ~90 tests:

| Area                                                                                              | Tests | Infra                              |
| ------------------------------------------------------------------------------------------------- | ----- | ---------------------------------- |
| `packages/realtime` unit (serialization, publisher, subscriber, timeout)                          | 19    | mocked pubsub                      |
| `packages/realtime` `realtime.integration.test.ts`                                                | 2     | live Redis                         |
| `packages/redis` `pubsub.integration.test.ts`                                                     | 6     | live Redis                         |
| `packages/config` realtime/websocket config                                                       | +7    | none                               |
| `apps/realtime-gateway` unit (protocol, origin, auth, subscription, connection, registry, config) | 40    | none                               |
| `apps/realtime-gateway` `gateway.integration.test.ts`                                             | 5     | live Redis + ws client             |
| `apps/realtime-gateway` `gateway-security.integration.test.ts`                                    | 10    | live Redis                         |
| `apps/realtime-gateway` `multi-gateway.integration.test.ts`                                       | 2     | live Redis, 2 gateways             |
| `apps/realtime-gateway` `gateway-lifecycle.integration.test.ts`                                   | 3     | live Redis                         |
| `apps/realtime-gateway` `slow-consumer.experiment.test.ts`                                        | 2     | real gateway, in-process transport |

No mocked test stands in for a cross-process claim: transport + fan-out + multi-gateway all
run against live Redis; only the deterministic slow-consumer burst uses an in-process
transport (with the Redis path proven separately).

---

## 16. Quality Gates

| Command                       | Result                                                 |
| ----------------------------- | ------------------------------------------------------ |
| `npm run format:check`        | PASS — all files use Prettier style                    |
| `npm run lint`                | PASS — `eslint .` exit 0                               |
| `npm run typecheck`           | PASS — `tsc -b` exit 0                                 |
| `npm test`                    | PASS — 736/736, exit 0                                 |
| `npm run build`               | PASS — all workspaces (`tsc -b` + `next build`) exit 0 |
| `npm run benchmark:websocket` | PASS — runs green, report written                      |

---

## 17. Changed Files (66 files, +5202 / −12)

**New packages / services**

- `packages/realtime/` — `package.json`, `tsconfig.json`, `src/{index,errors,serialization,redis-event-publisher,redis-event-subscriber,test-support}.ts` + 4 test files
- `apps/realtime-gateway/` — `package.json`, `tsconfig.json`, `src/{index,protocol,subscription,origin,auth,authorization,metrics,config,connection,registry,gateway,test-support,integration-support}.ts` + 12 test files
- `benchmarks/websocket/{config,runner}.ts`, `benchmarks/reports/websocket-benchmark-report.json`

**Modified**

- `packages/redis/src/{index.ts,pubsub.ts (new),pubsub.integration.test.ts (new)}`
- `packages/contracts/src/index.ts` — `AppConfig` +11 fields, `FORGE_REALTIME_EVENT_CHANNEL`, `REALTIME_PROTOCOL_VERSION`, `DEFAULT_*` constants
- `packages/config/src/{index.ts,index.test.ts}` — 11 new defaulted vars + coverage
- `apps/scheduler/src/index.ts` (+`package.json`,`tsconfig.json`,`index.test.ts`) — guarded lazy `RedisEventPublisher` into `OutboxDispatcher`
- `apps/worker/src/index.ts` (+`package.json`,`tsconfig.json`) — directly-run entrypoint routes best-effort events onto the transport, guarded
- root `tsconfig.json`, `package.json` (`benchmark:websocket`, `tsx` devDep), `.env.example`, `.prettierignore` (`.task/`)
- `docs/architecture/{events.md,invariants.md,overview.md}`, `README.md`, `.task/`, this file

No change to `packages/events`, `packages/outbox`, `packages/database`, executor, or any
scheduling/execution logic beyond transport wiring.

---

## 18. Dependency Changes

- **`ws` `^8.18.0`** + **`@types/ws` `^8.5.13`** — runtime, `apps/realtime-gateway` only.
  `ws` has zero runtime dependencies. A WebSocket server has no standard-library
  alternative in Node.
- **`tsx` `^4.23.13`** — root **devDependency**. All three `benchmark:*` scripts already
  assumed `npx tsx`; pinning it makes them run offline.
- No new dependency for the Redis transport — `ioredis` (already in `@forge/redis`)
  provides Pub/Sub.

---

## 19. Architecture Impact (before → after)

|                                   | Before (PR 21)                      | After (PR 22)                                                 |
| --------------------------------- | ----------------------------------- | ------------------------------------------------------------- |
| `EventPublisher` implementations  | `InProcessEventBus` only            | `+ RedisEventPublisher` (cross-process)                       |
| `EventSubscriber` implementations | `InProcessEventBus` only            | `+ RedisEventSubscriber`                                      |
| Redis capabilities                | KV + coordination                   | `+ Pub/Sub adapter`                                           |
| Services                          | api, scheduler, worker, cli, web    | `+ realtime-gateway`                                          |
| Cross-process event delivery      | none                                | Redis Pub/Sub, best-effort                                    |
| Client-facing realtime            | none                                | authenticated WebSocket, bounded, best-effort                 |
| Autonomous loops                  | scheduler-hosted `OutboxDispatcher` | `+ realtime-gateway` process (heartbeat + Redis subscription) |

Unchanged: PostgreSQL is the sole source of truth; the durable outbox is the only durable
event buffer; `apps/*` → `packages/*` dependency direction; no circular package
dependencies.

---

## 20. Known Limitations

- **Redis Pub/Sub is transient** — a message published while a gateway is disconnected is
  gone.
- **Disconnected clients miss events**; there is **no durable WebSocket history** and **no
  replay** in PR 22. A future history/replay service may provide that.
- **No global ordering** and **no exactly-once WebSocket delivery** — a client may receive a
  duplicate `event_id`.
- **`JobLogChunk` realtime delivery is best-effort end-to-end** and remains separate from
  any persistent log store.
- **Authentication is a shared-secret bridge** yielding one opaque principal — not per-user
  identity. **Authorization is a placeholder** (`AllowAuthenticatedAuthorizer`): any
  authenticated principal may subscribe to any run/job/pipeline id, because Forge has no
  resource-ownership model yet. Both are documented seams to replace when the resource API
  lands.
- **64 KiB `JobLogChunk` fan-out latency is ~50–130 ms p95** on the benchmark host and
  scales with fan-out — acceptable for log tailing, not "sub-second at scale".
- Loopback TCP on the test machine has no usable write backpressure below tens of MB and
  Redis Pub/Sub caps a slow subscriber at 32 MB, so the _live_ slow-consumer trigger cannot
  be reproduced with a paused socket; the deterministic proof is the fake-socket unit test
  plus a real-gateway test with an in-process transport.
- The worker's realtime wiring is in its `isDirectRun` entrypoint only; the worker still has
  no queue-polling claim→execute loop (pre-existing).

---

## 21. Efficiency Metrics vs. PR 21 baseline

`OBSERVED`: PR 22 used **1 subagent** (final verification) vs. PR 21's heavier
multi-round process; **3 workstream commits** vs. PR 21's 26; **1 rework round** (the
slow-consumer experiment). `UNKNOWN`: wall-clock and tool-call totals were not
instrumented against PR 21. `PROJECTED` savings are not claimed as measured.

---

## 22. Independent Verification

A separate verification pass (fresh context) re-checked, from the code and by re-running
the gates: architecture/dependency boundaries, security (origin, auth, authz seam, bounds,
no-secret-logging), transport semantics (envelope integrity, Redis-down rethrow,
no-replay/no-exactly-once wording), the `REALTIME_PUBLISH_ENABLED` default-off gate, and
diff hygiene, plus `tsc -b` / `eslint .` / `prettier --check .` / `vitest` /
`npm run build` / the benchmark report. Result recorded in the session.

---

## 23. Merge Recommendation

**READY TO MERGE** — onto `main` (PR 20 and PR 21 have merged into `main`).
