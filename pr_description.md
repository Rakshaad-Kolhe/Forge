# PR 06: Redis Coordination Foundation

## Summary

Establishes Forge V2's Redis coordination foundation following **ADR-003 (Redis for Transient Distributed Coordination)**. This PR introduces the `@forge/redis` shared infrastructure package with connection management, active health checks, graceful shutdown, generic coordination primitives, TTL support, atomic operation support, JSON serialization helpers, structured logging, and comprehensive integration testing against real Redis.

Crucially:
- **PostgreSQL remains the sole durable source of truth.**
- **Redis serves strictly as a transient distributed coordination layer.**
- Losing, restarting, or flushing Redis will never corrupt or erase historical records in PostgreSQL.

---

## Architectural Changes & Primitives

```text
       ┌────────────────────────┐
       │       PostgreSQL       │
       │  AUTHORITATIVE STATE   │
       │   - Pipelines          │
       │   - Pipeline Runs      │
       │   - Jobs               │
       │   - Job Attempts       │
       │   - Audit & History    │
       └────────────────────────┘

                   ▲
                   │ (Authoritative reconciliation)
                   ▼

       ┌────────────────────────┐
       │         Redis          │
       │ TRANSIENT COORDINATION │
       │   - Job Queues (Future)│
       │   - Ephemeral Leases   │
       │   - Distributed Sync   │
       │   - Atomic Claims      │
       └────────────────────────┘
```

1. **Client Technology**: Selected `ioredis` (^5.6.0) for robust TypeScript support, native promise API, rich event-driven lifecycle (`connect`, `ready`, `close`, `reconnecting`, `error`), and atomic command execution.
2. **Configuration**: Extended `@forge/config` and `@forge/contracts` with `REDIS_URL` (default: `redis://127.0.0.1:6379`).
3. **Local Infrastructure**: Added `redis:7.2-alpine` to `docker-compose.yml` with health checks.
4. **Connection Lifecycle**: Created `createRedisClient(config, logger)` providing idempotent connection management, active `healthCheck()` via `PING`, and graceful shutdown (`close()`).
5. **Generic Coordination Primitives**: Implemented `get`, `set` (with `EX`, `PX`, `NX`, `XX`), `del`, `exists`, `expire`, and `ttl`.
6. **Atomic Operation Primitives**: Implemented `setNx` (mutual exclusion primitive), `incr`, `decr`, and Lua `eval` execution.
7. **Key Naming & Serialization**: Established `createRedisKey` (`forge:{namespace}:{...}`) and deterministic `serializeJson`/`deserializeJson` with type safety.
8. **Real Redis Integration Tests**: Verified against real Redis without mocks, using isolated run prefixes (`forge:test_{timestamp}_{rand}:*`) without destructive `FLUSHALL`.

---

## What is NOT Implemented (Strict Scope Boundaries)

In accordance with PR 06 constraints:
- **NO Job Queue implementation** (FIFO, priority queues, enqueue/dequeue belong to future queue PR).
- **NO Distributed locks or mutexes** (generic `setNx` is provided as a low-level primitive, not a distributed lock).
- **NO Worker leases or lease renewal loops**.
- **NO Pub/Sub event architecture or WebSocket fanout**.
- **NO Caching layer**.
- **NO Scheduler or Worker dispatch logic**.

---

## Verification Results

- `npm run lint`: **PASS** (0 errors, 0 warnings)
- `npm run format:check`: **PASS** (All matched files use Prettier style)
- `npm run typecheck`: **PASS** (`tsc -b` compiled all packages and applications)
- `npm test`: **PASS** (18 test suites, 133/133 tests passing, including 30 Redis tests)
- `npm run build`: **PASS** (All packages, shells, and Next.js built cleanly)
- Manual smoke test against real Redis: **PASS** (Connect -> PING -> SET -> GET -> EXPIRE -> TTL -> DEL -> verify absence -> Close)
