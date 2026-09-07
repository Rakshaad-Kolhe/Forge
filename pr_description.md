# PR 08: Worker Registration & Heartbeat

## Summary

This pull request implements **PR 08: Worker Registration & Heartbeat** of Forge V2, establishing the worker-side distributed infrastructure for worker identity, durable metadata registration in PostgreSQL, transient liveness coordination in Redis with atomic TTL expiration, crash/stale detection, and graceful lifecycle management.

In accordance with **ADR-002 (PostgreSQL as Authoritative Source of Truth)** and **ADR-003 (Redis for Transient Distributed Coordination)**, worker coordination is strictly decoupled between durable capability persistence and high-frequency liveness checks.

---

## Architectural Separation

```text
┌───────────────────────────────────────────────────────────────────┐
│                       Distributed Workers                         │
└────────┬──────────────────────────────────────────────────┬───────┘
         │ 1. Registration / Deregistration                 │ 2. High-Frequency Heartbeat
         │    (Infrequent: boot, drain, shutdown)           │    (Periodic: 5-15s, lightweight)
         ▼                                                  ▼
┌───────────────────────────────┐                  ┌───────────────────────────────┐
│          PostgreSQL           │                  │             Redis             │
│   (Authoritative Registry)    │                  │      (Transient Liveness)     │
├───────────────────────────────┤                  ├───────────────────────────────┤
│ • Table: `workers`            │                  │ • Key: `forge:worker:{id}:hb` │
│ • Durable identity (workerId) │                  │ • Expiring string (TTL 15s)   │
│ • Hardware resources & specs  │                  │ • Zero database I/O on tick   │
│ • Executor capabilities       │                  │ • Auto-expires on crash       │
│ • Preserved indefinitely      │                  │ • Ephemeral state only        │
└───────────────────────────────┘                  └───────────────────────────────┘
```

---

## What Was Implemented

### 1. Database Persistence Layer (`packages/database`)

- **Migration `002_worker_registry.sql`**:
  - Adds the `workers` table with `id`, `status`, `hostname`, `executors` (JSONB), `resources` (JSONB), `registered_at`, and `updated_at`.
  - Enforces `chk_workers_status CHECK (status IN ('STARTING', 'READY', 'DRAINING', 'OFFLINE'))`.
  - Registered in `migrator.ts` both in `MIGRATIONS` and `resetDatabase`.
- **`WorkerRepository` Contract & `PgWorkerRepository` Implementation**:
  - Typed interface for `save`, `findById`, `list`, `updateStatus`, and `delete`.
  - Implements idempotent upsert via `ON CONFLICT (id) DO UPDATE`.
  - Preserves historical records; stale workers are never deleted.

### 2. Configuration & Contracts (`packages/contracts`, `@forge/config`, `.env.example`)

- Added `workerHeartbeatIntervalMs` (default: 5000ms) and `workerHeartbeatTtlSeconds` (default: 15s).
- Zod schema validation ensuring `(workerHeartbeatTtlSeconds * 1000) > workerHeartbeatIntervalMs`.
- Documented in `.env.example`.

### 3. Worker Registry Infrastructure (`packages/worker-registry`)

- **Worker Types (`src/types.ts`)**:
  - Branded `WorkerId` with `createWorkerId()` validator.
  - Lifecycle statuses: `STARTING`, `READY`, `DRAINING`, `OFFLINE`.
  - Liveness states: `ALIVE`, `STALE`.
  - Hardware capacity: `WorkerResources` (`cpuCores`, `memoryBytes`, optional `gpuCount`).
  - Execution capabilities: `WorkerCapabilities` (`executors`).
  - Ephemeral heartbeat: `WorkerHeartbeat` payload.
  - Unified view: `WorkerInfo` combining PostgreSQL metadata with Redis real-time liveness.
- **Heartbeat Store (`src/heartbeat.ts`)**:
  - Manages `forge:worker:{workerId}:heartbeat` keys using Redis `setJson(key, payload, { ttlSeconds })`.
  - High-frequency heartbeat renewals write exclusively to Redis with atomic TTL (`SET ... EX`).
  - Zero database queries generated during normal heartbeat ticks.
- **Worker Registry Coordinator (`src/registry.ts`)**:
  - Coordinates PostgreSQL durable writes and Redis transient keys.
  - `register()`: Idempotent upsert in PostgreSQL + initial Redis heartbeat key.
  - `heartbeat()`: Fast-path TTL renewal in Redis.
  - `deregister()`: Deletes Redis heartbeat key immediately and transitions PostgreSQL status to `OFFLINE`.
  - `getWorker()`: Composes PostgreSQL row with Redis liveness.
  - `listWorkers()`: Supports filtering by lifecycle status and liveness (`ALIVE` / `STALE`).

### 4. Worker Service Shell Integration (`apps/worker`)

- Inspects system compute capacity (`os.cpus().length`, `os.totalmem()`, `os.hostname()`).
- Automated registration on startup.
- Unref'd background periodic timer renewing heartbeat.
- Graceful deregistration and cleanup on `shell.stop()`, `SIGTERM`, and `SIGINT`.

### 5. Architectural Documentation (`docs/architecture/workers.md`)

- Complete architectural specification detailing durable vs. transient roles, state machines, crash semantics, and non-guarantees.
- Updated `docs/architecture/overview.md` and `README.md`.

---

## Architectural Invariants & Non-Guarantees (PR 08 Scope Boundary)

- **Heartbeat is NOT a Lease**: A worker heartbeat indicates node liveness. It does not grant or extend ownership of any specific job or pipeline.
- **No Job Claiming or Worker Dispatch**: Workers do NOT dequeue jobs or execute tasks in this PR. Scheduler-driven dispatch and container runners remain planned for subsequent PRs.
- **Stale Workers are Never Deleted**: Crashed or disconnected workers remain in PostgreSQL for auditability and post-mortem analysis.

---

## Verification & Testing

### Automated Test Suites

1. **Config Tests** (`packages/config`):
   - Validates interval, TTL, and relationship constraints (`(TTL * 1000) > interval`).
2. **Database Integration Tests** (`packages/database`):
   - All 30 tests pass, including migration application and rollback.
3. **Worker Registry Unit Tests** (`packages/worker-registry`):
   - 10 unit tests verifying ID branding, input validation, and heartbeat key naming.
4. **Worker Registry Real Integration Tests** (`packages/worker-registry`):
   - 7 integration tests against live PostgreSQL and Redis instances:
     - Worker registration and durable persistence.
     - Idempotent repeated registration.
     - Heartbeat TTL renewal without PostgreSQL writes.
     - Crash simulation and TTL expiry (STALE detection with preserved DB record).
     - Worker reconnection and revival.
     - Graceful deregistration (status OFFLINE, Redis key deleted).
     - Concurrent worker isolation and multi-worker filtering.
5. **Worker Service Shell Tests** (`apps/worker`):
   - Standalone startup and stop.
   - Registry coordination, background timer, and graceful shutdown.
6. **Full Monorepo Verification**:
   - `npm run typecheck`: Strict TypeScript compiler checks across all workspaces pass cleanly.
   - `npm run lint`: All ESLint checks pass.
   - `npm run format:check`: Formatting clean.
   - `npm run build`: Production build of all packages and applications succeeds.
