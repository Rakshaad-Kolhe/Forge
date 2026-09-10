# Forge V2

Forge V2 is a self-hosted distributed CI/CD orchestration engine.

This repository is currently at **PR 21: Durable Transactional Outbox**.

---

## Current Status

### Implemented

- **Repository Architecture**: Monorepo layout using standard NPM workspaces (`apps/*`, `packages/*`).
- **TypeScript Setup**: Strict TypeScript 5 with composite project references and shared compiler options.
- **Shared Packages**:
  - `@forge/contracts`: Shared data contracts, types, and interfaces (including `WorkerCapabilities`, `WorkerResources`, `JobRequirements`, `ScheduleDecision`, `JobPriority` constants `[-1000, 1000]`, `WorkerLease` ownership contracts, `Executor` / `ExecutionContext` / `ExecutionResult` abstractions, `RetryPolicy` / `BackoffPolicy` / `RetryDecision` specifications, `DeadLetterJob` / `DeadLetterReason` / `LeaseRecoveryOptions` / `RecoverExpiredLeasesResult` reliability models, and `QueueAgingConfig` / `EffectivePriorityInfo` fairness specifications).
  - `@forge/config`: Strongly typed runtime environment validation using Zod (including lease parameters, executor configurations, retry limits, backoff bounds, drain timeout limits, fairness aging interval, age bonus step, max age bonus ceilings, and schema refinements).
  - `@forge/logging`: Structured logger (human-readable in development, newline-delimited JSON in production).
  - `@forge/pipeline`: Core in-memory domain model (Pipelines, Runs, Jobs, Attempts, DAG resolution, State Machines, Job Execution Requirements, Job Priority validation, timestamps `createdAt` / `queuedAt`, pure deterministic capability/resource matching, pure retry evaluation via `evaluateRetry`, bounded exponential backoff with overflow protection, and attempt immutability).
  - `@forge/database`: PostgreSQL persistence layer (Connection pooling, schema migrations `001` through `008`, typed repositories including `PgDeadLetterRepository` and `PgOutboxRepository`, transactions with `tx.outbox` on every `TransactionContext`, state machine integrity enforcement, terminal state immutability, worker registry, persisted job requirements, priority index, `worker_leases` with partial unique index for single-active-lease exclusivity, `idx_jobs_retry_schedulable` partial index for high-throughput schedulable job discovery, `LeaseRecoveryService` with row-level locking `FOR UPDATE SKIP LOCKED`, durable `dead_letter_jobs` table, and the `outbox_events` transactional outbox table with fenced batch claiming).
  - `@forge/redis`: Redis coordination foundation (Connection management, health checks, low-level generic primitives, TTL, atomic operations, and real Redis integration tests).
  - `@forge/queue`: Redis-backed reliable FIFO job queue (At-least-once delivery, explicit acknowledgement, queue depth, in-flight visibility tracking, crash/unacknowledged recovery, and competing consumer coordination).
  - `@forge/worker-registry`: Distributed worker registration and liveness coordination (Durable worker metadata and hardware capacity in PostgreSQL, transient heartbeat state with TTL in Redis, crash/stale detection, graceful deregistration, and isolated lifecycle state machines decoupled from lease ownership).
  - `@forge/executor`: Sandboxed container execution engine implementing `Executor` with production-oriented `DockerExecutor`, ephemeral temporary workspace management, non-root user execution (`--user 1000:1000`), container isolation (no privileged mode, no host Docker socket mount, bridge networking), resource limit enforcement (CPU, memory, unverified GPU status), wall-clock timeout supervision (`docker stop` -> `docker kill`), bounded stdout/stderr capture with truncation protection, and guaranteed teardown in `finally` blocks.
  - `@forge/events`: Neutral, transport-independent typed event architecture — versioned `ForgeEventEnvelope` with Forge-generated immutable `event_id` (UUID v4) and domain correlation ids, discriminated `ForgeEvent` union with compile-time exhaustiveness, zod validation (`parseForgeEvent`, unknown-field stripping, unknown-version rejection), `EventPublisher` / `EventSubscriber` seam, `InProcessEventBus` (subscriber-failure isolation, explicit idempotent shutdown, no global singleton, no deduplication), and post-execution `JobLogChunk` derivation from the bounded PR 19 capture. Notifications of committed state transitions only — at-least-once, no exactly-once delivery, no global ordering; PostgreSQL stays authoritative. Lifecycle events tied to a `jobs` / `worker_leases` transition are now recorded durably via the PR 21 transactional outbox (see `@forge/outbox`); `JobLogChunk` / `WorkerHeartbeat` / `WorkerRegistered` stay best-effort.
  - `@forge/outbox`: Durable event transport — `OutboxDispatcher` polls the `outbox_events` table, fenced-claims a bounded batch (`claim_token` regenerated per claim/reclaim), publishes each event through the `EventPublisher` seam under a bounded timeout, and checkpoints `PUBLISHED` / retries with jitter-free exponential backoff / marks `DEAD` only after the configured genuine transport rejections. Conservative `PUBLISHED`-only retention (default 7 days; `DEAD` retained for manual triage). At-least-once, no exactly-once delivery; consumers must tolerate duplicates. Hosted by `apps/scheduler` when a database pool and publisher are configured.
  - `@forge/realtime`: Transient cross-process realtime transport (PR 22) — composes behind the PR 20 seam as `RedisEventPublisher implements EventPublisher` and `RedisEventSubscriber implements EventSubscriber` over a single logical Redis Pub/Sub channel (`forge:realtime:events`). Re-runs `parseForgeEvent` on the wire (envelope never mutated; unknown fields stripped; wrong version rejected). Redis is transient: a publish failure from the outbox dispatcher leaves the durable `outbox_events` row `PENDING` for retry. No replay, no exactly-once, no global ordering.
- **Service Shells & Applications**:
  - `apps/api`: Express HTTP server exposing only `GET /health`.
  - `apps/scheduler`: Task scheduler service (`@forge/scheduler`) providing operational eligibility evaluation (`READY + ALIVE`), exclusion of `DRAINING` workers, deterministic worker selection policy (`DeterministicFirstEligible`), baseline priority scheduling policy (`HighestPriorityFirstPolicy`), starvation-prevention queue aging policy (`FairAgingPriorityPolicy` computing dynamic effective priority with bounded age bonus), virtual time injection across ordering and placement, canonical alphanumeric tie-breaking, retry fairness reset invariant (`nextAttemptAt` anchor), non-blocking unschedulable semantics, batch evaluation, unacknowledged queue recoverability, atomic distributed worker lease acquisition via PostgreSQL, due retry job discovery (`scheduleDueJobs`) with non-blocking backoff awareness (`RETRY_BACKOFF_ACTIVE`), integrated lease recovery loop (`recoverExpiredLeases`, `startRecoveryLoop`), and opt-in best-effort typed lifecycle event emission (`JobClaimed` on lease acquisition, `WorkerLost` per reconciled lease) after the authoritative commit.
  - `apps/worker`: Worker daemon with automated registration, capability reporting, periodic heartbeat renewal, lease lifecycle management (`claimJob`, `renewLease`, `releaseLease`), containerized job execution (`executeJob`) with active lease validation, periodic lease renewal, definitive lease-loss abort protection, pure retry policy evaluation, transactional PostgreSQL persistence, per-attempt lease isolation enabling worker hopping, three-phase shutdown lifecycle (`READY -> DRAINING -> OFFLINE`), immediate claim/execution rejection during drain, bounded in-flight execution drain supervision, and opt-in best-effort typed lifecycle event emission (`WorkerRegistered`, `WorkerHeartbeat`, `JobStarted`, derived `JobLogChunk`, terminal `JobSucceeded` / `JobFailed` / `JobCancelled`, and `JobQueued` on retry re-queue) after each authoritative persist.
  - `apps/cli`: CLI executable supporting `--help` and `--version`.
  - `apps/realtime-gateway`: Dedicated WebSocket gateway service (PR 22) — subscribes to the realtime Redis channel and fans committed `ForgeEvent`s out to authenticated WebSocket clients. Versioned client protocol (`ready` / `subscribe` / `unsubscribe` / `event` / `pong` / `error`), handshake pipeline (origin allowlist → shared-secret auth via `Authorization: Bearer` or the `forge.v1.token.<token>` subprotocol → instance capacity), a pluggable `SubscriptionAuthorizer` seam (shipped `AllowAuthenticatedAuthorizer` is a documented placeholder pending a resource-ownership model), bounded connections / subscriptions / outbound queue with slow-consumer disconnect, ping/pong liveness (distinct from the worker heartbeat), and bounded idempotent graceful shutdown. No database access. Realtime delivery is best-effort — a disconnected client recovers authoritative state through the API.
  - `apps/web`: Next.js landing page displaying architectural boundaries.
- **Testing Foundation**: Vitest test runner configured with automated tests for config, logging, CLI, API health, pipeline domain core, capability/resource matching, job priority validation, PostgreSQL persistence, Redis coordination, FIFO job queue, worker registry, worker service shell, scheduler selection policies, priority ordering, worker lease lifecycle & concurrency races, Docker executor unit & live container integration, worker execution persistence integration, live end-to-end retry & attempt orchestration integration, PostgreSQL lease recovery & DLQ integration, live Docker worker loss recovery & graceful drain integration, and controlled starvation prevention experiments.
- **Linting & Code Style**: ESLint 9 flat configuration and Prettier.
- **Architecture Contracts & ADRs**: Formal architecture decision records (`ADR-001` through `ADR-005`), architectural glossary, invariants catalog, database persistence spec, Redis coordination spec, queue architecture spec, worker registration spec, resource matching spec, scheduler architecture spec, distributed worker leases spec, container executor spec, retry policies & attempt orchestration spec, reliability & worker loss recovery spec, fairness & queue aging spec, and typed event architecture spec (`events.md`) in `docs/architecture/`.

### Planned (Future PRs)

- Kubernetes executor (Pods and Jobs)
- Durable log history / event replay service (PR 22 realtime delivery is transient — no replay)
- Real per-resource authorization for realtime subscriptions (PR 22 ships a documented placeholder authorizer)
- Authentication, API keys, and role-based access control (PR 22 gateway auth is a shared-secret bridge, one opaque principal)
- Webhook ingestion (GitHub, GitLab)
- Production metrics and OpenTelemetry tracing
- Full CLI workflow commands (`forge run`, `forge logs`, `forge deploy`)

---

## Directory Structure

```text
forge/
├── apps/
│   ├── api/            # HTTP API server shell (Express)
│   ├── scheduler/      # Distributed scheduler service shell
│   ├── worker/         # Task worker daemon shell
│   ├── web/            # Web frontend shell (Next.js)
│   └── cli/            # Developer CLI shell
├── packages/
│   ├── contracts/      # Shared type definitions and interfaces
│   ├── config/         # Environment variable validation & typed config
│   ├── logging/        # Structured logging abstraction
│   ├── pipeline/       # Core pipeline domain model, DAG, state machines, retries
│   ├── database/       # PostgreSQL connection, migrations, repositories
│   ├── redis/          # Redis connection, health checks, coordination primitives
│   ├── queue/          # Redis-backed FIFO job queue and recovery primitives
│   ├── worker-registry/# Worker registration, metadata and heartbeat coordination
│   ├── executor/       # Container executor and ephemeral execution engine
│   ├── events/         # Typed lifecycle event contract, publisher seam, in-process bus
│   └── outbox/         # Durable outbox dispatcher (poll, fenced claim, publish, retry, retention)
├── benchmarks/
│   ├── scheduler/      # Reproducible scheduler performance benchmarking harness
│   └── outbox/         # Outbox overhead, dispatcher throughput, claim-query EXPLAIN harness
├── docs/
│   └── architecture/
│       ├── decisions/  # Architecture Decision Records (ADR-001 - ADR-005)
│       ├── database.md # PostgreSQL persistence architecture
│       ├── redis.md    # Redis coordination architecture
│       ├── queue.md    # Reliable FIFO job queue architecture
│       ├── workers.md  # Worker registration & heartbeat architecture
│       ├── resource-matching.md # Worker capability & resource matching architecture
│       ├── scheduler.md         # Task scheduler & deterministic worker selection
│       ├── leases.md            # Distributed worker leases & job claiming
│       ├── executor.md          # Container executor & sandboxed job execution
│       ├── retry.md             # Retry policies, backoff & attempt orchestration
│       ├── reliability.md       # Reliability, worker loss recovery & DLQ
│       ├── fairness.md          # Fairness, queue aging & starvation prevention
│       ├── events.md            # Typed event architecture & execution lifecycle events
│       ├── domain-model.md # Domain model & state machines specification
│       ├── glossary.md # Architectural domain glossary
│       ├── invariants.md # Non-negotiable architectural rules
│       ├── overview.md # System overview and roadmap
│       └── boundaries.md # Service boundaries and allowed roles
├── .env.example        # Foundational environment variable template
├── tsconfig.base.json  # Shared strict TypeScript configuration
├── package.json        # Workspace configuration and root scripts
└── vitest.config.ts    # Test runner configuration
```

---

## Getting Started

### Prerequisites

- **Node.js**: `v20.0.0` or later (`v25.2.1` tested)
- **npm**: `v9.0.0` or later (`v11.6.2` tested)

### Installation

```bash
npm install
```

### Build

Compile all packages and applications:

```bash
npm run build
```

### Typecheck

Run strict TypeScript compiler checks across all workspaces:

```bash
npm run typecheck
```

### Testing

Run automated tests:

```bash
npm test
```

### Linting & Formatting

```bash
npm run lint
npm run format:check
```

### Performance Benchmarking

```bash
npm run benchmark:scheduler
npm run benchmark:outbox
npm run benchmark:websocket   # needs Redis (docker compose up -d); measures publish -> client receipt latency
```

---

## Running Applications (PR 01 Shells)

### Realtime Gateway

```bash
docker compose up -d                         # Redis
export WEBSOCKET_AUTH_TOKEN=dev-secret        # required; the gateway refuses to start without it
export REALTIME_PUBLISH_ENABLED=true          # so the scheduler/worker publish onto the transport
npm run start -w apps/realtime-gateway        # listens on WEBSOCKET_PORT (default 3100)
```

### API Service

```bash
npm run start -w apps/api
# In another terminal:
curl http://localhost:3000/health
```

### Scheduler Shell

```bash
npm run start -w apps/scheduler
```

### Worker Shell

```bash
npm run start -w apps/worker
```

### CLI Shell

```bash
npx forge --help
npx forge --version
```

### Web Shell

```bash
npm run dev -w apps/web
# Or build for production:
npm run build -w apps/web
```

---

## Architecture Documentation

### Core Contracts & Specifications

- [Architecture Overview](docs/architecture/overview.md)
- [Service Boundaries & Ownership Contract](docs/architecture/boundaries.md)
- [Pipeline Domain Model Specification](docs/architecture/domain-model.md)
- [PostgreSQL Persistence Specification](docs/architecture/database.md)
- [Transactional Domain Persistence & State Integrity](docs/architecture/persistence-integrity.md)
- [Redis Coordination Foundation](docs/architecture/redis.md)
- [Reliable FIFO Job Queue](docs/architecture/queue.md)
- [Worker Registration & Heartbeat](docs/architecture/workers.md)
- [Worker Capability & Resource Matching](docs/architecture/resource-matching.md)
- [Task Scheduler & Deterministic Worker Selection](docs/architecture/scheduler.md)
- [Distributed Worker Leases & Job Claiming](docs/architecture/leases.md)
- [Container Executor & Sandboxed Job Execution](docs/architecture/executor.md)
- [Retry Policies, Backoff & Attempt Orchestration](docs/architecture/retry.md)
- [Reliability, Worker Loss Recovery & Graceful Shutdown](docs/architecture/reliability.md)
- [Fairness, Queue Aging & Starvation Prevention](docs/architecture/fairness.md)
- [Typed Event Architecture & Execution Lifecycle Events](docs/architecture/events.md)
- [Scheduler Baseline Performance Specification](docs/benchmarks/scheduler-baseline.md)
- [Architecture Glossary](docs/architecture/glossary.md)
- [Architectural Invariants Catalog](docs/architecture/invariants.md)

### Architecture Decision Records (ADRs)

- [ADR-001: Explicit Service and Package Boundaries](docs/architecture/decisions/ADR-001-service-boundaries.md)
- [ADR-002: PostgreSQL as Authoritative Source of Truth](docs/architecture/decisions/ADR-002-postgresql-source-of-truth.md)
- [ADR-003: Redis for Transient Distributed Coordination](docs/architecture/decisions/ADR-003-redis-coordination.md)
- [ADR-004: At-Least-Once Delivery and Idempotent State Transitions](docs/architecture/decisions/ADR-004-at-least-once-delivery.md)
- [ADR-005: Ephemeral Execution Environments](docs/architecture/decisions/ADR-005-ephemeral-execution.md)
