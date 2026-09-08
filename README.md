# Forge V2

Forge V2 is a self-hosted distributed CI/CD orchestration engine.

This repository is currently at **PR 16: Fairness, Queue Aging & Starvation Prevention**.

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
  - `@forge/database`: PostgreSQL persistence layer (Connection pooling, schema migrations `001` through `007`, typed repositories including `PgDeadLetterRepository`, transactions, state machine integrity enforcement, terminal state immutability, worker registry, persisted job requirements, priority index, `worker_leases` with partial unique index for single-active-lease exclusivity, `idx_jobs_retry_schedulable` partial index for high-throughput schedulable job discovery, `LeaseRecoveryService` with row-level locking `FOR UPDATE SKIP LOCKED`, and durable `dead_letter_jobs` table).
  - `@forge/redis`: Redis coordination foundation (Connection management, health checks, low-level generic primitives, TTL, atomic operations, and real Redis integration tests).
  - `@forge/queue`: Redis-backed reliable FIFO job queue (At-least-once delivery, explicit acknowledgement, queue depth, in-flight visibility tracking, crash/unacknowledged recovery, and competing consumer coordination).
  - `@forge/worker-registry`: Distributed worker registration and liveness coordination (Durable worker metadata and hardware capacity in PostgreSQL, transient heartbeat state with TTL in Redis, crash/stale detection, graceful deregistration, and isolated lifecycle state machines decoupled from lease ownership).
  - `@forge/executor`: Sandboxed container execution engine implementing `Executor` with production-oriented `DockerExecutor`, ephemeral temporary workspace management, non-root user execution (`--user 1000:1000`), container isolation (no privileged mode, no host Docker socket mount, bridge networking), resource limit enforcement (CPU, memory, unverified GPU status), wall-clock timeout supervision (`docker stop` -> `docker kill`), bounded stdout/stderr capture with truncation protection, and guaranteed teardown in `finally` blocks.
- **Service Shells & Applications**:
  - `apps/api`: Express HTTP server exposing only `GET /health`.
  - `apps/scheduler`: Task scheduler service (`@forge/scheduler`) providing operational eligibility evaluation (`READY + ALIVE`), exclusion of `DRAINING` workers, deterministic worker selection policy (`DeterministicFirstEligible`), baseline priority scheduling policy (`HighestPriorityFirstPolicy`), starvation-prevention queue aging policy (`FairAgingPriorityPolicy` computing dynamic effective priority with bounded age bonus), virtual time injection across ordering and placement, canonical alphanumeric tie-breaking, retry fairness reset invariant (`nextAttemptAt` anchor), non-blocking unschedulable semantics, batch evaluation, unacknowledged queue recoverability, atomic distributed worker lease acquisition via PostgreSQL, due retry job discovery (`scheduleDueJobs`) with non-blocking backoff awareness (`RETRY_BACKOFF_ACTIVE`), and integrated lease recovery loop (`recoverExpiredLeases`, `startRecoveryLoop`).
  - `apps/worker`: Worker daemon with automated registration, capability reporting, periodic heartbeat renewal, lease lifecycle management (`claimJob`, `renewLease`, `releaseLease`), containerized job execution (`executeJob`) with active lease validation, periodic lease renewal, definitive lease-loss abort protection, pure retry policy evaluation, transactional PostgreSQL persistence, per-attempt lease isolation enabling worker hopping, three-phase shutdown lifecycle (`READY -> DRAINING -> OFFLINE`), immediate claim/execution rejection during drain, and bounded in-flight execution drain supervision.
  - `apps/cli`: CLI executable supporting `--help` and `--version`.
  - `apps/web`: Next.js landing page displaying architectural boundaries.
- **Testing Foundation**: Vitest test runner configured with automated tests for config, logging, CLI, API health, pipeline domain core, capability/resource matching, job priority validation, PostgreSQL persistence, Redis coordination, FIFO job queue, worker registry, worker service shell, scheduler selection policies, priority ordering, worker lease lifecycle & concurrency races, Docker executor unit & live container integration, worker execution persistence integration, live end-to-end retry & attempt orchestration integration, PostgreSQL lease recovery & DLQ integration, live Docker worker loss recovery & graceful drain integration, and controlled starvation prevention experiments.
- **Linting & Code Style**: ESLint 9 flat configuration and Prettier.
- **Architecture Contracts & ADRs**: Formal architecture decision records (`ADR-001` through `ADR-005`), architectural glossary, invariants catalog, database persistence spec, Redis coordination spec, queue architecture spec, worker registration spec, resource matching spec, scheduler architecture spec, distributed worker leases spec, container executor spec, retry policies & attempt orchestration spec, reliability & worker loss recovery spec, and fairness & queue aging spec in `docs/architecture/`.

### Planned (Future PRs)

- Kubernetes executor (Pods and Jobs)
- Real-time WebSocket streaming for live logs and job statuses
- Authentication, API keys, and role-based access control
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
│   └── executor/       # Container executor and ephemeral execution engine
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

---

## Running Applications (PR 01 Shells)

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
- [Architecture Glossary](docs/architecture/glossary.md)
- [Architectural Invariants Catalog](docs/architecture/invariants.md)

### Architecture Decision Records (ADRs)

- [ADR-001: Explicit Service and Package Boundaries](docs/architecture/decisions/ADR-001-service-boundaries.md)
- [ADR-002: PostgreSQL as Authoritative Source of Truth](docs/architecture/decisions/ADR-002-postgresql-source-of-truth.md)
- [ADR-003: Redis for Transient Distributed Coordination](docs/architecture/decisions/ADR-003-redis-coordination.md)
- [ADR-004: At-Least-Once Delivery and Idempotent State Transitions](docs/architecture/decisions/ADR-004-at-least-once-delivery.md)
- [ADR-005: Ephemeral Execution Environments](docs/architecture/decisions/ADR-005-ephemeral-execution.md)
