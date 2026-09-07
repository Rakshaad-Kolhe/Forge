# Forge V2

Forge V2 is a self-hosted distributed CI/CD orchestration engine.

This repository is currently at **PR 12: Distributed Worker Leases & Job Claiming**.

---

## Current Status

### Implemented

- **Repository Architecture**: Monorepo layout using standard NPM workspaces (`apps/*`, `packages/*`).
- **TypeScript Setup**: Strict TypeScript 5 with composite project references and shared compiler options.
- **Shared Packages**:
  - `@forge/contracts`: Shared data contracts, types, and interfaces (including `WorkerCapabilities`, `WorkerResources`, `JobRequirements`, `ScheduleDecision`, `JobPriority` constants `[-1000, 1000]`, and `WorkerLease` ownership contracts).
  - `@forge/config`: Strongly typed runtime environment validation using Zod (including `WORKER_JOB_LEASE_DURATION_MS` and `WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS`).
  - `@forge/logging`: Structured logger (human-readable in development, newline-delimited JSON in production).
  - `@forge/pipeline`: Core in-memory domain model (Pipelines, Runs, Jobs, Attempts, DAG resolution, State Machines, Job Execution Requirements, Job Priority validation and propagation, and pure deterministic capability/resource matching).
  - `@forge/database`: PostgreSQL persistence layer (Connection pooling, schema migrations `001` through `005`, typed repositories, transactions, state machine integrity enforcement, terminal state immutability, worker registry, persisted job requirements, priority index, and `worker_leases` with partial unique index for single-active-lease exclusivity).
  - `@forge/redis`: Redis coordination foundation (Connection management, health checks, low-level generic primitives, TTL, atomic operations, and real Redis integration tests).
  - `@forge/queue`: Redis-backed reliable FIFO job queue (At-least-once delivery, explicit acknowledgement, queue depth, in-flight visibility tracking, crash/unacknowledged recovery, and competing consumer coordination).
  - `@forge/worker-registry`: Distributed worker registration and liveness coordination (Durable worker metadata and hardware capacity in PostgreSQL, transient heartbeat state with TTL in Redis, crash/stale detection, graceful deregistration, and isolated lifecycle state machines).
- **Service Shells & Applications**:
  - `apps/api`: Express HTTP server exposing only `GET /health`.
  - `apps/scheduler`: Task scheduler service (`@forge/scheduler`) providing operational eligibility evaluation (`READY + ALIVE`), deterministic worker selection policy (`DeterministicFirstEligible`), priority scheduling policy (`HighestPriorityFirstPolicy`), canonical alphanumeric tie-breaking, non-blocking unschedulable semantics, batch evaluation, unacknowledged queue recoverability, and atomic distributed worker lease acquisition via PostgreSQL.
  - `apps/worker`: Worker daemon with automated registration, capability reporting, periodic heartbeat renewal, lease lifecycle management (`claimJob`, `renewLease`, `releaseLease`), and graceful deregistration and lease release on shutdown.
  - `apps/cli`: CLI executable supporting `--help` and `--version`.
  - `apps/web`: Next.js landing page displaying architectural boundaries.
- **Testing Foundation**: Vitest test runner configured with automated tests for config, logging, CLI, API health, pipeline domain core, capability/resource matching, job priority validation, PostgreSQL persistence, Redis coordination, FIFO job queue, worker registry, worker service shell, scheduler selection policies, priority ordering, worker lease lifecycle & concurrency races, and full scheduler integration.
- **Linting & Code Style**: ESLint 9 flat configuration and Prettier.
- **Architecture Contracts & ADRs**: Formal architecture decision records (`ADR-001` through `ADR-005`), architectural glossary, invariants catalog, database persistence spec, Redis coordination spec, queue architecture spec, worker registration spec, resource matching spec, scheduler architecture spec, and distributed worker leases spec in `docs/architecture/`.

### Planned (Future PRs)

- Container executors (Docker daemon and Kubernetes job runners) (PR 13+)
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
│   ├── pipeline/       # Core pipeline domain model, DAG, state machines
│   ├── database/       # PostgreSQL connection, migrations, repositories
│   ├── redis/          # Redis connection, health checks, coordination primitives
│   ├── queue/          # Redis-backed FIFO job queue and recovery primitives
│   └── worker-registry/# Worker registration, metadata and heartbeat coordination
├── docs/
│   └── architecture/
│       ├── decisions/  # Architecture Decision Records (ADR-001 - ADR-005)
│       ├── database.md # PostgreSQL persistence architecture
│       ├── redis.md    # Redis coordination architecture
│       ├── queue.md    # Reliable FIFO job queue architecture
│       ├── workers.md  # Worker registration & heartbeat architecture
│       ├── resource-matching.md # Worker capability & resource matching architecture
│       ├── scheduler.md         # Task scheduler & deterministic worker selection
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
- [Architecture Glossary](docs/architecture/glossary.md)
- [Architectural Invariants Catalog](docs/architecture/invariants.md)

### Architecture Decision Records (ADRs)

- [ADR-001: Explicit Service and Package Boundaries](docs/architecture/decisions/ADR-001-service-boundaries.md)
- [ADR-002: PostgreSQL as Authoritative Source of Truth](docs/architecture/decisions/ADR-002-postgresql-source-of-truth.md)
- [ADR-003: Redis for Transient Distributed Coordination](docs/architecture/decisions/ADR-003-redis-coordination.md)
- [ADR-004: At-Least-Once Delivery and Idempotent State Transitions](docs/architecture/decisions/ADR-004-at-least-once-delivery.md)
- [ADR-005: Ephemeral Execution Environments](docs/architecture/decisions/ADR-005-ephemeral-execution.md)
