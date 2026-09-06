# Forge V2

Forge V2 is a self-hosted distributed CI/CD orchestration engine.

This repository is currently at **PR 06: Redis Coordination Foundation**.

---

## Current Status

### Implemented

- **Repository Architecture**: Monorepo layout using standard NPM workspaces (`apps/*`, `packages/*`).
- **TypeScript Setup**: Strict TypeScript 5 with composite project references and shared compiler options.
- **Shared Packages**:
  - `@forge/contracts`: Shared data contracts, types, and interfaces.
  - `@forge/config`: Strongly typed runtime environment validation using Zod.
  - `@forge/logging`: Structured logger (human-readable in development, newline-delimited JSON in production).
  - `@forge/pipeline`: Core in-memory domain model (Pipelines, Runs, Jobs, Attempts, DAG resolution, State Machines).
  - `@forge/database`: PostgreSQL persistence layer (Connection pooling, schema migrations, typed repositories, transactions, state machine integrity enforcement, terminal state immutability, and atomic aggregate persistence).
  - `@forge/redis`: Redis coordination foundation (Connection management, health checks, low-level generic primitives, TTL, atomic operations, and real Redis integration tests).
- **Minimal Service Shells**:
  - `apps/api`: Express HTTP server exposing only `GET /health`.
  - `apps/scheduler`: Process shell with structured startup/shutdown lifecycle.
  - `apps/worker`: Process shell with structured startup/shutdown lifecycle.
  - `apps/cli`: CLI executable supporting `--help` and `--version`.
  - `apps/web`: Next.js landing page displaying architectural boundaries.
- **Testing Foundation**: Vitest test runner configured with automated tests for config, logging, CLI, API health, pipeline domain core, PostgreSQL persistence, and Redis coordination.
- **Linting & Code Style**: ESLint 9 flat configuration and Prettier.
- **Architecture Contracts & ADRs**: Formal architecture decision records (`ADR-001` through `ADR-005`), architectural glossary, and invariants catalog in `docs/architecture/`.

### Planned (Future PRs)

- Redis-based job queues, distributed locks, and state synchronization
- DAG pipeline scheduler and execution graph resolver
- Container executors (Docker daemon and Kubernetes job runners)
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
│   └── redis/          # Redis connection, health checks, coordination primitives
├── docs/
│   └── architecture/
│       ├── decisions/  # Architecture Decision Records (ADR-001 - ADR-005)
│       ├── database.md # PostgreSQL persistence architecture
│       ├── redis.md    # Redis coordination architecture
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
- [Architecture Glossary](docs/architecture/glossary.md)
- [Architectural Invariants Catalog](docs/architecture/invariants.md)

### Architecture Decision Records (ADRs)

- [ADR-001: Explicit Service and Package Boundaries](docs/architecture/decisions/ADR-001-service-boundaries.md)
- [ADR-002: PostgreSQL as Authoritative Source of Truth](docs/architecture/decisions/ADR-002-postgresql-source-of-truth.md)
- [ADR-003: Redis for Transient Distributed Coordination](docs/architecture/decisions/ADR-003-redis-coordination.md)
- [ADR-004: At-Least-Once Delivery and Idempotent State Transitions](docs/architecture/decisions/ADR-004-at-least-once-delivery.md)
- [ADR-005: Ephemeral Execution Environments](docs/architecture/decisions/ADR-005-ephemeral-execution.md)
