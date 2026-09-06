# Forge V2 — Architecture Overview

## 1. What is Forge V2?

**Forge V2** is an open-source, self-hosted distributed CI/CD orchestration engine designed for high throughput, reliable container execution, and developer transparency.

It provides an orchestration plane that decouples pipeline scheduling, worker execution, user-facing APIs, and user interfaces into independent, scalable components.

---

## 2. Major Planned Components

Forge V2 is designed as a distributed system composed of:

- **API Service (`apps/api`)**: High-throughput REST API for job triggering, status inspection, webhook ingress, and orchestration management.
- **Scheduler Service (`apps/scheduler`)**: Distributed task scheduler handling DAG dependency resolution, priority queuing, lease management, and worker dispatching.
- **Distributed Workers (`apps/worker`)**: Task execution daemons managing local execution environments, container/Kubernetes sandboxes, log capture, and lifecycle heartbeats.
- **Command Line Interface (`apps/cli`)**: Developer CLI for local runs, pipeline validation, and remote engine administration.
- **Web Interface (`apps/web`)**: Next.js-based real-time dashboard for pipeline inspection, log streaming, and configuration management.
- **Persistence & State**: PostgreSQL for transactional metadata; Redis for distributed locks, queues, and transient lease management.
- **Execution Engines**: Docker daemon, isolated container runtimes, and Kubernetes job controllers.
- **Observability**: Structured JSON logging, OpenTelemetry metrics/tracing, and Prometheus scrapers.

---

## 3. Current Implementation Status (PR 01)

This repository currently contains **PR 01: Repository Foundation & Architecture Contract**.

In accordance with strict architectural discipline, PR 01 introduces **only the structural foundation and architectural contracts**:

### Implemented in PR 01

- **Monorepo Workspace**: NPM workspaces partitioning `apps/*` and `packages/*`.
- **Type System**: Strict TypeScript project references and shared base compiler options (`strict: true`).
- **Shared Packages**:
  - `@forge/contracts`: Boundary type contracts (`HealthResponse`, `ServiceName`, `LogEntry`, `AppConfig`).
  - `@forge/config`: Strongly typed runtime environment validation using Zod.
  - `@forge/logging`: Structured logging library supporting human-readable dev output and machine-readable JSON in production.
- **Minimal Application Shells**:
  - `apps/api`: Express shell exposing deterministic `GET /health`.
  - `apps/scheduler`: Executable Node service shell logging startup lifecycle.
  - `apps/worker`: Executable Node service shell logging startup lifecycle.
  - `apps/cli`: CLI shell supporting `--help` and `--version`.
  - `apps/web`: Next.js shell with responsive architectural dashboard.
- **Testing Foundation**: Vitest test runner with automated test suites for configuration validation, logging, and API health checks.
- **Linting & Formatting**: Repository-wide ESLint 9 flat config and Prettier rules.

### Planned for Future PRs (Out of Scope in PR 01)

- Database models, migrations, and PostgreSQL connection pooling.
- Redis queues, pub/sub, distributed locks, and state synchronization.
- DAG pipeline definition, dependency resolution, and execution scheduling.
- Docker / Kubernetes container sandboxing and task execution.
- Real-time WebSocket log streaming.
- Authentication, authorization, and secret isolation.
- CLI execution commands (`forge run`, `forge logs`, `forge deploy`).

---

## 4. Intended Evolution Path

```
PR 01: Repository Foundation & Architecture Contract (Current)
  │
  ├──► PR 02: Core Domain Model & Database Schema (PostgreSQL migrations)
  │
  ├──► PR 03: Queue Architecture & Redis State Coordination
  │
  ├──► PR 04: Scheduler Engine & DAG Execution Graphs
  │
  ├──► PR 05: Worker Runtime & Container Executor (Docker/K8s)
  │
  ├──► PR 06: API Ingress, Auth & Webhook Processing
  │
  ├──► PR 07: Real-Time Streaming & WebSocket Monitoring
  │
  └──► PR 08: CLI Workflows & Production Observability
```
