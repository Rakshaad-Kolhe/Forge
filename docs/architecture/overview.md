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

## 3. Current Implementation Status

- **PR 01: Repository Foundation & Architecture Contract (Completed)**: Monorepo workspaces, strict TypeScript configuration, service shells, shared packages (`@forge/contracts`, `@forge/config`, `@forge/logging`), baseline testing, and linting.
- **PR 02: Architecture Decision Records & Engineering Contracts (Current)**: Formal architectural decision records (`ADR-001` through `ADR-005`), architectural glossary, and invariants catalog.

---

## 4. Architecture Decisions & Contracts

- **[ADR-001: Explicit Service and Package Boundaries](decisions/ADR-001-service-boundaries.md)**
- **[ADR-002: PostgreSQL as Authoritative Source of Truth](decisions/ADR-002-postgresql-source-of-truth.md)**
- **[ADR-003: Redis for Transient Distributed Coordination](decisions/ADR-003-redis-coordination.md)**
- **[ADR-004: At-Least-Once Delivery and Idempotent State Transitions](decisions/ADR-004-at-least-once-delivery.md)**
- **[ADR-005: Ephemeral Execution Environments](decisions/ADR-005-ephemeral-execution.md)**
- **[Architecture Glossary](glossary.md)**
- **[Architectural Invariants Catalog](invariants.md)**
- **[Service Boundaries Specification](boundaries.md)**

---

## 5. Intended Evolution Path

```
PR 01: Repository Foundation & Architecture Contract (Completed)
  │
  ├──► PR 02: Architecture Decision Records & Engineering Contracts (Current)
  │
  ├──► PR 03: Core Domain Model & Database Schema (PostgreSQL migrations)
  │
  ├──► PR 04: Queue Architecture & Redis State Coordination
  │
  ├──► PR 05: Scheduler Engine & DAG Execution Graphs
  │
  ├──► PR 06: Worker Runtime & Container Executor (Docker/K8s)
  │
  ├──► PR 07: API Ingress, Auth & Webhook Processing
  │
  ├──► PR 08: Real-Time Streaming & WebSocket Monitoring
  │
  └──► PR 09: CLI Workflows & Production Observability
```
