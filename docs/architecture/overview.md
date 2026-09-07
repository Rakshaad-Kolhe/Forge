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
- **PR 02: Architecture Decision Records & Engineering Contracts (Completed)**: Formal architectural decision records (`ADR-001` through `ADR-005`), architectural glossary, and invariants catalog.
- **PR 03: Pipeline Domain Model & State Machines (Completed)**: Pure domain core for Pipelines, PipelineRuns, Jobs, Attempts, DAG dependency graph, cycle detection, and explicit state machines.
- **PR 04: PostgreSQL Persistence Foundation (Completed)**: PostgreSQL connection management, schema migrations, and typed repository layer.
- **PR 05: Transactional Domain Persistence & State Integrity (Completed)**: Protected persistence boundary, terminal state immutability, state machine enforcement, and atomic aggregate persistence.
- **PR 06: Redis Coordination Foundation (Completed)**: Managed Redis client, active health checking, key-value primitives, TTL, atomic coordination, and real Redis integration tests.
- **PR 07: Reliable FIFO Job Queue (Completed)**: Redis-backed FIFO queue abstraction (`@forge/queue`), atomic dispatch, in-flight visibility tracking, unacknowledged crash recovery, and competing consumer coordination.
- **PR 08: Worker Registration & Heartbeat (Completed)**: Distributed worker registration, durable metadata in PostgreSQL, transient liveness in Redis (`@forge/worker-registry`), crash/stale detection, and graceful lifecycle management.
- **PR 09: Worker Capability & Resource Matching (Completed)**: Pure, deterministic placement eligibility layer (`matchesWorker`, `filterEligibleWorkers`), job execution requirements, explainable failure diagnostics, and PostgreSQL persistence.
- **PR 10: Scheduler Foundation & Deterministic Worker Selection (Completed)**: First-class scheduler service (`@forge/scheduler`), operational eligibility (`READY + ALIVE`), deterministic baseline worker selection policy (`DeterministicFirstEligible`), explainable placement decisions, and unacknowledged queue recoverability.

---

## 4. Architecture Decisions & Contracts

- **[ADR-001: Explicit Service and Package Boundaries](decisions/ADR-001-service-boundaries.md)**
- **[ADR-002: PostgreSQL as Authoritative Source of Truth](decisions/ADR-002-postgresql-source-of-truth.md)**
- **[ADR-003: Redis for Transient Distributed Coordination](decisions/ADR-003-redis-coordination.md)**
- **[ADR-004: At-Least-Once Delivery and Idempotent State Transitions](decisions/ADR-004-at-least-once-delivery.md)**
- **[ADR-005: Ephemeral Execution Environments](decisions/ADR-005-ephemeral-execution.md)**
- **[Pipeline Domain Model Specification](domain-model.md)**
- **[PostgreSQL Persistence Specification](database.md)**
- **[Transactional Domain Persistence Specification](persistence-integrity.md)**
- **[Redis Coordination Foundation Specification](redis.md)**
- **[Reliable FIFO Job Queue Specification](queue.md)**
- **[Worker Registration & Heartbeat Specification](workers.md)**
- **[Worker Capability & Resource Matching Specification](resource-matching.md)**
- **[Task Scheduler & Deterministic Worker Selection Specification](scheduler.md)**
- **[Architecture Glossary](glossary.md)**
- **[Architectural Invariants Catalog](invariants.md)**
- **[Service Boundaries Specification](boundaries.md)**

---

## 5. Intended Evolution Path

```text
PR 01: Repository Foundation & Architecture Contract (Completed)
  │
  ├──► PR 02: Architecture Decision Records & Engineering Contracts (Completed)
  │
  ├──► PR 03: Pipeline Domain Model & State Machines (Completed)
  │
  ├──► PR 04: PostgreSQL Persistence Foundation (Completed)
  │
  ├──► PR 05: Transactional Domain Persistence & State Integrity (Completed)
  │
  ├──► PR 06: Redis Coordination Foundation (Completed)
  │
  ├──► PR 07: Reliable FIFO Job Queue (Completed)
  │
  ├──► PR 08: Worker Registration & Heartbeat (Completed)
  │
  ├──► PR 09: Worker Capability & Resource Matching (Completed)
  │
  ├──► PR 10: Scheduler Foundation & Deterministic Worker Selection (Completed)
  │
  ├──► PR 11: Priority Scheduling & Policy Layer (Planned)
  │
  └──► PR 12: Distributed Lease Allocation & Job Claiming (Planned)
```
