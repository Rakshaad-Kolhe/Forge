# ADR-001: Explicit Service and Package Boundaries

## Status

Accepted

## Context

Distributed CI/CD orchestration engines manage distinct operational concerns across their lifecycle: accepting user requests and webhook ingress, evaluating complex directed acyclic graphs (DAGs), coordinating worker leases and priority queues, managing host-level process isolation and container execution, and streaming real-time logs to user interfaces.

In many early CI/CD architectures, boundaries between these concerns become blurred. For example, API servers frequently spawn sub-processes or trigger containers directly, schedulers become tightly coupled with database polling loops or web controllers, and user interfaces execute privileged database queries. This tight coupling creates critical operational failure modes:

- An API thread executing or waiting on long-running container builds saturates HTTP connection pools, degrading ingress responsiveness.
- Heavy job execution monopolizes CPU and memory on scheduling nodes, starving queue evaluation loops and heartbeats.
- Embedding web concerns into backends complicates multi-tenant security isolation and API evolution.

Forge V2 requires an explicit architecture contract that guarantees strict separation of concerns across service entrypoints and shared packages before core domain code is implemented.

## Decision

We establish strict, non-negotiable boundaries across all Forge services and shared packages.

### Service Boundaries

1. **API (`apps/api`)**
   - **Role**: Ingress gateway, REST API management, payload validation, future user authentication/authorization, and state/intent registration.
   - **Boundary**: Responsible for persisting intended state to PostgreSQL and delegating execution to the scheduling plane. The API must **never** execute CI jobs directly, spawn container runtimes, or communicate directly with worker execution daemons.

2. **Scheduler (`apps/scheduler`)**
   - **Role**: Autonomous scheduling engine responsible for evaluating DAG execution graphs, managing queue priorities, reconciling worker leases, and dispatching job claims.
   - **Boundary**: The scheduler communicates state via PostgreSQL and transient coordination via Redis. It must **never** own public HTTP routes, process incoming webhooks, or perform container execution.

3. **Worker (`apps/worker`)**
   - **Role**: Execution daemon responsible for claiming assigned jobs, managing execution lifecycles, interfacing with container runtimes (Docker/Kubernetes), capturing output, and emitting heartbeats.
   - **Boundary**: The worker operates strictly as an execution node. It must **never** expose public HTTP endpoints, make global queue prioritization decisions, or execute direct database transactions.

4. **Web UI (`apps/web`)**
   - **Role**: User-facing presentation layer built with Next.js for pipeline visualization, build inspection, and developer ergonomics.
   - **Boundary**: Consumes public API endpoints exposed by `apps/api` and future WebSocket event streams. It must **never** connect directly to PostgreSQL or Redis.

5. **CLI (`apps/cli`)**
   - **Role**: Developer command-line interface for triggering runs, inspecting build statuses, and linting local pipeline definitions.
   - **Boundary**: Operates as an external client via HTTP. It must **never** embed backend orchestration logic or access internal databases directly.

### Shared Package Boundaries

1. **Contracts (`packages/contracts`)**
   - Contains shared TypeScript interfaces, enums, and data contracts that cross application boundaries (e.g., `HealthResponse`, `ServiceName`, `LogLevel`, `LogEntry`, `AppConfig`).
   - Must remain free of business logic, database queries, and external I/O.

2. **Config (`packages/config`)**
   - Contains strongly typed environment variable parsing and validation logic.
   - Strictly forbids speculative configuration for unbuilt infrastructure.

3. **Logging (`packages/logging`)**
   - Contains structured logging primitives formatting human-readable text in development and newline-delimited JSON in production.
   - Does not bundle heavy vendor-specific telemetry or tracing agents in PR 01/PR 02.

---

## Rationale

Decoupling these domains establishes crucial guarantees:

### Why API ≠ Scheduler

The API service optimizes for low-latency, high-throughput request/response lifecycles (sub-100ms response targets for webhooks and user commands). A scheduler optimizes for state machine reconciliation, batch DAG evaluation, and worker lease renewal. Coupling them would mean an influx of CI pipeline triggers could exhaust server worker pools, starving scheduling loops and causing lease timeouts. Decoupling them allows independent horizontal scaling: the API can autoscale based on HTTP RPS, while the scheduler scales or operates as a coordinated leader-follower cluster based on active queue depth.

### Why Scheduler ≠ Worker

The scheduler evaluates global state (e.g., queue order, concurrency caps, resource availability). Workers manage local machine resources (CPU cores, memory, container socket locks, workspace disk I/O). If the scheduler ran container builds locally, a single runaway compilation or memory leak in a user build could crash the scheduler process, halting CI across the entire enterprise.

### Why Web ≠ Backend

The web presentation layer evolves rapidly with UI frameworks and client-side assets. Decoupling the Next.js frontend from direct backend access ensures that all mutations pass through the API contract, preserving consistent audit logs, authentication guards, and validation rules across both the Web UI and CLI.

### Why Contracts ≠ Business Logic

Shared libraries that contain business logic become dumping grounds for domain models and ORM entities, causing cyclic dependencies and forcing client applications to bundle backend code. Keeping `packages/contracts` strictly to pure types ensures zero runtime overhead and clear interface boundaries.

---

## Consequences

### Positive

- **Independent Scalability**: Ingress, scheduling, execution, and UI presentation scale according to their actual resource profiles.
- **Blast Radius Containment**: A worker execution failure or container kernel panic cannot take down the API or scheduler.
- **Clear Mental Model**: Contributors and automated agents have an unambiguous blueprint of where new capabilities belong.
- **Testability**: Services can be unit-tested and integration-tested in complete isolation with mock boundaries.

### Negative / Trade-offs

- **Operational Complexity**: Multiple independent services require orchestration tooling, multiple process supervisors, and deployment configurations instead of a single binary.
- **Network Overhead**: Communication across service boundaries introduces network serialization overhead (handled via Redis queues and HTTP) compared to in-memory function calls.

---

## Invariants

1. `apps/api` must never execute a CI job directly or communicate directly with container daemons.
2. `apps/web` must never connect directly to PostgreSQL or Redis.
3. `apps/worker` must never expose public HTTP routing or API endpoints.
4. `apps/scheduler` must never act as a container executor or host user CI workspaces.
5. `packages/contracts` must remain strictly dependency-light and contain zero application business logic.

---

## Failure Behavior

- **API Failure**: Ingress requests fail with standard 5xx status codes. Active scheduler queues and running worker executions continue uninterrupted.
- **Scheduler Failure**: In-flight worker jobs continue executing until lease expiration. New jobs queue up in durable storage but are not dispatched until the scheduler recovers.
- **Worker Failure**: The specific jobs assigned to the failed worker stall until their lease expires. The scheduler detects heartbeating loss and safely reschedules the work under at-least-once rules.
- **Web Failure**: Frontend UI becomes unavailable. Core orchestration, pipeline execution, and webhook ingress remain fully operational.

---

## Alternatives Considered

### 1. Monolithic Single-Process Application

- _Trade-off_: Packaging API, scheduler, and worker into a single executable simplifies deployment and eliminates network boundaries for single-node installations.
- _Why Rejected_: CI/CD workloads are inherently untrusted and resource-intensive. Running user-defined shell scripts or container builds in the same operating system process or node as the control plane poses severe stability and security risks that undermine Forge V2's distributed goals.

### 2. Scheduler Embedded in API Service

- _Trade-off_: Reduces the number of deployed services by having background threads in the API service poll database queues.
- _Why Rejected_: Background scheduling threads compete with HTTP event loops. Under heavy webhook traffic, scheduling latency degrades; conversely, intensive DAG resolution degrades API responsiveness.

### 3. Scheduler Embedded in Worker

- _Trade-off_: Each worker self-schedules work directly from a central database.
- _Why Rejected_: Decentralized self-scheduling leads to thundering-herd issues, lock contention on database rows, and inability to enforce global concurrency quotas or complex DAG dependency ordering.

### 4. Web Directly Querying PostgreSQL

- _Trade-off_: Server-side rendered pages could query PostgreSQL directly via Next.js Server Components, reducing API boilerplate.
- _Why Rejected_: Bypasses the API contract, resulting in duplicate authorization logic, conflicting schema interpretations, and vulnerability to connection pool exhaustion from client traffic.

---

## Implementation Implications

- PR 01 established the physical workspace directories (`apps/api`, `apps/scheduler`, `apps/worker`, `apps/web`, `apps/cli`, `packages/contracts`, `packages/config`, `packages/logging`).
- Future PRs implementing features must place domain logic strictly within these established boundaries.
- CI linting and dependency checks should flag any cross-service imports that violate this contract.

---

## Validation

- Monorepo boundaries are validated by TypeScript project references and build scripts.
- `packages/contracts` has 0 external runtime dependencies.
- Verified in PR 01: every service builds and starts as an independent process shell without cross-service runtime dependencies.

---

## Related Decisions

- [ADR-002: PostgreSQL as Source of Truth](ADR-002-postgresql-source-of-truth.md)
- [ADR-003: Redis for Transient Distributed Coordination](ADR-003-redis-coordination.md)
- [ADR-005: Ephemeral Execution](ADR-005-ephemeral-execution.md)
