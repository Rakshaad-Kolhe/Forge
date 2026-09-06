# Forge V2 — Architectural Service Boundaries

To prevent architectural drift and premature coupling, this document defines strict ownership rules for all components within the Forge monorepo.

---

## 1. API (`apps/api`)

### Allowed Responsibilities

- Exposing HTTP REST endpoints for external consumers, Web UI, and CI/CD webhooks.
- Validating incoming HTTP request payloads.
- In future PRs: Authentication, authorization, token verification, and rate limiting.
- Enqueuing pipeline trigger requests into the scheduling coordination layer.
- Exposing health, readiness, and metrics endpoints.

### Prohibited Responsibilities

- **Must NOT execute CI jobs or pipeline steps.**
- **Must NOT interact directly with container runtimes (Docker/Kubernetes).**
- **Must NOT bypass the scheduler to assign work directly to workers.**
- **Must NOT contain worker execution or shell scripting logic.**

---

## 2. Scheduler (`apps/scheduler`)

### Allowed Responsibilities

- Managing task dependency graphs (DAG resolution) in future PRs.
- Managing task queues, priority ordering, and concurrency limits.
- Lease allocation, worker health tracking, and task timeouts.
- Emitting scheduling lifecycle events.

### Prohibited Responsibilities

- **Must NOT expose public HTTP routing or serve web traffic.**
- **Must NOT execute jobs directly or run shell/container commands.**
- **Must NOT own authentication or user management logic.**

---

## 3. Worker (`apps/worker`)

### Allowed Responsibilities

- Consuming assigned tasks from the scheduling layer (in future PRs).
- Spawning and supervising isolated task execution environments (Docker/Kubernetes).
- Streaming execution output and logs back to the storage/monitoring layer.
- Emitting worker heartbeats and hardware health metrics.

### Prohibited Responsibilities

- **Must NOT expose public HTTP API endpoints.**
- **Must NOT make global scheduling or queue prioritization decisions.**
- **Must NOT contain database transaction logic or direct user authorization checks.**

---

## 4. Web (`apps/web`)

### Allowed Responsibilities

- Providing the frontend user interface for pipeline visualization, runs, and settings.
- Consuming public API endpoints exposed by `apps/api`.
- In future PRs: Consuming WebSocket streams for live log rendering.

### Prohibited Responsibilities

- **Must NOT connect directly to PostgreSQL or Redis.**
- **Must NOT perform server-side execution of jobs.**
- **Must NOT bypass the API layer to manipulate database state.**

---

## 5. CLI (`apps/cli`)

### Allowed Responsibilities

- Providing developer-facing command-line workflows.
- Parsing local pipeline definitions (linting/syntax validation).
- Interacting with the remote API via HTTP for pipeline triggers and log fetching.

### Prohibited Responsibilities

- **Must NOT connect directly to production databases.**
- **Must NOT contain business orchestration logic reserved for the Scheduler or API.**

---

## 6. Contracts (`packages/contracts`)

### Allowed Responsibilities

- Defining shared TypeScript interfaces, enums, and types that cross boundaries (e.g. `HealthResponse`, `ServiceName`, `LogLevel`, `LogEntry`).
- Pure type definitions and protocols.

### Prohibited Responsibilities

- **Must NOT contain business logic or framework implementations.**
- **Must NOT perform network requests, file I/O, or database queries.**

---

## 7. Config (`packages/config`)

### Allowed Responsibilities

- Reading environment variables (`process.env`).
- Validating environment variables against defined schemas with explicit types.
- Failing loudly and descriptively on invalid configuration.

### Prohibited Responsibilities

- **Must NOT introduce speculative environment variables for unbuilt infrastructure.**
- **Must NOT store runtime application state.**

---

## 8. Logging (`packages/logging`)

### Allowed Responsibilities

- Providing a structured logging interface (`debug`, `info`, `warn`, `error`).
- Formatting logs as human-readable text in development and machine-readable JSON in production.
- Attaching service metadata, timestamps, and contextual request identifiers.

### Prohibited Responsibilities

- **Must NOT implement distributed tracing or vendor-specific telemetry in PR 01.**
- **Must NOT throw exceptions during normal logging operations.**
