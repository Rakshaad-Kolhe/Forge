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
- **PR 11: Priority Scheduling & Deterministic Job Ordering (Completed)**: Bounded job priority model `[-1000, 1000]`, PostgreSQL persistence with CHECK constraint, `HighestPriorityFirstPolicy` with canonical alphanumeric tie-breaking, non-blocking unschedulable semantics, prioritized batch evaluation, and FIFO queue decoupling.
- **PR 12: Distributed Worker Leases & Job Claiming (Completed)**: Authoritative PostgreSQL job ownership (`worker_leases` table), partial unique index for single-active-lease exclusivity, atomic claim/renew/release operations, database clock time authority, worker crash recovery via lease expiration, queue visibility timeout preservation, and 10-contestant concurrent claim race verification.
- **PR 13: Container Executor & Sandboxed Job Execution (Completed)**: Pluggable `Executor` abstraction and production-oriented `DockerExecutor` (`@forge/executor`), ephemeral workspaces, non-root execution (`--user 1000:1000`), container isolation (no privileged, no Docker socket, bridge network), CPU/memory resource enforcement, wall-clock timeout supervision (`docker stop` -> `docker kill`), bounded stdout/stderr capture with truncation protection, worker lease synchronization with split-brain abort protection, and transactional PostgreSQL persistence.
- **PR 14: Retry Policies, Exponential Backoff & Attempt Orchestration (Completed)**: Deterministic retry policies (`RetryPolicy`, `BackoffPolicy`, `evaluateRetry`), bounded exponential backoff with overflow protection, attempt immutability with collision-safe database uniqueness (`job_id, attempt_number`), durable PostgreSQL backoff scheduling (`jobs.next_attempt_at` and partial index `idx_jobs_retry_schedulable`), worker lease isolation enabling worker hopping, and non-blocking backoff scheduling.
- **PR 15: Dead-Letter Queue, Worker Loss Recovery & Graceful Shutdown (Completed)**: Decoupled Redis heartbeat liveness from PostgreSQL lease authority, atomic lease recovery with row-level locking (`FOR UPDATE SKIP LOCKED`), attempt historical immutability (`WORKER_LOST`), terminal job protection, Dead-Letter Queue (`dead_letter_jobs`) with unique constraint and idempotent upserts, three-phase worker shutdown lifecycle (`READY -> DRAINING -> OFFLINE`), bounded drain waiting with in-flight task supervision, and exclusion of draining workers from candidate placement.
- **PR 16: Fairness, Queue Aging & Starvation Prevention (Completed)**: Bounded queue aging model (`effective_priority = base_priority + age_bonus`), monotonic bounded aging bonus (`min(max_bonus, floor(waiting / interval) * step)`), retried job backoff-eligibility preservation (`nextAttemptAt`), zero mutation of durable base priority, deterministic alphanumeric tie-breaking, and empirical starvation prevention verification.
- **PR 17: Scheduler Benchmarking & Performance Validation (Completed)**: Reproducible scheduler benchmarking system (`benchmarks/scheduler`), micro/component/system test suites, high-resolution statistical distributions (P50, P95, P99), pure placement vs PostgreSQL lease contention profiling, query execution plan verification (`EXPLAIN (ANALYZE, BUFFERS)`), and baseline performance specification (`docs/benchmarks/scheduler-baseline.md`).
- **PR 18: Batched Worker Lease Claiming & Persistent Scheduler Optimization (Completed)**: Single-transaction multi-job lease acquisition (`claimBatch`), canonical ascending ID row locking (`ORDER BY id ASC FOR UPDATE`) for deadlock-free concurrency, bulk unnest multi-row insert, bounded batch chunking (`DEFAULT_LEASE_BATCH_SIZE = 50`), empirical 5.8x persistent throughput improvement (from ~70 to ~600+ jobs/sec leased), and 100% semantic equivalence verification.
- **PR 19: Execution Engine Foundation & Docker Executor (Completed)**: Hardened `DockerExecutor` execution engine (`@forge/executor`), strict Scheduler/Executor/Worker boundary decoupling, pre-execution lease ownership verification (`findActiveByJobId`), continuous ownership heartbeating with immediate `SIGTERM` -> `SIGKILL` container abort on lease loss (split-brain elimination), disposable ephemeral workspaces with path-traversal-immune identifier validation (`..`, `/`, `\`) and boundary-contained cleanup, non-root unprivileged container execution (`--user 1000:1000`, no `--privileged`, no host Docker socket mount), deterministic terminal signal precedence (`CANCELLED` > `TIMED_OUT` > `SUCCEEDED` / `FAILED`, with `exitCode: null` on timeout/cancellation), guaranteed `finally`-block teardown isolation, and label-based resource governance (`forge.managed=true`, canonical `forge-exec-` prefix targeting).
- **PR 20: Typed Event Architecture & Execution Lifecycle Events (Completed)**: Neutral, transport-independent event contract (`@forge/events`) — typed/versioned `ForgeEventEnvelope` with Forge-generated immutable `event_id` (UUID v4), correlation ids reusing existing domain id values, discriminated `ForgeEvent` union with compile-time exhaustiveness (`assertNever`), zod validation (`parseForgeEvent`, unknown-field stripping, unknown-version rejection), `EventPublisher`/`EventSubscriber` seam, `InProcessEventBus` (subscriber-failure isolation, explicit idempotent shutdown, no global singleton, no dedup), and post-execution `JobLogChunk` derivation from PR 19's bounded capture (`DEFAULT_LOG_CHUNK_BYTES`, per-stream monotonic `sequence`, `truncated`, `final`). Best-effort **persist-before-publish** integration (opt-in via `eventPublisher`): scheduler emits `JobClaimed` (lease acquired) and `WorkerLost` (recovery sweep, `NO_OP` excluded); worker emits `WorkerRegistered`, `WorkerHeartbeat`, `JobStarted`, `JobLogChunk*`, terminal `JobSucceeded`/`JobFailed`/`JobCancelled` (`failure_kind` ∈ `FAILED`/`TIMED_OUT`/`LEASE_LOST`/`EXECUTOR_ERROR`), and `JobQueued` on retry re-queue. Documented failure model: at-least-once, no exactly-once delivery, no global ordering, no DB-commit/publish atomicity, no outbox (future work). No WebSocket, no external broker, no PostgreSQL event store, no event sourcing. `Pipeline*` / first-enqueue `JobQueued` contracts defined but publication deferred (no producer yet).
- **PR 21: Durable Transactional Outbox (Completed)**: PostgreSQL-backed transactional outbox — `outbox_events` (migration `008`) + `PgOutboxRepository` + `tx.outbox` on every `TransactionContext` — and a polling delivery dispatcher (`@forge/outbox` `OutboxDispatcher`, hosted by `startScheduler` when a `pool` and an `EventPublisher` are configured). Lifecycle events tied to a `jobs` / `worker_leases` transition — `JobClaimed` (scheduler placement via the `claimBatch` `pendingOutbox` mapper), `JobStarted` / terminal `JobSucceeded` / `JobFailed` / `JobCancelled` / retry `JobQueued` (worker `executeJob` `withTransaction` blocks), and `WorkerLost` (scheduler recovery via the `LeaseRecoveryService` mapper) — are now recorded **in the same transaction** as the transition and delivered **at least once**; the five old best-effort `safePublish` calls for the worker lifecycle events are removed. Fenced claiming (`claim_token` regenerated per claim/reclaim, `FOR UPDATE SKIP LOCKED`, every mutation `WHERE status = 'CLAIMED' AND claim_token = $token`, `CLAIM_LOST` on mismatch, no `PUBLISHED → PENDING`); split retry accounting (`dispatch_count` diagnostic vs `delivery_attempt_count` — the latter driving `DEAD` only after `OUTBOX_MAX_DELIVERY_ATTEMPTS` genuine transport rejections; crashes and failed checkpoints burn no budget); conservative `PUBLISHED`-only retention (default 7 days, `0` disables; `DEAD` retained for manual triage); jitter-free `min(max, base·2ⁿ)` backoff via DB-time `available_at`; production-worker guard (`executeJob` throws when a publisher is wired without a `pool`). `JobLogChunk`, `WorkerHeartbeat`, and `WorkerRegistered` remain best-effort, so subscribers may observe a terminal event before that job's log chunks. Still at-least-once, no exactly-once delivery, no global ordering, no atomic DB↔transport commit; `InProcessEventBus` remains the only transport; no `LISTEN`/`NOTIFY` wake-up (polling only). Benchmarked (`npm run benchmark:outbox`): ≈ +0.5 ms mean per-event commit overhead; `claimBatch` selection currently plans as a Seq Scan (partial index unused — follow-up).

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
- **[Priority Scheduling Specification](scheduler.md)**
- **[Distributed Worker Leases & Job Claiming Specification](leases.md)**
- **[Container Executor & Sandboxed Job Execution Specification](executor.md)**
- **[Retry Policies, Backoff & Attempt Orchestration Specification](retry.md)**
- **[Reliability, Worker Loss Recovery & Graceful Shutdown Specification](reliability.md)**
- **[Fairness, Queue Aging & Starvation Prevention Specification](fairness.md)**
- **[Scheduler Baseline Performance Specification](../benchmarks/scheduler-baseline.md)**
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
  ├──► PR 11: Priority Scheduling & Deterministic Job Ordering (Completed)
  │
  ├──► PR 12: Distributed Worker Leases & Job Claiming (Completed)
  │
  ├──► PR 13: Container Executor & Sandboxed Job Execution (Completed)
  │
  ├──► PR 14: Retry Policies, Exponential Backoff & Attempt Orchestration (Completed)
  │
  ├──► PR 15: Dead-Letter Queue, Worker Loss Recovery & Graceful Shutdown (Completed)
  │
  ├──► PR 16: Fairness, Queue Aging & Starvation Prevention (Completed)
  │
  ├──► PR 17: Scheduler Benchmarking & Performance Validation (Completed)
  │
  ├──► PR 18: Batched Worker Lease Claiming & Persistent Optimization (Completed)
  │
  ├──► PR 19: Execution Engine Foundation & Docker Executor (Completed)
  │
  ├──► PR 20: Typed Event Architecture & Execution Lifecycle Events (Completed)
  │
  ├──► PR 21: Durable Transactional Outbox (Completed)
  │
  └──► PR 22: Realtime Event Transport & WebSocket Gateway (Current)
```
