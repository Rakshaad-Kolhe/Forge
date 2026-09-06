# ADR-002: PostgreSQL as Authoritative Source of Truth

## Status

Accepted

## Context

A distributed CI/CD orchestration engine manages a spectrum of data with divergent durability, consistency, and access characteristics:

- **Durable relational entities**: Users, organizations, project configurations, cryptographic secret references, pipeline definitions, execution runs, and artifact registries.
- **Transient coordination state**: In-flight job queues, worker heartbeats, ephemeral lease locks, and execution claims.
- **Streaming data**: Real-time stdout/stderr execution logs, WebSocket broadcasts, and live metrics.

A common architectural antipattern in distributed systems is conflating transient coordination state with durable system records. When transient stores (such as memory caches or key-value brokers) are treated as authoritative records, cache evictions, node reboots, or split-brain partitions cause unrecoverable loss of historical build records, corrupted job states, or orphan execution references.

Forge V2 requires an authoritative persistent data store with strict ACID transactional guarantees, robust schema enforcement, and clear abstraction layers.

---

## Decision

**PostgreSQL** is designated as the sole authoritative persistent store of record for Forge V2.

All authoritative business entities and lifecycle states must be durably committed to PostgreSQL:

- `users` and authorization credentials
- `projects` and VCS repository configurations
- `pipelines` (parsed workflow schemas and trigger definitions)
- `pipeline_runs` (run lifecycle, triggering event, overall status, timing)
- `jobs` (individual execution nodes in a DAG, dependency links, target criteria)
- `job_attempts` (execution attempts, start/end timestamps, exit codes, worker IDs)
- `workers` (registered worker records, host capabilities, status)
- `worker_leases` (persisted lease audit history)
- `artifacts` (persisted artifact metadata, checksums, and storage pointers)

### Repository Abstraction Layer

Application and domain logic must **never** execute raw SQL queries or bind directly to database drivers across domain boundaries. All access to persistent state must be mediated through typed **Repository interfaces** (e.g., `PipelineRunRepository`, `JobRepository`, `WorkerRepository`). This decouples business rule evaluation from database drivers, facilitates migration testing, and allows unit testing with in-memory test doubles.

---

## Data Layer Distinctions

To prevent architectural drift, Forge explicitly establishes five distinct data classifications:

1. **Authoritative Source of Truth (PostgreSQL)**
   - ACID-compliant, durable, relational state.
   - Survives complete cluster outages and process restarts.
   - Holds the immutable history of what was requested, attempted, and concluded.

2. **Transient Coordination Mechanism (Redis)**
   - Low-latency in-memory data structures for fast queueing, atomic worker lease claims, and short-lived lock management.
   - Not a durable database; state in Redis can be reconstructed or reconciled against PostgreSQL following an outage.

3. **Cache (Redis / In-Memory)**
   - Read-through optimizations for frequently queried metadata. Loss of cache simply results in a temporary performance degradation, not data corruption.

4. **Event Stream (Redis Pub/Sub / WebSocket Brokers)**
   - Ephemeral message delivery for real-time progress notifications to UI clients.
   - Missed events are recovered by querying the authoritative PostgreSQL API.

5. **Derived & Observability Data (Object Storage / Log Ingestion / Metrics)**
   - Bulk execution logs (stdout/stderr) and compiled artifacts stored in blob storage with metadata referenced in PostgreSQL.
   - Metrics and telemetry exported to external observability collectors.

PostgreSQL does **not** store live queue operations, raw real-time WebSocket payloads, or gigabytes of unindexed live log streams.

---

## Rationale

- **ACID Guarantees**: State transitions in CI/CD pipelines (e.g., marking a job as `completed` and unlocking dependent jobs) require atomic multi-table transactions. PostgreSQL provides strong transactional isolation that prevents corrupted partial state transitions.
- **Relational Integrity**: Pipeline runs have strict hierarchical relationships (`Project` → `Pipeline` → `PipelineRun` → `Job` → `JobAttempt`). Relational constraints and foreign keys enforce referential integrity across the system.
- **Proven Ecosystem**: PostgreSQL offers robust connection pooling, automated migrations, high-availability replication, and tooling familiar to self-hosted enterprise infrastructure operators.

---

## Consequences

### Positive

- **Deterministic Historical Auditing**: Every pipeline run, job attempt, and worker assignment is safely preserved in an audit-ready relational store.
- **Safe Crash Recovery**: Because persistent state is isolated from transient coordination, a crash in the coordination layer (Redis) cannot alter or corrupt historical records.
- **Domain Decoupling**: Repository abstractions isolate database schemas from application workflows, enabling clean domain testing.

### Negative / Trade-offs

- **Latency for Transient Operations**: Committing state changes to disk in PostgreSQL has higher latency than in-memory operations. High-frequency operations (such as per-second worker heartbeats or job queue pop operations) must be mediated through Redis to prevent database connection saturation.
- **Migration Overhead**: Schema modifications require disciplined, backwards-compatible migration scripts.

---

## Invariants

1. Redis state must never silently become the authoritative durable record for any core entity.
2. Transient coordination loss or Redis restart must never rewrite or delete historical PostgreSQL records.
3. Persistent job state transitions must be deterministic and verifiable via relational transactions.
4. Domain code must interact with persistent state exclusively through Repository abstractions, never direct SQL primitives in service layers.

---

## Failure Behavior

- **Database Connection Outage**: Ingress and state mutation requests fail gracefully with descriptive error logs and 503 responses. The scheduler and workers pause state transitions until the database connection is restored, preventing untracked execution.
- **Transient Disconnection**: Connection pools handle exponential backoff reconnects without process termination.
- **Transaction Deadlock / Collision**: Optimistic locking or explicit transaction isolation levels detect concurrent state modifications, rejecting conflicting writes and triggering safe retries.

---

## Alternatives Considered

### 1. Redis as Sole Source of Truth

- _Trade-off_: Ultra-high performance, built-in pub/sub and queue structures.
- _Why Rejected_: Redis persistence models (RDB snapshots / AOF) do not provide the strict ACID relational guarantees required for historical CI/CD records. Memory-bound storage makes retaining years of historical build logs and run metadata cost-prohibitive and vulnerable to memory pressure evictions.

### 2. Document Store (MongoDB) as Primary Store

- _Trade-off_: Flexible schema for heterogeneous pipeline definitions and dynamic step configurations.
- _Why Rejected_: Relational foreign key enforcement and multi-table transactional guarantees are weaker or more complex in document stores. CI/CD execution structures are inherently relational and benefit significantly from relational constraints.

### 3. Event Log (Kafka / Event Sourcing) as Primary Source of Truth

- _Trade-off_: Full immutable audit log of every event, natural historical replayability.
- _Why Rejected_: Excessive architectural complexity for Forge V2's self-hosted deployment target. Querying current state (e.g., "is job X running?", "show recent runs for project Y") requires maintaining complex materialized read projections, vastly increasing operational maintenance.

### 4. PostgreSQL + Event Sourcing

- _Trade-off_: Combines relational database reliability with complete event-sourced replay.
- _Why Rejected_: Adds significant code complexity and projection maintenance for marginal gain in early versions. Standard normalized relational tables with append-only attempt logs (`job_attempts`) provide sufficient auditability without the burden of full event sourcing.

---

## Implementation Implications

- Database migrations and connection pools will be introduced in future dedicated PRs (PR 03+).
- No database client, driver, or ORM dependency is added in PR 02.
- Domain interfaces in future PRs must specify repository contracts (e.g., `interface JobRepository`) before introducing concrete PostgreSQL implementations.

### PR 04 Implementation Note (PostgreSQL Persistence Foundation)

PR 04 implements the durable persistence foundation defined in this ADR:

- Added `@forge/database` package providing managed connection pooling (`pg.Pool`) and database health checks (`SELECT 1`).
- Implemented versioned migration system (`001_initial_schema`) tracking executed migrations in `forge_migrations`.
- Created durable tables with relational constraints: `pipelines`, `pipeline_runs`, `jobs`, and `job_attempts`.
- Defined and implemented repository contracts (`PipelineRepository`, `PipelineRunRepository`, `JobRepository`, `JobAttemptRepository`) with domain model reconstruction and transactional boundary support (`withTransaction`).
- Verified zero direct database dependencies in `@forge/pipeline`, preserving pure domain isolation.

---

## Validation

- Verified that PR 01 and PR 02 introduce zero database clients, schema files, or database dependencies.
- Documentation strictly defines PostgreSQL as authoritative and separates it from transient coordination.

---

## Related Decisions

- [ADR-001: Explicit Service and Package Boundaries](ADR-001-service-boundaries.md)
- [ADR-003: Redis for Transient Distributed Coordination](ADR-003-redis-coordination.md)
- [ADR-004: At-Least-Once Delivery and Idempotent State Transitions](ADR-004-at-least-once-delivery.md)
