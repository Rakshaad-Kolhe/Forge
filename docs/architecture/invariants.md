# Forge V2 — Architectural Invariants Catalog

This document establishes the binding architectural invariants for Forge V2. These rules are non-negotiable architectural axioms derived from ADR-001 through ADR-005. Every future pull request, domain implementation, and database schema must strictly preserve these invariants.

---

## 1. Service Boundaries

1. **`apps/api` must never execute a CI job directly.** All execution requests must be registered as intended state in PostgreSQL and coordinated via the Scheduler and Redis.
2. **`apps/web` must never connect directly to PostgreSQL or Redis.** The Web UI interacts exclusively with the public HTTP REST API and WebSocket notification endpoints.
3. **`apps/worker` must never expose public HTTP routes or API endpoints.** Workers act as outbound pull clients and execution supervisors.
4. **`apps/scheduler` must never act as a container executor or host user CI workspaces.** The scheduler performs scheduling analysis and lease coordination only.
5. **`packages/contracts` must remain strictly dependency-light and contain zero application business logic or I/O.**
6. **Shared packages must never import application services.** Dependencies flow strictly from applications to packages, never in reverse.

---

## 2. Persistence

1. **PostgreSQL is the sole authoritative durable source of truth for Forge V2.** All business entities, user accounts, pipeline runs, and historical job results must be committed to PostgreSQL under ACID guarantees.
2. **Redis is strictly a transient coordination layer and must never be treated as the authoritative persistent store.**
3. **Transient coordination loss, Redis restarts, or cache evictions must never rewrite, corrupt, or erase historical PostgreSQL records.**
4. **Domain and service layers must interact with persistent storage exclusively through Repository abstractions**, never through raw SQL queries or direct database driver handles embedded in business services.
5. **Database schema migrations must be backward-compatible and tested independently from application code deployments.**

---

## 3. Distributed Coordination

1. **Worker job claims must be time-bounded and enforced via explicit leases.** No worker may execute a job without an active, unexpired lease.
2. **Worker leases must expire automatically if heartbeats cease.** A crashed or partitioned worker must not hold a lock indefinitely.
3. **Reconciliation logic must exist to rebuild transient queue state from authoritative PostgreSQL records following a coordination outage.**
4. **The scheduling plane must tolerate concurrent worker claims without race conditions**, using atomic primitives in Redis.

---

## 4. Execution

1. **Forge assumes At-Least-Once Delivery paired with Idempotent State Transitions.** The system and documentation must never promise or advertise "exactly-once execution".
2. **User-defined CI job commands must never execute directly in the host worker's operating system environment.**
3. **Execution environments (containers or pods) must be strictly ephemeral and disposable.** An execution environment must never be shared across different jobs or reused across multiple execution attempts.
4. **When a job attempt completes, fails, or is cancelled, its execution environment and temporary workspace directory must be completely destroyed.**
5. **Worker host daemons must remain isolated from the containerized user processes they supervise.**

---

## 5. State Management

1. **State transitions must be explicit, conditional, and deterministic.** An invalid state transition (e.g., transitioning from `completed` back to `running`) must be rejected.
2. **Every physical execution attempt must be uniquely identified by the tuple `(pipeline_run_id, job_id, attempt_id)`.**
3. **Reprocessing a duplicate message or receiving a delayed completion event must never corrupt durable job state.**
4. **Marking a job attempt as completed or failed must be safely repeatable at the state layer.**
5. **Worker crashes or network partitions must not leave jobs permanently stranded in `running` status.**

---

## 6. Security

1. **Execution environments must enforce resource boundaries (CPU limits, memory limits, and wall-clock timeouts)** to prevent single-job denial-of-service across worker nodes.
2. **Job commands should execute under unprivileged user IDs (non-root)** within containers whenever possible to reduce container-escape attack surfaces.
3. **Host system directories, Docker daemon sockets, and host operating system filesystems must never be mounted writeable into untrusted user job containers.**
4. **Secrets and credentials must be injected dynamically into ephemeral job environments at runtime** and must never be persisted in build logs or committed to source repositories.

---

## 7. Observability

1. **All services must emit structured logs containing standardized contextual attributes (`service`, `environment`, `timestamp`, `level`, and `request_id` where applicable).**
2. **Production logs must be emitted as machine-readable newline-delimited JSON.**
3. **System logging and telemetry collection must be non-blocking and must never throw unhandled exceptions that disrupt primary application control loops.**
4. **Failure behavior must be explicitly defined and verifiable before implementation code is committed.**
