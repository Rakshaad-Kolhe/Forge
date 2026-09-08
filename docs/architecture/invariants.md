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
4. **The scheduling plane must tolerate concurrent worker claims without race conditions**, backed authoritatively by atomic row locking (`FOR UPDATE`) and partial unique indexing in PostgreSQL.
5. **PostgreSQL is the single authoritative source of truth for worker leases (`worker_leases`).** At most one active lease (`status = 'ACTIVE'`) is permitted per job at any point in time, enforced by the database partial unique index `uq_worker_leases_active_job`.
6. **The PostgreSQL database server clock (`NOW()`) is the sole authority for lease expiration.** Worker client clocks must never be relied upon for lease validity or timestamp calculations.
7. **Job claiming does not mutate domain job status to `RUNNING`.** Ownership is represented by an `ACTIVE` lease in `worker_leases`, decoupling ownership from attempt execution.
8. **Queue messages remain in visibility timeout and are NOT acknowledged upon scheduling or lease acquisition.** Recoverability is preserved until execution outcome is finalized.

---

## 4. Execution

1. **Forge assumes At-Least-Once Delivery paired with Idempotent State Transitions.** The system and documentation must never promise or advertise "exactly-once execution".
2. **User-defined CI job commands must never execute directly in the host worker's operating system environment.**
3. **Execution environments (containers or pods) must be strictly ephemeral and disposable.** An execution environment must never be shared across different jobs or reused across multiple execution attempts.
4. **When a job attempt completes, fails, or is cancelled, its execution environment and temporary workspace directory must be completely destroyed.**
5. **Worker host daemons must remain isolated from the containerized user processes they supervise.**
6. **A worker must hold a verified active lease before executing a job and must periodically renew the lease during execution.**
7. **If lease ownership is definitively lost during execution, the running container must be immediately terminated** to prevent split-brain duplicate concurrent execution across workers.
8. **Job attempt results and state transitions must be persisted transactionally in PostgreSQL before the worker lease is released.**
9. **Ephemeral workspace and container cleanup must run via guaranteed teardown (`finally`)**; cleanup failures must be logged without masking the primary execution result.

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
2. **Job commands should execute under unprivileged user IDs (non-root, `--user 1000:1000` by default)** within containers whenever possible to reduce container-escape attack surfaces.
3. **Host system directories, Docker daemon sockets, and host operating system filesystems must never be mounted into untrusted user job containers.** Privileged mode (`--privileged`) is strictly prohibited.
4. **Secrets and credentials must be injected dynamically into ephemeral job environments at runtime** and must never be persisted in build logs or committed to source repositories.
5. **Docker CLI commands must be executed via structured argument arrays (`spawn('docker', args)`), preventing host shell injection.**
6. **Host environment variables (`process.env`) must never be forwarded into execution containers**; only explicitly declared job environment variables are passed.

---

## 7. Observability

1. **All services must emit structured logs containing standardized contextual attributes (`service`, `environment`, `timestamp`, `level`, and `request_id` where applicable).**
2. **Production logs must be emitted as machine-readable newline-delimited JSON.**
3. **System logging and telemetry collection must be non-blocking and must never throw unhandled exceptions that disrupt primary application control loops.**
4. **Failure behavior must be explicitly defined and verifiable before implementation code is committed.**

---

## 8. Retry & Attempt Orchestration

1. **Execution failure $\ne$ Job permanently failed.** A failed execution attempt evaluates the pure `evaluateRetry` function before deciding whether the job transitions to terminal `FAILED` or re-enters `QUEUED` for retry.
2. **Attempt records (`job_attempts`) are strictly immutable historical facts.** Under no circumstances may an existing attempt record be mutated, overwritten, or re-run to represent a subsequent attempt.
3. **Attempt numbering is strictly collision-safe.** Guaranteed by the database uniqueness constraint `UNIQUE(job_id, attempt_number)`. Attempt IDs follow deterministic naming: `${job_id}-attempt-${attempt_number}`.
4. **Fresh worker lease per attempt.** The worker lease from an execution attempt is always released upon attempt completion, regardless of whether a retry is scheduled. Subsequent attempts must acquire a brand new lease, enabling worker hopping.
5. **Durable backoff persistence.** Retry delays are committed directly to PostgreSQL (`jobs.next_attempt_at`). Backoffs are evaluated against database time (`NOW()`), surviving process and node restarts, never using in-memory sleep loops or `setTimeout`.
6. **Non-blocking scheduler semantics.** A job currently waiting in active backoff (`next_attempt_at > NOW()`) produces an `UNSCHEDULABLE` decision with reason `RETRY_BACKOFF_ACTIVE` and never blocks other eligible jobs from being evaluated or scheduled.

---

## 9. Reliability, Worker Loss & Dead-Letter Queue

1. **Worker Loss Detection Authority**: Worker loss is inferred strictly via PostgreSQL lease expiration (`worker_leases.status = 'ACTIVE' AND expires_at <= NOW()`). Redis heartbeat liveness is purely ephemeral for scheduler candidate placement and must never be conflated with authoritative job ownership or used to revoke leases.
2. **Atomic Recovery Row Locking**: Lease recovery operations must utilize `SELECT ... FOR UPDATE SKIP LOCKED` within transactional boundaries to guarantee that exactly one recovery worker reconciles an expired lease, with concurrent recovery runners yielding safe, idempotent `NO_OP` results.
3. **Attempt Historical Integrity During Recovery**: Interrupted in-flight execution attempts reconciled during worker loss recovery must be marked `FAILED` with explicit `failure_reason = 'WORKER_LOST'` and final timestamp. Existing attempt records remain strictly immutable historical facts and must never be overwritten or deleted.
4. **Terminal Job Protection**: Jobs in a terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT`) must never be resurrected to `QUEUED` during lease recovery. If an operator cancelled a job while the worker was lost, the lease is marked `EXPIRED` and the job status remains unchanged.
5. **Dead-Letter Queue (DLQ) Durability & Idempotency**: Jobs with exhausted retries or non-retryable failures must be durably recorded in `dead_letter_jobs` with structured reason taxonomy. The database unique constraint `UNIQUE(job_id)` combined with idempotent upserts prevents duplicate DLQ records under at-least-once recovery loops.
6. **Graceful Worker Drain Precedence**: A worker entering `DRAINING` status must immediately reject new job claims (`NOT_CLAIMABLE`) and direct execution requests, while maintaining heartbeats with status `DRAINING` to ensure exclusion from scheduler candidate placement. In-flight tasks must be given a bounded grace period (`drainTimeoutMs`) to finish and release leases before final deregistration and transition to `OFFLINE`.

---

## 10. Queue Aging, Fairness & Starvation Prevention

1. **Base Priority Immutability**: Base priority (`job.priority`) is a durable, immutable property of the job. It must never be mutated or overwritten by queue aging, fairness calculations, or scheduler passes.
2. **Zero Database Migrations for Transient Signals**: Effective priority is a dynamic scheduler-derived calculation and must not be persisted to PostgreSQL or Redis. Waiting timestamps are derived exclusively from authoritative timestamps: `jobs.created_at` (initial attempt) and `jobs.next_attempt_at` (retried attempts).
3. **Bounded Age Bonus Ceiling**: Age bonus is strictly bounded by `maxAgeBonus` ($\text{effective\_priority} \le \text{base\_priority} + \text{max\_age\_bonus}$). Queue aging must never allow low-priority work to overtake critical or emergency priority bands whose base priority exceeds the ceiling.
4. **Retry Age Reset Invariant**: When a job enters retry scheduling, its waiting duration resets to start at `jobs.next_attempt_at` (the exact instant the retry backoff delay expired and the job became eligible for placement). Retried jobs must never inherit or carry over queue aging accumulated prior to failure or during backoff sleep.
5. **Deterministic Tie-Breaking & Permutation Invariance**: Job ordering under queue aging must break ties strictly by alphanumeric `jobId` ascending code-point ordering. The ordering must be permutation-invariant and time-deterministic for any fixed evaluation instant `now`.
6. **Hard Safety Gates Preserved**: Queue aging governs candidate job evaluation order only. It must never bypass, weaken, or alter PR 09 capability/resource matching, PR 10 deterministic worker selection, PR 12 distributed worker leases, or PR 14 active backoff gates.

---

## 11. Performance Measurement & Benchmarking

1. **Evidence-First Benchmark Rigor**: All documented performance envelopes and latency claims must be derived from reproducible, executable benchmark runs (`npm run benchmark:scheduler`) with recorded hardware, platform, and dependency version manifests. Hand-waved, estimated, or fabricated performance numbers are strictly prohibited.
2. **Explicit Warmup Isolation**: All benchmark suites must separate warmup cycles from measured executions. Warmup iterations allow V8 JIT optimization, garbage collection stabilization, and connection pool initialization without polluting sample distributions.
3. **Deterministic Workload Generation**: Benchmark workloads must utilize deterministic pseudo-random generation (Mulberry32 PRNG with fixed seeds). Benchmarks must never depend on non-deterministic `Math.random()` or uncontrolled clock seeds for sample generation.
4. **Clean Teardown Guarantee**: Benchmark runs interacting with live PostgreSQL or Redis instances must utilize isolated entity identifiers (`bench-*`) and execute guaranteed cleanup within `finally` blocks. No persistent records, test queues, or leases may remain after execution.
5. **Zero Semantic Mutation**: Benchmark suites must measure actual production interfaces and domain models directly. The scheduler and persistence layers must not include benchmark-only shortcuts, bypassed locks, or relaxed consistency guarantees to artificially inflate metrics.
6. **Comprehensive Statistical Profiles**: Performance must be reported via multi-metric percentile distributions (`P50`, `P95`, `P99`, `min`, `max`, `mean`, `stdDev`, `ops/sec`). Single-point averages or best-case outliers must never be presented as representative system throughput.
