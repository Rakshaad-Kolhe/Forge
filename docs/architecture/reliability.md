# Forge V2 — Reliability, Worker Loss Recovery & Graceful Shutdown

## 1. Overview & Core Philosophy

Forge V2 is built on an explicit operational philosophy:

> **At-least-once delivery + idempotent state transitions.**
> Forge does **NOT** claim exactly-once execution or zero data loss across catastrophic infrastructure failures.

Distributed worker nodes will crash, suffer network partitions, exhaust memory, or be terminated by cloud auto-scalers. In a robust CI/CD orchestration engine:

1. **Worker loss must be detectable** without ambiguous state or split-brain ownership.
2. **In-flight jobs must be safely recovered** into schedulable state or routed to an operational holding area.
3. **Historical execution records must remain immutable** for forensic auditability.
4. **Shutdown must be graceful and bounded**, ensuring in-flight tasks finish while preventing new workload ingestion.

```text
+-----------------------------------------------------------------------------+
|                            RELIABILITY TOPOLOGY                             |
|                                                                             |
|   +-----------------------+              +------------------------------+   |
|   |   PostgreSQL Leases   |              |       Redis Heartbeats       |   |
|   |  - Authoritative      |              |  - Ephemeral Liveness        |   |
|   |  - Ownership boundary |              |  - High-frequency cadence    |   |
|   |  - Expiration triggers|              |  - Expiry marks STALE        |   |
|   |    Lease Recovery     |              |  - NEVER triggers recovery   |   |
|   +-----------------------+              +------------------------------+   |
|               ^                                         ^                   |
|               |                                         |                   |
|               +--------------------+--------------------+                   |
|                                    |                                        |
|                          +-------------------+                              |
|                          |    Worker Node    |                              |
|                          |  - READY          |                              |
|                          |  - DRAINING       |                              |
|                          |  - OFFLINE        |                              |
|                          +-------------------+                              |
+-----------------------------------------------------------------------------+
```

---

## 2. Decoupled Architecture: Heartbeat vs. Lease Authority

A common architectural antipattern in distributed systems is conflating **ephemeral node liveness** (e.g., Redis heartbeat keys) with **authoritative job ownership** (e.g., distributed worker leases).

Forge V2 strictly decouples these two planes:

| Dimension          | Redis Heartbeats (`WorkerRegistry`)                      | PostgreSQL Leases (`WorkerLeaseRepository`)                       |
| :----------------- | :------------------------------------------------------- | :---------------------------------------------------------------- |
| **Storage Plane**  | Redis (ephemeral in-memory KV)                           | PostgreSQL (`worker_leases` table)                                |
| **Primary Role**   | Dynamic worker discovery & scheduler candidate filtering | Authoritative mutual exclusion & execution ownership              |
| **Cadence**        | Frequent (e.g., every 2s, 5s TTL)                        | Coarse (e.g., 30s lease duration, renewed periodically)           |
| **Loss Semantics** | Missing key marks worker `STALE`                         | Expired timestamp (`expires_at <= NOW()`) triggers lease recovery |
| **Failure Effect** | Scheduler excludes node from receiving _new_ jobs        | `LeaseRecoveryService` reconciles the abandoned job attempt       |

### Invariant: Redis Heartbeat Loss Never Revokes Job Ownership

If a worker's Redis heartbeat key expires due to transient Redis latency or a temporary network blip:

- The worker registry marks the node as `STALE`.
- The scheduler immediately stops placing **new** jobs on that node.
- **However, active jobs running on the worker continue uninterrupted** as long as the worker can still renew its PostgreSQL lease before `expires_at`.
- Only when the PostgreSQL lease `expires_at` timestamp elapses without renewal is the job deemed abandoned.

---

## 3. Worker Loss Detection & Atomic Recovery

### 3.1 Expiration Detection

Worker loss is detected strictly via SQL query against PostgreSQL:

```sql
SELECT id, job_id, worker_id, expires_at
FROM worker_leases
WHERE status = 'ACTIVE'
  AND expires_at <= NOW()
ORDER BY expires_at ASC
LIMIT 50;
```

### 3.2 Concurrency-Safe Transactional Recovery

Multiple scheduler instances or dedicated recovery processes can execute recovery loops concurrently. To prevent double-recovery races and eliminate duplicate attempts, recovery uses row-level locking with `FOR UPDATE SKIP LOCKED`:

```sql
SELECT *
FROM worker_leases
WHERE id = $1 AND status = 'ACTIVE'
FOR UPDATE SKIP LOCKED;
```

If another recovery worker has already locked or transitioned the lease, the competing worker receives zero rows and safely exits with a harmless `NO_OP`.

```text
Worker A (Crashed) holds Lease L1 (ACTIVE, expires_at <= NOW())
                         │
                         ▼
        ┌───────────────────────────────────┐
        │       LeaseRecoveryService        │
        │ - SELECT FOR UPDATE SKIP LOCKED   │
        └───────────────────────────────────┘
                         │
         ┌───────────────┴───────────────┐
         ▼                               ▼
[Candidate Locked]               [Already Recovered / Locked]
         │                               │
         │                               ▼
         │                             NO_OP
         ▼
Reconcile In-Flight Attempt:
- Find Attempt (status = RUNNING)
- Transition -> FAILED (WORKER_LOST)
- Timestamp finished_at
- Save Attempt (Immutable!)
         │
         ▼
Check Terminal Preconditions:
- If Job is CANCELLED / SUCCEEDED / TIMED_OUT:
  Mark Lease EXPIRED, Action: SKIPPED_TERMINAL
         │
         ▼
Evaluate Retry Policy: evaluateRetry(attempt, job.retryPolicy)
         │
         ├───────────────────────────────┐
         ▼                               ▼
[Retryable: ACTION=RETRY]        [Exhausted / Non-Retryable]
         │                               │
         ▼                               ▼
- Transition Job -> QUEUED       - Transition Job -> FAILED
- Set next_attempt_at (backoff)  - Insert dead_letter_jobs (DLQ)
- Mark Lease -> EXPIRED          - Mark Lease -> EXPIRED
- Action: REQUEUED               - Action: DEAD_LETTERED
```

### 3.3 State Transition Guarantees

1. **Attempt Immutability**: The interrupted in-flight `JobAttempt` is marked `FAILED` with `failure_reason = 'WORKER_LOST'`. The attempt row is never overwritten or deleted; it remains part of the permanent execution log.
2. **Terminal Job Protection**: If an operator cancelled the job while the worker was disconnected, recovery marks the lease `EXPIRED` but **never** resurrects the job to `QUEUED`.
3. **Fresh Lease Acquisition**: When a requeued job becomes schedulable, it must be claimed via a brand new lease (`lease_id`). The old worker cannot resume execution under its old lease ID.

---

## 4. Dead-Letter Queue (DLQ)

### 4.1 Purpose

The Dead-Letter Queue in Forge is **not** a trash bin or data destruction mechanism. It is an **authoritative holding and operational inspection table** for jobs that cannot proceed automatically.

### 4.2 Schema (`dead_letter_jobs`)

```sql
CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    pipeline_run_id VARCHAR(64) NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    reason VARCHAR(64) NOT NULL,
    failed_attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_id VARCHAR(64) REFERENCES job_attempts(id) ON DELETE SET NULL,
    last_worker_id VARCHAR(64),
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dead_letter_jobs_job_id UNIQUE (job_id)
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_pipeline_run ON dead_letter_jobs (pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_reason ON dead_letter_jobs (reason);
CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_created_at ON dead_letter_jobs (created_at DESC);
```

### 4.3 DLQ Reason Taxonomy

| Reason Code                   | Description                                                                                 |
| :---------------------------- | :------------------------------------------------------------------------------------------ |
| `WORKER_LOSS_RETRY_EXHAUSTED` | The worker holding the lease disappeared, and the job's retry policy reached `maxAttempts`. |
| `MAX_RETRIES_EXCEEDED`        | The container/process completed with non-zero exit or error, exhausting all retry attempts. |
| `NON_RETRYABLE_FAILURE`       | Job execution or worker loss occurred, but the job's retry policy does not allow retries.   |
| `EXECUTION_CANCELLED`         | Job execution was halted due to cancellation or pipeline termination.                       |
| `UNKNOWN_UNRECOVERABLE`       | System failure or unrecoverable error during orchestration.                                 |

### 4.4 Idempotent Upsert

Because recovery operations may execute under at-least-once semantics, `PgDeadLetterRepository.save()` uses an idempotent upsert:

```sql
INSERT INTO dead_letter_jobs (
    id, job_id, pipeline_run_id, reason, failed_attempt_count,
    last_attempt_id, last_worker_id, error_message, metadata, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (job_id) DO UPDATE SET
    reason = EXCLUDED.reason,
    failed_attempt_count = EXCLUDED.failed_attempt_count,
    last_attempt_id = EXCLUDED.last_attempt_id,
    last_worker_id = EXCLUDED.last_worker_id,
    error_message = EXCLUDED.error_message,
    metadata = EXCLUDED.metadata,
    updated_at = EXCLUDED.updated_at;
```

This guarantees that duplicate recovery attempts cannot violate database constraints or create duplicate records for the same dead-lettered job.

---

## 5. Graceful Worker Shutdown

### 5.1 Lifecycle State Machine

A worker shell undergoes three distinct lifecycle phases:

```text
[ START ]
    │
    ▼
┌──────────────────┐
│      READY       │ ── Accepts claims, executes jobs, sends heartbeats (READY)
└──────────────────┘
    │
    │ (SIGTERM / stop({ drain: true }))
    ▼
┌──────────────────┐
│     DRAINING     │ ── Rejects new claims (NOT_CLAIMABLE)
└──────────────────┘    Rejects new execution calls
    │                   Heartbeat sends status DRAINING (scheduler excludes node)
    │                   Waits for in-flight executions (Promise.allSettled + timeout)
    │
    │ (In-flight tasks complete OR drain timeout expires)
    ▼
┌──────────────────┐
│     OFFLINE      │ ── Releases active leases
└──────────────────┘    Stops heartbeat timer
                        Deregisters from Redis
                        Closes DB / resources
```

### 5.2 Shutdown Sequence

Upon receiving `SIGTERM` or invoking `worker.stop()`:

1. **Enter DRAINING State**:
   - `currentStatus` is immediately set to `'DRAINING'`.
   - Any concurrent call to `worker.claimJob()` returns `{ status: 'NOT_CLAIMABLE', reason: 'JOB_NOT_CLAIMABLE' }`.
   - Any direct call to `worker.executeJob()` immediately throws an error.
2. **Heartbeat During Drain**:
   - The worker continues transmitting heartbeats with status `'DRAINING'`.
   - The scheduler checks `worker.status === 'READY' && liveness === 'ALIVE'`, strictly excluding `DRAINING` workers from candidate lists.
3. **Bounded In-Flight Wait**:
   - In-flight execution promises tracked in `activeTasks` are awaited via `Promise.race([Promise.allSettled(activeTasks), timeoutPromise])`.
   - Bounded by `drainTimeoutMs` (default: 30,000ms).
4. **Lease Release & Cleanup**:
   - Completed jobs release their leases normally.
   - Any remaining leases held by the worker are explicitly released via `leaseRepository.release()`.
5. **Deregistration & OFFLINE**:
   - Heartbeat interval is cleared.
   - Registry `deregister(workerId)` is called, removing the transient Redis key and setting worker record to `OFFLINE` in PostgreSQL.
6. **Idempotency**:
   - Repeated calls to `stop()` or racing signal handlers share the cached `shutdownPromise`, guaranteeing that teardown hooks execute exactly once.

---

## 6. Failure Matrix & Behavioral Invariants

| Failure Scenario                                    | Immediate System Behavior                                                        | Recovery Mechanism                                                                                   | Final Guaranteed State                                                                         |
| :-------------------------------------------------- | :------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------- |
| **Worker Host Crashes (Kernel Panic / Power Loss)** | Redis heartbeat TTL expires (worker becomes `STALE`). Scheduler excludes worker. | Leases expire at `expires_at`. `LeaseRecoveryService` runs. Attempt marked `FAILED` (`WORKER_LOST`). | Job requeued with backoff if retryable; moved to DLQ if retries exhausted.                     |
| **Worker Hangs / Process Freeze**                   | Worker stops renewing lease. Lease expires at `expires_at`.                      | `LeaseRecoveryService` expires lease, requeues job.                                                  | New worker acquires fresh lease. If old worker awakens, renewal is rejected (`LEASE_EXPIRED`). |
| **Transient Network Partition to Redis**            | Redis heartbeat key expires. Worker marked `STALE`.                              | PostgreSQL connection remains active. Worker continues renewing lease and executing jobs.            | Job completes normally. Worker returns to `ALIVE` once Redis connection restores.              |
| **Operator Cancels Job During Worker Crash**        | Job status set to `CANCELLED` in database.                                       | Lease expires. Recovery sees `job.status === 'CANCELLED'`. Marks lease `EXPIRED` and skips job.      | Job remains `CANCELLED`. Historical attempt recorded. No resurrection.                         |
| **Graceful Worker Restart (SIGTERM)**               | Worker enters `DRAINING`. Rejects new claims. Waits for in-flight tasks.         | In-flight container completes, results saved, lease released. Worker exits cleanly.                  | Zero lost jobs, zero orphaned leases, zero recovery required.                                  |
| **Drain Timeout Exceeded**                          | In-flight tasks exceed `drainTimeoutMs`. Worker aborts execution.                | Worker releases leases and exits.                                                                    | Unfinished attempts fail; lease expires or is released; job requeued.                          |
| **Concurrent Recovery Race**                        | Multiple schedulers scan expired leases at the same instant.                     | `FOR UPDATE SKIP LOCKED` gives lease to exactly 1 scheduler. Losers receive 0 rows.                  | Exactly 1 attempt record generated. Exactly 1 state transition. Zero duplicate DLQ records.    |
