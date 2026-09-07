# Distributed Worker Leases & Job Ownership

## 1. Executive Summary

Forge V2 implements distributed job ownership through renewable, time-bounded **worker leases** backed authoritatively by PostgreSQL.

Prior to PR 12, Forge established:

- Pure domain state machine and pipeline execution DAG (PR 03)
- Authoritative PostgreSQL persistence and transactional integrity (PR 04, PR 05)
- Redis coordination foundation (PR 06)
- Reliable FIFO job queuing with visibility timeout recovery (PR 07)
- Worker registration, heartbeat, and lifecycle tracking (PR 08)
- Capability and hardware resource matching (PR 09)
- Deterministic worker selection (PR 10)
- Priority scheduling (PR 11)

PR 12 establishes **distributed job ownership**:

> **A selected worker atomically claims a job through a renewable, time-bounded lease backed authoritatively by PostgreSQL.**

---

## 2. Core Invariant

> **At most one active lease owner is accepted for a job at a given point in time, according to the authoritative PostgreSQL state and atomic claim operation.**

Forge explicitly rejects lock-free dual writes, distributed consensus layers, or Redis-as-authoritative-lock architectures for job ownership. PostgreSQL is the single source of truth for lease state and ownership.

---

## 3. Distributed Failure Model

```text
Worker disappears / process crashes
       ↓
Heartbeat stops in Redis
       ↓
Lease reaches expires_at in PostgreSQL
       ↓
Lease becomes expired according to DB NOW()
       ↓
Queue visibility timeout recovers message OR scheduler re-evaluates job
       ↓
Another eligible worker claims the job
       ↓
Prior lease transitioned to EXPIRED; new ACTIVE lease granted
       ↓
If original worker revives, its renewal or release attempts are rejected
```

---

## 4. PostgreSQL Relational Schema

Authoritative lease state is persisted in the `worker_leases` table created in migration `005_worker_leases`:

```sql
CREATE TABLE IF NOT EXISTS worker_leases (
  id VARCHAR(255) PRIMARY KEY,
  job_id VARCHAR(255) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  worker_id VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL,
  duration_ms INTEGER NOT NULL CHECK (duration_ms > 0),
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_worker_leases_status CHECK (
    status IN ('ACTIVE', 'RELEASED', 'EXPIRED')
  )
);

-- Crucial: Guarantees at most one ACTIVE lease can exist per job
CREATE UNIQUE INDEX IF NOT EXISTS uq_worker_leases_active_job
  ON worker_leases(job_id)
  WHERE status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_worker_leases_job_id ON worker_leases(job_id);
CREATE INDEX IF NOT EXISTS idx_worker_leases_worker_id ON worker_leases(worker_id);
CREATE INDEX IF NOT EXISTS idx_worker_leases_status ON worker_leases(status);
CREATE INDEX IF NOT EXISTS idx_worker_leases_expires_at ON worker_leases(expires_at);
```

### Key Schema Characteristics

1. **Partial Unique Index (`uq_worker_leases_active_job`)**: Enforces database-level mutual exclusivity across concurrent transactions: at most one row with `status = 'ACTIVE'` can exist for any given `job_id`.
2. **Audit History Preserved**: Released and expired leases remain in `worker_leases` with their full audit metadata (`acquired_at`, `renewed_at`, `expires_at`, `status`).

---

## 5. Clock Authority & Time Invariant

- **Database Clock as Sole Authority**: All lease timestamps (`acquired_at`, `renewed_at`, `expires_at`) and expiry comparisons are computed directly in PostgreSQL using `NOW()` and interval arithmetic:
  ```sql
  expires_at = NOW() + ($durationMs * INTERVAL '1 millisecond')
  ```
- **Zero Clock Skew Vulnerability**: Workers never supply client-side timestamps for lease validity checks. Skew across worker hosts cannot cause premature lease expiration or split-brain ownership.

---

## 6. Atomic Lease Operations

### 6.1 `claim(options: ClaimJobOptions): Promise<ClaimJobResult>`

Executed inside an ACID transaction on PostgreSQL:

1. **Row Lock**: `SELECT id, status FROM jobs WHERE id = $1 FOR UPDATE;` serializes concurrent claims for the same job.
2. **Status Check**: Verifies job exists and is in `QUEUED` status (otherwise returns `NOT_CLAIMABLE`).
3. **Active Lease Evaluation**: Checks if an active lease already exists:
   - **Unexpired & Same Worker**: Returns idempotent success (`ACQUIRED`, `isIdempotent: true`).
   - **Unexpired & Different Worker**: Returns `CONFLICT` (`LEASE_ALREADY_HELD`, `currentOwnerId`, `expiresAt`).
   - **Expired**: Marks prior lease `EXPIRED` (`UPDATE worker_leases SET status = 'EXPIRED' WHERE id = ...`) and proceeds to step 4.
4. **Lease Insertion**: Inserts new row with `status = 'ACTIVE'`, `expires_at = NOW() + ($durationMs * INTERVAL '1 millisecond')`. Returns `ACQUIRED` (`isIdempotent: false`).

### 6.2 `renew(options: RenewLeaseOptions): Promise<RenewLeaseResult>`

Atomic update:

```sql
UPDATE worker_leases
SET renewed_at = NOW(),
    expires_at = NOW() + (COALESCE($4, duration_ms) * INTERVAL '1 millisecond'),
    duration_ms = COALESCE($4, duration_ms)
WHERE id = $1
  AND job_id = $2
  AND worker_id = $3
  AND status = 'ACTIVE'
  AND expires_at > NOW()
RETURNING *;
```

If 0 rows match, queries lease to return precise diagnostic reason (`LEASE_NOT_FOUND`, `LEASE_OWNER_MISMATCH`, or `LEASE_EXPIRED`).

### 6.3 `release(options: ReleaseLeaseOptions): Promise<ReleaseLeaseResult>`

Atomic transition:

```sql
UPDATE worker_leases
SET status = 'RELEASED'
WHERE id = $1
  AND job_id = $2
  AND worker_id = $3
  AND status = 'ACTIVE'
RETURNING *;
```

Transitions status to `RELEASED`. If 0 rows match, inspects reason (`LEASE_NOT_FOUND`, `LEASE_OWNER_MISMATCH`, or `LEASE_ALREADY_INACTIVE`).

### 6.4 `reclaimExpiredLeases(): Promise<number>`

Bulk transition for background sweeps:

```sql
UPDATE worker_leases
SET status = 'EXPIRED'
WHERE status = 'ACTIVE'
  AND expires_at <= NOW();
```

---

## 7. Interaction with Queue & Scheduler

1. **At-Least-Once Delivery**: When `scheduleNext` or `scheduleNextBatch` dequeues a message and claims a lease, it **does NOT acknowledge (`ACK`)** the queue message.
2. **Recoverability**: The message remains under Redis visibility timeout. If the worker crashes, its lease expires, and the queue visibility timeout expires, making the message available for re-scheduling.
3. **Non-Blocking Conflict**: If a job placement encounters a `LEASE_CONFLICT` during prioritized batch evaluation, it is recorded as `UNSCHEDULABLE` and evaluation immediately proceeds to remaining jobs.

---

## 8. Verification & Empirical Evidence

- **Section 51 (20-step verification)**: Verified end-to-end against live PostgreSQL and Redis in `apps/scheduler/src/lease.smoke.test.ts`.
- **Section 52 (Concurrency race)**: 10 concurrent claimants racing simultaneously for an unleased job:
  - Exactly 1 winner (`ACQUIRED`, `isIdempotent: false`).
  - Exactly 9 conflicts (`CONFLICT`, `LEASE_ALREADY_HELD`).
  - Database contains exactly 1 `ACTIVE` lease row.
