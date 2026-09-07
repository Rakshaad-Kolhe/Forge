# Forge V2 — Retry Policies, Exponential Backoff & Attempt Orchestration

## 1. Overview & Core Philosophy

In a distributed CI/CD engine, execution failure is an expected operational occurrence rather than an immediate terminal state. Transient network blips, resource contention, and third-party rate limits can cause process failures that succeed upon subsequent execution.

However, naive retry mechanisms introduce significant operational hazards:

- **Hidden in-memory retry loops** within workers hold onto worker leases, monopolize execution nodes, and fail silently during worker crashes.
- **Tight retry loops** stampede shared infrastructure and downstream services.
- **Mutating attempt records** destroys historical auditability, execution metrics, and debugging evidence.

Forge V2 implements retries as a **first-class, durable, testable orchestration primitive**:

1. **Decoupled Evaluation**: The decision to retry is evaluated via a pure, deterministic state-machine function (`evaluateRetry`).
2. **Attempt Immutability**: Every physical execution attempt generates a distinct, immutable `JobAttempt` record with collision-safe attempt numbering `(job_id, attempt_number)`.
3. **Durable Backoff Persistence**: Delayed retries are scheduled authoritatively in PostgreSQL via `next_attempt_at` and a high-performance partial index, surviving worker, scheduler, and node restarts.
4. **Fresh Worker Leases**: The worker lease from the failed attempt is immediately released. Future attempts require a brand new lease acquisition, allowing retries to float to other healthy worker nodes ("worker hopping").
5. **Non-Blocking Scheduling**: Jobs in active backoff never block ready or lower-priority jobs from being scheduled.

```
+-----------------------------------------------------------------------------+
|                               EXECUTION PLANE                               |
|                                                                             |
|   +-----------------------+              +------------------------------+   |
|   |   Worker Node Alpha   |              |        Docker Executor       |   |
|   |  - Holds Lease A      | -----------> | - Spawns container           |   |
|   |  - Runs Attempt #1    |              | - Captures exit code 1       |   |
|   +-----------------------+              +------------------------------+   |
|               |                                         |                   |
|               v                                         v                   |
|   +-----------------------+              +------------------------------+   |
|   |  Attempt #1: FAILED   |              |       evaluateRetry()        |   |
|   | (Immutable Record)    |              | - Pure function              |   |
|   +-----------------------+              | - delay = min(max, base*f^0) |   |
|               |                          | - Result: ACTION=RETRY       |   |
|               |                          +------------------------------+   |
|               v                                         |                   |
|   +-----------------------+                             |                   |
|   | Release Lease A       | <---------------------------+                   |
|   +-----------------------+                                                 |
+---------------+-------------------------------------------------------------+
                |
                v (Transactionally persisted to PostgreSQL)
+-----------------------------------------------------------------------------+
|                            DURABLE STORAGE PLANE                            |
|                                                                             |
|   PostgreSQL `jobs` table:                                                  |
|     status: 'QUEUED'                                                        |
|     next_attempt_at: NOW() + 1000ms                                         |
|                                                                             |
|   Index: `idx_jobs_retry_schedulable` (WHERE status = 'QUEUED')             |
+-----------------------------------------------------------------------------+
                |
                | (Time elapses: next_attempt_at <= NOW())
                v
+-----------------------------------------------------------------------------+
|                           SCHEDULING & RETRY PLANE                          |
|                                                                             |
|   +-----------------------+              +------------------------------+   |
|   |    ForgeScheduler     |              |       Worker Node Beta       |   |
|   | - findSchedulableJobs | -----------> | - Claims Fresh Lease B       |   |
|   | - High Priority First |              | - Executes Attempt #2        |   |
|   +-----------------------+              +------------------------------+   |
+-----------------------------------------------------------------------------+
```

---

## 2. Deterministic Retry Evaluation

Retry evaluation is decoupled from I/O, database access, and timers. The pure function `evaluateRetry(attempt, policy)` evaluates the execution outcome against the step's policy.

### 2.1 Interface & Types

```typescript
export type RetryCondition = 'FAILED' | 'TIMED_OUT';

export interface BackoffPolicy {
  readonly strategy: 'EXPONENTIAL' | 'FIXED';
  readonly baseDelayMs: number;
  readonly factor?: number;
  readonly maxDelayMs: number;
}

export interface RetryPolicy {
  readonly maxAttempts: number;
  readonly backoff?: BackoffPolicy;
  readonly retryOn?: readonly RetryCondition[];
}

export type RetryDecision =
  | {
      readonly action: 'RETRY';
      readonly attemptNumber: number;
      readonly nextAttemptNumber: number;
      readonly delayMs: number;
      readonly reason: string;
    }
  | {
      readonly action: 'FINAL_FAILURE';
      readonly attemptNumber: number;
      readonly reason: 'MAX_ATTEMPTS_EXHAUSTED' | 'OUTCOME_NOT_RETRYABLE';
      readonly details: string;
    }
  | {
      readonly action: 'NOT_RETRYABLE';
      readonly attemptNumber: number;
      readonly reason: 'NO_POLICY' | 'SUCCEEDED' | 'CANCELLED';
      readonly details: string;
    };
```

### 2.2 Decision Matrix

| Attempt Status | Policy Configured? | Attempt vs Max   | `retryOn` Match? | Decision Action | Transition                     |
| :------------- | :----------------- | :--------------- | :--------------- | :-------------- | :----------------------------- |
| `SUCCEEDED`    | Any                | Any              | N/A              | `NOT_RETRYABLE` | Job `SUCCEEDED`                |
| `CANCELLED`    | Any                | Any              | N/A              | `NOT_RETRYABLE` | Job `CANCELLED`                |
| `FAILED`       | None               | Any              | N/A              | `NOT_RETRYABLE` | Job `FAILED`                   |
| `FAILED`       | Yes                | `< maxAttempts`  | Yes              | `RETRY`         | Job `QUEUED` + `nextAttemptAt` |
| `FAILED`       | Yes                | `>= maxAttempts` | Yes              | `FINAL_FAILURE` | Job `FAILED`                   |
| `FAILED`       | Yes                | `< maxAttempts`  | No               | `FINAL_FAILURE` | Job `FAILED`                   |
| `TIMED_OUT`    | Yes                | `< maxAttempts`  | Yes              | `RETRY`         | Job `QUEUED` + `nextAttemptAt` |
| `TIMED_OUT`    | Yes                | `>= maxAttempts` | Yes              | `FINAL_FAILURE` | Job `TIMED_OUT`                |

---

## 3. Bounded Exponential Backoff

### 3.1 Mathematical Specification

For an attempt number $k \in \mathbb{N}_{\ge 1}$, base delay $D_{\text{base}} \in \mathbb{R}^+$, factor $F \ge 1$, and cap $D_{\text{max}} \ge D_{\text{base}}$:

$$\text{delay}(k) = \min\left(D_{\text{max}}, D_{\text{base}} \times F^{k - 1}\right)$$

### 3.2 Overflow Protection & Edge Cases

When calculating $F^{k - 1}$ with high attempt counts or extreme factors, floating-point numbers can exceed `Number.MAX_SAFE_INTEGER` or reach `Infinity`. The calculation in `calculateBackoffDelay` incorporates explicit bounds:

```typescript
export function calculateBackoffDelay(attemptNumber: number, backoff?: BackoffPolicy): number {
  if (!backoff || backoff.strategy === 'FIXED') {
    return Math.min(
      backoff?.maxDelayMs ?? DEFAULT_MAX_BACKOFF_MS,
      backoff?.baseDelayMs ?? DEFAULT_RETRY_BASE_DELAY_MS,
    );
  }

  const exponent = Math.max(0, attemptNumber - 1);
  const factor = backoff.factor ?? 2;
  const rawMultiplier = Math.pow(factor, exponent);

  if (!Number.isFinite(rawMultiplier) || rawMultiplier > Number.MAX_SAFE_INTEGER) {
    return backoff.maxDelayMs;
  }

  const calculated = backoff.baseDelayMs * rawMultiplier;
  if (!Number.isFinite(calculated) || calculated >= backoff.maxDelayMs) {
    return backoff.maxDelayMs;
  }

  return Math.round(calculated);
}
```

### 3.3 PR 14 Jitter Boundary

PR 14 intentionally omits random jitter. Deterministic backoff values are necessary for reproducible, assertion-driven verification in distributed regression suites. Random jitter may be added as an optional extension in future PRs without altering the backoff interface.

---

## 4. Durable Storage & Schema Changes

To survive node crashes and scheduler failovers, retry schedules are committed directly to PostgreSQL.

### 4.1 Migration `006_job_retries.sql`

```sql
-- Migration 006: Add retry policies and durable backoff scheduling
ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS retry_policy JSONB,
ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

-- Partial index for high-throughput schedulable job discovery
CREATE INDEX IF NOT EXISTS idx_jobs_retry_schedulable
ON jobs(status, next_attempt_at, priority DESC)
WHERE status = 'QUEUED';
```

### 4.2 Partial Index Performance Justification

The partial index `idx_jobs_retry_schedulable` filters strictly for `WHERE status = 'QUEUED'`. In a high-throughput production engine where completed historical jobs number in the millions, this partial index contains only active queued jobs (typically hundreds or thousands).

The composite indexing on `(status, next_attempt_at, priority DESC)` enables PostgreSQL index scans to satisfy queries of the form:

```sql
SELECT * FROM jobs
WHERE status = 'QUEUED'
  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
ORDER BY priority DESC, created_at ASC
LIMIT $1;
```

---

## 5. Attempt Immutability & Collision Safety

Historical integrity is a non-negotiable architectural invariant (ADR-004). Attempt records in the `job_attempts` table represent physical execution episodes and are strictly immutable once created.

- **No Overwrites**: Attempt 1 is never updated or overwritten to reflect Attempt 2.
- **Collision-Safe Unique Constraint**: The database enforces `UNIQUE(job_id, attempt_number)`. Any race condition attempting to spawn two identical attempt numbers for the same job is rejected at the database engine level.
- **Deterministic ID Generation**: Attempt IDs follow the format `${job_id}-attempt-${attempt_number}`.

---

## 6. Lease Isolation & Worker Hopping

A common failure mode in distributed CI systems is **worker pinning during retry**: if a worker fails a job due to a host-specific issue (disk full, corrupted local Docker cache, bad network adapter), immediately retrying on the same worker repeats the failure.

Forge V2 strictly decouples attempts via **Lease Isolation**:

1. When Attempt 1 completes (success, failure, or timeout), the worker immediately releases its lease via `releaseLease(leaseId, jobId)`.
2. The job enters `QUEUED` status with `next_attempt_at` set to a future timestamp.
3. Neither Worker Node Alpha nor any other worker holds a lock or lease during the backoff period.
4. When `next_attempt_at <= NOW()`, the scheduler exposes the job to candidate selection.
5. Any available, capable worker (e.g., Worker Node Beta) can claim the job with a **fresh lease ID**.

---

## 7. Verification Evidence

The implementation was validated against a strict test matrix using real PostgreSQL and real Docker daemons:

| Test Scenario                       | Verification Method                                                           | Outcome                                             |
| :---------------------------------- | :---------------------------------------------------------------------------- | :-------------------------------------------------- |
| **Validation & Clamping**           | Unit test suite (`retry.test.ts`) covering 20 edge cases                      | Pass (20/20)                                        |
| **Backoff Formula Bounds**          | Verified $\min(D_{\text{max}}, D_{\text{base}} \times F^{k-1})$ with overflow | Pass                                                |
| **Single Attempt Failure**          | Integration test: `maxAttempts=1`, exit 42 $\to$ final `FAILED`               | Pass (1 attempt record, lease released)             |
| **Fail then Succeed**               | Integration test: Attempt 1 fails, Attempt 2 succeeds                         | Pass (2 attempt records, fresh leases, `SUCCEEDED`) |
| **Retry Exhaustion**                | Integration test: 3 consecutive failures $\to$ terminal `FAILED`              | Pass (3 attempt records, 3 unique leases)           |
| **Backoff Delay Accuracy**          | Integration test: measured DB `next_attempt_at` vs wall-clock                 | Pass (within container tolerance)                   |
| **Worker Hopping**                  | Integration test: Attempt 1 on Alpha, Attempt 2 on Beta                       | Pass (verified distinct worker IDs in attempts)     |
| **Scheduler Priority with Retries** | Integration test: High-priority due retry placed before low-priority          | Pass                                                |
| **Cancellation Override**           | Integration test: User cancel on queued retry sets `CANCELLED`                | Pass (removed from schedulable set)                 |
