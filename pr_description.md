# PR 15: Dead-Letter Queue, Worker Loss Recovery & Graceful Shutdown

## Summary

This pull request implements **PR 15: Dead-Letter Queue, Worker Loss Recovery & Graceful Shutdown** for Forge V2. It establishes Forge V2's core reliability, failure recovery, and graceful lifecycle management plane.

When a distributed worker node crashes, hangs, or partitions, Forge detects the loss authoritatively through PostgreSQL lease expiration (`worker_leases.status = 'ACTIVE' AND expires_at <= NOW()`), atomically recovers the abandoned in-flight attempt via transactional row locking (`FOR UPDATE SKIP LOCKED`), reconciles the attempt as `FAILED` (`failure_reason = 'WORKER_LOST'`), evaluates the pure retry policy, and either requeues the job with backoff or durably routes it to the Dead-Letter Queue (`dead_letter_jobs`).

Additionally, it introduces a formal three-phase worker shutdown lifecycle (`READY -> DRAINING -> OFFLINE`) allowing in-flight containerized tasks to complete naturally while rejecting new job claims and execution requests.

Forge strictly preserves **at-least-once delivery + idempotent state transitions**; no claims of exactly-once execution or zero data loss across catastrophic infrastructure failures.

---

## Key Architectural Guarantees & Invariants

1. **Decoupled Liveness vs. Lease Authority**:
   - Worker loss is detected strictly via PostgreSQL lease expiration (`worker_leases.status = 'ACTIVE' AND expires_at <= NOW()`).
   - Ephemeral Redis heartbeat expiration marks a worker `STALE` for scheduler candidate placement, but **never** modifies or revokes PostgreSQL leases. Active containers continue running as long as the worker can renew its PostgreSQL lease.

2. **Atomic Recovery via Row-Level Locking**:
   - `LeaseRecoveryService` discovers expired active leases and locks candidate rows with `SELECT ... FOR UPDATE SKIP LOCKED`.
   - Concurrent recovery sweeps (e.g. multiple schedulers racing on the same expired lease) execute safely: exactly one recovery worker reconciles the lease, while competing runners receive a harmless `NO_OP`.

3. **Attempt Historical Immutability**:
   - In-flight execution attempts interrupted by worker loss are marked `FAILED` with `failure_reason = 'WORKER_LOST'` and timestamped.
   - Historical `JobAttempt` records are write-once and strictly immutable (never overwritten, never deleted).

4. **Terminal Job Protection**:
   - Jobs in a terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT`) are never resurrected to `QUEUED`. If an operator cancelled a job while the worker was disconnected, the lease is marked `EXPIRED` and the job remains `CANCELLED`.

5. **Durable Dead-Letter Queue (DLQ)**:
   - Authoritative PostgreSQL table `dead_letter_jobs` with database constraint `UNIQUE(job_id)`.
   - Idempotent upsert (`ON CONFLICT (job_id) DO UPDATE`) prevents duplicate records under at-least-once recovery loops.
   - DLQ serves as an operational inspection holding area, not data destruction.

6. **Graceful Worker Drain Precedence**:
   - When stopping, worker immediately enters `DRAINING`.
   - Rejects new job claims (`NOT_CLAIMABLE`) and direct execution requests.
   - Continues transmitting heartbeats with status `DRAINING` to ensure exclusion from scheduler candidate placement.
   - Waits up to `drainTimeoutMs` for in-flight container tasks to finish, persists results, releases leases, and marks itself `OFFLINE`.

---

## What Was Implemented

### 1. Contracts Package (`packages/contracts`)
- Added `DeadLetterReason` taxonomy: `'WORKER_LOSS_RETRY_EXHAUSTED' | 'MAX_RETRIES_EXCEEDED' | 'NON_RETRYABLE_FAILURE' | 'EXECUTION_CANCELLED' | 'UNKNOWN_UNRECOVERABLE'`.
- Added `DeadLetterJob` interface for durable DLQ records.
- Added `RecoveryAction` (`REQUEUED`, `DEAD_LETTERED`, `SKIPPED_TERMINAL`, `NO_OP`).
- Added `RecoveredLeaseRecord` and `RecoverExpiredLeasesResult`.
- Added `LeaseRecoveryOptions` with configurable `batchSize` and `now`.
- Added `'DRAINING'` to `WorkerStatus`.
- Added `ClaimJobNotClaimableResult` (`status: 'NOT_CLAIMABLE'`) to `ClaimJobResult`.

### 2. Database Migration & Repositories (`packages/database`)
- **Migration `007_dead_letter_jobs.sql`**:
  - Creates `dead_letter_jobs` table with foreign keys to `jobs(id)` and `pipeline_runs(id)`.
  - Enforces `uq_dead_letter_jobs_job_id UNIQUE(job_id)`.
  - Creates indexes on `pipeline_run_id`, `reason`, and `created_at DESC`.
  - Updated `migrator.ts` and `resetDatabase()`.
- **`DeadLetterRepository` & `PgDeadLetterRepository`**:
  - Implemented typed repository with idempotent `save()`, `findByJobId()`, `list()`, and `count()`.
- **`LeaseRecoveryService`**:
  - Transactional recovery with `FOR UPDATE SKIP LOCKED`.
  - Reconciles active attempt as `FAILED` (`WORKER_LOST`).
  - Evaluates `evaluateRetry(attempt, job.retryPolicy)`.
  - Requeues retryable jobs to `QUEUED` with authoritative `nextAttemptAt`.
  - Inserts exhausted/non-retryable jobs into `dead_letter_jobs`.
  - Skips terminal jobs without resurrection.
  - Safely evaluates parent pipeline run completion.
- **`TransactionContext`**:
  - Added `deadLetterJobs` repository to transactional context.

### 3. Worker Service Shell (`apps/worker`)
- Implemented 3-phase shutdown lifecycle: `READY -> DRAINING -> OFFLINE`.
- Added `drain(timeoutMs)` and updated `stop(options)` with bounded `drainTimeoutMs`.
- `claimJob()` returns `{ status: 'NOT_CLAIMABLE', reason: 'JOB_NOT_CLAIMABLE' }` during `DRAINING`.
- `executeJob()` rejects new invocations during `DRAINING`.
- In-flight container tasks tracked in `activeTasks` and awaited up to timeout.
- Heartbeats continue transmitting status `DRAINING` during drain.
- Leases released cleanly and worker deregistered upon transition to `OFFLINE`.
- Cached `shutdownPromise` ensures idempotency across concurrent signals.

### 4. Scheduler Service (`apps/scheduler`)
- `isOperationallyEligible` strictly enforces `status === 'READY' && liveness === 'ALIVE'`, excluding `DRAINING` workers.
- Integrated `LeaseRecoveryService`: added `recoverExpiredLeases()`, `startRecoveryLoop()`, and `stopRecoveryLoop()` with overlap prevention.

### 5. Architecture Documentation
- Created `docs/architecture/reliability.md`: Full architecture specification for worker loss recovery, DLQ, graceful drain, and failure matrices.
- Updated `docs/architecture/invariants.md`: Added Section 9 (Reliability, Worker Loss & Dead-Letter Queue).
- Updated `docs/architecture/overview.md`: Added PR 15 to status and evolution path.
- Updated `docs/architecture/glossary.md`: Added definitions for DLQ, Worker Loss Detection, Lease Recovery, Graceful Worker Drain, and DRAINING State.
- Updated `README.md`.

---

## Verification Evidence

### 1. PostgreSQL Lease Recovery & DLQ Integration Tests (`@forge/database`)
```text
 ✓ packages/database/src/repositories/lease-recovery.integration.test.ts (5 tests) 744ms
   ✓ recovers expired lease for retryable job: reconciles attempt as FAILED, requeues job with backoff
   ✓ concurrent recovery race: two concurrent recovery workers racing on same expired lease
   ✓ retry exhaustion: transitions job to FAILED and atomically inserts single DLQ record
   ✓ rejects stale owner renewal after lease has been expired and recovered
   ✓ never resurrects terminal jobs that were cancelled during worker execution
```

### 2. Live Docker Worker Loss Recovery & Graceful Drain Tests (`apps/worker`)
```text
 ✓ apps/worker/src/worker-loss-recovery.integration.test.ts (3 tests) 4204ms
   ✓ recovers lost worker executing Docker container, requeues with backoff, and Worker B succeeds on next attempt
   ✓ sends job to DLQ when worker is lost and retry policy is exhausted
   ✓ gracefully drains in-flight Docker execution: completes active container, releases lease, and rejects new work
```

### 3. Redis Liveness & Ephemeral Decoupling Tests (`@forge/worker-registry`)
```text
 ✓ packages/worker-registry/src/registry.test.ts (8 tests) 4433ms
   ✓ heartbeat loss marks worker as STALE without mutating database status (PostgreSQL remains authority)
   ✓ detects a crashed worker as STALE after TTL expires while preserving durable record
```

### 4. Scheduler Operational Eligibility & Recovery Delegation Tests (`apps/scheduler`)
```text
 ✓ apps/scheduler/src/scheduler.test.ts (32 tests) 190ms
   ✓ rejects DRAINING worker from candidate placement
   ✓ delegates recoverExpiredLeases to configured recoveryService
   ✓ starts and cleanly stops periodic recovery loop without overlapping sweeps
```

### 5. Worker Lifecycle & Drain Unit Tests (`apps/worker`)
```text
 ✓ apps/worker/src/index.test.ts (12 tests) 212ms
   ✓ transitions READY -> DRAINING -> OFFLINE on graceful stop
   ✓ rejects new job claims and execution invocations when DRAINING
   ✓ waits for in-flight tasks during drain before stopping
   ✓ ensures stop is idempotent when called multiple times concurrently
```

### 6. Full Repository Quality Verification
```text
npm run format:check  --> Passed: All matched files use Prettier code style!
npm run lint          --> Passed: 0 errors, 0 warnings across all workspaces!
npm run typecheck     --> Passed: tsc -b exited with code 0!
npm run build         --> Passed: All workspaces and Next.js production build succeeded!
npm test              --> Passed: 39 test files passed, 425 tests passed (0 failures)!
```
