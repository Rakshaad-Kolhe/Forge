# PR 14: Retry Policies, Exponential Backoff & Attempt Orchestration

## Summary

This pull request implements **PR 14: Retry Policies, Exponential Backoff & Attempt Orchestration** for Forge V2. It transforms execution retries from a hidden, volatile worker-level loop into a **first-class, durable, testable orchestration primitive**.

When a containerized job fails or times out, Forge evaluates a pure, deterministic retry policy, calculates bounded exponential backoff, persists the retry state and schedule authoritatively in PostgreSQL, immediately releases the worker lease to prevent worker pinning, and allows any compatible worker to claim subsequent attempts with fresh leases when the backoff delay elapses.

---

## Key Architectural Decisions & Guarantees

1. **Pure Deterministic Retry Evaluation (`evaluateRetry`)**:
   - Zero I/O, zero side-effects, fully idempotent pure function in `@forge/pipeline`.
   - Returns a typed discriminated union `RetryDecision` (`RETRY`, `FINAL_FAILURE`, or `NOT_RETRYABLE`).
   - Distinguishes terminal failures (e.g. `SUCCEEDED`, `CANCELLED`, unconfigured outcomes) from retryable outcomes (`FAILED`, `TIMED_OUT`).

2. **Bounded Exponential Backoff with Overflow Protection**:
   - Formally implemented: $\text{delay} = \min\left(D_{\text{max}}, D_{\text{base}} \times F^{k - 1}\right)$ with $k = \text{attemptNumber}$.
   - Safe against floating-point overflow (`Number.MAX_SAFE_INTEGER`), `Infinity`, and NaN.
   - Enforces configurable upper bounds (`MAX_RETRY_BACKOFF_LIMIT_MS = 3600000ms`, default `60000ms`).
   - PR 14 strictly omits random jitter, ensuring 100% deterministic, reproducible regression testing.

3. **Attempt Immutability & Collision Safety**:
   - Historical auditability invariant (ADR-004): Every physical execution attempt generates an independent, immutable `JobAttempt` record.
   - Previous attempt records are **never** mutated, overwritten, or re-used.
   - Collision safety is enforced authoritatively by PostgreSQL constraint `UNIQUE(job_id, attempt_number)`.
   - Attempt IDs follow deterministic naming: `${job_id}-attempt-${attempt_number}`.

4. **Durable PostgreSQL Scheduling & Partial Index**:
   - Migration `006_job_retries.sql` adds `retry_policy JSONB` and `next_attempt_at TIMESTAMPTZ` columns to `jobs`.
   - High-throughput partial index `idx_jobs_retry_schedulable ON jobs(status, next_attempt_at, priority DESC) WHERE status = 'QUEUED'`.
   - Retry schedules survive worker crashes, scheduler failovers, and node restarts.

5. **Lease Isolation & Worker Hopping**:
   - Worker leases are **always released** upon attempt completion, regardless of whether a retry is scheduled.
   - Neither the original worker nor any other worker holds a lease during the backoff period.
   - Sequential attempts of the same job can be claimed and executed by different worker nodes ("worker hopping"), eliminating host-specific failure loops.

6. **Non-Blocking Scheduler Semantics**:
   - Jobs in active backoff (`next_attempt_at > NOW()`) evaluate to `UNSCHEDULABLE` with reason `RETRY_BACKOFF_ACTIVE`.
   - Active backoffs never block ready jobs or lower-priority jobs from being scheduled.

---

## What Was Implemented

### 1. Contracts Package (`packages/contracts`)

- Added `RetryCondition = 'FAILED' | 'TIMED_OUT'`.
- Added `BackoffPolicy`, `RetryPolicy`, and `RetryDecision` interfaces and types.
- Added system limits: `DEFAULT_MAX_ATTEMPTS = 1`, `MAX_JOB_ATTEMPTS_LIMIT = 10`, `DEFAULT_RETRY_BASE_DELAY_MS = 1000`, `DEFAULT_MAX_BACKOFF_MS = 60000`, `MAX_RETRY_BACKOFF_LIMIT_MS = 3600000`.
- Added `RETRY_BACKOFF_ACTIVE` to `UnschedulableReason`.
- Extended `AppConfig` with default and maximum retry bounds.

### 2. Configuration Package (`packages/config`)

- Extended `configSchema` with validation for `DEFAULT_MAX_ATTEMPTS`, `MAX_JOB_ATTEMPTS`, `DEFAULT_RETRY_BASE_DELAY_MS`, and `MAX_RETRY_BACKOFF_MS`.
- Added schema refinement rules: `MAX_JOB_ATTEMPTS >= DEFAULT_MAX_ATTEMPTS` and `MAX_RETRY_BACKOFF_MS >= DEFAULT_RETRY_BASE_DELAY_MS`.
- Unit tests covering configuration bounds and schema refinements (11/11 passing).

### 3. Pipeline Domain Core (`packages/pipeline`)

- Added `retry.ts`:
  - `validateRetryPolicy`: Validates and normalizes `maxAttempts`, `backoff`, and `retryOn`.
  - `calculateBackoffDelay`: Implements bounded exponential backoff with overflow protection.
  - `evaluateRetry`: Pure decision function evaluating attempt outcome and policy.
- Added `RetryPolicyValidationError` in `errors.ts`.
- Updated `Job` domain model: supports `retryPolicy`, `nextAttemptAt`, `setNextAttemptAt`, `clearNextAttemptAt`, `createAttempt`, and serialization.
- Updated `state-machine.ts`: added `QUEUED` to allowed transitions from `RUNNING` (`RUNNING: ['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'QUEUED']`).
- Updated `PipelineStep` and `PipelineRun`: propagates step retry policies to constituent jobs; pipeline run completion evaluation respects retryable `QUEUED` jobs.
- 20 unit tests in `retry.test.ts` (132/132 tests passing in `@forge/pipeline`).

### 4. Database Package (`packages/database`)

- Created migration `006_job_retries.sql` and updated `migrator.ts`.
- Extended `JobRow` and `JobRepository` with `retry_policy`, `next_attempt_at`, and `findSchedulableJobs(options?: { now?: Date; limit?: number })`.
- Updated `PgJobRepository`:
  - Transactionally saves and maps `retry_policy` and `next_attempt_at`.
  - Implements `findSchedulableJobs` querying `idx_jobs_retry_schedulable` ordered by priority and creation time.
- Repository tests verifying retry policy and backoff query mechanics against live PostgreSQL (26/26 passing).

### 5. Task Scheduler Service (`apps/scheduler`)

- Extended `evaluatePlacement`: inspects `job.nextAttemptAt`; returns `UNSCHEDULABLE` with `reason: 'RETRY_BACKOFF_ACTIVE'` if delay has not elapsed.
- Extended `ForgeScheduler`: added `scheduleDueJobs(options?: { now?: Date; limit?: number })` discovering and prioritizing due retries.
- Extended `createJobSourceFromRepository` to delegate `findSchedulableJobs`.
- 28 unit tests in `scheduler.test.ts` verifying retry backoff awareness, timing, and prioritized due job scheduling.

### 6. Worker Service Shell (`apps/worker`)

- Extended `executeJob` in `index.ts`:
  - Evaluates `evaluateRetry(attempt, job.retryPolicy)` upon attempt failure or timeout.
  - If retry scheduled: transitions job to `QUEUED`, sets `nextAttemptAt`, emits structured `job execution failed but retry scheduled` log, persists transactionally, and releases lease.
  - If retry exhausted: transitions job to terminal `FAILED` or `TIMED_OUT`, clears `nextAttemptAt`, persists transactionally, and releases lease.
  - Lease loss invariant: if lease ownership was lost during execution, worker immediately aborts and does not trigger retry from that node.
- 9 unit tests in `index.test.ts`.
- 8 live integration tests in `worker-retry.integration.test.ts` testing full end-to-end execution against real PostgreSQL and Docker engines.

### 7. Architecture Documentation (`docs/architecture/`)

- Created `docs/architecture/retry.md` detailing system design, backoff mathematics, schema changes, lease isolation, and verification matrix.
- Updated `invariants.md`: added Section 8 (Retry & Attempt Orchestration).
- Updated `overview.md`: added PR 14 to component roadmap and evolutions.
- Updated `glossary.md`: added terms (`Retry Policy`, `Backoff Policy`, `Retry Decision`, `nextAttemptAt`, `Attempt Immutability`, `Worker Hopping`).

---

## Verification & Quality Gates

```bash
npm run format:check  # Passed: Prettier code style verified
npm run lint          # Passed: 0 errors, 0 warnings across all workspaces
npm run typecheck     # Passed: Clean compilation across all composite project references
npm test              # Passed: All unit and integration test suites passing
npm run build         # Passed: Clean production build across all packages and apps
```
