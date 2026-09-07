# PR 11: Priority Scheduling & Deterministic Job Ordering

## Summary

This pull request implements **PR 11: Priority Scheduling & Deterministic Job Ordering** for Forge V2. It extends the deterministic scheduler foundation established in PR 10 with explicit, bounded job priority and deterministic job ordering (`HighestPriorityFirstPolicy`), ensuring higher-priority eligible jobs receive placement before lower-priority jobs while preserving existing capability/resource matching and worker selection boundaries.

---

## Architectural Objectives & Non-Blocking Semantics

1. **Priority Domain Model**:
   - Job priority is a strictly bounded integer: `[-1000, 1000]` with `DEFAULT_JOB_PRIORITY = 0`.
   - Priority is declared on `PipelineStep.priority`, propagated deterministically during `PipelineRun.create()` onto `Job.priority`, and persisted in PostgreSQL under `jobs.priority`.
   - Full domain validation (`checkJobPriorityValidity`, `validateJobPriority`, `InvalidJobPriorityError`) rejects out-of-range or non-integer values.

2. **Database Persistence & Integrity**:
   - Migration `004_job_priority.sql` adds column `priority INTEGER NOT NULL DEFAULT 0` with table check constraint `chk_jobs_priority CHECK (priority >= -1000 AND priority <= 1000)` and index `idx_jobs_priority ON jobs(priority DESC)`.
   - `PgJobRepository` persists and reconstructs `priority` accurately across `save`, `findById`, and `findByPipelineRunId`.

3. **Deterministic Job Ordering Policy (`HighestPriorityFirstPolicy`)**:
   - Compares jobs by priority descending: `(b.priority ?? 0) - (a.priority ?? 0)`.
   - Canonical tie-breaking: when priorities match, ties are broken deterministically by job ID in ascending code-point order (`(idA < idB ? -1 : (idA > idB ? 1 : 0))`).
   - Permutation-invariant: shuffling candidate jobs yields the identical evaluation sequence and selected placements.

4. **Non-Blocking Unschedulable Semantics**:
   - If a higher-priority job cannot be scheduled (e.g. requires 64 CPU cores when only 4-core workers exist), it is marked `UNSCHEDULABLE` with explainable failure diagnostics.
   - Evaluation proceeds immediately to the next highest priority jobs in the batch.
   - An unschedulable high-priority job NEVER blocks lower-priority eligible jobs from being scheduled.

5. **Decoupled Queue Transport**:
   - Strict FIFO queue semantics (`LPUSH` / `RPOP` in Redis) are preserved in `@forge/queue`.
   - Priority is a scheduler policy concern, not a transport concern.
   - `ForgeScheduler.scheduleNextBatch` dequeues batches and evaluates them in priority order without prematurely acknowledging messages, preserving unacknowledged recoverability under visibility timeout.

---

## What Was Implemented

### 1. Contracts Package (`packages/contracts`)

- Added priority constants: `DEFAULT_JOB_PRIORITY = 0`, `MIN_JOB_PRIORITY = -1000`, `MAX_JOB_PRIORITY = 1000`.
- Added optional `priority?: number` field to `ScheduledDecision` and `UnschedulableDecision`.

### 2. Pipeline Domain Package (`packages/pipeline`)

- `InvalidJobPriorityError extends PipelineValidationError`.
- Pure validator functions: `checkJobPriorityValidity` and `validateJobPriority`.
- Added `priority` to `PipelineStep`, `Job`, and serialization representations (`PipelineStepSerialized`, `JobSerialized`).
- Updated `PipelineRun.create()` to copy step priority to job priority upon run creation.
- 16 new unit tests in `priority.test.ts`.

### 3. Database Package (`packages/database`)

- Migration `004_job_priority.sql` adding column, check constraint, and index.
- Registered migration in `migrator.ts`.
- Added `priority?: number` to `JobRow`.
- Updated `PgJobRepository.save()`, `findById()`, `findByPipelineRunId()`, and `mapRowToDomain()` to persist and rehydrate priority.
- Integration tests in `repositories.test.ts` verifying round-trip persistence and database CHECK constraint enforcement.

### 4. Scheduler Service (`apps/scheduler`)

- Created `job-policy.ts`:
  - `JobOrderingPolicy` interface.
  - `compareJobPriority` comparator.
  - `orderJobsByPriority` helper.
  - `HighestPriorityFirstPolicy` class and `highestPriorityFirstPolicy` singleton.
- Updated `scheduler.ts`:
  - `evaluatePlacement` attaches `priority` to all scheduling decisions.
  - Added pure batch evaluator `evaluatePrioritizedWork`.
  - Added `ForgeScheduler.schedulePrioritized(jobsOrIds)`.
  - Added `ForgeScheduler.scheduleNextBatch(queue, batchSize, options)`.
- Re-exported `job-policy.js` from `index.ts`.
- Comprehensive unit, integration, and smoke tests.

### 5. Architectural Documentation (`docs/architecture/`)

- Updated `docs/architecture/scheduler.md` with priority scheduling architecture, tie-breaking rules, non-blocking unschedulable semantics, and updated non-goals.
- Updated `docs/architecture/overview.md` with PR 11 completion and evolution path.
- Updated `docs/architecture/glossary.md` with definitions for `Job Priority`, `HighestPriorityFirst`, and `Non-Blocking Unschedulable Semantics`.
- Updated `README.md`.

---

## Explicit Non-Goals for PR 11

- **No Fairness / Starvation Prevention**: No aging, priority decay, round-robin, or anti-starvation boost.
- **No Worker Capacity Reservation**: Selecting a worker does not mutate worker capacity or decrement available resources.
- **No Worker Leases / Job Claims**: Deferred to PR 12.
- **No Job State Mutation to RUNNING**: Selecting a worker does not mark persistent job status as RUNNING.
- **No Active Resource Accounting**: No tracking of active CPU cores, memory bytes, or job counts.
- **No Job Execution**: No Docker, shell, or Kubernetes execution.

---

## Verification Evidence

- **Linting**: `npm run lint` passed (0 errors, 0 warnings).
- **Formatting**: `npm run format:check` passed (All matched files use Prettier code style).
- **Typechecking**: `npm run typecheck` passed (`tsc -b` with 0 errors).
- **Monorepo Build**: `npm run build` passed across all 11 workspaces (`contracts`, `config`, `logging`, `pipeline`, `database`, `redis`, `queue`, `worker-registry`, `api`, `scheduler`, `worker`, `cli`, `web`).
- **Test Suite**: `npm test` executed across all workspaces:
  - **29 test files passed (100%)**
  - **286 tests passed (100%)**
