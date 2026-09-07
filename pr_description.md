# PR 12: Distributed Worker Leases & Job Claiming

## Summary

This pull request implements **PR 12: Distributed Worker Leases & Job Claiming** for Forge V2. It establishes Forge's distributed job ownership foundation: a selected worker atomically claims a job through a renewable, time-bounded lease backed authoritatively by PostgreSQL.

---

## Key Architectural Decisions & Guarantees

1. **PostgreSQL as Authoritative Source of Truth for Leases**:
   - Explicitly rejects lock-free dual writes or Redis-as-authoritative-lock architectures for job ownership.
   - Authoritative lease state is stored in table `worker_leases` created via migration `005_worker_leases.sql`.
   - Single-active-lease exclusivity is enforced by partial unique index:
     ```sql
     CREATE UNIQUE INDEX uq_worker_leases_active_job
       ON worker_leases(job_id)
       WHERE status = 'ACTIVE';
     ```

2. **Atomic Claim, Renewal, and Release**:
   - `claim()`: Acquires an exclusive row lock on the job (`SELECT ... FOR UPDATE`), checks existing lease state, evaluates expiration according to DB `NOW()`, and inserts new active lease or returns idempotent success / conflict.
   - `renew()`: Atomic `UPDATE` checking `status = 'ACTIVE'`, owner `worker_id`, and `expires_at > NOW()`. Stale owners touch 0 rows and are rejected.
   - `release()`: Atomic transition to `RELEASED`.
   - `reclaimExpiredLeases()`: Bulk transition of expired active leases to `EXPIRED`.

3. **Time Authority & Clock Invariant**:
   - The PostgreSQL database clock (`NOW()`) is the sole authority for lease expiration.
   - Expiration timestamps are calculated directly in PostgreSQL: `NOW() + ($durationMs * INTERVAL '1 millisecond')`.
   - Workers never supply local timestamps for lease evaluations, eliminating clock skew vulnerabilities.

4. **Distributed Failure Model & Recoverability**:
   - When a worker crashes, its heartbeat in Redis stops and its lease expires in PostgreSQL.
   - Queue messages remain in visibility timeout without premature acknowledgement (`ACK`), preserving recoverability.
   - An expired lease is automatically transitioned to `EXPIRED` upon replacement claim or background sweep.
   - Stale workers that revive cannot renew or release expired/replaced leases.

5. **Concurrency & Race Condition Elimination**:
   - Concurrent claim requests for the same unleased job are serialized via PostgreSQL row locks and enforced by the partial unique index.
   - Verified with 10 concurrent claimants: exactly 1 winner (`ACQUIRED`, `isIdempotent: false`) and 9 conflicts (`CONFLICT`, `LEASE_ALREADY_HELD`).

---

## What Was Implemented

### 1. Contracts Package (`packages/contracts`)

- Added `JobLeaseStatus = 'ACTIVE' | 'RELEASED' | 'EXPIRED'`.
- Added `WorkerLease` interface with timestamps and audit metadata.
- Added `ClaimJobOptions`, `ClaimJobResult`, `RenewLeaseOptions`, `RenewLeaseResult`, `ReleaseLeaseOptions`, `ReleaseLeaseResult`.
- Updated `ScheduledDecision` to optionally attach `lease?: WorkerLease`.
- Added `'LEASE_CONFLICT'` to `UnschedulableReason`.
- Added `workerJobLeaseDurationMs` and `workerJobLeaseRenewalIntervalMs` to `AppConfig`.

### 2. Configuration Package (`packages/config`)

- Added `WORKER_JOB_LEASE_DURATION_MS` (default `30000`, min `1000`).
- Added `WORKER_JOB_LEASE_RENEWAL_INTERVAL_MS` (default `10000`, min `500`).
- Added refinement rule ensuring `durationMs > renewalIntervalMs`.
- Unit tests covering default values, valid overrides, and invariant violations.

### 3. Database Package (`packages/database`)

- Created migration `005_worker_leases.sql` and registered in `migrator.ts`.
- Updated `resetDatabase` to cascade drop `worker_leases`.
- Added `WorkerLeaseRow` to `types.ts`.
- Created `WorkerLeaseRepository` contract and `PgWorkerLeaseRepository` implementation with atomic `claim`, `renew`, `release`, `findActiveByJobId`, `findById`, `findByWorkerId`, and `reclaimExpiredLeases`.
- Added `workerLeases` to `TransactionContext`.
- Integration tests (16 tests) in `worker-lease-repository.test.ts` covering lifecycle, idempotent claiming, conflict rejection, renewal, release, expiry reclamation, 10-contestant concurrency races, and stale lease owner replacement.

### 4. Scheduler Service (`apps/scheduler`)

- Updated `SchedulerOptions` with `leaseRepository?: WorkerLeaseRepository` and `leaseDurationMs?: number`.
- In `ForgeScheduler.schedule()`, upon selecting an eligible worker, atomically claims a worker lease when `leaseRepository` is configured.
- Attaches `lease` to `ScheduledDecision` on success; returns `UNSCHEDULABLE` (`LEASE_CONFLICT`) on conflict.
- In `schedulePrioritized()`, claims leases for placed jobs in priority order with non-blocking conflict semantics.
- Preserves unacknowledged queue delivery under visibility timeout without calling ACK upon scheduling/claiming.

### 5. Worker Service Shell (`apps/worker`)

- Added `leaseRepository?: WorkerLeaseRepository` and `defaultLeaseDurationMs?: number` to `StartWorkerOptions`.
- Added `claimJob`, `renewLease`, `releaseLease`, `getActiveLeases` to `WorkerShell`.
- Implemented graceful release of all held active leases upon worker `stop()`.
- Unit tests covering lease lifecycle and graceful shutdown release.

### 6. Smoke Tests & Verification (`apps/scheduler/src/lease.smoke.test.ts`)

- **Section 51**: 20-step sequential verification against live PostgreSQL and Redis.
- **Section 52**: 10 concurrent claimants racing simultaneously on live PostgreSQL.

### 7. Documentation

- Created `docs/architecture/leases.md`.
- Updated `docs/architecture/overview.md`, `docs/architecture/glossary.md`, `docs/architecture/invariants.md`, `docs/architecture/scheduler.md`, and `README.md`.

---

## Test Execution Summary

All test suites pass cleanly across all workspaces:

- `@forge/config`: 8 tests passed
- `@forge/contracts`: types and interfaces verified
- `@forge/pipeline`: 100 tests passed
- `@forge/database`: 49 integration tests passed (including 16 worker lease tests)
- `@forge/redis`: 15 integration tests passed
- `@forge/queue`: 8 integration tests passed
- `@forge/worker-registry`: 15 integration tests passed
- `@forge/scheduler`: 72 tests passed (including Section 51 & 52 live smoke test matrix)
- `@forge/worker`: 3 tests passed
- `@forge/api`: 3 tests passed
- `@forge/cli`: 2 tests passed
- `@forge/web`: 1 test passed
