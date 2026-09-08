# PR 16: Fairness, Queue Aging & Starvation Prevention

## Summary

This pull request implements **PR 16: Fairness, Queue Aging & Starvation Prevention** for Forge V2.

Building directly upon the verified scheduler foundation (PR 10 deterministic worker selection, PR 11 priority scheduling, PR 12 distributed worker leases, PR 14 retry/attempt orchestration, and PR 15 worker loss recovery), PR 16 addresses the starvation vulnerability inherent in pure priority scheduling:

> **Preserve priority as the primary scheduling signal while allowing sufficiently old work to gain scheduling weight so that continuously lower-priority jobs cannot starve indefinitely under sustained higher-priority load.**

### Core Mathematical Model

Effective priority is computed dynamically by the scheduler during candidate job ordering:

$$\text{effective\_priority} = \text{base\_priority} + \text{age\_bonus}$$

$$\text{age\_bonus} = \min\left(\text{max\_age\_bonus}, \left\lfloor \frac{\text{waiting\_ms}}{\text{aging\_interval\_ms}} \right\rfloor \times \text{age\_bonus\_step}\right)$$

---

## Key Architectural Guarantees & Invariants

1. **Base Priority Immutability**:
   - `job.priority` is a durable, immutable property of the job. It is **never** mutated by fairness calculations or queue aging.
2. **Zero Database Migrations for Transient Signals**:
   - `effective_priority` is computed dynamically in-memory by the scheduler during candidate job ordering. No `effective_priority` column is persisted to PostgreSQL or Redis.
   - Timestamps are derived exclusively from authoritative columns: `jobs.created_at` (initial attempt) and `jobs.next_attempt_at` (retried attempts).
3. **Bounded Age Bonus Ceiling**:
   - Age bonus is strictly bounded by `maxAgeBonus` ($\text{effective\_priority} \le \text{base\_priority} + \text{max\_age\_bonus}$). Queue aging can never allow low-priority work to overtake critical or emergency priority bands whose base priority exceeds the ceiling.
4. **Retry Age Reset Invariant**:
   - When a job enters retry scheduling, its waiting duration resets to start at `jobs.next_attempt_at` (the exact instant the backoff delay expired and the job became eligible for placement). Retried jobs never inherit or carry over queue aging accumulated prior to failure or during backoff sleep.
5. **Deterministic Tie-Breaking & Permutation Invariance**:
   - Ties in effective priority are broken strictly by alphanumeric `jobId` ascending code-point ordering. The ordering is permutation-invariant and time-deterministic for any fixed evaluation instant `now`.
6. **Hard Safety Gates Preserved**:
   - Queue aging governs candidate job evaluation order only. It never bypasses PR 09 capability/resource matching, PR 10 worker selection, PR 12 distributed worker leases, or PR 14 active backoff gates.

---

## What Was Implemented

### 1. Contracts Package (`packages/contracts`)

- Added `QueueAgingConfig` interface:
  ```ts
  export interface QueueAgingConfig {
    readonly agingIntervalMs: number;
    readonly ageBonusStep: number;
    readonly maxAgeBonus: number;
  }
  ```
- Added `EffectivePriorityInfo` interface:
  ```ts
  export interface EffectivePriorityInfo {
    readonly basePriority: number;
    readonly ageBonus: number;
    readonly effectivePriority: number;
    readonly waitingSince: Date;
    readonly waitingMs: number;
  }
  ```
- Added fairness constants:
  - `DEFAULT_FAIRNESS_AGING_INTERVAL_MS = 60000` (1 minute)
  - `MIN_FAIRNESS_AGING_INTERVAL_MS = 1000`
  - `DEFAULT_FAIRNESS_AGE_BONUS_STEP = 10`
  - `MIN_FAIRNESS_AGE_BONUS_STEP = 1`
  - `DEFAULT_FAIRNESS_MAX_AGE_BONUS = 500`
  - `MAX_FAIRNESS_AGE_BONUS_LIMIT = 2000`
- Extended `AppConfig` with typed fairness properties.

### 2. Configuration Package (`packages/config`)

- Added `FAIRNESS_AGING_INTERVAL_MS`, `FAIRNESS_AGE_BONUS_STEP`, and `FAIRNESS_MAX_AGE_BONUS` to `configSchema` with integer validation, boundary constraints, and refinement check (`FAIRNESS_MAX_AGE_BONUS >= FAIRNESS_AGE_BONUS_STEP`).
- Added unit tests in `packages/config/src/index.test.ts` covering defaults, overrides, and refinement failure modes.

### 3. Pipeline Domain Package (`packages/pipeline`)

- Extended `Job` domain model with `createdAt` and `queuedAt` timestamp tracking.
- Updated `markQueued(timestamp?)` to optionally accept explicit virtual or wall-clock timestamps.
- Updated `JobOptions` and `JobSerialized` interfaces.
- Added unit tests in `packages/pipeline/src/job.test.ts` verifying timestamp capture and JSON serialization.

### 4. Database Persistence Package (`packages/database`)

- Updated `PgJobRepository.mapRowToDomain` to map `row.created_at` to domain `Job.createdAt`.

### 5. Scheduler Service (`apps/scheduler`)

- Updated `JobOrderingPolicy` interface to accept optional `now?: Date` parameter for virtual-time deterministic evaluation.
- Implemented `FairAgingPriorityPolicy` in `apps/scheduler/src/fairness-policy.ts`:
  - `getJobEligibleWaitingSince`: Resolves waiting instant (`nextAttemptAt` for due retries, else `queuedAt ?? createdAt`).
  - `calculateAgeBonus`: Computes step-wise integer age bonus bounded by `maxAgeBonus`.
  - `calculateEffectivePriority`: Computes effective priority and returns `EffectivePriorityInfo` without mutating `job.priority`.
  - `compareFairAgingPriority`: Deterministic comparator sorting effective priority descending, breaking ties by `jobId` ascending code-point ordering.
  - `orderJobsWithFairAging`: Pure ordering function preserving input array immutability.
  - `fairAgingPriorityPolicy`: Singleton instance.
- Updated `HighestPriorityFirstPolicy.orderJobs(jobs, now?)` to conform to `JobOrderingPolicy`.
- Updated `evaluatePrioritizedWork` and `ForgeScheduler` methods (`schedule`, `schedulePrioritized`, `scheduleDueJobs`, `scheduleNextBatch`) to accept and propagate virtual time `now`.
- Added unit tests in `apps/scheduler/src/fairness-policy.test.ts` (21 tests).
- Added controlled starvation experiment in `apps/scheduler/src/fairness.experiment.test.ts` (4 tests).
- Added fairness scheduler tests in `apps/scheduler/src/scheduler.test.ts`.

### 6. Architecture Documentation (`docs/architecture/`)

- Created `docs/architecture/fairness.md` detailing the queue-aging formula, boundedness, retry age reset, tie-breaking, empirical starvation experiment data, configuration, and limitations.
- Updated `docs/architecture/invariants.md` with Section 10: Queue Aging, Fairness & Starvation Prevention Invariants.
- Updated `docs/architecture/overview.md`, `glossary.md`, and `README.md`.

---

## Controlled Starvation Experiment Empirical Results

From `apps/scheduler/src/fairness.experiment.test.ts`:

### Scenario:

- **Worker Slot**: 1
- **Low-Priority Job**: Base priority `10`, queued at $t_0 = 0$.
- **High-Priority Stream**: Continuous stream of high-priority jobs with base priority `50`, arriving at each 1-minute interval.
- **Fairness Config**: `agingIntervalMs = 60000` (1 min), `ageBonusStep = 10`, `maxAgeBonus = 100`.

### Measured Results:

| Metric                                        | Strict Priority (`HighestPriorityFirst`)     | Queue Aging (`FairAgingPriority`) |
| :-------------------------------------------- | :------------------------------------------- | :-------------------------------- |
| **Total Rounds Tested**                       | 10 rounds                                    | 10 rounds                         |
| **Bypass Count Before Overtake**              | $\infty$ (Starved indefinitely, 10 bypasses) | **5 rounds**                      |
| **Overtake Timestamp**                        | Never                                        | **$t = 300,000$ ms (5 minutes)**  |
| **Target Job Effective Priority at Overtake** | 10 (constant)                                | **60 ($10 + 50$)**                |
| **Placement Outcome**                         | `UNSCHEDULABLE` (queued)                     | `SCHEDULED` (placed on worker)    |

### Cycle-by-Cycle Trace Under Queue Aging:

```text
Round 0 (t = 0m):  Target eff = 10, HighStream eff = 50  → HighStream wins (Bypass 1)
Round 1 (t = 1m):  Target eff = 20, HighStream eff = 50  → HighStream wins (Bypass 2)
Round 2 (t = 2m):  Target eff = 30, HighStream eff = 50  → HighStream wins (Bypass 3)
Round 3 (t = 3m):  Target eff = 40, HighStream eff = 50  → HighStream wins (Bypass 4)
Round 4 (t = 4m):  Target eff = 50, HighStream eff = 50  → Tie-break by jobId (Bypass 5)
Round 5 (t = 5m):  Target eff = 60, HighStream eff = 50  → TARGET OVERTAKES & SCHEDULES!
```

### Saturation Boundary Verification:

- Urgent stream of priority `200` tested against target job ($P_{\text{low}} = 10, \text{maxAgeBonus} = 100$):
  - Target job effective priority reaches ceiling of $10 + 100 = 110$.
  - Target job is bypassed on all 15 rounds.
  - Proves empirically that bounded fairness preserves emergency priority bands.

---

## Verification & Quality Gates

- **Formatting Check (`npm run format:check`)**: PASS (0 code style issues).
- **ESLint (`npm run lint`)**: PASS (0 errors, 0 warnings).
- **TypeScript Compilation (`npm run typecheck`)**: PASS (0 errors across all composite workspaces).
- **Automated Tests (`npm test`)**: PASS (41 test files, 457 tests passed).
- **Monorepo Production Build (`npm run build`)**: PASS (All 14 workspaces built cleanly).
