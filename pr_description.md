# PR 10: Scheduler Foundation & Deterministic Worker Selection

## Summary

This pull request implements **PR 10: Scheduler Foundation & Deterministic Worker Selection** of Forge V2. It establishes the **first real scheduler decision layer** (`@forge/scheduler`) on top of the already-verified queue (`@forge/queue`), worker registry (`@forge/worker-registry`), and capability/resource matcher (`@forge/pipeline`).

This PR is the architectural transition from:
> *"Which workers can run this job?"* (PR 09 matching)

to:
> *"Which eligible worker should the scheduler select?"* (PR 10 deterministic selection)

---

## Architectural Sequence

Forge explicitly defines the scheduler as a decoupled, first-class service rather than logic embedded inside the API or worker. The scheduling sequence is:

```text
Queued Job
    │
    ▼
Candidate Workers (WorkerRegistry)
    │
    ▼
Operational Eligibility (READY + ALIVE)
    │
    ▼
PR 09 Capability / Resource Matcher
    │
    ▼
PR 10 Deterministic Selection (DeterministicFirstEligible)
    │
    ▼
Scheduling Decision (SCHEDULED vs UNSCHEDULABLE)
    │
    ▼
Future PR: Distributed Lease / Claim
    │
    ▼
Future PR: Worker Daemon Execution
```

---

## What Was Implemented

### 1. Canonical Scheduling Decision Contracts (`packages/contracts`)
Added typed placement decision contracts to `@forge/contracts`:
- `ScheduleDecisionStatus`: `'SCHEDULED' | 'UNSCHEDULABLE'`
- `UnschedulableReason`: `'NO_ELIGIBLE_WORKER' | 'INVALID_JOB_REQUIREMENTS'`
- `ScheduledDecision`:
  ```ts
  {
    status: 'SCHEDULED';
    jobId: string;
    workerId: string;
    candidateWorkerCount: number;
    eligibleWorkerCount: number;
    reason?: string;
  }
  ```
- `UnschedulableDecision`:
  ```ts
  {
    status: 'UNSCHEDULABLE';
    jobId: string;
    candidateWorkerCount: number;
    eligibleWorkerCount: number;
    reason: UnschedulableReason;
    failureReasons?: readonly string[];
  }
  ```

### 2. Deterministic Worker Selection Policy (`DeterministicFirstEligible`)
- Implemented `DeterministicFirstEligiblePolicy` in `apps/scheduler/src/policy.ts`.
- **Canonical Ordering**: Orders matching workers by `workerId` ascending using locale-independent character code-point comparison (`(a < b ? -1 : (a > b ? 1 : 0))`).
- **Order-Invariance**: Extensively verified across all permutations of candidate worker arrays—always yields the identical selected worker.
- **Zero Hidden Heuristics**: No `Math.random()`, no load estimation, no CPU/memory preference, no registration timestamp bias.

### 3. Pure Placement Evaluator & Service Class (`apps/scheduler/src/scheduler.ts`)
- **`evaluatePlacement(job, candidates, policy, matcher)`**:
  - Pure synchronous placement evaluation with zero I/O and zero side-effects.
  - Validates requirements safety (flags `INVALID_JOB_REQUIREMENTS`).
  - Restricts candidates to operational workers (`status === 'READY'` and `liveness === 'ALIVE'`).
  - Evaluates PR 09 capability and capacity matching via `filterEligibleWorkers`.
  - Applies selection policy and returns explainable `ScheduleDecision`.
- **`ForgeScheduler` Service**:
  - Layered architecture with Dependency Injection (`WorkerSource`, `JobSource`, `WorkerSelectionPolicy`, `EligibilityMatcher`, `Logger`).
  - Asynchronous `schedule(jobOrId)` method.
  - Queue evaluation via `scheduleNext(queue)`: inspects message without permanently acknowledging it, preserving message recoverability under visibility timeouts.

### 4. Architecture Documentation
- Created `docs/architecture/scheduler.md` detailing scheduler responsibility, eligibility pipeline, `DeterministicFirstEligible` policy, and queue recoverability.
- Updated `docs/architecture/overview.md` roadmap.
- Updated `docs/architecture/glossary.md` with entries for `Operational Eligibility`, `DeterministicFirstEligible`, and `ScheduleDecision`.
- Updated `README.md`.

---

## Architectural Invariants & Non-Goals

The following hard boundaries are strictly preserved:
- **No Worker Capacity Reservation**: Selecting a worker does NOT reserve CPU/memory or mutate capacity.
- **No Job Claiming or Leases**: Selection is decoupled from distributed ownership tokens (reserved for future lease/claim PR).
- **No State Mutation to RUNNING**: Selecting a worker does not start execution or mutate job status.
- **No Priority or Fairness Algorithms**: Pure deterministic baseline policy without unmodelled heuristics.
- **No Global Distributed Scheduler Serialization**: PR 10 provides local deterministic scheduling policy; distributed serialization belongs to the future lease phase.

---

## Verification & Test Results

### 1. Verification Commands
- `npm run format:check`: **PASSED** (all files match Prettier code style)
- `npm run lint`: **PASSED** (0 errors, 0 warnings)
- `npm run typecheck`: **PASSED** (0 errors across composite project references)
- `npm test`: **PASSED** (249/249 tests passing across 27 test files)
- `npm run build`: **PASSED** (clean build across all workspaces)

### 2. Manual Verification Matrix (Section 34)
Executed via `apps/scheduler/src/smoke.test.ts`:
```text
Workers:
  Worker-A: docker, 4 CPU, 8 GB, 0 GPU
  Worker-B: shell,  8 CPU, 16 GB, 0 GPU
  Worker-C: docker, 2 CPU, 4 GB, 1 GPU

Step 1: Job 1 (docker, 2 CPU, 4 GB, 0 GPU) with [Worker-A, Worker-B, Worker-C]
  -> Result: SCHEDULED -> Selected: Worker-A (candidates: 3, eligible: 2)

Step 2: Job 1 with shuffled candidates [Worker-C, Worker-B, Worker-A]
  -> Result: SCHEDULED -> Selected: Worker-A (identical selection confirmed)

Step 3: Job 2 (docker, 8 CPU, 16 GB, 0 GPU) with [Worker-B, Worker-C]
  -> Result: UNSCHEDULABLE -> Reason: NO_ELIGIBLE_WORKER
     Failure reasons: [EXECUTOR_UNSUPPORTED, INSUFFICIENT_CPU, INSUFFICIENT_MEMORY]
```

### 3. Integration Tests
- Real PostgreSQL + Redis + `WorkerRegistry` + `PgJobRepository` + `JobQueue` + `ForgeScheduler`:
  - Persistent job retrieval from PostgreSQL.
  - Live worker candidate query from `WorkerRegistry`.
  - Unacknowledged queue message recoverability under visibility timeout.
  - Automatic exclusion of crashed workers (`STALE` in Redis).
