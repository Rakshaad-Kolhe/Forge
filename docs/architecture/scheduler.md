# Forge V2 — Task Scheduler Architecture & Deterministic Worker Selection

## 1. Overview & Service Responsibilities

The **Scheduler Service (`apps/scheduler`)** is a first-class service within Forge V2 responsible for making placement decisions for queued work items.

Forge decouples scheduling policy from job claiming, leases, and worker execution. The scheduler's sole responsibility is answering:

> **"Given this job and the workers currently visible to me, which eligible worker does my current scheduling policy select?"**

It explicitly does **NOT** answer:

- _"Does this worker own the job?"_ (Deferred to future distributed lease/claim mechanism)
- _"Can this worker reserve the hardware capacity?"_ (Deferred to future resource accounting)
- _"Has this worker started running the job?"_ (Deferred to worker daemon execution)
- _"Will this job execute exactly once?"_ (Non-goal; Forge guarantees at-least-once delivery with idempotent execution)

```text
Queued Jobs (Batch)
    │
    ▼
Job Priority Ordering (HighestPriorityFirst: priority DESC, jobId ASC)
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
Scheduling Decisions (SCHEDULED vs UNSCHEDULABLE with Non-Blocking Semantics)
    │
    ▼
Future PR: Distributed Lease / Claim
    │
    ▼
Future PR: Worker Daemon Execution
```

---

## 2. Worker Eligibility Pipeline

Placement evaluation operates in a strict, deterministic sequence:

1. **Candidate Worker Acquisition**:
   - The scheduler queries `WorkerRegistry.listWorkers({ status: 'READY', liveness: 'ALIVE' })`.
   - The scheduler consumes the public registry API and never directly queries PostgreSQL or Redis for heartbeats.
2. **Operational Eligibility Filtering**:
   - Candidates must have durable `status === 'READY'` and transient Redis liveness `liveness === 'ALIVE'`.
   - Workers that are `DRAINING`, `OFFLINE`, `STARTING`, or `STALE` (missed heartbeat TTL) are strictly excluded.
3. **Capability & Resource Matching (PR 09)**:
   - Evaluates job requirements against worker declared capabilities and capacity via `filterEligibleWorkers`.
   - Checks executor support, CPU core sufficiency (`worker.cpu >= job.cpu`), memory byte sufficiency, and GPU count sufficiency.
4. **Deterministic Selection Policy**:
   - Applies `DeterministicFirstEligiblePolicy` to the surviving eligible set.
5. **Explainable Decision Emission**:
   - Returns a typed `ScheduleDecision` explaining placement or diagnostics explaining failure, including job priority.

---

## 3. Worker Selection Policy: `DeterministicFirstEligible`

PR 10 establishes the baseline deterministic selection policy: **`DeterministicFirstEligible`**.

### Semantics

1. Take the filtered set of eligible workers that satisfy capability and resource requirements.
2. Order workers canonically using exact ascending string code-point comparison on `workerId` (`(idA < idB ? -1 : (idA > idB ? 1 : 0))`).
3. Select the first eligible worker in that canonical sequence (`sorted[0]`).
4. If no eligible workers exist, return an explicit `UNSCHEDULABLE` decision.

### Rationale & Design Invariants

- **Reproducible**: Given identical jobs and worker sets, the selected worker is identical across runs and across machines, regardless of system locale or input array order.
- **Order-Invariant**: Shuffling candidate workers in memory produces the exact same placement decision.
- **Baseline Policy**: Establishes an un-optimized, pure baseline against which future fairness algorithms can be evaluated.
- **Explicit Prohibitions**: Does NOT use `Math.random()`, load estimation, current active job counts, network latency, or registration timestamps.

---

## 4. Priority Scheduling Policy: `HighestPriorityFirst` (PR 11)

PR 11 introduces the **first explicit priority scheduling policy** in Forge.

### 4.1 Priority Domain Model & Validation Bounds

- Job priority is a strictly bounded integer:
  - `MIN_JOB_PRIORITY = -1000`
  - `MAX_JOB_PRIORITY = 1000`
  - `DEFAULT_JOB_PRIORITY = 0`
- Any non-integer (floats, `NaN`, `Infinity`) or out-of-bounds value fails domain validation immediately via `InvalidJobPriorityError`.
- Priority is declared on `PipelineStep.priority`, propagated deterministically during `PipelineRun.create()` onto `Job.priority`, and persisted in PostgreSQL under `jobs.priority`.
- PostgreSQL schema enforces bounds at the persistence layer:
  ```sql
  ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE jobs ADD CONSTRAINT chk_jobs_priority CHECK (priority >= -1000 AND priority <= 1000);
  CREATE INDEX idx_jobs_priority ON jobs(priority DESC);
  ```

### 4.2 Priority Ordering & Canonical Tie-Breaking

The scheduler evaluates ready jobs in strict priority order using `HighestPriorityFirstPolicy`:

```ts
export function compareJobPriority<
  T extends { readonly priority?: number; readonly id?: string; readonly jobId?: string },
>(a: T, b: T): number {
  const prioA = typeof a.priority === 'number' ? a.priority : DEFAULT_JOB_PRIORITY;
  const prioB = typeof b.priority === 'number' ? b.priority : DEFAULT_JOB_PRIORITY;

  if (prioA !== prioB) {
    return prioB - prioA; // Descending: higher priority first
  }

  const idA = a.id ?? a.jobId ?? '';
  const idB = b.id ?? b.jobId ?? '';

  if (idA < idB) return -1;
  if (idA > idB) return 1;
  return 0; // Deterministic tie-breaker: ascending code-point order
}
```

- **Higher Priority First**: Jobs with higher integer priorities are evaluated before lower integer priorities (e.g., `100 > 0 > -50`).
- **Canonical Tie-Breaker**: When two jobs have identical priority, the tie is broken deterministically by job ID in ascending alphanumeric (code-point) order (`job-a` before `job-b`).
- **Permutation Invariance**: Shuffling the input job list produces the identical evaluation sequence and outcomes across all runs.

### 4.3 Non-Blocking Unschedulable Semantics

A critical invariant of Forge priority scheduling is **non-blocking evaluation**:

- If a higher-priority job cannot be scheduled (e.g., requires 64 CPU cores when only 4-core workers exist), it is marked as `UNSCHEDULABLE` with clear failure reasons.
- **Evaluation continues**: The scheduler immediately evaluates the next highest priority jobs in the batch.
- **No Blocking**: An unschedulable high-priority job NEVER blocks eligible lower-priority jobs from receiving placement.

### 4.4 Queue Transport Decoupling

- **Transport FIFO Preserved**: The Redis queue (`@forge/queue`) remains a strict FIFO transport (`LPUSH` / `RPOP`). Forge does NOT introduce Redis sorted sets or complex multi-queue structures in this PR.
- **Priority as Scheduler Policy**: Jobs are retrieved in batches from the queue or persistence layer, and ordered by priority in the scheduler layer.
- **Recoverability**: Unacknowledged message recoverability under visibility timeout is strictly maintained (`scheduleNextBatch` does NOT acknowledge dequeued messages).

---

## 5. Scheduling Decision Model

Decisions are strictly typed via `@forge/contracts`:

```ts
export type ScheduleDecision = ScheduledDecision | UnschedulableDecision;

export interface ScheduledDecision {
  readonly status: 'SCHEDULED';
  readonly jobId: string;
  readonly workerId: string;
  readonly candidateWorkerCount: number;
  readonly eligibleWorkerCount: number;
  readonly reason?: string;
  readonly priority?: number;
}

export interface UnschedulableDecision {
  readonly status: 'UNSCHEDULABLE';
  readonly jobId: string;
  readonly candidateWorkerCount: number;
  readonly eligibleWorkerCount: number;
  readonly reason: 'NO_ELIGIBLE_WORKER' | 'INVALID_JOB_REQUIREMENTS';
  readonly failureReasons?: readonly string[];
  readonly priority?: number;
}
```

### Explainability

- Successful placements identify the target `workerId`, along with `candidateWorkerCount`, `eligibleWorkerCount`, and `priority`.
- Unsuccessful placements identify the specific reason (`NO_ELIGIBLE_WORKER` or `INVALID_JOB_REQUIREMENTS`) along with fine-grained match failure reasons (`EXECUTOR_UNSUPPORTED`, `INSUFFICIENT_CPU`, `INSUFFICIENT_MEMORY`, `INSUFFICIENT_GPU`, `INVALID_REQUIREMENTS`) and `priority`.

---

## 6. Queue Interaction & Message Recoverability

In Forge V2:

```text
Queue Delivery ≠ Worker Claim ≠ Worker Lease
```

When the scheduler consumes job messages from `@forge/queue` via `scheduler.scheduleNext(queue)` or `scheduler.scheduleNextBatch(queue, batchSize)`:

- Messages are dequeued with an active visibility timeout.
- The scheduler inspects the messages, sorts them by priority, and evaluates placement against candidate workers.
- **CRITICAL**: The scheduler intentionally does **NOT** call `queue.acknowledge(messageId)`.
- A scheduling decision is not a job claim or lease. If a worker crashes, or if the scheduler restarts, or if the job is unschedulable, messages remain safely in Redis in-flight storage until reclaimed when their visibility timeout expires.
- Permanent message removal (ACK) is deferred to the future claim and execution phase.

---

## 7. Critical Distributed-Systems Limitations

### No Distributed Scheduler Serialization

PR 11 provides **deterministic local priority scheduling policy**, NOT globally serialized scheduling. If multiple scheduler instances run concurrently:

```text
Scheduler A ──► selects Worker X for Job Y
Scheduler B ──► may concurrently observe Job Y or select Worker X
```

This is an intentional boundary. Global serialized ownership will be introduced via distributed lease allocation in PR 12.

### No Worker Capacity Reservation

Selecting a worker does **NOT** mutate the worker's capacity or decrement available resources. Multiple jobs scheduled in sequence will all observe the worker's full capacity until active resource accounting is introduced.

---

## 8. Explicit Non-Goals for PR 11

The following features are intentionally out of scope:

- **No Fairness / Starvation Prevention**: No aging, priority decay, round-robin, or anti-starvation boost.
- **No Worker Leases / Job Claims**: No compare-and-swap tokens, lease renewal, or expiration (deferred to PR 12).
- **No Job State Mutation to RUNNING**: Selecting a worker does not mutate persistent job status to RUNNING.
- **No Active Resource Accounting**: No tracking of active CPU cores, memory bytes, or job counts.
- **No Job Execution**: No Docker, shell, or Kubernetes execution.
