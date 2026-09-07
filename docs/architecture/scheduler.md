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
   - Returns a typed `ScheduleDecision` explaining placement or diagnostics explaining failure.

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
- **Baseline Policy**: Establishes an un-optimized, pure baseline against which future priority (PR 11) and fairness algorithms can be evaluated.
- **Explicit Prohibitions**: Does NOT use `Math.random()`, load estimation, current active job counts, network latency, or registration timestamps.

---

## 4. Scheduling Decision Model

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
}

export interface UnschedulableDecision {
  readonly status: 'UNSCHEDULABLE';
  readonly jobId: string;
  readonly candidateWorkerCount: number;
  readonly eligibleWorkerCount: number;
  readonly reason: 'NO_ELIGIBLE_WORKER' | 'INVALID_JOB_REQUIREMENTS';
  readonly failureReasons?: readonly string[];
}
```

### Explainability

- Successful placements identify the target `workerId`, along with `candidateWorkerCount` and `eligibleWorkerCount`.
- Unsuccessful placements identify the specific reason (`NO_ELIGIBLE_WORKER` or `INVALID_JOB_REQUIREMENTS`) along with fine-grained match failure reasons (`EXECUTOR_UNSUPPORTED`, `INSUFFICIENT_CPU`, `INSUFFICIENT_MEMORY`, `INSUFFICIENT_GPU`, `INVALID_REQUIREMENTS`).

---

## 5. Queue Interaction & Message Recoverability

In Forge V2:

```text
Queue Delivery ≠ Worker Claim ≠ Worker Lease
```

When the scheduler consumes a job message from `@forge/queue` via `scheduler.scheduleNext(queue)`:

- The message is dequeued with an active visibility timeout.
- The scheduler inspects the message and evaluates placement against candidate workers.
- **CRITICAL**: The scheduler intentionally does **NOT** call `queue.acknowledge(messageId)`.
- A scheduling decision is not a job claim or lease. If a worker crashes, or if the scheduler restarts, or if the job is unschedulable, the message remains safely in Redis in-flight storage until reclaimed when its visibility timeout expires.
- Permanent message removal (ACK) is deferred to the future claim and execution phase.

---

## 6. Critical Distributed-Systems Limitations

### No Distributed Scheduler Serialization

PR 10 provides **deterministic local scheduling policy**, NOT globally serialized scheduling. If multiple scheduler instances run concurrently:

```text
Scheduler A ──► selects Worker X for Job Y
Scheduler B ──► may concurrently observe Job Y or select Worker X
```

This is an intentional boundary for PR 10. We do NOT mask this limitation with artificial locks. Global serialized ownership will be introduced via distributed lease allocation in future PRs.

### No Worker Capacity Reservation

Selecting a worker does **NOT** mutate the worker's capacity or decrement available resources. Multiple jobs scheduled in sequence will all observe the worker's full capacity until active resource accounting is introduced.

---

## 7. Explicit Non-Goals for PR 10

The following features are intentionally out of scope:

- **No Worker Leases / Job Claims**: No compare-and-swap tokens, lease renewal, or expiration.
- **No Job State Mutation to RUNNING**: Selecting a worker does not mean the job has started running.
- **No Priority Scheduling**: No priority queues, effective priority, or aging (PR 11).
- **No Fairness**: No round-robin, weighted allocation, starvation prevention, or quotas.
- **No Active Resource Accounting**: No tracking of active CPU cores, memory bytes, or job counts.
- **No Job Execution**: No Docker, shell, or Kubernetes execution.
