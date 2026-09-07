# Forge V2 — Worker Capability & Resource Matching Specification

This document defines the architectural design, matching semantics, failure taxonomy, and operational guarantees for **Worker Capability and Resource Matching** in Forge V2.

---

## 1. Core Objectives & Problem Statement

In a distributed CI/CD orchestration engine, jobs declare execution requirements while heterogenous worker nodes advertise hardware capacities and supported runtime executors.

Before the scheduler can schedule, rank, or dispatch a job to a worker, it must answer:

> **"Does this worker node possess the capabilities and hardware capacity required to run this job?"**

PR 09 implements this pure, deterministic **placement eligibility layer**.

```text
                 Job
                  │ (requirements)
                  ▼
        ┌──────────────────┐
        │ Capability Match │
        │    + Capacity    │
        │   Compatibility  │
        └────────┬─────────┘
                 │
           eligible workers
                 │
                 ▼
          Future Scheduler
                 │
        ┌────────┴────────┐
        ▼                 ▼
     Priority          Fairness
       PR 11             PR 12
```

---

## 2. Models: Job Requirements vs. Worker Capabilities

### Job Execution Requirements (`JobRequirements`)

Declared on a pipeline step or job:

```ts
export interface JobRequirements {
  /**
   * Required runtime executor (e.g. 'shell', 'docker', 'kubernetes').
   * If unspecified, any worker executor is acceptable.
   */
  readonly executor?: string;

  /**
   * Required minimum CPU core count.
   * If unspecified, no CPU constraint is enforced.
   */
  readonly cpuCores?: number;

  /**
   * Required minimum memory in bytes.
   * If unspecified, no memory constraint is enforced.
   */
  readonly memoryBytes?: number;

  /**
   * Required minimum dedicated GPU count.
   * If unspecified or 0, no GPU is required.
   */
  readonly gpuCount?: number;
}
```

### Worker Capabilities & Advertised Capacity (`WorkerCapabilities` & `WorkerResources`)

Reported by worker nodes during registration (PR 08):

```ts
export interface WorkerCapabilities {
  readonly executors: readonly string[];
}

export interface WorkerResources {
  readonly cpuCores: number;
  readonly memoryBytes: number;
  readonly gpuCount?: number;
}
```

---

## 3. Matching Semantics & Rules

The matching predicate `matchesWorker(jobOrRequirements, worker)` evaluates four independent rules:

| Resource     | Rule                                      | Failure Reason         |
| :----------- | :---------------------------------------- | :--------------------- |
| **Executor** | `worker.executors.includes(job.executor)` | `EXECUTOR_UNSUPPORTED` |
| **CPU**      | `worker.cpuCores >= job.cpuCores`         | `INSUFFICIENT_CPU`     |
| **Memory**   | `worker.memoryBytes >= job.memoryBytes`   | `INSUFFICIENT_MEMORY`  |
| **GPU**      | `(worker.gpuCount ?? 0) >= job.gpuCount`  | `INSUFFICIENT_GPU`     |

### Specific Rules:

1. **Unspecified Requirements**: Any unspecified (`undefined`) field is treated as unconstrained and automatically passes.
2. **Zero GPU**: If a job requires `gpuCount: 0` or leaves it undefined, any worker (even with `gpuCount: 0` or undefined) is eligible.
3. **Exact String Match**: Executor matching is strict and case-sensitive. No implicit alias mapping or fallback execution is performed.

---

## 4. Invalid Input Handling

To uphold the non-negotiable invariant:

> **"Invalid requirements must never accidentally produce an eligible worker."**

The matcher rigorously identifies invalid requirements:

- Negative CPU (`cpuCores < 0` or `<= 0`)
- Negative memory (`memoryBytes < 0` or `<= 0`)
- Negative GPU (`gpuCount < 0`)
- Non-finite numbers (`NaN`, `Infinity`, `-Infinity`)
- Empty or whitespace-only executor strings (`""`, `"   "`)
- Non-string or non-numeric types

When invalid requirements are encountered:

1. `validateJobRequirements()` throws `JobRequirementsValidationError` when authoring pipelines or constructing domain entities.
2. `matchesWorker()` returns `{ matched: false, reasons: ['INVALID_REQUIREMENTS'] }`.
3. `filterEligibleWorkers()` returns an empty array (`[]`).

---

## 5. Explainable Matching & Failure Taxonomy

Rather than returning a bare boolean, `matchesWorker` produces an explainable diagnostic result:

```ts
export type MatchFailureReason =
  | 'EXECUTOR_UNSUPPORTED'
  | 'INSUFFICIENT_CPU'
  | 'INSUFFICIENT_MEMORY'
  | 'INSUFFICIENT_GPU'
  | 'INVALID_REQUIREMENTS';

export type WorkerMatchResult =
  | {
      readonly matched: true;
    }
  | {
      readonly matched: false;
      readonly reasons: readonly MatchFailureReason[];
    };
```

All applicable failures are collected simultaneously. This enables the future scheduler to answer:

1. **"Why was worker W excluded from candidate placement?"** (e.g. `['INSUFFICIENT_CPU', 'INSUFFICIENT_GPU']`).
2. **"Why is this job unschedulable across the entire cluster?"** (e.g. no worker with 32 GB RAM exists).

---

## 6. Multi-Worker Filtering

`filterEligibleWorkers(jobOrRequirements, workers)` filters an array of candidate workers:

- Evaluates `matchesWorker` on each candidate.
- Preserves the exact input order of workers.
- Does **not** attempt to rank, score, or pick a "best" worker.

---

## 7. CRITICAL DISTINCTION: Capacity Compatibility vs. Live Availability

This PR establishes **capacity compatibility**, NOT **live resource allocation**.

```text
Worker:
  cpuCores: 8
  memoryBytes: 16 GB

Job:
  cpuCores: 4
  memoryBytes: 8 GB

Capacity-compatible: YES (8 >= 4, 16 >= 8)
Currently available: UNKNOWN (requires live concurrency & job lease tracking)
```

PR 09 intentionally does **not** implement:

- Active resource counters (`usedCpu`, `availableMemory`)
- Job reservation or slot locking
- Concurrency limiting per worker

Live availability and resource subtraction will be coordinated in future scheduler and lease PRs.

---

## 8. Non-Guarantees & Explicit Boundaries

1. **Not a Job Claim**: Compatibility matching does not reserve the worker or lock the job.
2. **Not a Liveness Check**: Matching inspects advertised capabilities only. The scheduler must separately check worker liveness via `@forge/worker-registry` (`liveness === 'ALIVE'`).
3. **No Execution**: No processes, Docker containers, or Kubernetes pods are launched.
