# PR 09: Worker Capability & Resource Matching

## Summary

This pull request implements **PR 09: Worker Capability & Resource Matching** of Forge V2. It establishes the **placement eligibility layer** that deterministically evaluates whether candidate worker nodes possess the capabilities and hardware capacity required to execute a job based on:

1. **Executor capability** (e.g. `shell`, `docker`, `kubernetes`).
2. **CPU core capacity** (`cpuCores`).
3. **Memory capacity** (`memoryBytes`).
4. **Dedicated GPU capacity** (`gpuCount`).

This PR provides pure, deterministic, explainable matching logic (`matchesWorker` and `filterEligibleWorkers`), propagates job execution requirements across the domain layer, and persists requirements durably in PostgreSQL.

---

## Architectural Context

Forge V2 defines the scheduler progression as:

```text
1. Correct FIFO queue (PR 07)
2. Worker registration and heartbeat (PR 08)
3. Capability/resource matching (PR 09 - Current)
4. Priority scheduling (PR 10)
5. Fairness and starvation prevention (PR 11)
6. Scheduler benchmarking (PR 12)
```

In PR 09, Forge introduces the **eligibility filter** that narrows down registered workers to a compatible candidate set:

```text
                 Job
                  │ (declared requirements)
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
       PR 10             PR 11
```

---

## What Was Implemented

### 1. Canonical Contracts (`packages/contracts`)

- Defined canonical shared interfaces in `@forge/contracts`:
  - `WorkerCapabilities`: `{ readonly executors: readonly string[]; }`
  - `WorkerResources`: `{ readonly cpuCores: number; readonly memoryBytes: number; readonly gpuCount?: number; }`
  - `JobRequirements`: `{ readonly executor?: string; readonly cpuCores?: number; readonly memoryBytes?: number; readonly gpuCount?: number; }`

### 2. Domain Representation & Pure Matcher (`packages/pipeline`)

- **Requirements Model ([packages/pipeline/src/requirements.ts](file:///c:/Users/Rakshaad/OneDrive/Desktop/Forge/packages/pipeline/src/requirements.ts))**:
  - `validateJobRequirements(input)`: Validates and normalizes requirements; throws `JobRequirementsValidationError` on invalid inputs (negative values, non-finite numbers, empty executor strings).
  - `checkJobRequirementsValidity(input)`: Non-throwing verification helper.
- **Pure Matcher & Multi-Worker Filter ([packages/pipeline/src/matcher.ts](file:///c:/Users/Rakshaad/OneDrive/Desktop/Forge/packages/pipeline/src/matcher.ts))**:
  - `matchesWorker(jobOrRequirements, worker)`:
    - Pure, deterministic, side-effect-free matching predicate.
    - Independent of PostgreSQL, Redis, Docker, and queue mechanics.
    - Produces explainable `WorkerMatchResult` containing all applicable `MatchFailureReason` flags:
      - `EXECUTOR_UNSUPPORTED`
      - `INSUFFICIENT_CPU`
      - `INSUFFICIENT_MEMORY`
      - `INSUFFICIENT_GPU`
      - `INVALID_REQUIREMENTS`
  - `filterEligibleWorkers(jobOrRequirements, workers)`:
    - Filters candidate worker arrays while strictly preserving deterministic input ordering.
  - `WorkerCandidate` Polymorphism:
    - Seamlessly matches against `WorkerMetadata`, `WorkerInfo`, `WorkerRecord`, or plain `{ capabilities, resources }` target objects without manual transformations.
- **Domain Integration ([job.ts](file:///c:/Users/Rakshaad/OneDrive/Desktop/Forge/packages/pipeline/src/job.ts), [pipeline-step.ts](file:///c:/Users/Rakshaad/OneDrive/Desktop/Forge/packages/pipeline/src/pipeline-step.ts), [pipeline-run.ts](file:///c:/Users/Rakshaad/OneDrive/Desktop/Forge/packages/pipeline/src/pipeline-run.ts))**:
  - `PipelineStep` accepts and validates `requirements`.
  - `Job` encapsulates `requirements`.
  - `PipelineRun.create(id, pipeline)` deterministically propagates `step.requirements` to each instantiated `Job`.

### 3. PostgreSQL Persistence Foundation (`packages/database`)

- **Migration `003_job_requirements.sql`**:
  - Adds `requirements JSONB NOT NULL DEFAULT '{}'::jsonb` to the `jobs` table.
  - Registered in `migrator.ts` in `MIGRATIONS` and `resetDatabase`.
- **`PgJobRepository` Persistence**:
  - Saves `job.requirements` as JSONB during `jobRepo.save(job)`.
  - Selects and reconstructs `job.requirements` during `findById(jobId)` and `findByPipelineRunId(pipelineRunId)`.

### 4. Worker Registry Alignment (`packages/worker-registry`)

- Re-exports `WorkerCapabilities` and `WorkerResources` from `@forge/contracts` to guarantee full backwards compatibility without code duplication.

### 5. Architectural Documentation (`docs/architecture/resource-matching.md`)

- Complete architectural specification detailing:
  - Models: Job requirements vs. worker capabilities.
  - Matching semantics and rules.
  - Invalid input handling and invariant preservation.
  - Explainable failure taxonomy.
  - Critical distinction between capacity compatibility and live availability.
  - Non-guarantees and architectural boundaries.
- Updated `docs/architecture/overview.md` and `README.md`.

---

## Matching Semantics & Rules

| Resource     | Rule                                      | Failure Reason         |
| :----------- | :---------------------------------------- | :--------------------- |
| **Executor** | `worker.executors.includes(job.executor)` | `EXECUTOR_UNSUPPORTED` |
| **CPU**      | `worker.cpuCores >= job.cpuCores`         | `INSUFFICIENT_CPU`     |
| **Memory**   | `worker.memoryBytes >= job.memoryBytes`   | `INSUFFICIENT_MEMORY`  |
| **GPU**      | `(worker.gpuCount ?? 0) >= job.gpuCount`  | `INSUFFICIENT_GPU`     |

- **Unspecified Requirements**: Any unspecified (`undefined`) requirement is unconstrained and passes.
- **Zero GPU**: `gpuCount: 0` or undefined matches workers with 0 or more GPUs.
- **Invalid Requirements**: Requirements with negative numbers, non-finite values (`NaN`, `Infinity`), or empty executor strings return `matched: false` with reason `INVALID_REQUIREMENTS` and are excluded from eligible worker lists.

---

## Critical Distinction: Capacity Compatibility vs. Live Availability

This PR establishes **capacity compatibility**, NOT **live resource allocation**:

- Capacity compatibility answers: _"Does this worker have the hardware specifications to run this job?"_
- Active resource counters, in-flight job concurrency subtraction, and slot reservation will be implemented in future scheduler and lease PRs.

---

## Verification & Testing

### Automated Test Suites

1. **Matcher Unit Tests** (`packages/pipeline/src/matcher.test.ts`):
   - 40 unit tests covering executor matching, CPU capacity, memory capacity, GPU counts, combined requirements, multi-worker filtering, invalid input safety, and determinism.
2. **Database Integration Tests** (`packages/database/src/repositories/repositories.test.ts`):
   - Real PostgreSQL integration test verifying that `job.requirements` are persisted and reconstructed with exact fidelity via `save`, `findById`, and `findByPipelineRunId`.
3. **Monorepo Quality Gates**:
   - `npm run format:check`: Passed.
   - `npm run lint`: Passed (0 errors, 0 warnings).
   - `npm run typecheck`: Passed (`tsc -b` passed with 0 errors).
   - `npm test`: Passed (23 test files, 216 tests passed).
   - `npm run build`: Passed (all packages and applications compiled cleanly).

### Manual Verification Smoke Test

Verified deterministic placement against Section 25 test matrix:

- **Worker A**: `[shell, docker]`, 4 CPU, 8 GB, 0 GPU
- **Worker B**: `[shell]`, 8 CPU, 16 GB, 0 GPU
- **Worker C**: `[docker]`, 2 CPU, 4 GB, 1 GPU
- **Job 1** (`docker`, 4 CPU, 8 GB, 0 GPU) -> Eligible: `[Worker A]` (B lacks docker; C has insufficient CPU/RAM).
- **Job 2** (`docker`, 2 CPU, 4 GB, 1 GPU) -> Eligible: `[Worker C]` (A and B lack GPU; B lacks docker).
