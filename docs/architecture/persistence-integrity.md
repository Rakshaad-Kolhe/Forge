# Transactional Domain Persistence & State Integrity

## 1. Overview & Problem Statement

Forge V2 requires strict lifecycle semantics for pipelines, pipeline runs, jobs, and job attempts. While PR 03 established pure in-memory domain models with deterministic finite state machines (`@forge/pipeline`), and PR 04 established PostgreSQL persistence and repository abstractions (`@forge/database`), a critical architectural gap remained:

> **Persistence Bypass Risk**: Direct database update operations (such as `updateStatus(id, status)`) or naive upsert logic could allow callers, background workers, or corrupted processes to bypass domain lifecycle constraints, perform illegal state jumps, or regress terminal states.

**PR 05 resolves this gap** by establishing an airtight transactional persistence boundary where:

- Domain state machines are the **sole authority** on lifecycle transitions.
- Persistence mechanisms **enforce** lifecycle validity before issuing SQL updates.
- Terminal states are **strictly immutable** at the database layer.
- Aggregates (`PipelineRun` + constituent `Job`s) persist **atomically** under PostgreSQL ACID transactions.
- Domain reconstruction guards against corrupted or invalid persistent data.

---

## 2. Elimination of Direct Status Updates

In PR 04, repository contracts included convenience methods such as `updateStatus(id, status)`. While functional, these methods bypassed domain state machine verification:

```ts
// REMOVED: Unsafe bypass method
await pipelineRunRepo.updateStatus(runId, 'SUCCEEDED');
```

In PR 05, `updateStatus` has been **completely eliminated** from:

- `PipelineRunRepository` contract and `PgPipelineRunRepository` implementation.
- `JobRepository` contract and `PgJobRepository` implementation.

### Enforced Call Pattern

Callers must load the domain entity, execute domain state machine transition methods, and persist the updated entity through `save()`:

```ts
// ENFORCED: Domain-authoritative pattern
const run = await pipelineRunRepo.findById(runId);
if (!run) throw new EntityNotFoundError('PipelineRun', runId);

run.start(); // Enforces QUEUED -> RUNNING state machine transition
await pipelineRunRepo.save(run); // Pre-save validation verifies against PostgreSQL row
```

---

## 3. Pre-Save State Machine Validation

When `save(entity)` is called on `PgPipelineRunRepository`, `PgJobRepository`, or `PgJobAttemptRepository`:

```text
save(entity)
    │
    ▼
Query existing row status: SELECT status FROM table WHERE id = $1
    │
    ├── Row does not exist ──────► Execute INSERT (Initial status validated by entity constructor)
    │
    └── Row exists:
            │
            ├── existingStatus === entity.status ──► No-op state transition, proceed to upsert
            │
            └── existingStatus !== entity.status
                    │
                    ▼
            Instantiate authoritative StateMachine(entityId, existingStatus)
                    │
                    ▼
            Call sm.transitionTo(entity.status)
                    │
                    ├── Permitted ──► Proceed to execute SQL UPDATE
                    │
                    └── Forbidden ──► Throws InvalidStateTransitionError
                                      (Zero SQL executed, DB untouched)
```

### Authoritative Domain State Machines

The repository does not invent secondary SQL-level status logic. Instead, it reuses the pure domain state machine factories from `@forge/pipeline`:

- `createPipelineRunStateMachine(id, existingStatus)`
- `createJobStateMachine(id, existingStatus)`
- `createJobAttemptStateMachine(id, existingStatus)`

If an illegal transition or terminal state regression is attempted (e.g. `SUCCEEDED -> RUNNING` or `PENDING -> SUCCEEDED`), `transitionTo` throws `InvalidStateTransitionError`, immediately aborting the save operation before any database mutation occurs.

---

## 4. Terminal State Immutability

Terminal states are permanent and immutable across all domain entities:

| Entity          | Terminal States                                 |
| :-------------- | :---------------------------------------------- |
| **PipelineRun** | `SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT` |
| **Job**         | `SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT` |
| **JobAttempt**  | `SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT` |

Once an entity transitions to any terminal state in PostgreSQL, any subsequent call to `save()` with a different status will throw `InvalidStateTransitionError`:

```ts
// Attempting to regress terminal state throws:
// InvalidStateTransitionError: Cannot transition PipelineRun "run-1" from status "SUCCEEDED" to "RUNNING"
// Reason: Terminal states cannot transition to another state
```

Database rows remain unmodified upon rejection.

---

## 5. Atomic Aggregate Persistence

A `PipelineRun` is an aggregate root encompassing constituent `Job` instances for every step defined in the pipeline.

To prevent partial execution states:

1. **Managed Pool Transactions**: When `save(run)` is called on a pool-backed `PgPipelineRunRepository`, it automatically acquires a client, begins a transaction, persists the run and all constituent jobs, and commits.
2. **Transactional Client Reuse**: When called within an explicit `withTransaction(pool, callback)` context, `tx.pipelineRuns.save(run)` executes directly on the existing transaction client without nesting transactions.
3. **Atomic Rollback**: If any constituent job fails validation, encounters a unique constraint collision, or attempts an illegal state transition, the entire transaction is rolled back.

```text
BEGIN
  INSERT INTO pipeline_runs ...
  INSERT INTO jobs (step 1) ...
  INSERT INTO jobs (step 2) ... [FAILS or INVALID TRANSITION]
ROLLBACK
(Zero partial state remains in PostgreSQL)
```

---

## 6. Domain Reconstruction Integrity

Repositories rehydrate domain models using domain class constructors rather than raw unchecked object literals. If database data is corrupted:

1. **Cyclic Steps in Pipelines**: If the `steps` JSON column in `pipelines` is corrupted with cyclic dependencies, `new Pipeline(props)` detects the cycle via topological sorting and throws `CycleDetectedError`, which the repository maps to `PersistenceError`.
2. **Missing Dependencies**: If `steps` references non-existent step dependencies, `InvalidDependencyError` is caught and wrapped into `PersistenceError`.
3. **Corrupt Status Values**: If a status column contains an unrecognized string, `mapRowToDomain` throws a typed `PersistenceError`.
4. **Invalid Attempt Numbering**: If a job attempt has `attempt_number < 1`, `new JobAttempt(props)` validation fails and triggers `PersistenceError`.

---

## 7. Verification Summary

The persistence integrity contracts are verified by automated tests running against PostgreSQL:

- **103 total tests** passing across the monorepo.
- **30 integration tests** in `@forge/database` verifying:
  - Valid lifecycle progressions (`PENDING -> QUEUED -> RUNNING -> SUCCEEDED/FAILED/TIMED_OUT/CANCELLED`).
  - Terminal state regression rejection and row preservation.
  - Invalid state jump rejection.
  - Same-state idempotent saves.
  - Transaction isolation (uncommitted writes invisible outside transaction).
  - Atomic aggregate rollback on constituent job failure.
  - Domain reconstruction rejection of cyclic DAGs and malformed dependencies.
