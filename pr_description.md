# PR 05: Transactional Domain Persistence & State Integrity

## Summary

Establishes an airtight transactional persistence boundary between Forge V2's domain state machines (`@forge/pipeline`), repository layer (`@forge/database`), and PostgreSQL database.

This PR ensures that persistence mechanisms **cannot accidentally bypass domain lifecycle semantics**. It eliminates direct status-bypass methods, validates all state transitions against existing persistent records before issuing SQL, guarantees terminal state immutability at the database layer, ensures atomic aggregate persistence for pipeline runs, and validates domain reconstruction against corrupted database rows.

---

## Architectural Changes & State Integrity Contracts

```text
Caller (Domain Model Mutation)
       │
       ▼
sm.transitionTo(nextStatus)   ──► [Pure In-Memory State Machine Validation]
       │
       ▼
repository.save(entity)
       │
       ▼
Query PostgreSQL Row Status: SELECT status FROM table WHERE id = $1
       │
       ├── Row exists & status changed:
       │       │
       │       ▼
       │   createStateMachine(id, existingStatus).transitionTo(entity.status)
       │       │
       │       ├── Permitted ──► Issue Parameterized SQL UPDATE
       │       │
       │       └── Forbidden ──► Throws InvalidStateTransitionError
       │                         (Zero SQL executed; DB row unchanged)
       │
       └── Aggregate Atomicity:
               │
               ▼
           withTransaction (PoolClient BEGIN ... COMMIT / ROLLBACK)
           (PipelineRun + all constituent Jobs roll back together on failure)
```

1. **Elimination of Bypass Methods**:
   - Removed `updateStatus(id, status)` from `PipelineRunRepository` and `JobRepository` contracts and PostgreSQL implementations.
   - Callers must load the domain entity, execute domain state machine transitions, and persist via `save()`.

2. **Pre-Save State Machine Validation**:
   - `PgPipelineRunRepository.save(run)`, `PgJobRepository.save(job)`, and `PgJobAttemptRepository.save(attempt)` check existing persistent status before updating.
   - If status has changed, authoritative domain state machines (`createPipelineRunStateMachine`, `createJobStateMachine`, `createJobAttemptStateMachine`) validate the transition.
   - Same-state saves proceed idempotently without throwing.

3. **Terminal State Immutability**:
   - Terminal states (`SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT`) cannot be regressed by any caller.
   - Any attempt to regress a terminal state throws `InvalidStateTransitionError`, leaving the database row untouched.

4. **Atomic Aggregate Persistence**:
   - `PgPipelineRunRepository.save(run)` wraps the persistence of the run and its constituent jobs in a PostgreSQL ACID transaction.
   - If any constituent job fails validation or encounters an invalid transition, the entire transaction rolls back cleanly with zero partial state.

5. **Domain Reconstruction Integrity**:
   - Entities rehydrated from database rows validate DAG acyclicity, step dependency existence, and attempt numbering.
   - Corrupted rows (e.g. cyclic DAGs injected into `pipelines.steps`) throw typed `PersistenceError`.

---

## What is NOT Implemented (Strict Scope Boundaries)

- **NO Redis** (transient queues, locks, and pub/sub remain future scope).
- **NO Worker execution or dispatch logic**.
- **NO Scheduler dispatch loops or lease acquisition**.
- **NO Docker / Kubernetes executors**.
- **NO WebSockets or HTTP pipeline trigger endpoints**.

---

## Verification Results

- `npm run lint`: **PASS** (0 errors, 0 warnings)
- `npm run format:check`: **PASS** (All files match Prettier style)
- `npm run typecheck`: **PASS** (`tsc -b` compiled all packages and applications)
- `npm test`: **PASS** (16 test suites, 103/103 tests passing)
- `npm run build`: **PASS** (All workspaces, apps, and Next.js built cleanly)
