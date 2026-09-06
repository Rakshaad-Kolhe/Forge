# ADR-004: At-Least-Once Delivery and Idempotent State Transitions

## Status

Accepted

## Context

Distributed task execution systems face the classic two-generals and network partition problems. In a distributed CI/CD platform, failures occur across multiple vectors:

- Workers may crash, lose power, or suffer hardware faults during execution.
- Network links between workers, schedulers, and coordination brokers may drop momentarily.
- Container execution processes (Docker/Kubernetes) may be abruptly terminated by the host OS out-of-memory (OOM) killer without emitting exit traps.
- Heartbeat acknowledgments may be delayed or dropped even if a job ran to completion.

Under these conditions, a distributed system must guarantee that tasks are not silently lost while ensuring that retried or redelivered tasks do not corrupt system state, trigger duplicate contradictory mutations, or overwrite finished results.

A naive or deceptive promise commonly seen in distributed platforms is claiming "exactly-once execution". In reality, the physical laws of distributed networks make true physical exactly-once execution impossible when arbitrary external side effects (e.g., executing arbitrary user shell scripts or network requests) are involved.

---

## Decision

Forge V2 explicitly adopts **At-Least-Once Delivery combined with Idempotent State Transitions** as its core distributed execution model.

Forge V2 **strictly prohibits** claiming or advertising "exactly-once execution".

### The Core Failure Scenario

The architecture is designed specifically around the reality of worker crash recovery:

```text
1. Worker A claims Job X (lease established for Attempt #1).
2. Worker A begins local container execution.
3. Worker A experiences an ungraceful hardware failure / network severance.
4. Worker A fails to renew its heartbeat or acknowledge completion.
5. The coordination lease expires in Redis.
6. The Scheduler detects the expired lease and marks Attempt #1 as timed-out/failed.
7. Job X is requeued and becomes visible again.
8. Worker B claims Job X (lease established for Attempt #2).
9. Worker B executes Job X.
10. Durable state transitions in PostgreSQL remain strictly safe and deterministic.
```

If Worker A subsequently recovers from a network partition and attempts to submit results for Attempt #1, the state store **rejects** the stale submission because the active lease/attempt counter has advanced.

---

## The Idempotency & Identity Model

To prevent duplicate execution from corrupting persistent state, Forge enforces a strict hierarchical identity tuple for all execution units:

$$\text{Execution Identity} = (\text{pipeline\_run\_id},\ \text{job\_id},\ \text{attempt\_id})$$

- `pipeline_run_id`: Uniquely identifies the top-level workflow trigger.
- `job_id`: Identifies the distinct DAG node within that pipeline.
- `attempt_id`: A monotonically increasing integer identifying the specific execution attempt of that job.

### Crucial Architectural Distinctions

To avoid conflation, Forge formally decouples four concepts:

1. **Message Delivery** (Transport Layer):
   - The act of passing a job notification across Redis to a worker.
   - Can occur multiple times for the same job due to network retries.
2. **Job Execution** (Runtime Layer):
   - The physical execution of shell commands inside a container.
   - Because user scripts can produce external side effects (e.g. `npm publish`, pushing git tags, calling third-party APIs), the platform cannot guarantee that an aborted job had zero side effects.
3. **State Transition** (Storage Layer):
   - The mutation of job status in PostgreSQL (`pending` → `queued` → `running` → `completed` / `failed`).
   - **Must be strictly idempotent and conditional**. A transition for an outdated `attempt_id` is a no-op or rejected.
4. **External Side Effects** (Domain Layer):
   - User-defined actions that occur outside Forge's database. Users must be encouraged via documentation to make their pipeline scripts resilient to retries.

---

## Rationale

- **Guaranteed Forward Progress**: At-least-once delivery guarantees that jobs are never permanently orphaned or stranded due to transient worker crashes.
- **Deterministic Relational State**: Enforcing idempotency at the database transition layer ensures that regardless of how many times a message is delivered or retried, the final stored outcome reflects only valid, ordered state transitions.
- **Honest Engineering Contract**: Acknowledging that external user scripts cannot be guaranteed to run "exactly once" allows the platform to focus on what it can guarantee: robust crash recovery, explicit attempt tracking, and atomic state protection.

---

## Consequences

### Positive

- **Resilience to Chaos**: Workers can be abruptly terminated (`kill -9`, spot instance revocation, node reboot) without permanently stalling the CI pipeline.
- **Audit Trail**: Tracking each execution as a distinct `attempt_id` records a complete history of worker failures, timeouts, and retries.
- **No Phantom Completions**: Stale completion events from disconnected workers are rejected via attempt verification.

### Negative / Trade-offs

- **User Script Responsibility**: Because a crashed attempt may have partially executed before being retried, user scripts with non-idempotent external side effects (e.g. raw database seeds) may experience partial execution before the second attempt runs.
- **State Machine Complexity**: State transition handlers must check preconditions and attempt counters on every update rather than performing blind database writes.

---

## Invariants

1. Reprocessing a message or receiving a duplicate delivery must never corrupt durable job state.
2. Invalid state transitions (e.g., transition from `completed` to `running`, or an update matching an expired `attempt_id`) must be strictly rejected.
3. Marking a job as completed must be safely repeatable at the state layer without duplicate downstream triggers.
4. Worker crashes or network disconnections must not permanently strand jobs in an unresolvable state.
5. The system and documentation must never advertise or promise exactly-once execution semantics.

---

## Failure Behavior

- **Worker Crash Mid-Job**: Lease expires in Redis; scheduler moves current attempt to `timed_out` or `failed` and enqueues a new attempt if retry quota permits.
- **Duplicate Message Delivery**: Worker checks current job status before starting; if already claimed or finished, the duplicate message is acknowledged and discarded.
- **Zombie Worker Returns**: If a partitioned worker finishes execution and attempts to write completion after its lease expired, the update is rejected because the attempt is closed in PostgreSQL. The isolated container is cleaned up.

---

## Alternatives Considered

### 1. At-Most-Once Delivery

- _Description_: Fire-and-forget message delivery. If a worker crashes while running a job, the job is never retried.
- _Why Rejected_: CI/CD pipelines require reliability. Dropping a build because of a temporary worker node reboot or spot eviction frustrates developers and requires manual re-triggering.

### 2. Exactly-Once Execution

- _Description_: Promising that a job will execute exactly one time, under all circumstances, with zero side effects repeated.
- _Why Rejected_: Mathematically and physically impossible in distributed systems with untrusted user-supplied code. If a worker process loses power after issuing an HTTP POST to an external service but before writing its acknowledgment, no distributed protocol can undo the HTTP POST. Claiming "exactly-once" is false advertising that leads to dangerous architectural assumptions.

### 3. At-Least-Once Delivery without Idempotent State Transitions

- _Description_: Retrying dropped jobs without tracking attempt IDs or validating transition states.
- _Why Rejected_: Leads to race conditions where two workers concurrently process the same job, overwrite each other's status, or trigger dependent DAG downstream jobs multiple times.

---

## Implementation Implications

- Database schema for jobs in future PRs must include `attempt_count`, and a dedicated `job_attempts` table linked to `jobs.id`.
- The State Machine implementation in future PRs must enforce conditional updates (e.g., `UPDATE jobs SET status = 'completed' WHERE id = :id AND status = 'running' AND current_attempt = :attempt`).
- Client libraries and documentation must educate users on writing retry-friendly pipeline steps.

---

## Validation

- Future PRs will validate this decision through chaos/crash-recovery tests simulating worker process termination during active job runs.
- PR 02 verifies that no codebase components or documentation claim "exactly-once" semantics.

---

## Related Decisions

- [ADR-001: Explicit Service and Package Boundaries](ADR-001-service-boundaries.md)
- [ADR-002: PostgreSQL as Source of Truth](ADR-002-postgresql-source-of-truth.md)
- [ADR-003: Redis for Transient Distributed Coordination](ADR-003-redis-coordination.md)
- [ADR-005: Ephemeral Execution](ADR-005-ephemeral-execution.md)
