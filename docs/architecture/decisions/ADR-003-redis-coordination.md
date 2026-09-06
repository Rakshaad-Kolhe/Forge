# ADR-003: Redis for Transient Distributed Coordination

## Status

Accepted

## Context

In a distributed CI/CD orchestration engine, multiple independent worker daemons concurrently contend for executable jobs across diverse project queues. Simultaneously, the scheduler must dispatch work, monitor worker liveness via periodic heartbeats, enforce execution leases to prevent duplicate work, and distribute real-time state change events to websocket subscribers.

Managing these high-frequency, transient synchronization primitives directly inside a relational database like PostgreSQL introduces significant bottlenecks:

- Polling database tables every few milliseconds creates massive lock contention and generates excessive WAL (Write-Ahead Logging) write amplification.
- Relational row locks for ephemeral worker leases do not naturally handle TTL (time-to-live) expirations when workers crash without releasing locks.
- Relational notification mechanisms (e.g., PostgreSQL `LISTEN`/`NOTIFY`) are not designed for high-throughput buffering or distributed worker lease arbitration.

At the same time, introducing an overly complex distributed messaging cluster (such as Apache Kafka or complex AMQP brokers) adds heavy operational overhead for self-hosted installations.

---

## Decision

**Redis** is selected as Forge V2's primary coordination layer for **transient distributed synchronization, execution queues, and ephemeral leases**.

### Future Responsibilities for Redis

When implemented in future PRs, Redis will handle:

- **Job Queues**: Low-latency FIFO and priority task queues (using Redis Streams or sorted sets/lists) for dispatching jobs to workers.
- **Worker Leases & Heartbeats**: Fast atomic key leases with TTLs (e.g., `SET NX EX`) allowing workers to claim execution exclusive locks that expire automatically if the worker crashes.
- **Distributed Claims**: Ensuring atomic single-worker acquisition of jobs without relational table locking.
- **Ephemeral Event Transport**: Lightweight Pub/Sub distribution for streaming state changes to UI WebSocket brokers.

### The Core Architectural Boundary

```text
┌───────────────────────────────────────────────────────────┐
│              PostgreSQL: Authoritative Truth              │
│  - Durable historical records                              │
│  - Schema integrity, users, runs, job outcomes            │
└───────────────────────────────────────────────────────────┘
                             ▲
                             │ (Reconciliation / Persistence)
                             ▼
┌───────────────────────────────────────────────────────────┐
│            Redis: Transient Coordination Layer            │
│  - In-flight queues, worker leases, heartbeats            │
│  - High throughput, low latency, auto-expiring keys       │
└───────────────────────────────────────────────────────────┘
```

Redis is **strictly a transient coordination mechanism**, not a durable database for Forge's core truth.

---

## Rationale

- **Atomic Primitives**: Redis provides single-threaded atomic operations (`RPUSH`, `BLPOP`, Lua scripts, `SET ... NX PX`) ideal for race-condition-free job claims and distributed leases.
- **Built-In TTL Semantics**: Ephemeral worker leases expire automatically upon worker heartbeat failure, solving the "zombie worker" problem without manual database cleanup sweeps.
- **Lightweight Self-Hosted Footprint**: Running a Redis container requires minimal memory and zero JVM/ZooKeeper/broker cluster dependencies, preserving Forge's commitment to clean, accessible self-hosted deployments.
- **Sub-Millisecond Latency**: Decouples high-frequency worker heartbeats from persistent disk I/O.

---

## Consequences

### Positive

- **Protects PostgreSQL**: Prevents database connection pool exhaustion and table bloat from continuous worker polling and lease renewals.
- **Fast Queue Dispatch**: Sub-millisecond queue pop operations ensure immediate job execution when workers become idle.
- **Operational Simplicity**: A single Redis instance or Sentinel/Cluster pair handles queues, locks, and pub/sub without multiple distinct messaging technologies.

### Negative / Trade-offs

- **Additional Infrastructure Component**: Requires maintaining, monitoring, and backing up Redis in production topologies.
- **Data Rehydration Requirement**: Because Redis is transient, restarting Redis clears active queue buffers. The scheduler must possess reconciliation logic to rebuild in-flight queues from authoritative PostgreSQL records upon reconnection.

---

## Invariants

1. PostgreSQL remains the sole durable source of truth; Redis state must never be assumed to be permanent.
2. A total loss or restart of Redis must never corrupt or delete historical records stored in PostgreSQL.
3. Redis failure must degrade system throughput but must not cause catastrophic unrecoverable data loss.
4. Transient execution claims in Redis must always link back to valid, existing `job_id` and `pipeline_run_id` records in PostgreSQL.

---

## Failure Behavior

### 1. Redis Unavailability / Crash

- **Detection**: Services detect disconnection via connection timeouts.
- **Immediate Behavior**: API ingress continues to record new pipeline runs into PostgreSQL, but new queue pushes pause. Workers pause job claims. Running jobs continue executing in their local isolated containers.
- **Authoritative Protection**: Authoritative historical state in PostgreSQL remains completely uncorrupted.

### 2. Redis Recovery & State Reconciliation

- **Recovery Principle**: When Redis reconnects, the scheduler queries PostgreSQL for jobs in `queued` or `running` states.
- **Lease Reconciliation**: If a worker lost its lease due to Redis downtime, the scheduler reconciles worker heartbeats against active lease timestamps before deciding whether to re-queue the job under at-least-once rules.

---

## Alternatives Considered

### 1. PostgreSQL-Backed Queue (e.g. `SKIP LOCKED`)

- _Trade-off_: Eliminates Redis entirely; uses PostgreSQL tables with `SELECT ... FOR UPDATE SKIP LOCKED`.
- _Why Rejected_: While viable for low-throughput workloads, heavy polling from dozens of workers increases WAL write amplification, fills table vacuum queues, and competes with API transactions for database locks and connection pools.

### 2. RabbitMQ / AMQP

- _Trade-off_: Feature-rich enterprise message broker with mature queuing semantics, routing keys, and dead-letter queues.
- _Why Rejected_: Adds significant operational overhead (Erlang runtime, cluster management, complex administrative interfaces). Lacks the general-purpose key-value and distributed lease capabilities that Redis provides natively.

### 3. NATS

- _Trade-off_: Extremely high performance, minimal footprint, modern JetStream persistence.
- _Why Rejected_: While excellent for messaging, NATS is primarily an event bus rather than a multi-purpose distributed coordination tool. Forge would still require a key-value store with TTLs for worker leases, resulting in running both NATS and Redis.

### 4. Apache Kafka

- _Trade-off_: Infinite event retention, partitioned log streaming, enterprise replay capabilities.
- _Why Rejected_: Severe operational complexity (ZooKeeper/KRaft, large JVM memory footprints, complex partition rebalancing). CI/CD queueing requires point-to-point task consumption with atomic lease acknowledgments, which is antithetical to Kafka's append-only partition model.

---

## Implementation Implications

- PR 01 and PR 02 introduce zero Redis dependencies, drivers, or connection logic.
- Future PRs will introduce a dedicated configuration block (`REDIS_URL`, `REDIS_PASSWORD`) and a clean infrastructure abstraction (e.g., `QueueService`, `LeaseManager`) in the core packages.
- Scheduler and worker services must wrap Redis access inside clear boundary contracts rather than direct raw Redis client calls in business services.

---

## Validation

- Verified that PR 01 and PR 02 contain no Redis client libraries or premature queue implementations.
- Architecture documentation establishes the explicit boundary between PostgreSQL durability and Redis transient coordination.

---

## Related Decisions

- [ADR-001: Explicit Service and Package Boundaries](ADR-001-service-boundaries.md)
- [ADR-002: PostgreSQL as Source of Truth](ADR-002-postgresql-source-of-truth.md)
- [ADR-004: At-Least-Once Delivery and Idempotent State Transitions](ADR-004-at-least-once-delivery.md)
