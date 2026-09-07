# Reliable FIFO Job Queue Architecture

## 1. Overview & Purpose

PR 07 introduces Forge V2's first distributed coordination mechanism: a reliable, Redis-backed FIFO job queue implemented in `@forge/queue`.

The job queue serves as a transient dispatch mechanism connecting the scheduling plane to execution workers. It is designed specifically around Forge's architectural contract established in ADR-003 and ADR-004:

```text
AT-LEAST-ONCE DELIVERY
+
IDEMPOTENT CONSUMER PROCESSING
```

### The Architectural Distinction: References vs. Aggregates

The queue transports lightweight **job-dispatch references**, not mutable domain aggregates or heavy configurations:

```text
Redis Queue Message
=
Transient work notification / dispatch intent
(messageId, jobId, pipelineRunId, stepName, attemptNumber)

PostgreSQL
=
Authoritative, durable domain state
(DAG definitions, step commands, attempts, exit codes, logs, terminal states)
```

Workers receiving a queue message use the stable identifiers (`jobId`, `pipelineRunId`) to interact with PostgreSQL and the domain state machine. Redis never duplicates the domain aggregate, and PostgreSQL remains the single source of truth.

---

## 2. Queue Lifecycle

```text
         ENQUEUE
            │
            ▼
        [ READY ] ─────────────► queue.depth()
            │
            ▼ DEQUEUE (atomic Lua)
       [ IN-FLIGHT ] ──────────► queue.inFlightCount()
            │
            ├───────────────► ACK (atomic Lua)
            │                   │
            │                   ▼
            │              [ COMPLETED ] (removed from Redis)
            │
            └── no ACK (crash/timeout)
                    │
                    ▼ visibility timeout expires
              [ RECOVERABLE ]
                    │
                    ▼ queue.reclaimExpired()
              [ REDELIVERY ] (pushed back to READY, deliveryCount++)
```

---

## 3. Data Structure Decision & Rationale

To balance strict FIFO ordering, atomic delivery, visibility tracking, and operational simplicity, the queue uses a hybrid composite pattern across four Redis keys:

```text
forge:queue:{queueName}:ready       ──► Redis List (LPUSH on enqueue, RPOP on dequeue)
forge:queue:{queueName}:messages    ──► Redis Hash (messageId -> JSON serialized payload)
forge:queue:{queueName}:in_flight   ──► Redis Sorted Set (score = visibilityExpiresAt ms, member = messageId)
forge:queue:{queueName}:meta        ──► Redis Hash (messageId -> { deliveryCount, enqueuedAt, deliveredAt })
```

### Why This Mechanism Over Alternatives?

| Mechanism                             | Strengths                                                                                                                                             | Trade-offs / Limitations                                                                                                             | Decision                                                                                                          |
| :------------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------- |
| **Redis List + Hash + ZSET** (Chosen) | Strict FIFO via `LPUSH`/`RPOP`; simple, deterministic atomic transitions via Lua; explicit recovery through score indexing; minimal memory footprint. | Requires explicit Lua scripts for multi-key atomicity.                                                                               | **Selected**: Clean, deterministic, completely fulfills Forge PR 07 requirements with zero external dependencies. |
| **Pure Redis Streams**                | Built-in consumer groups and pending entries list (PEL).                                                                                              | Higher operational complexity; consumer group management overhead; ACK/PEL trimming nuances; overkill for simple reference dispatch. | Deferred until complex multi-group pub/sub streams are needed.                                                    |
| **Pure Redis List (`RPOPLPUSH`)**     | Simple two-list atomic rotation.                                                                                                                      | Processing list does not naturally track time-based visibility expiration; requires manual scanning to detect crashed workers.       | Rejected in favor of ZSET-based visibility indexing.                                                              |

---

## 4. Message Schema

The queue payload carries minimal, JSON-serializable domain references:

```typescript
interface QueueMessage {
  readonly messageId: string; // Unique ID for this message instance (e.g. msg_<uuid>)
  readonly jobId: string; // Authoritative domain JobId in PostgreSQL
  readonly pipelineRunId: string; // Enclosing PipelineRunId in PostgreSQL
  readonly stepName: string; // Name of the step being executed
  readonly attemptNumber: number; // 1-based attempt sequence number
  readonly enqueuedAt: string; // ISO 8601 timestamp
}
```

### Excluded Data

The following are strictly **forbidden** from queue payloads:

- Docker / Kubernetes configs or pod specs
- Database connections or raw SQL fragments
- Secrets, passwords, API keys, or JWT tokens
- Full step command scripts or environment variable bundles
- Large domain aggregates or graph structures

---

## 5. FIFO Semantics

- **Ready Queue FIFO**: Messages in the ready list are strictly ordered first-in, first-out. A message enqueued before another will be dequeued before it.
- **Concurrent Consumers**: Under competing consumers, Redis's single-threaded command processing serializes `dequeue()` script invocations. Each consumer receives a distinct message in FIFO order without duplication or contention.
- **Redelivery Priority**: When unacknowledged messages expire and are reclaimed via `reclaimExpired()`, they are pushed to the head of the pop queue (`RPUSH`) so that delayed or recovered jobs are addressed promptly without starvation.

---

## 6. Delivery & Recovery Semantics

### At-Least-Once Delivery

Forge guarantees **at-least-once delivery**. A message will be delivered one or more times until explicitly acknowledged.

### Visibility Timeout & Recovery

When a consumer dequeues a message:

1. It is popped from `ready` and added to `in_flight` with a score of `nowMs + visibilityTimeoutMs`.
2. Its `deliveryCount` is incremented.
3. If the consumer processes the message and calls `acknowledge(messageId)`, the message is atomically purged from `in_flight`, `messages`, and `meta`.
4. If the consumer crashes, hangs, or loses network connectivity, the visibility timeout expires.
5. An invocation of `reclaimExpired()` detects all members in `in_flight` where `score <= nowMs`, removes them from `in_flight`, and restores them to the `ready` list.
6. The next consumer dequeues the recovered message with `deliveryCount > 1`.

### Acknowledgement Idempotency

Calling `acknowledge(messageId)` on an already-acknowledged or non-existent message returns `false` deterministically without throwing an error.

---

## 7. Failure Behavior

- **Redis Unreachable**: When Redis is down or disconnected, queue operations (`enqueue`, `dequeue`, `acknowledge`, `depth`, `reclaimExpired`) throw `QueueUnavailableError`. Operations **never** silently swallow connection failures into `null` or empty responses.
- **Empty Queue**: Calling `dequeue()` on a running, reachable queue with zero ready messages returns `null` deterministically.
- **Crash Without ACK**: Unacknowledged messages remain safely in the Redis `in_flight` sorted set and `messages` hash, recoverable at any time once their visibility timeout elapses.

---

## 8. Explicit Non-Guarantees (Out of Scope for PR 07)

The queue is a low-level coordination primitive, not a high-level scheduling or execution platform. PR 07 explicitly does **NOT** provide:

1. **Exactly-Once Execution**: Impossible across distributed network boundaries. Forge relies on at-least-once delivery + domain state machine idempotency.
2. **Worker Leases / Heartbeats**: Worker liveness and ownership leases belong to subsequent worker-management PRs.
3. **Scheduler Orchestration**: No background polling loops or timers exist inside `@forge/queue`.
4. **Retry Policies / Backoff**: The queue does not compute exponential backoffs or retry limits.
5. **Dead-Letter Queues (DLQ)**: Poison message handling belongs to future reliability orchestration.
6. **Priority Scheduling / Fairness**: The queue is strictly FIFO. Priority and weighted fairness belong to the scheduler.
