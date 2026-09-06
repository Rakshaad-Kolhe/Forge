# PR 07: Reliable FIFO Job Queue

## Summary

Establishes Forge V2's first distributed coordination mechanism: a reliable, Redis-backed FIFO job queue implemented in `@forge/queue`. Built on top of the `@forge/redis` infrastructure package, this PR provides deterministic enqueue, dequeue, explicit acknowledgement, queue depth inspection, in-flight visibility tracking, crash/unacknowledged recovery, duplicate delivery tolerance, and safe reference-only serialization under Forge's **at-least-once delivery + idempotent consumer** architectural model.

Crucially:

- **PostgreSQL remains the sole durable source of truth.** The queue carries lightweight job-dispatch references (`messageId`, `jobId`, `pipelineRunId`, `stepName`, `attemptNumber`), not mutable aggregates, cluster configs, or secrets.
- **At-least-once delivery is preserved; exactly-once execution is explicitly NOT claimed.**
- **Zero new external runtime dependencies.** Built 100% natively on `@forge/redis`.
- **Zero background daemons, worker loops, or scheduler polling inside `@forge/queue`.**

---

## Architectural Lifecycle

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

## Key Technical Decisions & Data Structures

Composite pattern across four Redis keys:

- `forge:queue:{queueName}:ready`: Redis List (`LPUSH` on enqueue, `RPOP` on dequeue for strict FIFO).
- `forge:queue:{queueName}:messages`: Redis Hash (`messageId` -> JSON serialized reference payload).
- `forge:queue:{queueName}:in_flight`: Redis Sorted Set (score = `visibilityExpiresAt` in ms, member = `messageId`).
- `forge:queue:{queueName}:meta`: Redis Hash (`messageId` -> `{ deliveryCount, enqueuedAt, deliveredAt, deliveryId }`).

### Atomic Lua Scripts

1. **FIFO Dequeue**: Pops from `ready`, inspects `messages`, updates `meta`, and inserts into `in_flight` with visibility timeout in a single Redis transaction. Competing consumers never receive the same ready message concurrently.
2. **Explicit Acknowledgement**: Atomically removes message from `in_flight`, `messages`, and `meta`. Returns `true` on first ACK, `false` on repeat calls idempotently.
3. **Reclaim Expired**: Finds in-flight entries where `score <= nowMs`, removes them from `in_flight`, and restores them to `ready` with priority (`RPUSH`), incrementing `deliveryCount` upon subsequent delivery.

---

## What is NOT Implemented (Strict Scope Discipline)

- **NO Scheduler engine** (scheduling loops and DAG resolution belong to future PR).
- **NO Worker claiming / leases / heartbeats** (worker management belongs to future PR).
- **NO Priority scheduling or weighted fairness** (queue is strictly FIFO).
- **NO Retry policies, exponential backoff, or Dead-Letter Queues (DLQ)**.
- **NO Pub/Sub, WebSockets, or event fanout**.
- **NO Dual-writes or distributed transactions between PostgreSQL and Redis**.

---

## Verification Results

- `npm run lint`: **PASS** (0 errors, 0 warnings)
- `npm run format:check`: **PASS** (All matched files use Prettier style)
- `npm run typecheck`: **PASS** (`tsc -b` compiled all packages and applications)
- `npm test`: **PASS** (20 test suites, 156/156 tests passing, including 23 `@forge/queue` tests and 30 `@forge/redis` tests)
- `npm run build`: **PASS** (All packages, shells, and Next.js built cleanly)
- **Manual smoke test against real Redis**: **PASS** (Enqueue A, B, C -> Dequeue A, B -> ACK A, B -> simulate unacknowledged crash on C -> visibility timeout expiry -> reclaimExpired -> redeliver C with deliveryCount=2 -> ACK C -> depth=0, inFlight=0)
