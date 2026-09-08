# PR 21 — Durable Event Transport & PostgreSQL Transactional Outbox — Design

Status: **DRAFT — awaiting user review**
Branch: `feat/pr-21-outbox` (worktree `.claude/worktrees/pr-21-outbox`), cut from `42cdf33` (PR 20 tip, `feat/pr-20-typed-events`).
Supersedes: the initial PR 21 design proposal (revised after an adversarial architecture review — see §12).

---

## 1. Objective

Close the PR 20 failure window: a best-effort `safePublish` after a committed PostgreSQL
transaction can be lost if the transport is unavailable.

**Guarantee established by this PR:**

> A lifecycle event associated with a PostgreSQL state transition is durably recorded in the
> **same transaction** as that state transition. The dispatcher then delivers it
> **at least once**. Dispatcher crashes, publisher failures, and stale dispatchers cannot
> silently erase or corrupt a recorded event.

**Not claimed:** exactly-once delivery, global ordering, or atomic DB↔external-transport commit.

Delivery-semantics wording used verbatim in docs:

> "Forge durably records events transactionally with PostgreSQL state and delivers them at
> least once; consumers must tolerate duplicates."

---

## 2. Repository inspection findings (`[VERIFIED]` unless noted)

### 2.1 Events (`packages/events/`)

- `ForgeEvent` — discriminated union of 13 types; `ForgeEventEnvelope<TType,TPayload>` carries
  `event_id` (UUID v4, `EVENT_ID_PATTERN`), `event_type`, `occurred_at` (ISO-8601), `version`
  (`EVENT_SCHEMA_VERSION = 1`), optional correlation ids, `payload`. Frozen after `createForgeEvent`.
- `createForgeEvent(type, {correlation?, payload}, {now?, eventId?})` — allocation-light, no validation.
- `parseForgeEvent(input)` / `safeParseForgeEvent(input)` — zod `forgeEventSchema`
  (discriminated union). Rejects bad `event_id`, non-ISO `occurred_at`, unknown `version`,
  unknown `event_type`, payload mismatch. Strips unknown fields.
- `EventPublisher.publish(event): Promise<void>` — the transport seam.
- `safePublish(publisher|undefined, event, logger?)` — never throws/blocks; no-op when publisher
  undefined; logs+swallows failures.
- `InProcessEventBus.publish` — awaits each subscriber handler **sequentially**, **no timeout**;
  handler failure is caught+logged+isolated. `close()` idempotent; no global singleton.
- Producers currently wired (PR 20, all best-effort `safePublish` **after** commit):
  - Scheduler: `JobClaimed` (on lease `ACQUIRED`, single + batch), `WorkerLost` (per reconciled
    lease in `recoverExpiredLeases`, `NO_OP` excluded).
  - Worker: `WorkerRegistered`, `WorkerHeartbeat`, `JobStarted`, `JobLogChunk` (derived post-exec),
    terminal `JobSucceeded`/`JobFailed`/`JobCancelled`, `JobQueued` (retry re-queue).
  - Contract-only (no producer): `PipelineCreated`, `PipelineQueued`, `PipelineCompleted`,
    first-enqueue `JobQueued`.

### 2.2 Database (`packages/database/`)

- `withTransaction(pool, cb)` → `TransactionContext { client, pipelines, pipelineRuns, jobs,
jobAttempts, workerLeases, deadLetterJobs }` — each repo bound to the one `pg.PoolClient`.
  `BEGIN` → cb → `COMMIT`; on throw `ROLLBACK`; always `client.release()`.
- `migrator.ts`: migrations run from **inline `*_SQL` string constants** in `MIGRATIONS[]`
  (`001`–`007`), applied by `runMigrations(client)` each inside its own `BEGIN/COMMIT`.
  The `src/migrations/sql/*.sql` files are **drifted, unused copies** (001 differs 72 vs 65 lines).
  → PR 21 adds `OUTBOX_EVENTS_SQL` + `{name:'008_outbox_events', sql: OUTBOX_EVENTS_SQL}` to
  `MIGRATIONS`; also add `sql/008_outbox_events.sql` for directory consistency, noting the drift.
- `PgWorkerLeaseRepository.claimBatch(options: BatchClaimOptions)`:
  - Wraps body in `withTransactionClient`. When the repo is constructed on a **pool** (scheduler
    usage: `new PgWorkerLeaseRepository(pool)`), `isDedicatedClient === true` → real
    `BEGIN`/`COMMIT`/`ROLLBACK`. A throw inside the callback → full `ROLLBACK` → rethrow wrapped
    in `PersistenceError`. **Lease INSERT + any co-inserted outbox rows commit or roll back together.**
  - `ACQUIRED` results carry `isIdempotent?: boolean` (`false` = fresh INSERT, `true` = lease
    already held by same worker).
  - `claimBatch?` is **optional** in `WorkerLeaseRepository`; `ForgeScheduler` feature-detects
    `typeof this.leaseRepository.claimBatch === 'function'`.
- `LeaseRecoveryService.recoverSingleLease(leaseId, now)` → `withTransaction(this.pool, tx => …)`.
  Locks lease `FOR UPDATE SKIP LOCKED`, marks `EXPIRED`, reconciles attempt, `evaluateRetry`,
  requeues (`REQUEUED`) or `job.fail()` + `tx.deadLetterJobs.save()` (`DEAD_LETTERED`), or
  `SKIPPED_TERMINAL` / `NO_OP`. **Already inside a `TransactionContext`** — `tx.outbox.enqueue`
  drops straight in.
- `dead_letter_jobs`: job-failure domain — FKs to `jobs`/`pipeline_runs`, `reason` enum is
  job-failure vocabulary, `UNIQUE(job_id)`. Not reusable for event-delivery failure.
- Migration id/type conventions: PK `VARCHAR(255)`, `TIMESTAMPTZ … DEFAULT NOW()`, `JSONB`,
  `CHECK (... IN (...))`, `CREATE TABLE/INDEX IF NOT EXISTS`, `uq_`/`idx_`/`chk_` prefixes.
- Integration-test harness (e.g. `repositories/lease-recovery.integration.test.ts`):
  `createDatabasePool({connectionString: DEFAULT_DATABASE_URL})`; `resetDatabase` + `runMigrations`
  in `beforeAll`; `DELETE FROM …` in `beforeEach`; `resetDatabase` + `pool.close()` in `afterAll`.
  Files named `*.integration.test.ts`; `vitest` `fileParallelism: false`.

### 2.3 Worker (`apps/worker/src/index.ts`)

- `executeJob` persists twice, **both already via `withTransaction`** when `options.pool` is set:
  1. RUNNING commit (`tx.jobs.save` + `tx.jobAttempts.save`), then best-effort `JobStarted`.
  2. Terminal commit (`tx.jobs.save` + `tx.jobAttempts.save`), then best-effort log chunks +
     terminal event + `JobQueued` on retry.
- **Non-transactional fallback**: `else if (options?.jobRepository) { await options.jobRepository.save(job) }`
  — persists job state with **no transaction, no outbox**.
- Production entrypoint (`isDirectRun`) calls `startWorker()` with **no args** → no `pool`, no
  `jobRepository`, no `eventPublisher`, and never calls `executeJob` (shell only, per CLAUDE.md).
  → The durability guarantee is **contingent on the future worker loop passing `pool`**; nothing
  enforces it today. Addressed in §7.

### 2.4 Scheduler (`apps/scheduler/`)

- `ForgeScheduler.publishJobClaimed(...)` / `publishWorkerLostForRecovery(result)` — currently
  `safePublish`. Called from single-claim (`claimLeaseForDecision`) and batch paths, and from
  `recoverExpiredLeases` (wraps `recoveryService.recoverExpiredLeases`).
- `startRecoveryLoop(intervalMs = 5000)` — `setInterval` + `timer.unref()` + `isSweeping`
  reentrancy guard; `stopRecoveryLoop()` clears + waits. **Pattern to mirror for the dispatcher.**
- `apps/scheduler/src/index.ts` `startScheduler()` — PR-01 log-only shell returning `{ stop() }`.

### 2.5 Config / contracts

- `packages/config`: one big zod `configSchema`; each var `z.string().regex(/^\d+$/).default(...)
.transform(Number).pipe(z.number().int().min(...))`; cross-field `.refine(...)`; `loadConfig(env=process.env)`
  → typed `AppConfig`. Mirror fields in `packages/contracts` `AppConfig` (a single-file package,
  **zero runtime deps**).
- `DockerExecutor` takes plain numeric options, **not** `@forge/config` — the precedent for
  keeping library packages config-loader-free.

---

## 3. Architecture & boundaries

```
apps/worker ── withTransaction ─┐
                                ├─►  jobs + job_attempts + outbox_events   (ONE commit)
apps/scheduler ─ claimBatch ────┤     worker_leases  + outbox_events       (ONE commit)
apps/scheduler ─ recovery ──────┘     jobs + dead_letter_jobs + outbox_events (ONE commit)
                                              │
                                              ▼
                                     OutboxDispatcher (@forge/outbox)
                                     poll → claim(batch, new claim_token)
                                          → parseForgeEvent(payload)
                                          → publish(event)  [bounded timeout]
                                          → conditional markPublished(id, token)
                                            / markRetry(id, token) / → DEAD
                                              │
                                              ▼
                                     EventPublisher  (InProcessEventBus today)
                                              │  at-least-once
                                              ▼
                                        Consumers  (must dedupe on event_id)
```

| Unit                                               | Package                         | Responsibility                                                         | New deps                  |
| -------------------------------------------------- | ------------------------------- | ---------------------------------------------------------------------- | ------------------------- |
| `008_outbox_events` migration                      | `@forge/database`               | `OUTBOX_EVENTS_SQL` const + `MIGRATIONS` entry + `sql/008_*.sql`       | —                         |
| `OutboxRepository` contract + `PgOutboxRepository` | `@forge/database`               | all `outbox_events` SQL (see §5)                                       | —                         |
| `tx.outbox`                                        | `@forge/database`               | `PgOutboxRepository` on the tx client, added to `TransactionContext`   | —                         |
| `pendingOutboxRows` param + mapper                 | `@forge/database`               | narrow pure-data param on `claimBatch` / recovery (see §6)             | —                         |
| `OutboxDispatcher`, backoff, retention             | **new `@forge/outbox`**         | poll/claim/publish/mark/retry/dead/retention; `start`/`stop`/`runOnce` | database, events, logging |
| dispatcher wiring                                  | `apps/scheduler/src/index.ts`   | construct + `start()`/`stop()` beside `startRecoveryLoop`              | outbox                    |
| producer integration                               | `apps/worker`, `apps/scheduler` | enqueue durable events in-transaction; drop their `safePublish`        | —                         |
| `benchmarks/outbox/`                               | repo root                       | overhead + throughput + EXPLAIN                                        | —                         |

**Dependency rules honoured:** `@forge/database` does **not** import `@forge/events`
(see §5.4). `@forge/outbox` does **not** import `@forge/config` (takes `OutboxDispatcherConfig`).
`@forge/outbox` carries **no `apps/scheduler` import** (relocatable to its own process later).
`packages/contracts` stays zero-dep (new outbox types are plain).

---

## 4. `outbox_events` schema (migration `008`)

```sql
CREATE TABLE IF NOT EXISTS outbox_events (
  id                     VARCHAR(255) PRIMARY KEY,          -- outbox row id: 'outbox_' || uuid
  event_id               VARCHAR(255) NOT NULL,             -- ForgeEvent.event_id (UUID v4) — dedup identity
  event_type             VARCHAR(64)  NOT NULL,
  version                INTEGER      NOT NULL,
  occurred_at            TIMESTAMPTZ  NOT NULL,             -- from envelope; primary ordering key

  pipeline_id            VARCHAR(255),
  run_id                 VARCHAR(255),
  job_id                 VARCHAR(255),
  attempt_id             VARCHAR(255),
  worker_id              VARCHAR(255),

  payload                JSONB        NOT NULL,             -- COMPLETE validated ForgeEvent envelope

  status                 VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
  delivery_attempt_count INTEGER      NOT NULL DEFAULT 0,   -- real publish attempts only (§6.3)
  dispatch_count         INTEGER      NOT NULL DEFAULT 0,   -- diagnostics: claims+reclaims; never drives DEAD

  available_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  claimed_at             TIMESTAMPTZ,
  claimed_by             VARCHAR(255),                      -- dispatcher instance id — DIAGNOSTIC ONLY
  claim_token            VARCHAR(255),                      -- fencing token; new value every (re)claim
  published_at           TIMESTAMPTZ,
  last_error             TEXT,                              -- truncated to 2000 chars

  created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

  CONSTRAINT uq_outbox_events_event_id UNIQUE (event_id),
  CONSTRAINT chk_outbox_events_status  CHECK (status IN ('PENDING','CLAIMED','PUBLISHED','DEAD'))
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_claimable
  ON outbox_events (available_at, occurred_at, id) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_outbox_events_claimed
  ON outbox_events (claimed_at) WHERE status = 'CLAIMED';
CREATE INDEX IF NOT EXISTS idx_outbox_events_retention
  ON outbox_events (published_at) WHERE status = 'PUBLISHED';
```

- **No FK to `jobs`/`pipeline_runs`.** An event is a historical fact; it must survive later
  job-row changes/deletes (correlation columns are for diagnostics/indexing only).
- `payload` stores the **whole envelope** (redundant with the extracted columns) so the
  dispatcher reconstructs via `parseForgeEvent(payload)` with nothing else.
- `dispatch_count` vs `delivery_attempt_count` — the adversarial-review fix (§6.3, §12-A).

### 4.1 Status model

```
PENDING ──claim (new token, dispatch_count++)──► CLAIMED
CLAIMED ──publish ok────────────────────────────► PUBLISHED         (conditional on token)
CLAIMED ──publish attempted + failed, budget left► PENDING          (delivery_attempt_count++, available_at = NOW()+backoff)
CLAIMED ──publish attempted + failed, budget out─► DEAD             (delivery_attempt_count++, row retained)
CLAIMED ──claim lease expired (claimed_at < staleBefore)──► reclaimed as CLAIMED (new token, dispatch_count++, NO delivery_attempt_count++)
```

- `PENDING`/`CLAIMED`/`DEAD` are never deleted by retention. `PUBLISHED` rows are eligible
  (§9). `DEAD` accumulates and needs manual triage (documented, no auto-cleanup in PR 21).
- **No `PUBLISHED → PENDING` transition exists.** Guaranteed structurally by the conditional
  `WHERE status = 'CLAIMED' AND claim_token = $token` on every mutation (§5.2).

---

## 5. `@forge/database` — repository & transaction API

### 5.1 Contracts (`repositories/contracts/outbox-repository.contract.ts`)

```ts
import type { OutboxEnqueueInput, OutboxEventRecord, OutboxStatus } from '@forge/contracts';

export interface OutboxClaimOptions {
  readonly dispatcherId: string;
  readonly limit: number;             // bounded batch
  readonly staleClaimBefore: Date;    // CLAIMED rows with claimed_at < this are reclaimable
  readonly now?: Date;                // test seam; defaults to DB NOW()
}
export interface OutboxClaimedRow extends OutboxEventRecord {
  readonly claimToken: string;        // caller MUST pass this back to mark* calls
}
export type OutboxMarkOutcome = 'OK' | 'CLAIM_LOST';   // CLAIM_LOST ⇒ 0 rows matched the fence

export interface OutboxRetryInput {
  readonly id: string;
  readonly claimToken: string;
  readonly availableAt: Date;
  readonly lastError: string;
  readonly exhausted: boolean;        // true ⇒ transition to DEAD instead of PENDING
}

export interface OutboxStats {
  readonly pending: number;
  readonly claimed: number;
  readonly published: number;
  readonly dead: number;
  readonly oldestPendingAgeMs: number | null;
}

export interface OutboxRepository {
  /** Insert one event. Structural + event_id-format + payload-size validation. Throws
   *  OutboxPayloadError (oversize/invalid) or ConstraintViolationError (duplicate event_id).
   *  Runs on whatever client the repo was constructed with (pool OR tx client). */
  enqueue(input: OutboxEnqueueInput): Promise<void>;
  enqueueMany(inputs: readonly OutboxEnqueueInput[]): Promise<void>;

  /** One transaction, FOR UPDATE SKIP LOCKED. Selects
   *    (status='PENDING' AND available_at<=NOW())
   *    OR (status='CLAIMED' AND claimed_at < staleClaimBefore)
   *  ORDER BY occurred_at, id LIMIT $limit.
   *  Per row: status='CLAIMED', claimed_at=NOW(), claimed_by=$dispatcherId,
   *  claim_token=<fresh uuid>, dispatch_count=dispatch_count+1.
   *  Does NOT touch delivery_attempt_count. Returns rows incl. their new claimToken. */
  claimBatch(options: OutboxClaimOptions): Promise<OutboxClaimedRow[]>;

  /** UPDATE ... SET status='PUBLISHED', published_at=NOW(), claimed_at=NULL, claimed_by=NULL,
   *  claim_token=NULL WHERE id=$1 AND status='CLAIMED' AND claim_token=$2.
   *  0 rows ⇒ 'CLAIM_LOST'. */
  markPublished(id: string, claimToken: string): Promise<OutboxMarkOutcome>;

  /** exhausted=false: SET status='PENDING', available_at=$availableAt, last_error=$err,
   *  delivery_attempt_count=delivery_attempt_count+1, claimed_*/claim_token=NULL
   *  WHERE id=$1 AND status='CLAIMED' AND claim_token=$2.
   *  exhausted=true: same but status='DEAD' and ownership columns retained for diagnosis.
   *  0 rows ⇒ 'CLAIM_LOST'. */
  markRetry(input: OutboxRetryInput): Promise<OutboxMarkOutcome>;

  /** DELETE ... WHERE status='PUBLISHED' AND published_at < $cutoff
   *  AND id IN (SELECT id ... LIMIT $limit). Returns deleted count. */
  deletePublishedBefore(cutoff: Date, limit: number): Promise<number>;

  stats(): Promise<OutboxStats>;

  // read helpers for tests/diagnostics
  findByEventId(eventId: string): Promise<OutboxEventRecord | null>;
  listByStatus(status: OutboxStatus, limit: number): Promise<OutboxEventRecord[]>;
}
```

### 5.2 `PgOutboxRepository`

- Mirrors `PgDeadLetterRepository` style: constructor `(client: DatabaseClient)`, every method
  wrapped, errors → `PersistenceError` (except the typed ones above).
- `delivery_attempt_count` is incremented **only** in `markRetry` (a real publish was attempted).
- Every mutating statement is fenced: `WHERE id = $1 AND status = 'CLAIMED' AND claim_token = $2`.
- `enqueue` validation (no `@forge/events` import): `input.payload` is an object;
  `input.eventId` matches `^[0-9a-f-]{36}$` (UUID shape — full v4 check is the producer's job
  via `parseForgeEvent`); `Buffer.byteLength(JSON.stringify(input.payload),'utf8') <=
OUTBOX_MAX_PAYLOAD_BYTES` else `OutboxPayloadError`. Column values (`event_type`, `version`,
  `occurred_at`, correlations) are taken from `input`, not re-derived from `payload`.

### 5.3 `TransactionContext` extension (`transaction.ts`)

```ts
export interface TransactionContext {
  client: pg.PoolClient;
  pipelines;
  pipelineRuns;
  jobs;
  jobAttempts;
  workerLeases;
  deadLetterJobs;
  outbox: PgOutboxRepository; // NEW — new PgOutboxRepository(client)
}
```

All existing `withTransaction` callers gain `tx.outbox` (unused unless they call it).

### 5.4 Why `@forge/database` does not import `@forge/events`

`OutboxEnqueueInput` (defined in zero-dep `@forge/contracts`) carries the already-validated
envelope as `payload: Record<string, unknown>` plus the extracted scalar columns. Producers
(`apps/worker`, `apps/scheduler` — both already import `@forge/events`) build the `ForgeEvent`,
call `parseForgeEvent` to validate, then `toOutboxEnqueueInput(event)` — a ~10-line pure
field-projection helper that lives in **`@forge/events`** (`src/outbox-input.ts`; it only needs
the `ForgeEvent` type and the plain `OutboxEnqueueInput` contract type, so it stays within
`@forge/events`'s existing dep set of `@forge/contracts` + `@forge/logging`). The dispatcher
(`@forge/outbox`, which _does_ depend on `@forge/events`) calls `parseForgeEvent(row.payload)`
before `publish`. `@forge/database` only ever handles the plain input/record shapes and imports
no events package. Net effect: **no `apps/*` gains a new package dependency for enqueueing** —
`apps/worker` uses `@forge/events` + `@forge/database` (both already deps); only
`apps/scheduler/src/index.ts` adds `@forge/outbox`, and only for the dispatcher class.

### 5.5 `@forge/contracts` additions (zero-dep)

```ts
export type OutboxStatus = 'PENDING' | 'CLAIMED' | 'PUBLISHED' | 'DEAD';

export interface OutboxEnqueueInput {
  readonly id: string; // 'outbox_' || uuid, caller-generated
  readonly eventId: string;
  readonly eventType: string;
  readonly version: number;
  readonly occurredAt: string; // ISO-8601
  readonly correlation: {
    readonly pipelineId?: string;
    readonly runId?: string;
    readonly jobId?: string;
    readonly attemptId?: string;
    readonly workerId?: string;
  };
  readonly payload: Record<string, unknown>; // complete, pre-validated envelope
}

export interface OutboxEventRecord {
  readonly id: string;
  readonly eventId: string;
  readonly eventType: string;
  readonly version: number;
  readonly occurredAt: Date;
  readonly pipelineId?: string;
  readonly runId?: string;
  readonly jobId?: string;
  readonly attemptId?: string;
  readonly workerId?: string;
  readonly payload: Record<string, unknown>;
  readonly status: OutboxStatus;
  readonly deliveryAttemptCount: number;
  readonly dispatchCount: number;
  readonly availableAt: Date;
  readonly claimedAt?: Date;
  readonly claimedBy?: string;
  readonly publishedAt?: Date;
  readonly lastError?: string;
  readonly createdAt: Date;
}

export const DEFAULT_OUTBOX_DISPATCH_POLL_INTERVAL_MS = 1000;
export const DEFAULT_OUTBOX_DISPATCH_BATCH_SIZE = 100;
export const DEFAULT_OUTBOX_CLAIM_TIMEOUT_MS = 60000;
export const DEFAULT_OUTBOX_PUBLISH_TIMEOUT_MS = 10000;
export const DEFAULT_OUTBOX_MAX_DELIVERY_ATTEMPTS = 10;
export const DEFAULT_OUTBOX_DELIVERY_BASE_BACKOFF_MS = 500;
export const DEFAULT_OUTBOX_DELIVERY_MAX_BACKOFF_MS = 60000;
export const DEFAULT_OUTBOX_MAX_PAYLOAD_BYTES = 65536;
export const DEFAULT_OUTBOX_RETENTION_MAX_AGE_MS = 604800000; // 7 days; 0 disables
export const DEFAULT_OUTBOX_RETENTION_BATCH_SIZE = 500;
export const MIN_OUTBOX_MAX_DELIVERY_ATTEMPTS = 1;
export const MAX_OUTBOX_MAX_DELIVERY_ATTEMPTS = 100;
```

---

## 6. Transactional coupling

### 6.1 Worker (`apps/worker/src/index.ts`)

Inside the **existing** `withTransaction` blocks:

| Block           | Existing                                            | Added                                                                    |
| --------------- | --------------------------------------------------- | ------------------------------------------------------------------------ |
| RUNNING commit  | `tx.jobs.save(job)`, `tx.jobAttempts.save(attempt)` | `tx.outbox.enqueue(toOutboxEnqueueInput(JobStarted))`                    |
| Terminal commit | `tx.jobs.save(job)`, `tx.jobAttempts.save(attempt)` | `tx.outbox.enqueue(<terminal>)`; if retry `tx.outbox.enqueue(JobQueued)` |

- The now-redundant best-effort `safePublish` for `JobStarted`, `JobSucceeded`, `JobFailed`,
  `JobCancelled`, `JobQueued` is **removed** (single authoritative producer — §10).
- `JobLogChunk` **keeps** its best-effort `safePublish` loop (excluded from the outbox — §11).
- The terminal events are built from already-computed local state (`execResult`, `attempt`,
  `job`, `retryDecision`, `ownershipLost`, `executorThrew`) exactly as PR 20 builds them.

### 6.2 Scheduler

**`JobClaimed` — via `claimBatch` pure-data param** (`PgWorkerLeaseRepository`):

```ts
// contracts: BatchClaimOptions gains
readonly pendingOutbox?: {
  readonly rowForAcquired: (item: BatchClaimItem, lease: WorkerLease) => OutboxEnqueueInput;
};
```

Inside `claimBatch`'s `withTransactionClient` callback, after `finalResults` is built, for each
result with `status === 'ACQUIRED' && isIdempotent !== true`, call
`options.pendingOutbox.rowForAcquired(item, lease)` and
`await new PgOutboxRepository(txClient).enqueueMany(rows)` — **same transaction**. A throw
propagates out of the callback → `withTransactionClient` `ROLLBACK` → lease INSERT undone too →
`PersistenceError`. `rowForAcquired` is a **pure mapper** (returns data, runs no SQL, is not
given the tx client). It must be a function rather than a pre-built `Map<jobId, row>` because
the `JobClaimed` payload needs the freshly-minted `lease_id` / `lease_expires_at`, which do not
exist until the lease INSERT has run inside this transaction. `ForgeScheduler` supplies the
mapper and builds+validates the `JobClaimed` `ForgeEvent` inside it.

**`WorkerLost` — via `tx.outbox` inside `recoverSingleLease`**:
`recoverExpiredLeases(options)` / `recoverSingleLease(leaseId, now, mapper?)` gain an optional
`outboxRowForRecord?: (record: RecoveredLeaseRecord) => OutboxEnqueueInput | null`. Inside
`recoverSingleLease`'s existing `withTransaction`, immediately before each `return record` where
`record.action` ∈ {`REQUEUED`,`DEAD_LETTERED`,`SKIPPED_TERMINAL`}, if a mapper is supplied call
it and `if (row) await tx.outbox.enqueue(row)`. `NO_OP` never enqueues. `ForgeScheduler` supplies
the mapper (builds+validates `WorkerLost`).

- `ForgeScheduler.publishJobClaimed` / `publishWorkerLostForRecovery` change from `safePublish`
  to "supply the mapper on the repo call". The best-effort path for these two is removed.
- `SchedulerOptions.eventPublisher` stays (nothing else uses it now, but keeps the seam; it is
  no longer used for `JobClaimed`/`WorkerLost`). May be removed if unused after wiring — decide
  in the plan.

### 6.3 Retry accounting (adversarial-review fix A / §12-A)

- **`dispatch_count`** — incremented on every claim and reclaim. Diagnostics only. Never
  triggers `DEAD`.
- **`delivery_attempt_count`** — incremented **only** in `markRetry`, i.e. only after
  `publisher.publish(event)` was actually invoked and threw/timed out. Checkpoint: "Forge handed
  this event to the configured publisher and the attempt did not confirm success."
- Crash-after-claim (never published): reclaim bumps `dispatch_count`, not
  `delivery_attempt_count` → no budget consumed.
- Publish-succeeded-then-crash-before-`markPublished`: next dispatcher reclaims, re-publishes
  (the intended duplicate), then `markPublished`. `delivery_attempt_count` incremented once per
  _actual_ failed attempt only; a _successful_ re-publish consumes no budget.
- `DEAD` iff `delivery_attempt_count + 1 >= OUTBOX_MAX_DELIVERY_ATTEMPTS` at a genuine failure.
  Termination: an event dies only after `OUTBOX_MAX_DELIVERY_ATTEMPTS` real transport rejections
  — never because a process died or a checkpoint write failed.

---

## 7. Worker production-bypass guard (adversarial-review fix / §12-C)

- The worker execution loop MUST persist via `withTransaction` (which always carries
  `tx.outbox`). New invariant text (invariants.md §14): _"The production worker execution loop
  MUST NOT commit job state without the corresponding transactional outbox event. Committing job
  state through a non-transactional repository path is a test-only affordance and is prohibited
  in production wiring."_
- Enforcement in `executeJob`: the dangerous combination is **non-transactional persistence +
  event publishing** — i.e. `options.jobRepository` set, `options.pool` **not** set, and
  `options.eventPublisher` set. In that exact case `executeJob` `throw`s at entry:
  `"worker durable events require a transactional pool; jobRepository-only persistence cannot
guarantee the outbox"`. Permitted, unchanged: `pool` + `eventPublisher` (the target — durable
  lifecycle events + best-effort `JobLogChunk`); `pool` only; `jobRepository` only with no
  publisher (pure unit tests); no persistence at all (today's shell).
- Docs stop describing the no-pool path as a "degraded production mode"; it is named as a
  durability hole that the future worker daemon must not fall into.

---

## 8. `@forge/outbox` package

### 8.1 `OutboxDispatcherConfig` (plain object; mapped from `loadConfig()` by `apps/scheduler`)

```ts
export interface OutboxDispatcherConfig {
  readonly pollIntervalMs: number;
  readonly batchSize: number;
  readonly claimTimeoutMs: number; // stale-claim recovery threshold
  readonly publishTimeoutMs: number; // per-event publish() timeout — SEPARATE from claimTimeout
  readonly maxDeliveryAttempts: number;
  readonly baseBackoffMs: number;
  readonly maxBackoffMs: number;
  readonly retentionMaxAgeMs: number; // 0 disables
  readonly retentionBatchSize: number;
  readonly retentionEveryNTicks: number; // default 60
  readonly dispatcherId?: string; // default `outbox-${randomUUID()}`
}
```

Constraint (validated where mapped): `claimTimeoutMs >= publishTimeoutMs + pollIntervalMs`
(a claim must outlive a legitimate in-flight publish); `maxBackoffMs >= baseBackoffMs`.

### 8.2 `OutboxDispatcher`

```ts
class OutboxDispatcher {
  constructor(deps: {
    repository: OutboxRepository;
    publisher: EventPublisher;
    logger?: Logger;
    config: OutboxDispatcherConfig;
  });
  start(): void; // idempotent; setInterval(tick); timer.unref()
  stop(): Promise<void>; // idempotent; clearInterval; await in-flight tick; no timer survives
  runOnce(): Promise<OutboxTickSummary>; // single tick — deterministic tests
}
interface OutboxTickSummary {
  claimed: number;
  published: number;
  retried: number;
  dead: number;
  claimLost: number;
  reclaimed: number;
  retentionDeleted: number;
}
```

`tick()` (guarded by `isDispatching`, mirrors scheduler `isSweeping`):

1. `rows = repository.claimBatch({ dispatcherId, limit: batchSize,
staleClaimBefore: new Date(Date.now() - claimTimeoutMs) })`.
2. For each row: `event = parseForgeEvent(row.payload)` (on parse failure →
   `markRetry({exhausted:true})` with `last_error='unparseable payload'` → `DEAD`; a stored
   validated payload should never fail, so this is a corruption guard).
   Then `await withTimeout(publisher.publish(event), publishTimeoutMs)`:
   - resolved → `outcome = markPublished(row.id, row.claimToken)`.
   - threw / timed out → `exhausted = row.deliveryAttemptCount + 1 >= maxDeliveryAttempts`;
     `outcome = markRetry({ id, claimToken: row.claimToken, availableAt: NOW()+backoff(row.deliveryAttemptCount),
lastError: truncate(err), exhausted })`.
   - `outcome === 'CLAIM_LOST'` → log `outbox.claim_lost`, **discard**, do not touch the row
     (a newer owner holds it).
3. Every `retentionEveryNTicks` ticks and when `retentionMaxAgeMs > 0`:
   `deletePublishedBefore(NOW() - retentionMaxAgeMs, retentionBatchSize)`.

- Never mutates job/attempt/lease/DLQ state.
- `backoff(n) = min(maxBackoffMs, baseBackoffMs * 2^n)` — pure, **no jitter** (determinism
  rule), applied via `available_at` (DB-time, no `setTimeout`).
- A timed-out `publish()` may still complete later in the background; the fencing token makes
  its (by-then stale) dispatcher unable to mutate the row — see §12-B.

### 8.3 `toOutboxEnqueueInput(event: ForgeEvent): OutboxEnqueueInput`

Pure helper in **`@forge/events`** (`src/outbox-input.ts`, re-exported from the package index):
generates `id = 'outbox_' + randomUUID()`, copies `event_id/event_type/version/occurred_at`,
lifts correlation ids, sets `payload = event` (the frozen envelope). Producers
`parseForgeEvent(event)` first (or build via `createForgeEvent` + validate in tests). Lives in
`@forge/events` — not `@forge/outbox` — so `apps/worker` / `apps/scheduler` enqueue without
taking any new package dependency.

---

## 9. Retention (adversarial-review fix / §12 Q7)

- `OUTBOX_RETENTION_MAX_AGE_MS` default **7 days** (`604800000`); `0` disables. Rationale in
  docs: incident forensics + consumer catch-up need a window materially longer than 24h.
- `deletePublishedBefore` deletes **only** `status='PUBLISHED' AND published_at < cutoff`, in
  bounded batches (`LIMIT` via `id IN (SELECT … LIMIT n)`). Never `PENDING`/`CLAIMED`/`DEAD`.
- Retention code is **not exercised until** the fencing + conditional-mutation tests (§11 test
  matrix "Race") pass — enforced by ordering the implementation plan, and by a guard test that
  a concurrent reclaim + retention cannot drop a row that a losing dispatcher later touches.
- `DEAD` rows: no auto-deletion in PR 21. Docs: _"DEAD outbox events accumulate and require
  manual investigation/triage; a bounded DEAD-retention job is future work."_

---

## 10. Producer authority (adversarial-review fix / §12 Q4)

| Durable event                                                                  | Sole authoritative producer                                                | Structural guard                                                                                                                                                                                                                                                   |
| ------------------------------------------------------------------------------ | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `JobClaimed`                                                                   | `ForgeScheduler` placement → `claimBatch(..., pendingOutbox)`              | `PgWorkerLeaseRepository.claim` (single-item, worker path) **does not accept** `pendingOutbox`; only `claimBatch` does. Worker `claimJob`→`claim`→`claimBatch` passes no `pendingOutbox`. Regression test: worker `claimJob` path writes **zero** `outbox_events`. |
| `JobStarted`, `JobSucceeded`, `JobFailed`, `JobCancelled`, `JobQueued` (retry) | `apps/worker` `executeJob` `withTransaction` blocks                        | only these two blocks enqueue them; `safePublish` for them removed                                                                                                                                                                                                 |
| `WorkerLost`                                                                   | `ForgeScheduler` recovery → `recoverSingleLease` mapper (`NO_OP` excluded) | `LeaseRecoveryService` enqueues only when the scheduler supplies the mapper                                                                                                                                                                                        |

Documented, not fixed (pre-existing PR 20 semantics; out of scope):

- One physical lease loss yields both `JobFailed{failure_kind:'LEASE_LOST'}` (worker) and
  `WorkerLost{recovery_action:'REQUEUED'}` (scheduler) — different vantage points, both true.
- `LeaseRecoveryService` requeue emits `WorkerLost` but **not** `JobQueued` — queue-depth
  consumers relying on `JobQueued` undercount recovery re-queues.

---

## 11. Delivery classes & ordering disclosure (adversarial-review fix / §12 Q8)

`docs/architecture/events.md` gains an explicit table:

| Event                                                                                              | Delivery class                                                                                |
| -------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `JobClaimed`, `JobStarted`, `JobSucceeded`, `JobFailed`, `JobCancelled`, `JobQueued`, `WorkerLost` | **Durable, at-least-once** (outbox → dispatcher)                                              |
| `JobLogChunk`, `WorkerHeartbeat`                                                                   | **Best-effort** (direct `safePublish`, may be lost on publisher failure / crash / closed bus) |
| `WorkerRegistered`                                                                                 | **Best-effort** (registry path; not transactionally coupled in PR 21)                         |
| `PipelineCreated`, `PipelineQueued`, `PipelineCompleted`, first-enqueue `JobQueued`                | contract-only, no producer                                                                    |

**Ordering caveat (new behaviour, must be documented):** `JobLogChunk` publishes immediately
(best-effort) while terminal lifecycle events now go outbox → dispatcher (delayed by ≥ one poll
interval). Therefore subscribers may observe `JobSucceeded`/`JobFailed` **before** some or all
`JobLogChunk` events for the same job. PR 20 invariant §14.8 ("per-producer sequential
publication guarantees per-job logical order") is updated: it holds **within** a delivery class,
not across the durable/best-effort boundary.

---

## 12. How each adversarial finding is resolved

| #     | Finding                                                                                                | Resolution in this design                                                                                                                                                                                            |
| ----- | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A     | `attempt_count`++ at claim conflated crash / checkpoint-fail / transport-reject                        | §4, §6.3 — split `dispatch_count` (claims, diagnostic) vs `delivery_attempt_count` (real publish attempts only, drives `DEAD`)                                                                                       |
| B     | No fencing; stale dispatcher can mutate a newer owner's row                                            | §4, §5.1, §8.2 — `claim_token` regenerated every (re)claim; **every** mutation fenced `WHERE status='CLAIMED' AND claim_token=$token`; `0 rows ⇒ CLAIM_LOST`; dispatcher discards                                    |
| —     | `markPublished`/`markRetry` could resurrect `PUBLISHED`→`PENDING`                                      | §4.1, §5.1 — conditional `WHERE status='CLAIMED' …`; no code path sets `PENDING` from `PUBLISHED`                                                                                                                    |
| —     | Generic `(txClient)=>Promise<void>` escape hatch                                                       | §6.2 — replaced with pure-data `rowForAcquired` / `outboxRowForRecord` mappers; all SQL stays in `PgOutboxRepository`; repo owns the tx                                                                              |
| C     | Durability contingent on unenforced worker `pool` wiring                                               | §7 — `executeJob` throws when a publisher/outbox is configured without `pool`; new invariant; docs reframed                                                                                                          |
| Q4    | `JobClaimed` single-producer enforced only by convention                                               | §10 — only `claimBatch` (scheduler path) accepts `pendingOutbox`; `claim` (worker path) does not; regression test                                                                                                    |
| Q5/Q6 | Concurrent duplicate delivery beyond crash window; reclaim races live slow dispatcher                  | §8.1 — separate `publishTimeoutMs`; `claimTimeoutMs >= publishTimeoutMs + pollIntervalMs`; §8.2 bounded `publish()` timeout; §12-B fencing makes the stale writer harmless; mandatory slow-publisher race test (§13) |
| Q7    | 24h retention too aggressive; retention + unguarded UPDATE ⇒ silent loss                               | §9 — default 7 days / `0`; retention ships only after fencing tests pass; `DEAD` never auto-deleted                                                                                                                  |
| Q8    | `JobLogChunk` exclusion — undocumented guarantee + ordering inversion                                  | §11 — explicit delivery-class table; §14.8 invariant updated; ordering caveat documented                                                                                                                             |
| Q9    | `@forge/outbox`→`@forge/config` coupling; dispatcher in `apps/scheduler` deviates from target topology | §3, §8.1 — `@forge/outbox` takes `OutboxDispatcherConfig`; zero `apps/scheduler` imports in the package; hosting in `apps/scheduler` is wiring only                                                                  |
| —     | outbox param typed against `@forge/events` on a contract                                               | §5.4, §5.5 — `OutboxEnqueueInput` in zero-dep `@forge/contracts`; `@forge/database` never imports `@forge/events`                                                                                                    |

---

## 13. Test plan (all `[VERIFIED]` = real PostgreSQL via the existing harness)

### `@forge/database`

- `pg-outbox-repository.integration.test.ts`: enqueue; duplicate `event_id` → `ConstraintViolationError`;
  oversize payload → `OutboxPayloadError` (inside a tx → rolls back a co-write); claimable selection
  (PENDING+due, and stale CLAIMED); `claimBatch` sets token/`dispatch_count`, not
  `delivery_attempt_count`; `markPublished` OK + `CLAIM_LOST` on wrong token; `markRetry` →
  PENDING+backoff+`delivery_attempt_count++`; `markRetry{exhausted}` → DEAD; ordering
  (`occurred_at, id`); `deletePublishedBefore` bounds + never touches non-PUBLISHED; `stats`.
- `outbox-transaction.integration.test.ts`: `withTransaction` doing `jobs.save` + `outbox.enqueue`
  → COMMIT (both rows present) / callback throws after enqueue → ROLLBACK (neither present) /
  oversize payload → ROLLBACK (job row not persisted).
- `pg-worker-lease-repository` extension: `claimBatch` with `pendingOutbox` → lease row + JobClaimed
  row co-committed; forced outbox failure → **both** rolled back (no lease granted); idempotent
  re-claim (`isIdempotent`) enqueues nothing.
- `lease-recovery-service` extension: `REQUEUED`/`DEAD_LETTERED` → `WorkerLost` row co-committed
  with jobs/`dead_letter_jobs`; `NO_OP` → no outbox row; forced outbox failure → recovery tx rolls back.

### `@forge/outbox`

- `dispatcher.test.ts` (fake `EventPublisher`, fake or real repo): empty / one / many / batch
  limit; publisher success → PUBLISHED; publisher throws → PENDING + backoff + `last_error`;
  publisher hangs > `publishTimeoutMs` → treated as failed; retry exhaustion → DEAD (retains
  payload/attempts/last_error/timestamps); `start`/`stop` idempotent, no tick after `stop`;
  `runOnce` summary.
- `dispatcher-concurrency.integration.test.ts`: 2 / 5 / 10 `OutboxDispatcher` on one PG, N events
  → every event PUBLISHED (no loss); no permanent CLAIMED after a quiescent period; transport
  dedupe by `event_id` shows only the documented at-least-once window; no two dispatchers hold
  the same row concurrently (token check); no invalid status transition.
- `slow-publisher-fencing.race.test.ts` **(mandatory)**: A claims (token T1); A's publish sleeps
  > `claimTimeoutMs`; B reclaims (token T2), publishes, `markPublished(T2)`; A wakes,
  > `markPublished(id, T1)` → `CLAIM_LOST`; A performs no further mutation; row stays `PUBLISHED`;
  > **assert no `PUBLISHED → PENDING`**.
- `at-least-once.experiment.test.ts` **(mandatory)**: A publishes OK, A "crashes" before
  `markPublished`; B reclaims, publishes again; assert transport observed the **same `event_id`
  twice** and this is expected (not a failure); `delivery_attempt_count` reflects real attempts.
- `publisher-failure.test.ts`: `publish` throws → `delivery_attempt_count++`, retry scheduled,
  `last_error` stored, ownership cleared, row retryable.
- `retention.integration.test.ts`: only eligible PUBLISHED deleted; PENDING/CLAIMED/DEAD retained;
  bounded batch; concurrent dispatch safe.

### Producers

- `apps/worker` integration: after `executeJob` (with `pool`), `outbox_events` holds `JobStarted`
  - the correct terminal event (+ `JobQueued` on retry), co-committed with `jobs`/`job_attempts`;
    rollback test; **`executeJob` throws when `eventPublisher` set without `pool`** (§7).
- `apps/scheduler` integration: placement → lease row + `JobClaimed` row co-committed; recovery
  sweep → `WorkerLost` rows co-committed; worker `claimJob` path → **zero** `JobClaimed` rows.

### Regression

- All `packages/events` PR 20 tests unchanged and green.
- `apps/worker`, `apps/scheduler`, `packages/executor`, `packages/database` existing suites green
  (lease recovery, worker loss, retry, backoff, DLQ, fairness, priority, resource matching,
  Docker execution, cleanup).

### Failure scenarios explicitly asserted + documented (prompt §45)

PG unavailable → state + outbox fail together; outbox INSERT failure → rollback; publisher
unavailable → retryable; publisher timeout → retryable or DEAD per attempt count; dispatcher
crash after claim → reclaimable, **no delivery budget burned**; dispatcher crash after publish →
duplicate expected; stale dispatcher after reclaim → fenced; retention → only eligible PUBLISHED;
DEAD → retained.

---

## 14. Benchmarks (`benchmarks/outbox/`, `npm run benchmark:outbox` → `npx tsx benchmarks/outbox/runner.ts`)

Mirror `benchmarks/scheduler/` conventions (Mulberry32 PRNG from `utils/prng.ts`, `utils/reporter.ts`,
deterministic fixtures, no `Math.random()` / wall-clock seeds, report JSON in `benchmarks/reports/`).

- **A — transactional overhead**: `withTransaction` doing `jobs.save`+`jobAttempts.save`
  **without** vs **with** `outbox.enqueue`; realistic batch sizes; record mean / median / P95 / P99.
- **B — dispatcher throughput**: 1 / 10 / 50 / 100 / 500 events; events/sec, batch latency,
  publish latency, PG activity, memory. Fake in-process publisher.
- **C — claim query `EXPLAIN (ANALYZE, BUFFERS)`**: capture actual plan + timing for the
  `claimBatch` SELECT against a seeded table; save output; interpret (expect
  `idx_outbox_events_claimable` partial-index scan).

No production-capacity claims from local numbers.

---

## 15. Documentation (`[VERIFIED]` files exist)

- `docs/architecture/events.md`: new sections — Durable Publication (state+outbox = one tx),
  Delivery (outbox → dispatcher → EventPublisher), Guarantees / Non-guarantees, Fencing (why
  `claim_token`), Retry accounting (claim recovery ≠ delivery attempt), Delivery classes table
  (§11), Ordering caveat (§11), Retention + DEAD policy, residual failure window (external
  publish OK → `markPublished` fails → duplicate possible).
- `docs/architecture/invariants.md` §14: revise §14.2 (Persist-Before-Publish → transactional
  outbox + at-least-once + residual duplicate window) and §14.10 (outbox now exists); add:
  claim-lease ≠ job-lease, fencing mandatory, `claimed_by` diagnostic-only, terminal `DEAD` is
  "attempts exhausted without confirmed success" (not "delivered but unrecorded"), production
  worker must persist via `withTransaction`, retention conservative + bounded, update §14.8
  ordering scope.
- `docs/architecture/overview.md`: PR 21 entry.
- `CLAUDE.md`: migrations `001`–**`008`**; new `packages/outbox`; `tx.outbox`; `benchmark:outbox`;
  status line.
- `README.md`: PR 21 line; keep "no exactly-once" language.
- `.env.example`: add the 11 `OUTBOX_*` vars with comments.

---

## 16. Config additions (`packages/config` + `AppConfig` mirror)

| env var                            | default     | validation           |
| ---------------------------------- | ----------- | -------------------- |
| `OUTBOX_DISPATCH_POLL_INTERVAL_MS` | `1000`      | int ≥ 100            |
| `OUTBOX_DISPATCH_BATCH_SIZE`       | `100`       | int 1–1000           |
| `OUTBOX_CLAIM_TIMEOUT_MS`          | `60000`     | int ≥ 1000           |
| `OUTBOX_PUBLISH_TIMEOUT_MS`        | `10000`     | int ≥ 500            |
| `OUTBOX_MAX_DELIVERY_ATTEMPTS`     | `10`        | int 1–100            |
| `OUTBOX_DELIVERY_BASE_BACKOFF_MS`  | `500`       | int ≥ 50             |
| `OUTBOX_DELIVERY_MAX_BACKOFF_MS`   | `60000`     | int ≥ 1000           |
| `OUTBOX_MAX_PAYLOAD_BYTES`         | `65536`     | int ≥ 1024           |
| `OUTBOX_RETENTION_MAX_AGE_MS`      | `604800000` | int ≥ 0 (0 disables) |
| `OUTBOX_RETENTION_BATCH_SIZE`      | `500`       | int ≥ 1              |
| `OUTBOX_RETENTION_EVERY_N_TICKS`   | `60`        | int ≥ 1              |

Refinements: `OUTBOX_CLAIM_TIMEOUT_MS >= OUTBOX_PUBLISH_TIMEOUT_MS + OUTBOX_DISPATCH_POLL_INTERVAL_MS`;
`OUTBOX_DELIVERY_MAX_BACKOFF_MS >= OUTBOX_DELIVERY_BASE_BACKOFF_MS`.

---

## 17. File manifest

**New**

- `packages/database/src/migrations/migrator.ts` — `OUTBOX_EVENTS_SQL` + `MIGRATIONS` entry (edit)
- `packages/database/src/migrations/sql/008_outbox_events.sql`
- `packages/database/src/repositories/contracts/outbox-repository.contract.ts`
- `packages/database/src/repositories/pg-outbox-repository.ts`
- `packages/database/src/repositories/pg-outbox-repository.integration.test.ts`
- `packages/database/src/outbox-transaction.integration.test.ts`
- `packages/events/src/outbox-input.ts` (+ export from `packages/events/src/index.ts`) +
  `packages/events/src/outbox-input.test.ts`
- `packages/outbox/` — `package.json`, `tsconfig.json`, `src/index.ts`, `src/dispatcher.ts`,
  `src/backoff.ts`, `src/retention.ts`, `src/errors.ts`,
  `src/dispatcher.test.ts`, `src/dispatcher-concurrency.integration.test.ts`,
  `src/slow-publisher-fencing.race.test.ts`, `src/at-least-once.experiment.test.ts`,
  `src/publisher-failure.test.ts`, `src/retention.integration.test.ts`
- `benchmarks/outbox/` — `runner.ts`, `config.ts`, `explain.ts`, `suites/overhead.bench.ts`,
  `suites/throughput.bench.ts`, `utils/fixtures.ts`

**Changed**

- `packages/contracts/src/index.ts` — `OutboxStatus`, `OutboxEnqueueInput`, `OutboxEventRecord`,
  `DEFAULT_OUTBOX_*`; `BatchClaimOptions.pendingOutbox?`
- `packages/database/src/transaction.ts` — `outbox` on `TransactionContext`
- `packages/database/src/types.ts` — `OutboxEventRow`
- `packages/database/src/index.ts` — exports
- `packages/database/src/errors.ts` — `OutboxPayloadError`
- `packages/database/src/repositories/pg-worker-lease-repository.ts` — `pendingOutbox` in `claimBatch`
- `packages/database/src/repositories/contracts/worker-lease-repository.contract.ts` — doc + type
- `packages/database/src/lease-recovery-service.ts` — `outboxRowForRecord` mapper param
- `packages/config/src/index.ts` — 11 `OUTBOX_*` vars + refinements
- `apps/worker/src/index.ts` — in-tx enqueues; drop `safePublish` for the 5 lifecycle events;
  `pool`-required guard (§7). No new package dep (`@forge/events` + `@forge/database` already deps).
- `apps/scheduler/src/scheduler.ts` — `JobClaimed`/`WorkerLost` via mappers; drop `safePublish`
- `apps/scheduler/src/index.ts` — construct + `start`/`stop` `OutboxDispatcher`
- `apps/scheduler/tsconfig.json` / `package.json` — dep on `@forge/outbox` (dispatcher wiring only)
- `tsconfig.json` (root refs), root `package.json` (`benchmark:outbox`)
- `.env.example`, `README.md`, `docs/architecture/{events,invariants,overview}.md`, `CLAUDE.md`

---

## 18. Out of scope (explicit)

WebSockets / browser delivery; Kafka / NATS / Redis Streams / RabbitMQ; event sourcing; event
history/replay service; Prometheus exporter / distributed tracing; `LISTEN`/`NOTIFY` wake-up
(polling is the only correctness path); making `JobLogChunk` / `WorkerHeartbeat` durable;
`WorkerRegistered` transactional coupling; bounded `DEAD` retention; any scheduler/executor
change not required for the above.

---

## 19. Definition of Done

Per prompt §47 — all boxes. Quality gates run **in the worktree**:
`npm run format:check`, `npm run lint`, `npm run typecheck`, `npm test`
(Postgres + Redis + Docker up), `npm run build`, `npm run benchmark:outbox`. Final report per
prompt §48, ending `READY TO MERGE` or `NOT READY — <reason>`.
