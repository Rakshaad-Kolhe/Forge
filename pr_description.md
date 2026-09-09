# PR 21: Durable Event Transport & PostgreSQL Transactional Outbox

Branch `feat/pr-21-outbox`, cut from `42cdf33` (PR 20 tip). 26 commits
(`git log --oneline 42cdf33..HEAD`): 1 spec, 1 plan, 1 SDD-doc format pass, 20 feature/test
commits, 3 fix rounds, 1 docs commit, 1 pre-gate `chore` commit (this task).

**Guarantee established:** a lifecycle event associated with a PostgreSQL `jobs` /
`worker_leases` state transition is durably recorded in the **same transaction** as that
transition; a dispatcher then delivers it. In the delivery-semantics wording used verbatim in
the docs:

> Forge durably records events transactionally with PostgreSQL state and delivers them at least
> once; consumers must tolerate duplicates.

**Not claimed:** exactly-once delivery, global ordering, or atomic DB↔external-transport commit.

---

## 1. Repository Inspection

Findings that shaped the design (all `[VERIFIED]` against the PR-20 tree):

- **`packages/events/`** — `ForgeEvent` is a 13-variant discriminated union; every producer wired
  in PR 20 publishes best-effort via `safePublish` **after** the domain commit
  (`InProcessEventBus.publish` awaits subscribers sequentially, no timeout). That post-commit
  `safePublish` is the failure window this PR closes.
- **`packages/database/`** — `withTransaction(pool, cb)` hands the callback a `TransactionContext`
  with every repo bound to one `pg.PoolClient` (`BEGIN → cb → COMMIT`, `ROLLBACK` on throw).
  Migrations run from **inline `*_SQL` string constants** in `migrator.ts` `MIGRATIONS[]`
  (`001`–`007`); the `src/migrations/sql/*.sql` files are drifted, unused copies.
  `PgWorkerLeaseRepository.claimBatch` already runs a real `BEGIN/COMMIT/ROLLBACK` when
  constructed on a pool (the scheduler path), so a lease INSERT and any co-inserted rows commit
  or roll back together. `LeaseRecoveryService.recoverSingleLease` already runs inside a
  `withTransaction`. `dead_letter_jobs` is a job-failure domain table (FKs, job-failure `reason`
  vocabulary) — not reusable for event-delivery failure.
- **`apps/worker/src/index.ts`** — `executeJob` already persists RUNNING and terminal state via
  `withTransaction` when `options.pool` is set; a non-transactional `options.jobRepository`
  fallback path also exists (no transaction, no outbox). The production entrypoint calls
  `startWorker()` with no args (shell only) — the durability guarantee is contingent on the
  future worker loop passing `pool`, which nothing enforced.
- **`apps/scheduler/`** — `publishJobClaimed` / `publishWorkerLostForRecovery` were `safePublish`;
  `startRecoveryLoop` (`setInterval` + `timer.unref()` + `isSweeping` reentrancy guard) is the
  pattern mirrored for the dispatcher. `startScheduler()` is still a PR-01 log-only shell.
- **`packages/config` / `packages/contracts`** — one big zod `configSchema` with the
  `z.string().regex(/^\d+$/).default(...).transform(Number).pipe(...)` idiom + cross-field
  `.refine`; `@forge/contracts` is a single zero-runtime-dep file. `DockerExecutor` takes plain
  numeric options, not `@forge/config` — the precedent for keeping library packages
  config-loader-free.

## 2. Revised Design (adversarial review)

The first PR-21 proposal was rewritten after an adversarial architecture review. The nine
findings and how the shipped design resolves each:

| #     | Finding                                                                                                                | Resolution                                                                                                                                                                                              |
| ----- | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A     | A single `attempt_count`++ at claim conflated process crash, checkpoint-write failure, and genuine transport rejection | Split into `dispatch_count` (claims + reclaims; diagnostic; never drives `DEAD`) and `delivery_attempt_count` (incremented **only** in `markRetry`, after a real `publish` threw/timed out)             |
| B     | No fencing — a stale dispatcher could mutate a row a newer owner already reclaimed                                     | `claim_token` regenerated on every (re)claim; **every** mutating statement carries `WHERE id = $1 AND status = 'CLAIMED' AND claim_token = $2`; 0 rows ⇒ `CLAIM_LOST`, dispatcher discards              |
| —     | `markPublished` / `markRetry` could resurrect `PUBLISHED → PENDING`                                                    | Structural: the fenced `WHERE status = 'CLAIMED'` on every mutation means no code path can set `PENDING` from `PUBLISHED`                                                                               |
| —     | A generic `(txClient) => Promise<void>` escape hatch on `claimBatch`                                                   | Replaced with pure-data mappers (`rowForAcquired`, `outboxRowForRecord`) that return an `OutboxEnqueueInput` and run no SQL; all SQL stays in `PgOutboxRepository`, which owns the tx                   |
| C     | Durability contingent on unenforced worker `pool` wiring                                                               | `executeJob` throws at entry when `eventPublisher` is configured without `pool`; new invariant §14.15; docs reframe the no-pool path as a durability hole, not a "degraded mode"                        |
| Q4    | `JobClaimed` single-producer enforced only by convention                                                               | Only `claimBatch` (scheduler path) accepts `pendingOutbox`; the single-item `claim` (worker path) does not; regression test asserts the worker `claimJob` path writes zero `JobClaimed` rows            |
| Q5/Q6 | Concurrent duplicate delivery beyond the crash window; a reclaim racing a slow live dispatcher                         | Separate `publishTimeoutMs`; refinement `claimTimeoutMs >= publishTimeoutMs + pollIntervalMs`; bounded `publish()` timeout; fencing makes the stale writer harmless; mandatory slow-publisher race test |
| Q7    | 24 h retention too aggressive; retention + an unguarded UPDATE ⇒ silent loss                                           | Default 7 days (`0` disables); retention deletes only `PUBLISHED` rows under `FOR UPDATE SKIP LOCKED`; ships behind the fencing tests; `DEAD` never auto-deleted                                        |
| Q8    | `JobLogChunk` outbox exclusion was an undocumented guarantee + an ordering inversion                                   | Explicit delivery-class table in `events.md`; invariant §14.8 rescoped to "within a delivery class"; ordering caveat documented                                                                         |
| Q9    | `@forge/outbox → @forge/config` coupling; dispatcher hosted in `apps/scheduler`                                        | `@forge/outbox` takes a plain `OutboxDispatcherConfig`; zero `apps/scheduler` / `apps/*` imports in the package; hosting in `apps/scheduler` is wiring only, relocatable                                |
| —     | An outbox param typed against `@forge/events` on a contract                                                            | `OutboxEnqueueInput` lives in zero-dep `@forge/contracts` as plain data; `@forge/database` never imports `@forge/events`                                                                                |

## 3. Transactional Proof

State transition and its outbox row commit or roll back as one unit — asserted on real
PostgreSQL:

- **`packages/database/src/outbox-transaction.integration.test.ts`** (3):
  `commits the job row and the outbox row together`;
  `rolls back the job row when the callback throws after enqueue` (neither row present);
  `rolls back the job row when the outbox payload is oversized` (the `OutboxPayloadError` from
  `tx.outbox.enqueue` unwinds the co-written `jobs.save`).
- **`packages/database/src/repositories/pg-worker-lease-repository.integration.test.ts`** (4):
  `co-commits a JobClaimed outbox row with a fresh lease`;
  `does not enqueue for an idempotent re-claim`;
  `rolls back the lease when the outbox enqueue fails` (no lease granted **and** no outbox row);
  `the single-item claim() path writes no outbox row`.
- **`packages/database/src/repositories/lease-recovery.integration.test.ts`** (3 new, +5 PR-20
  unchanged): `co-commits a WorkerLost row when a lease is REQUEUED`;
  `emits no outbox row for a NO_OP reconciliation`;
  `rolls the recovery transaction back when the outbox enqueue fails` (job / `dead_letter_jobs`
  writes unwound too).
- **`packages/outbox/src/dispatcher.test.ts`** — `reconstructs the event from the stored payload
(not from job state)` confirms the recovery path never reads live job rows.

## 4. Fencing Proof

**`packages/outbox/src/slow-publisher-fencing.race.test.ts`** — 1 test,
`a stale dispatcher cannot mutate a row a newer owner reclaimed; no PUBLISHED→PENDING`. On real
PG: dispatcher A claims (token `T1`); A's `publish` sleeps past `claimTimeoutMs`; dispatcher B
reclaims (token `T2`), publishes, `markPublished(T2)` → `OK`; A wakes and calls
`markPublished(id, T1)` → **`CLAIM_LOST`**; A performs no further mutation; the row stays
`PUBLISHED`. Guard assertions rule out a false positive (B's summary shows `published === 1`,
the final status is `PUBLISHED`, never `PENDING`). **Result: PASS.**

Fencing is also exercised by `dispatcher.test.ts` (`on markPublished CLAIM_LOST → counts
claimLost, does not touch the row`), `pg-outbox-repository.integration.test.ts`
(`markPublished with a stale token → CLAIM_LOST and no mutation`,
`markRetry with a stale token → CLAIM_LOST, no PUBLISHED→PENDING resurrection`), and the
N-dispatcher concurrency test below.

## 5. Retry Semantics

Two counters, deliberately distinct (`outbox_events` columns, invariant §14.14):

- **`dispatch_count`** — incremented on every claim and reclaim in `claimBatch`. Diagnostic
  only. Never drives `DEAD`. A dispatcher that crashes after claiming but before publishing
  costs one `dispatch_count` and **zero** delivery budget.
- **`delivery_attempt_count`** — incremented **only** in `markRetry`, i.e. only after
  `publisher.publish(event)` was actually invoked and threw or timed out. A successful
  re-publish (the intended duplicate after a crash-before-`markPublished`) consumes no budget.

`DEAD` iff `deliveryAttemptCount + 1 >= OUTBOX_MAX_DELIVERY_ATTEMPTS` at a **genuine** publish
failure. `DEAD` means "delivery attempts exhausted without a confirmed success" — not "delivered
but unrecorded", and never "a process died". A `DEAD` row retains its full payload,
`last_error`, attempt counters, and ownership columns for triage.

Asserted by `pg-outbox-repository.integration.test.ts`
(`claimBatch ... bumps dispatch_count only`,
`markRetry(exhausted=false) → PENDING, backoff, delivery_attempt_count++`,
`markRetry(exhausted=true) → DEAD, retains payload + last_error + attempts`),
`dispatcher.test.ts` (`transitions to DEAD after maxDeliveryAttempts genuine failures`,
`counts reclaimed rows (dispatchCount >= 2)`), and `publisher-failure.test.ts`.

## 6. Producer Authority

Each durable event has exactly one authoritative producer (invariant §14.6, spec §10):

| Durable event                                                                  | Sole authoritative producer                                                | Structural guard                                                                                                               |
| ------------------------------------------------------------------------------ | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `JobClaimed`                                                                   | `ForgeScheduler` placement → `claimBatch(..., pendingOutbox)`              | the single-item `PgWorkerLeaseRepository.claim` (worker path) does not accept `pendingOutbox`; only `claimBatch` does          |
| `JobStarted`, `JobSucceeded`, `JobFailed`, `JobCancelled`, `JobQueued` (retry) | `apps/worker` `executeJob` `withTransaction` blocks                        | only the RUNNING-commit and terminal-commit blocks enqueue them; the PR-20 best-effort `safePublish` for these five is removed |
| `WorkerLost`                                                                   | `ForgeScheduler` recovery → `recoverSingleLease` mapper (`NO_OP` excluded) | `LeaseRecoveryService` enqueues only when the scheduler supplies the mapper                                                    |

The single-producer boundary for `JobClaimed` is proven by
**`apps/worker/src/worker-execution.integration.test.ts`** →
`writes zero JobClaimed rows to outbox_events on the worker claimJob path (scheduler is the sole
producer)`, and by **`apps/scheduler/src/scheduler.integration.test.ts`** →
`does not call eventPublisher.publish for JobClaimed when the outbox is enabled` (no
double-emit).

Documented, not fixed (pre-existing PR-20 semantics, out of scope): one physical lease loss
yields both `JobFailed{failure_kind:'LEASE_LOST'}` (worker) and
`WorkerLost{recovery_action:'REQUEUED'}` (scheduler); `LeaseRecoveryService` requeue emits
`WorkerLost` but not `JobQueued`.

## 7. Failure Model

Every scenario from spec §13, with the asserting test:

| Scenario                                                   | Behaviour                                                                                         | Asserting test                                                                                                       |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL unavailable                                     | state commit and outbox INSERT fail together (one tx)                                             | `outbox-transaction.integration.test.ts` (rollback cases)                                                            |
| Outbox INSERT failure (oversize / invalid payload)         | whole transaction rolls back; domain row not persisted                                            | `outbox-transaction.integration.test.ts` → oversize case; `pg-worker-lease-repository` rollback case                 |
| Publisher unavailable (`publish` throws)                   | row → `PENDING`, `delivery_attempt_count++`, backoff, `last_error`, ownership cleared             | `publisher-failure.test.ts`; `dispatcher.test.ts` → publisher-throw case                                             |
| Publisher timeout (`publish` hangs > `publishTimeoutMs`)   | treated as a failed attempt → retry or `DEAD` per attempt count                                   | `dispatcher.test.ts` → `on publisher hang beyond publishTimeoutMs → treated as failed`                               |
| Dispatcher crash **after claim**, before publish           | row reclaimable once `claimed_at < staleClaimBefore`; **no delivery budget burned**               | `pg-outbox-repository.integration.test.ts` → stale-CLAIMED reclaim case; `dispatcher.test.ts` → reclaimed-count case |
| Dispatcher crash **after publish**, before `markPublished` | next dispatcher reclaims and re-publishes the **same `event_id`** — expected                      | `at-least-once.experiment.test.ts`                                                                                   |
| Stale dispatcher wakes after a reclaim                     | fenced out — `CLAIM_LOST`, no mutation, no `PUBLISHED → PENDING`                                  | `slow-publisher-fencing.race.test.ts`                                                                                |
| N dispatchers on one PG                                    | every event `PUBLISHED` at least once; no permanent `CLAIMED`; no two hold a row at once          | `dispatcher-concurrency.integration.test.ts` (`n` = 2, 5, 10)                                                        |
| Retention sweep                                            | deletes only aged `PUBLISHED`; `PENDING` / `CLAIMED` / `DEAD` untouched; bounded; concurrent-safe | `retention.integration.test.ts` (3)                                                                                  |
| `DEAD` row                                                 | retained in full, no auto-deletion                                                                | `pg-outbox-repository.integration.test.ts` → `markRetry(exhausted=true)` case                                        |

## 8. Performance

From `benchmarks/reports/outbox-benchmark-report.json` (regenerated this task; PG 18.6 on
WSL2, Node v25.2.1, i7-14650HX, seed `909090`). Local numbers — **no production-capacity claim**.

**A — transactional overhead** (`withTransaction` doing `jobs.save` + `jobAttempts.save`,
2000 measured iterations per phase, all committed):

| Phase                                   | Mean (ms) | Median | P95    | P99     | Ops/sec |
| --------------------------------------- | --------- | ------ | ------ | ------- | ------- |
| baseline (jobs.save + jobAttempts.save) | 6.6710    | 6.6721 | 8.7220 | 10.9906 | 148.9   |
| with-outbox (+ `outbox.enqueue`)        | 7.5646    | 7.7496 | 9.7070 | 10.9871 | 131.5   |

**Mean added cost of the co-committed `outbox.enqueue`: ≈ +0.5–0.9 ms/event** (this run:
`overheadDeltaMs` = **+0.8936 ms**; the Task-18 reviewed run measured +0.52 ms — both are within
the ~1.7 ms tx-noise stdev). A single extra fenced-column INSERT on the existing client.

**B — dispatcher throughput** (`OutboxDispatcher.runOnce` → subscriber-less `InProcessEventBus`,
20 iterations per batch size):

| Batch (events) | Events/sec | Mean batch (ms) | Mean publish (ms) |
| -------------- | ---------- | --------------- | ----------------- |
| 1              | 118.4      | 8.44            | 0.008             |
| 10             | 214.8      | 46.55           | 0.008             |
| 50             | 245.6      | 203.61          | 0.007             |
| 100            | 240.7      | 415.40          | 0.006             |
| 500            | 249.5      | 2004.13         | 0.008             |

Single-event ticks are claim/round-trip-bound (~90–120 events/s); batching amortises the claim
round-trip and plateaus at **~210–260 events/s** against a no-op subscriber. Publish latency is
negligible here — a real transport is the real ceiling.

**C — claim-query `EXPLAIN (ANALYZE, BUFFERS)`** — see §9.

## 9. EXPLAIN

Actual plan for the `claimBatch` claimable SELECT against a table seeded with 5000 `PENDING`
rows:

```text
Limit  (cost=1837.60..1838.85 rows=100 width=32) (actual time=2.187..2.257 rows=100.00 loops=1)
  Buffers: shared hit=1634
  ->  LockRows  (cost=1837.60..1900.10 rows=5000 width=32) (actual time=2.186..2.250 rows=100.00 loops=1)
        Buffers: shared hit=1634
        ->  Sort  (cost=1837.60..1850.10 rows=5000 width=32) (actual time=2.180..2.189 rows=100.00 loops=1)
              Sort Key: occurred_at, id
              Sort Method: quicksort  Memory: 466kB
              Buffers: shared hit=1534
              ->  Seq Scan on outbox_events  (cost=0.00..1646.50 rows=5000 width=32) (actual time=0.135..1.280 rows=5000.00 loops=1)
                    Filter: ((((status)::text = 'PENDING'::text) AND (available_at <= now())) OR (((status)::text = 'CLAIMED'::text) AND (claimed_at < '2026-01-01 00:00:00.001+00'::timestamp with time zone)))
                    Buffers: shared hit=1534
Planning Time: 0.180 ms
Execution Time: 2.285 ms
```

**Interpretation:** the partial index `idx_outbox_events_claimable` (`WHERE status = 'PENDING'`)
is **not** used. The claim predicate is a disjunction —
`(status = 'PENDING' AND available_at <= NOW()) OR (status = 'CLAIMED' AND claimed_at < $stale)`
— and the `CLAIMED` arm is outside every partial index's `WHERE`, so the planner falls back to a
`Seq Scan` + `quicksort` on `(occurred_at, id)`. At 5000 rows this is `hit=1534` shared buffers,
exec **2.3 ms** — acceptable for the current scale, `O(table)` as the table grows.
See Known Limitations (a) for the follow-up.

## 10. Tests

`npm test` (`vitest run`, `fileParallelism: false`, full repo incl. every live PG / Redis /
Docker-over-TCP integration test): **64 files, 645 tests, all passing**, ~100 s.

New / changed test files for PR 21:

| Package / app      | File                                                              | Tests                     |
| ------------------ | ----------------------------------------------------------------- | ------------------------- |
| `@forge/contracts` | (type-only — none)                                                | —                         |
| `@forge/config`    | `src/index.test.ts` (edit)                                        | 20 (11 new outbox cases)  |
| `@forge/events`    | `src/outbox-input.test.ts`                                        | 3                         |
| `@forge/database`  | `src/migrations/migrator.test.ts` (edit)                          | 4                         |
| `@forge/database`  | `src/repositories/pg-outbox-repository.integration.test.ts`       | 20                        |
| `@forge/database`  | `src/outbox-transaction.integration.test.ts`                      | 3                         |
| `@forge/database`  | `src/repositories/pg-worker-lease-repository.integration.test.ts` | 4                         |
| `@forge/database`  | `src/repositories/lease-recovery.integration.test.ts` (edit)      | 8 (3 new)                 |
| `@forge/outbox`    | `src/backoff.test.ts`                                             | 4                         |
| `@forge/outbox`    | `src/dispatcher.test.ts`                                          | 9                         |
| `@forge/outbox`    | `src/dispatcher-concurrency.integration.test.ts`                  | 3 (n = 2, 5, 10)          |
| `@forge/outbox`    | `src/slow-publisher-fencing.race.test.ts` (mandatory)             | 1                         |
| `@forge/outbox`    | `src/at-least-once.experiment.test.ts` (mandatory)                | 1                         |
| `@forge/outbox`    | `src/publisher-failure.test.ts`                                   | 1                         |
| `@forge/outbox`    | `src/retention.integration.test.ts`                               | 3                         |
| `apps/worker`      | `src/worker-execution.integration.test.ts` (edit)                 | 12 (7 new durable-outbox) |
| `apps/worker`      | `src/index.test.ts` (edit)                                        | 23                        |
| `apps/scheduler`   | `src/scheduler.integration.test.ts` (edit)                        | 9 (3 new outbox)          |
| `apps/scheduler`   | `src/scheduler.test.ts` (edit)                                    | 42                        |

All PR-20 `packages/events` tests and the existing `apps/worker` / `apps/scheduler` /
`packages/executor` / `packages/database` suites (lease recovery, worker loss, retry, backoff,
DLQ, fairness, priority, Docker execution) remain green.

## 11. Quality Gates

Run in the worktree. Postgres + Redis on `127.0.0.1` (WSL2), Docker over TCP
`tcp://172.31.91.254:2375`. `docker compose` not used (Docker Desktop npipe is down); the
integration tests connect to the running services directly.

| Gate             | Command                    | Result                                                              |
| ---------------- | -------------------------- | ------------------------------------------------------------------- |
| Format           | `npm run format:check`     | **PASS** (green after the pre-gate `.prettierignore` change — §12)  |
| Lint             | `npm run lint`             | **PASS** (`eslint .`, exit 0; `no-explicit-any: error` clean)       |
| Typecheck        | `npm run typecheck`        | **PASS** (`tsc -b`, exit 0)                                         |
| Test             | `npm test`                 | **PASS** — 64 files / 645 tests, 0 failures                         |
| Build            | `npm run build`            | **PASS** (all 15 workspaces incl. `@forge/outbox` and `next build`) |
| Outbox benchmark | `npm run benchmark:outbox` | **PASS** (exit 0; report regenerated — numbers in §8)               |

No test failures, no skips, no `.only`. No pre-existing failures encountered.

## 12. Changed Files

`git diff --stat 42cdf33..HEAD` (63 files, +10 246 / −415; the two large docs are the SDD
spec + plan):

```text
 .env.example                                       |   22 +
 .prettierignore                                    |    9 +
 README.md                                          |   14 +-
 apps/scheduler/package.json                        |    1 +
 apps/scheduler/src/index.ts                        |   42 +-
 apps/scheduler/src/scheduler.integration.test.ts   |  136 +-
 apps/scheduler/src/scheduler.test.ts               |  154 +-
 apps/scheduler/src/scheduler.ts                    |  255 +-
 apps/scheduler/tsconfig.json                       |    4 +-
 apps/worker/src/index.test.ts                      |  209 +-
 apps/worker/src/index.ts                           |  300 +-
 apps/worker/src/worker-execution.integration.test.ts |  376 +-
 benchmarks/outbox/config.ts                        |   47 +
 benchmarks/outbox/explain.ts                       |  138 +
 benchmarks/outbox/runner.ts                        |  252 ++
 benchmarks/outbox/suites/overhead.bench.ts         |   87 +
 benchmarks/outbox/suites/throughput.bench.ts       |  173 +
 benchmarks/outbox/utils/fixtures.ts                |  147 +
 benchmarks/reports/outbox-benchmark-report.json    |  301 ++
 docs/architecture/events.md                        |  245 +-
 docs/architecture/invariants.md                    |   13 +-
 docs/architecture/overview.md                      |    5 +-
 docs/superpowers/plans/2026-09-08-pr21-outbox.md   | 3934 ++++++++++
 docs/superpowers/specs/2026-09-08-pr21-outbox-design.md |  829 +++
 package-lock.json                                  |   18 +
 package.json                                       |    3 +-
 packages/config/src/index.test.ts                  |   76 +
 packages/config/src/index.ts                       |  132 +
 packages/contracts/src/index.ts                    |  145 +
 packages/database/src/errors.ts                    |   12 +
 packages/database/src/index.ts                     |   20 +-
 packages/database/src/lease-recovery-service.ts    |   43 +-
 packages/database/src/migrations/migrator.test.ts  |   42 +-
 packages/database/src/migrations/migrator.ts       |   40 +
 packages/database/src/migrations/sql/008_outbox_events.sql |   34 +
 packages/database/src/outbox-transaction.integration.test.ts |  126 +
 packages/database/src/repositories/contracts/outbox-repository.contract.ts |  103 +
 packages/database/src/repositories/contracts/worker-lease-repository.contract.ts |    4 +
 packages/database/src/repositories/lease-recovery.integration.test.ts |  125 +-
 packages/database/src/repositories/pg-outbox-repository.integration.test.ts |  368 ++
 packages/database/src/repositories/pg-outbox-repository.ts |  318 ++
 packages/database/src/repositories/pg-worker-lease-repository.integration.test.ts |  168 +
 packages/database/src/repositories/pg-worker-lease-repository.ts |   23 +
 packages/database/src/transaction.ts               |    9 +
 packages/database/src/types.ts                     |   27 +
 packages/events/src/index.ts                       |    1 +
 packages/events/src/outbox-input.test.ts           |   51 +
 packages/events/src/outbox-input.ts                |   29 +
 packages/outbox/package.json                       |   26 +
 packages/outbox/src/at-least-once.experiment.test.ts |   94 +
 packages/outbox/src/backoff.test.ts                |   22 +
 packages/outbox/src/backoff.ts                     |   14 +
 packages/outbox/src/dispatcher-concurrency.integration.test.ts |   91 +
 packages/outbox/src/dispatcher.test.ts             |  138 +
 packages/outbox/src/dispatcher.ts                  |  246 ++
 packages/outbox/src/errors.ts                      |   13 +
 packages/outbox/src/index.ts                       |   11 +
 packages/outbox/src/publisher-failure.test.ts      |   31 +
 packages/outbox/src/retention.integration.test.ts  |  139 +
 packages/outbox/src/slow-publisher-fencing.race.test.ts |   85 +
 packages/outbox/src/test-support.ts                |  126 +
 packages/outbox/tsconfig.json                      |   14 +
 tsconfig.json                                      |    1 +
```

## 13. Dependencies

- **No new external npm dependency.** `@forge/outbox` reuses `pg` (via `@forge/database`) and
  the existing `@forge/*` packages; `package-lock.json` changes are only the new workspace link.
- **New workspace package `@forge/outbox`** — deps: `@forge/contracts`, `@forge/database`,
  `@forge/events`, `@forge/logging`. It imports **no** `@forge/config` and **no** `apps/*`, so
  it can move to its own process later.
- **`apps/scheduler` gains `@forge/outbox`** (`package.json` + `tsconfig.json` project ref) for
  the dispatcher wiring in `src/index.ts`, and a `@forge/database` project ref because
  `index.ts` now value-imports `PgOutboxRepository`.
- `@forge/database` still imports **no** `@forge/events` (a comment in
  `pg-outbox-repository.ts` records the constraint; enqueue validation is structural only).
- `@forge/contracts` stays zero-runtime-dep — the new `OutboxStatus` / `OutboxEnqueueInput` /
  `OutboxEventRecord` / `DEFAULT_OUTBOX_*` symbols are `type` / `interface` / `const number`,
  and `BatchClaimOptions.pendingOutbox?` is a function type in type position only.
- New root script `benchmark:outbox`; new root `tsconfig.json` project ref
  `packages/outbox`.

## 14. Architecture Impact

**Before (PR 20):**

```text
apps/worker  ── withTransaction ─► jobs + job_attempts   (COMMIT)
                                       └─ then safePublish(JobStarted / terminal / JobQueued)   ← lost if transport down
apps/scheduler ─ claimBatch ─────► worker_leases          (COMMIT)
                                       └─ then safePublish(JobClaimed)                          ← lost if transport down
apps/scheduler ─ recovery ───────► jobs + dead_letter_jobs (COMMIT)
                                       └─ then safePublish(WorkerLost)                          ← lost if transport down
```

**After (PR 21):**

```text
apps/worker    ── withTransaction ─┐
                                   ├─►  jobs + job_attempts + outbox_events          (ONE commit)
apps/scheduler ─ claimBatch ───────┤     worker_leases      + outbox_events          (ONE commit)
apps/scheduler ─ recovery ─────────┘     jobs + dead_letter_jobs + outbox_events     (ONE commit)
                                              │
                                              ▼
                                  OutboxDispatcher (@forge/outbox, hosted in apps/scheduler)
                                  poll → claimBatch (fresh claim_token, FOR UPDATE SKIP LOCKED)
                                       → parseForgeEvent(payload)
                                       → publish(event)  [bounded publishTimeoutMs]
                                       → fenced markPublished / markRetry / → DEAD
                                       → every N ticks: deletePublishedBefore (retention)
                                              │  at least once
                                              ▼
                                  EventPublisher (InProcessEventBus today)  →  consumers dedupe on event_id
```

`JobLogChunk`, `WorkerHeartbeat`, and `WorkerRegistered` stay on the direct best-effort
`safePublish` path and are **not** in the outbox.

## 15. Known Limitations

a. **Claim query Seq-Scans.** The `PENDING OR CLAIMED` disjunction in the `claimBatch` WHERE
leaves the `idx_outbox_events_claimable` partial index unused (§9) — `Seq Scan` +
`quicksort`, `O(table)`. Fine at current scale (2.3 ms / 5000 rows); the follow-up is a
`UNION ALL` rewrite (one arm per status, each index-eligible), gated on re-running the
fencing and ordering tests (T6 / T13) since it changes the claim SQL.
b. **Recovery-only scheduler loses best-effort `WorkerLost`.** A scheduler configured with
`{ recoveryService, eventPublisher }` but no `leaseRepository` no longer emits `WorkerLost`,
because the recovery mapper is gated on `outboxEnabled` (which requires `claimBatch`). No
durability is lost (this path was only ever best-effort), and every realistic
recovery-capable scheduler carries a `leaseRepository`. Follow-up: gate the recovery mapper
on `Boolean(this.eventPublisher)` (a `recoveryOutboxEnabled` getter) instead.
c. **`CLAUDE.md` was updated in the main working tree only.** It is git-untracked in this repo,
so the migrations `001`–`008` / `packages/outbox` / `tx.outbox` / `benchmark:outbox` / status
edits made to it are **not** in this branch's diff.
d. **`JobLogChunk` / `WorkerHeartbeat` / `WorkerRegistered` remain best-effort.** They may be
lost on publisher failure or crash, and — for the same job — may arrive _after_ the durable
terminal event, because the durable path is delayed by ≥ one poll interval while
`JobLogChunk` publishes immediately (see `events.md` §18.4 and invariant §14.8).
e. **`DEAD` outbox rows accumulate.** No auto-cleanup in PR 21; retention never touches them.
Manual triage; a bounded `DEAD`-retention job is future work.
f. **Residual duplicate window.** If the external `publish` succeeds and then `markPublished`
fails (or the dispatcher dies before it), the next dispatcher reclaims and re-publishes the
same `event_id`. This is the at-least-once contract, not a defect — consumers must dedupe.
g. **`OutboxDispatcher` is hosted inside `apps/scheduler`**, not its own process. The package
carries zero `apps/*` imports so relocation is wiring-only, but today a scheduler must be
running to drain the outbox.
h. **No `LISTEN` / `NOTIFY` wake-up.** The dispatcher polls at `pollIntervalMs`; there is no
push notification when a row is enqueued. Polling is the only correctness path (explicitly
in scope per spec §18); latency is bounded below by one poll interval.

## 16. Merge Recommendation

READY TO MERGE

---

🤖 Generated with [Claude Code](https://claude.com/claude-code)
