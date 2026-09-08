# PR 20: Typed Event Architecture & Execution Lifecycle Events

## 1. Repository Inspection

Before writing code the repository was inspected end to end for existing event infrastructure:

- **Searched** every `packages/*` and `apps/*` for `event`, `EventEmitter`, `publish(`,
  `subscribe(`, `pub/sub`, `bus`, `observer`, `emit`, and domain-event patterns.
- **Result: none existed.** `packages/events` did not exist. `packages/redis` has no
  pub/sub primitive (only `client.ts`'s internal `registerLifecycleEvents` for ioredis
  connection state). The only `addEventListener` in the tree is the executor's `AbortSignal`
  handling. There was nothing to reuse and nothing to duplicate.
- **Confirmed the integration surfaces** first-hand:
  - `apps/scheduler/src/scheduler.ts` — `ForgeScheduler` uses constructor injection via
    `SchedulerOptions`. Leases are acquired in `claimLeaseForDecision` and the batched
    `schedulePrioritized` path (`claimBatch`). `recoverExpiredLeases` returns
    `RecoverExpiredLeasesResult { details: RecoveredLeaseRecord[] }` with `action` ∈
    `REQUEUED | DEAD_LETTERED | SKIPPED_TERMINAL | NO_OP`. The scheduler never transitions a
    job to `QUEUED`.
  - `apps/worker/src/index.ts` — `startWorker` uses `StartWorkerOptions`. Lifecycle points:
    `registry.register().then()`, the heartbeat `setInterval`, and inside `executeJob`
    the `RUNNING` persist, the terminal state branch (`ownershipLost` / `SUCCEEDED` /
    `CANCELLED` / `FAILED`/`TIMED_OUT` with `evaluateRetry`), and the retry re-queue
    (`job.transitionTo('QUEUED')`).
  - `packages/executor` — `OutputCollector` captures bounded stdout/stderr and a `truncated`
    flag, returned only on the final `ExecutionResult`. **No streaming / chunk callback.**
  - `packages/pipeline` — branded ids `PipelineId` / `PipelineRunId` / `JobId` /
    `JobAttemptId`; `PipelineRun.evaluateCompletion()` exists but no service drives run
    completion, so pipeline-level events have no producer.
  - `zod ^3.24.2` is already a dependency (`@forge/config`); `tsc -b` project references;
    vitest `fileParallelism: false`, explicit imports, `.js` specifiers, `vi.fn()` object
    mocks, `now`-injection for determinism.

## 2. Architecture

```
Producer (scheduler / worker / execution lifecycle)
      -> createForgeEvent(type, { correlation?, payload }, { now?, eventId? })
            typed, versioned, id-stamped, frozen ForgeEventEnvelope
      -> safePublish(EventPublisher, event, logger)     best-effort, AFTER the PostgreSQL commit
      -> EventPublisher.publish(event)
      -> InProcessEventBus  ->  EventSubscriber handlers  (isolated; per-producer order only)
```

- **`@forge/events`** is a neutral package. It depends only on `@forge/logging` (bus failure
  diagnostics) and, transitively, `@forge/contracts`. Nothing in `database`, scheduler
  internals, or worker internals is imported by it. `apps/scheduler` and `apps/worker` now
  depend on `@forge/events`. No circular dependency.
- The bus is **in-process only**, has **no global singleton**, and requires explicit
  `close()`. It is not a distributed transport.

## 3. Event Catalog

`emitted` — real producer in this PR. `deferred` — contract only, no producer yet.

| Event              | Status   | Authoritative producer                          | Payload (key fields)                                                     |
| ------------------ | -------- | ---------------------------------------------- | --------------------------------------------------------------------- |
| `JobClaimed`       | emitted  | Scheduler (lease `ACQUIRED`)                    | `job_id`, `worker_id`, `lease_id`, `lease_expires_at`                |
| `WorkerLost`       | emitted  | Scheduler (recovery sweep; `NO_OP` excluded)    | `worker_id`, `job_id`, `lease_id`, `recovery_action`, `dead_letter_reason?` |
| `WorkerRegistered` | emitted  | Worker (`registry.register()` resolved)         | `worker_id`, `hostname?`, `capabilities[]`, `cpu_cores`, `memory_bytes` |
| `WorkerHeartbeat`  | emitted  | Worker (each successful heartbeat tick)         | `worker_id`, `status`                                                |
| `JobStarted`       | emitted  | Worker (after `RUNNING` persisted)              | `job_id`, `attempt_id`, `worker_id`, `attempt_number`               |
| `JobLogChunk`      | emitted  | Worker (derived post-execution)                 | `job_id`, `attempt_id`, `sequence`, `stream`, `chunk`, `byte_offset`, `truncated`, `final` |
| `JobSucceeded`     | emitted  | Worker (after terminal persist)                 | `… duration_ms`, `exit_code`                                        |
| `JobFailed`        | emitted  | Worker (after terminal persist)                 | `… failure_kind`, `reason`, `exit_code`, `retry_scheduled`, `next_attempt_at?` |
| `JobCancelled`     | emitted  | Worker (after terminal persist)                 | `job_id`, `attempt_id`, `worker_id`, `attempt_number`               |
| `JobQueued`        | emitted  | Worker (retry re-queue only)                    | `job_id`, `run_id`, `priority`, `attempt_number`, `next_attempt_at?` |
| `PipelineCreated`  | deferred | future API/ingress                             | `pipeline_id`, `name`, `step_count`                                 |
| `PipelineQueued`   | deferred | future API/ingress                             | `pipeline_id`, `run_id`                                             |
| `PipelineCompleted`| deferred | future run-aggregation service                 | `run_id`, `status`, `job_count`                                    |

`failure_kind` ∈ `FAILED` (non-zero exit) · `TIMED_OUT` (wall-clock; `exit_code=null`) ·
`LEASE_LOST` (ownership lost mid-execution) · `EXECUTOR_ERROR` (executor threw).

First-time enqueue `JobQueued` is deferred because the API/ingress plane does not exist yet;
the retry re-queue transition is a real, persisted state change and is emitted.

## 4. State / Event Relationship

`domain transition -> persist to PostgreSQL -> publish event`. An event always represents
**committed** state. PostgreSQL stays the sole authoritative store; the event layer never
reconstructs job state and Forge is not event-sourced. Known un-hidden window: **DB commit
succeeds, event publish fails** → state is correct and durable, the notification is lost.
There is **no outbox** and **no transaction** binding the commit and the publication (future
work). Emission is opt-in — with no `eventPublisher`, scheduler and worker behave exactly as
before PR 20.

## 5. Ordering Guarantees

- **Guaranteed:** per-producer logical order. `InProcessEventBus.publish()` runs subscribers
  sequentially in subscription order and resolves after the last settles, so a producer that
  awaits `publish(a)` before `publish(b)` has every subscriber observe `a` before `b`. The
  worker emits its per-attempt events this way: `JobStarted -> JobLogChunk* -> terminal
  [-> JobQueued]`.
- **Not guaranteed:** global ordering, cross-producer ordering, or `occurred_at` wall-clock
  order as causal order.

## 6. Failure Model

| Scenario                       | Behaviour                                                                      |
| ------------------------------ | -------------------------------------------------------------------------- |
| Publisher `publish()` rejects  | `safePublish` catches, logs with event context, swallows. Control loop unaffected; DB state unchanged. |
| DB commit ok, publish fails    | Documented window. No rollback, no retry, no false atomicity claim.        |
| Subscriber throws / rejects    | Caught, logged, isolated. Other subscribers still receive the event. No propagation, no retry. |
| Bus `close()`                  | Idempotent. Subsequent `publish()` rejects `EventBusClosedError` (caught by `safePublish`); `subscribe()` throws; subscribers dropped; in-flight publishes past the check complete. |
| Duplicate publication          | Bus does not deduplicate — same `event_id` delivered again. No false dedup claim. Consumers dedupe on `event_id` + idempotent handlers. |

## 7. Integration

- **`apps/scheduler/src/scheduler.ts`** — `SchedulerOptions.eventPublisher?` added.
  `publishJobClaimed(...)` fires on `ACQUIRED` in both `claimLeaseForDecision` and the
  batched `claimBatch` loop. `publishWorkerLostForRecovery(...)` fires in
  `recoverExpiredLeases` for each `RecoveredLeaseRecord` whose action is `REQUEUED` /
  `DEAD_LETTERED` / `SKIPPED_TERMINAL` (`NO_OP` excluded). Both wrapped in `safePublish`.
- **`apps/worker/src/index.ts`** — `StartWorkerOptions.eventPublisher?` added, plus a local
  `publish()` helper. `WorkerRegistered` after `register()` resolves; `WorkerHeartbeat`
  after each successful heartbeat tick; `JobStarted` after the `RUNNING` persist;
  `deriveLogChunkEvents(...)` + the terminal event after the terminal persist and before
  lease release; `JobQueued` after a retry re-queue. All emission is gated on
  `options?.eventPublisher` so no events are constructed when the feature is off.

## 8. Tests

Run with `npx vitest run` (root, `fileParallelism: false`).

- **`packages/events` — 56 tests**, 8 files: `schema.test.ts` (23 — every type validates,
  missing/invalid `event_id`, non-ISO `occurred_at`, wrong `version`, unknown `event_type`,
  payload mismatch, unknown-field stripping, empty-id rejection, union ↔ runtime list
  parity), `event-id.test.ts` (4 — v4 shape, 10k uniqueness, not type-derived),
  `factory.test.ts` (6 — stamping, passthrough, determinism, frozen envelope, validates),
  `in-process-bus.test.ts` (8 — no subscribers, multi, failure isolation + logging,
  unsubscribe idempotency, post-close reject, mid-dispatch subscribe, no-dedup),
  `ordering.test.ts` (2 — `JobQueued→JobClaimed→JobStarted→JobSucceeded`, per-subscriber
  order), `concurrency.test.ts` (4 — 2/5/10 concurrent producers: no loss, frozen, unique
  ids, race-safe close), `exhaustiveness.test.ts` (3 — every type summarised, `assertNever`,
  closed union), `log-chunk.test.ts` (6 — empty, small, large split + lossless reassembly,
  `final`/`truncated`, correlation + schema, deterministic ids).
- **`apps/scheduler/src/scheduler.test.ts` — +5 tests**: `JobClaimed` on single claim,
  `JobClaimed` per acquired batch item, silent with no publisher, `WorkerLost` for
  `REQUEUED` + `DEAD_LETTERED` skipping `NO_OP`, publisher failure never breaks placement.
  (42/42 in the file pass.)
- **`apps/worker/src/index.test.ts` — +8 tests**: success `JobStarted→JobLogChunk→
  JobSucceeded`, non-retryable `JobFailed(FAILED, retry_scheduled=false)`, `JobCancelled`,
  timeout `JobFailed(TIMED_OUT)`, retry `JobFailed(retry_scheduled)→JobQueued(attempt 2)`,
  `WorkerRegistered` + `WorkerHeartbeat` identity, failing publisher does not break
  execution (job still persisted, failure logged), no-publisher no-op. (20/20 in the file
  pass.)

### Full suite

```
Test Files  53 passed (53)
Tests       570 passed (570)
```

Includes live PostgreSQL + Redis integration and live Docker executor integration (a Docker
TCP daemon was reachable). No test was skipped.

## 9. Quality Gates

| Command                | Result |
| ---------------------- | ------ |
| `npm run format:check` | PASS — "All matched files use Prettier code style!" |
| `npm run lint`         | PASS — `eslint .`, 0 problems |
| `npm run typecheck`    | PASS — `tsc -b`, 0 errors |
| `npm test`             | PASS — 570/570 across 53 files |
| `npm run build`        | PASS — `tsc -b` all workspaces |

## 10. Changed Files

**New — `packages/events/`:** `package.json`, `tsconfig.json`, `src/{index,envelope,events,
event-id,factory,schema,publisher,in-process-bus,log-chunk,summarize,errors}.ts`, and tests
`src/{schema,event-id,factory,in-process-bus,ordering,concurrency,exhaustiveness,log-chunk}.test.ts`.

**New — docs:** `docs/architecture/events.md`.

**Modified:**

- `tsconfig.json` — add `packages/events` reference.
- `apps/scheduler/{package.json,tsconfig.json}` — add `@forge/events`.
- `apps/scheduler/src/types.ts` — `SchedulerOptions.eventPublisher?`.
- `apps/scheduler/src/scheduler.ts` — `publishJobClaimed`, `publishWorkerLostForRecovery`,
  emit sites, imports.
- `apps/scheduler/src/scheduler.test.ts` — +5 tests, import.
- `apps/worker/{package.json,tsconfig.json}` — add `@forge/events`.
- `apps/worker/src/index.ts` — `StartWorkerOptions.eventPublisher?`, `publish` helper,
  `executorThrew` flag, emit sites, imports.
- `apps/worker/src/index.test.ts` — +8 tests, import.
- `docs/architecture/invariants.md` — new §14 (12 invariants).
- `docs/architecture/overview.md` — §3 PR 20 entry + evolution path.
- `README.md` — PR marker, `@forge/events` package entry, scheduler/worker bullets, tree,
  spec list, doc links.
- `package-lock.json` — `@forge/events` workspace link.

## 11. Dependencies

- **Added:** `@forge/events` declares `zod ^3.24.2` (already used by `@forge/config`),
  `@forge/contracts` `*`, `@forge/logging` `*`, dev `@types/node ^22.13.4`. `apps/scheduler`
  and `apps/worker` add `@forge/events` `*`.
- **No new external dependency.** `zod` and Node's `crypto.randomUUID` are already in the
  tree. **Removed:** none.

## 12. Architecture Impact

Boundaries preserved: Scheduler still schedules only; Executor still executes only; Worker
still orchestrates the execution lifecycle; PostgreSQL still authoritative; Redis still
transient. New boundary: **Event = communication / observation**, a neutral contract
producers depend on. Dependency graph stays acyclic:
`apps/{scheduler,worker} -> @forge/events -> @forge/logging -> @forge/contracts`.

## 13. Known Limitations

- **No durable event outbox** — the DB-commit / event-publish window can drop a notification
  (state stays correct).
- **No distributed event transport** — `InProcessEventBus` is process-local.
- **No WebSocket / browser path**, no metrics exporter, no tracing backend.
- **No exactly-once event delivery**, no global ordering.
- **`JobLogChunk` is derived post-execution**, not streamed live; boundaries are byte
  boundaries and may bisect a multi-byte UTF-8 sequence.
- **`PipelineCreated` / `PipelineQueued` / `PipelineCompleted` and first-enqueue
  `JobQueued`** are contract-only — no producer exists yet.
- Integration tests still require external PostgreSQL / Redis / Docker (unchanged).

## 14. Merge Recommendation

```
READY TO MERGE
```
