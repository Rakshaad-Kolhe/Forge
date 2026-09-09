/**
 * PRNG-seeded builders for the outbox benchmark suites.
 *
 * Every identifier, timestamp offset and payload value is drawn from the shared Mulberry32
 * PRNG (`benchmarks/scheduler/utils/prng.ts`) — no `Math.random()`, no wall-clock seeds —
 * so a given `outboxBenchmarkConfig.seed` reproduces an identical workload.
 */
import { PgPipelineRepository, PgPipelineRunRepository, type DatabasePool } from '@forge/database';
import { createForgeEvent, toOutboxEnqueueInput } from '@forge/events';
import type { OutboxEnqueueInput } from '@forge/contracts';
import {
  createJobAttemptId,
  createJobId,
  createPipelineId,
  createPipelineRunId,
  Job,
  JobAttempt,
  Pipeline,
  PipelineRun,
} from '@forge/pipeline';
import { SeededPRNG } from '../../scheduler/utils/prng.js';

/** Fixed reference epoch for deterministic timestamp derivation (not a wall clock). */
const EPOCH_BASE_MS = Date.parse('2026-01-01T00:00:00.000Z');

const BENCH_PIPELINE_ID = createPipelineId('bench-outbox-pipeline');
const BENCH_RUN_ID = createPipelineRunId('bench-outbox-run');

let parentsSeeded = false;

/** Draws `length` lowercase hex characters from the PRNG. */
function randomHex(rng: SeededPRNG, length: number): string {
  let out = '';
  for (let i = 0; i < length; i += 1) {
    out += rng.nextInt(0, 15).toString(16);
  }
  return out;
}

/**
 * Deterministic RFC-4122 v4-shaped UUID sourced entirely from the PRNG. Satisfies the
 * `UUID_SHAPE` check in `PgOutboxRepository` and the stricter `EVENT_ID_PATTERN` in
 * `@forge/events`.
 */
export function makeDeterministicUuid(rng: SeededPRNG): string {
  const h = randomHex(rng, 30);
  const variant = ['8', '9', 'a', 'b'][rng.nextInt(0, 3)]!;
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(12, 15)}-${variant}${h.slice(15, 18)}-${h.slice(18, 30)}`;
}

/** Deterministic ISO-8601 timestamp within `spanMs` of the fixed reference epoch. */
export function deterministicTimestamp(rng: SeededPRNG, spanMs: number): string {
  return new Date(EPOCH_BASE_MS + rng.nextInt(0, spanMs)).toISOString();
}

/**
 * Builds a `JobStarted` {@link OutboxEnqueueInput} via a real `createForgeEvent` envelope
 * projected through `toOutboxEnqueueInput`. All ids/timestamps are PRNG-derived.
 */
export function makeJobStartedInput(rng: SeededPRNG): OutboxEnqueueInput {
  const jobId = `bench-job-${randomHex(rng, 12)}`;
  const attemptId = `${jobId}-attempt-1`;
  const workerId = `bench-worker-${rng.nextInt(0, 63)}`;

  const event = createForgeEvent(
    'JobStarted',
    {
      correlation: { job_id: jobId, attempt_id: attemptId, worker_id: workerId },
      payload: { job_id: jobId, attempt_id: attemptId, worker_id: workerId, attempt_number: 1 },
    },
    {
      eventId: makeDeterministicUuid(rng),
      now: new Date(EPOCH_BASE_MS + rng.nextInt(0, 7 * 24 * 60 * 60 * 1000)),
    },
  );

  return toOutboxEnqueueInput(event);
}

/** A fresh, unsaved job + its first attempt, both already transitioned to `RUNNING`. */
export interface SeededJob {
  readonly job: Job;
  readonly attempt: JobAttempt;
}

/**
 * Ensures the shared bench pipeline + run rows exist (idempotent `ON CONFLICT` upserts,
 * done once per process) and returns a fresh `RUNNING` job + attempt with PRNG-unique ids —
 * suitable for `tx.jobs.save(job)` / `tx.jobAttempts.save(attempt)` in the overhead suite.
 */
export async function seedPipelineRunAndJob(
  pool: DatabasePool,
  rng: SeededPRNG,
): Promise<SeededJob> {
  if (!parentsSeeded) {
    const pipelineRepo = new PgPipelineRepository(pool);
    const runRepo = new PgPipelineRunRepository(pool);

    await pipelineRepo.save(
      new Pipeline({
        id: BENCH_PIPELINE_ID,
        name: 'bench-outbox-pipeline',
        steps: [{ name: 'run', command: 'echo bench' }],
      }),
    );

    const run = new PipelineRun({
      id: BENCH_RUN_ID,
      pipelineId: BENCH_PIPELINE_ID,
      pipelineName: 'bench-outbox-pipeline',
    });
    run.markQueued();
    run.start();
    await runRepo.save(run);

    parentsSeeded = true;
  }

  const jobId = createJobId(`bench-oh-job-${randomHex(rng, 16)}`);
  // A PRNG-unique step name per call: the shared bench run row is seeded once, so successive
  // `tx.jobs.save` inserts would otherwise all collide on `uq_jobs_run_step`
  // UNIQUE (pipeline_run_id, step_name) and roll the transaction back before the outbox row.
  const job = new Job({
    id: jobId,
    pipelineRunId: BENCH_RUN_ID,
    stepName: `run-${randomHex(rng, 12)}`,
    command: 'echo bench',
    priority: rng.nextInt(0, 100),
    initialStatus: 'QUEUED',
  });
  job.start();

  const attempt = new JobAttempt({
    id: createJobAttemptId(`${jobId}-attempt-1`),
    jobId,
    attemptNumber: 1,
    initialStatus: 'RUNNING',
    startedAt: deterministicTimestamp(rng, 1_000_000),
  });

  return { job, attempt };
}

/** Resets the process-local parent-seed guard (used when the schema is reset mid-process). */
export function resetFixtureState(): void {
  parentsSeeded = false;
}
