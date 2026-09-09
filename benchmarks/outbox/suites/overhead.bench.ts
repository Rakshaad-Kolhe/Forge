/**
 * Transactional-overhead suite — the durability cost of co-committing an outbox row.
 *
 * Two phases, each `overheadIterations` measured iterations with warmup:
 *   - baseline:     `withTransaction` { jobs.save + jobAttempts.save }
 *   - with-outbox:  the same, plus one `outbox.enqueue(...)` in the same transaction
 *
 * The per-phase mean/median/p95/p99 and the mean delta (with-outbox − baseline) are the
 * output. Distinct job ids per iteration; tables are cleared between phases.
 */
import { withTransaction, type DatabasePool } from '@forge/database';
import { runBenchmark, type BenchmarkRunResult } from '../../scheduler/utils/timer.js';
import { SeededPRNG } from '../../scheduler/utils/prng.js';
import { outboxBenchmarkConfig } from '../config.js';
import { makeJobStartedInput, seedPipelineRunAndJob, type SeededJob } from '../utils/fixtures.js';

async function clearRows(pool: DatabasePool): Promise<void> {
  await pool.query('DELETE FROM outbox_events;');
  await pool.query('DELETE FROM job_attempts;');
  await pool.query('DELETE FROM jobs;');
}

export async function runOverheadSuite(
  pool: DatabasePool,
  rng: SeededPRNG,
): Promise<BenchmarkRunResult[]> {
  const measuredIterations = outboxBenchmarkConfig.overheadIterations;
  const warmupIterations = outboxBenchmarkConfig.warmup.overhead;

  let pending: SeededJob | null = null;

  await clearRows(pool);
  const baseline = await runBenchmark({
    name: 'overhead:baseline (jobs.save + jobAttempts.save)',
    warmupIterations,
    measuredIterations,
    beforeIteration: async () => {
      pending = await seedPipelineRunAndJob(pool, rng);
    },
    fn: async () => {
      const { job, attempt } = pending!;
      await withTransaction(pool, async (tx) => {
        await tx.jobs.save(job);
        await tx.jobAttempts.save(attempt);
      });
    },
  });

  await clearRows(pool);
  const withOutbox = await runBenchmark({
    name: 'overhead:with-outbox (+ outbox.enqueue)',
    warmupIterations,
    measuredIterations,
    beforeIteration: async () => {
      pending = await seedPipelineRunAndJob(pool, rng);
    },
    fn: async () => {
      const { job, attempt } = pending!;
      await withTransaction(pool, async (tx) => {
        await tx.jobs.save(job);
        await tx.jobAttempts.save(attempt);
        await tx.outbox.enqueue(makeJobStartedInput(rng));
      });
    },
  });

  await clearRows(pool);

  return [baseline, withOutbox];
}
