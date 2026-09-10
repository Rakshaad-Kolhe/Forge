import { DEFAULT_DATABASE_URL } from '@forge/database';

/**
 * Static configuration for the `@forge/outbox` benchmark suite.
 *
 * Shape mirrors `benchmarks/scheduler/config.ts`: a single frozen object with a fixed
 * numeric `seed` so every workload is reproducible through the shared Mulberry32 PRNG
 * (`benchmarks/scheduler/utils/prng.ts`). No wall-clock or `Math.random()` seeding.
 */
export interface OutboxBenchmarkConfig {
  /** Deterministic PRNG seed shared by every suite. */
  readonly seed: number;
  /** PostgreSQL connection string (env override, else the `@forge/database` dev default). */
  readonly databaseUrl: string;
  /** Measured iterations for each phase of the transactional-overhead suite. */
  readonly overheadIterations: number;
  /** Measured iterations per size in the dispatcher-throughput suite. */
  readonly throughputIterations: number;
  /** Event batch sizes swept by the dispatcher-throughput suite. */
  readonly throughputSizes: readonly number[];
  /** Dispatcher `batchSize` — kept >= max(`throughputSizes`) so one `runOnce()` drains a batch. */
  readonly dispatcherBatchSize: number;
  /** PENDING rows seeded before the claim-query EXPLAIN capture. */
  readonly explainSeedRows: number;
  /** `LIMIT` used by the claim query (verbatim with `PgOutboxRepository.claimBatch`). */
  readonly claimQueryLimit: number;
  /** Unrecorded warmup iterations per phase. */
  readonly warmup: {
    readonly overhead: number;
    readonly throughput: number;
  };
}

export const outboxBenchmarkConfig: OutboxBenchmarkConfig = {
  seed: 909090,
  databaseUrl: process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL,
  overheadIterations: 2000,
  throughputIterations: 20,
  throughputSizes: [1, 10, 50, 100, 500] as const,
  dispatcherBatchSize: 1000,
  explainSeedRows: 5000,
  claimQueryLimit: 100,
  warmup: {
    overhead: 100,
    throughput: 3,
  },
};
