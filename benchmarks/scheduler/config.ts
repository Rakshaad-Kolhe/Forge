import { DEFAULT_DATABASE_URL } from '@forge/database';
import { DEFAULT_REDIS_URL } from '@forge/redis';

export interface BenchmarkConfig {
  readonly seed: number;
  readonly databaseUrl: string;
  readonly redisUrl: string;
  readonly prefix: string;
  readonly queueName: string;
  readonly warmup: {
    readonly micro: number;
    readonly component: number;
    readonly system: number;
  };
  readonly iterations: {
    readonly micro: number;
    readonly component: number;
    readonly system: number;
  };
  readonly scales: {
    readonly jobCounts: readonly number[];
    readonly inMemoryJobCounts: readonly number[];
    readonly workerCounts: readonly number[];
  };
  readonly fairness: {
    readonly agingRate: number;
    readonly maxBonus: number;
    readonly agingIntervalMs: number;
  };
}

export const benchmarkConfig: BenchmarkConfig = {
  seed: 424242,
  databaseUrl: process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL,
  redisUrl: process.env.REDIS_URL ?? DEFAULT_REDIS_URL,
  prefix: 'bench-',
  queueName: 'bench-scheduler-queue',
  warmup: {
    micro: 50,
    component: 10,
    system: 5,
  },
  iterations: {
    micro: 500,
    component: 50,
    system: 20,
  },
  scales: {
    jobCounts: [10, 25, 50, 100],
    inMemoryJobCounts: [10, 25, 50, 100, 500, 1000],
    workerCounts: [1, 10, 50, 100],
  },
  fairness: {
    agingRate: 0.05,
    maxBonus: 50,
    agingIntervalMs: 1000,
  },
};
