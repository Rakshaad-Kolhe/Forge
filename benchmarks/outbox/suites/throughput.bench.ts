/**
 * Dispatcher-throughput suite — how fast `OutboxDispatcher.runOnce()` drives a batch of
 * staged rows to `PUBLISHED` against a subscriber-less {@link InProcessEventBus}.
 *
 * For each size in `throughputSizes`: enqueue `size` `JobStarted` rows, then time
 * `runOnce()` call(s) until every row is `PUBLISHED`. Reports events/sec, mean batch
 * latency (`stats.mean`), mean publish latency, and the measured-window RSS delta.
 */
import { PgOutboxRepository, type DatabasePool } from '@forge/database';
import { InProcessEventBus, type ForgeEvent } from '@forge/events';
import type { EventPublisher } from '@forge/events';
import { OutboxDispatcher, type OutboxDispatcherConfig } from '@forge/outbox';
import {
  calculateStats,
  type BenchmarkRunResult,
  type FailureStats,
  type ResourceStats,
} from '../../scheduler/utils/timer.js';
import { SeededPRNG } from '../../scheduler/utils/prng.js';
import { outboxBenchmarkConfig } from '../config.js';
import { makeJobStartedInput } from '../utils/fixtures.js';

/** Extra dispatcher-throughput metrics carried alongside the standard result fields. */
export interface OutboxThroughputExtra {
  readonly eventCount: number;
  readonly batchCount: number;
  readonly eventsPerSec: number;
  readonly meanPublishLatencyMs: number;
}

export type OutboxThroughputResult = BenchmarkRunResult & OutboxThroughputExtra;

/** Delegates to the in-process bus while accumulating per-publish wall time. */
class TimingPublisher implements EventPublisher {
  public totalMs = 0;
  public count = 0;

  constructor(private readonly inner: InProcessEventBus) {}

  public async publish(event: ForgeEvent): Promise<void> {
    const start = performance.now();
    await this.inner.publish(event);
    this.totalMs += performance.now() - start;
    this.count += 1;
  }
}

function dispatcherConfig(): OutboxDispatcherConfig {
  return {
    pollIntervalMs: 3_600_000,
    batchSize: outboxBenchmarkConfig.dispatcherBatchSize,
    claimTimeoutMs: 60_000,
    publishTimeoutMs: 10_000,
    maxDeliveryAttempts: 5,
    baseBackoffMs: 100,
    maxBackoffMs: 1_000,
    retentionMaxAgeMs: 0,
    retentionBatchSize: 100,
    retentionEveryNTicks: 1,
    dispatcherId: 'bench-outbox-dispatcher',
  };
}

async function drainAll(dispatcher: OutboxDispatcher): Promise<number> {
  let batches = 0;
  for (;;) {
    const summary = await dispatcher.runOnce();
    batches += 1;
    if (summary.claimed === 0) {
      return batches;
    }
    if (batches > 100_000) {
      throw new Error('drainAll exceeded 100000 batches — dispatcher not converging');
    }
  }
}

export async function runThroughputSuite(
  pool: DatabasePool,
  rng: SeededPRNG,
): Promise<OutboxThroughputResult[]> {
  const repo = new PgOutboxRepository(pool);
  const measured = outboxBenchmarkConfig.throughputIterations;
  const warmup = outboxBenchmarkConfig.warmup.throughput;
  const results: OutboxThroughputResult[] = [];

  for (const size of outboxBenchmarkConfig.throughputSizes) {
    const bus = new InProcessEventBus();
    const publisher = new TimingPublisher(bus);
    const dispatcher = new OutboxDispatcher({
      repository: repo,
      publisher,
      config: dispatcherConfig(),
    });

    let lastBatches = 0;
    const runIteration = async (): Promise<number> => {
      await pool.query('DELETE FROM outbox_events;');
      for (let i = 0; i < size; i += 1) {
        await repo.enqueue(makeJobStartedInput(rng));
      }
      const start = performance.now();
      const batches = await drainAll(dispatcher);
      const elapsed = performance.now() - start;
      const stats = await repo.stats();
      if (stats.pending !== 0 || stats.claimed !== 0 || stats.dead !== 0) {
        throw new Error(
          `throughput drain left rows unpublished: ${JSON.stringify(stats)} (size=${size})`,
        );
      }
      lastBatches = batches;
      return elapsed;
    };

    for (let i = 0; i < warmup; i += 1) {
      await runIteration();
    }

    publisher.totalMs = 0;
    publisher.count = 0;
    let batchTotal = 0;
    const samples: number[] = [];
    const memBefore = process.memoryUsage();
    const cpuBefore = process.cpuUsage();

    for (let i = 0; i < measured; i += 1) {
      samples.push(await runIteration());
      batchTotal += lastBatches;
    }

    const cpuAfter = process.cpuUsage(cpuBefore);
    const memAfter = process.memoryUsage();
    await bus.close();

    const stats = calculateStats(samples);
    const totalDurationMs = samples.reduce((acc, val) => acc + val, 0);
    const eventCount = size * samples.length;
    const eventsPerSec = totalDurationMs > 0 ? (eventCount / totalDurationMs) * 1000 : 0;

    const resources: ResourceStats = {
      heapUsedDeltaBytes: memAfter.heapUsed - memBefore.heapUsed,
      rssDeltaBytes: memAfter.rss - memBefore.rss,
      cpuUserMicros: cpuAfter.user,
      cpuSystemMicros: cpuAfter.system,
    };
    const failures: FailureStats = {
      total: measured,
      success: measured,
      failures: 0,
      conflicts: 0,
      timeouts: 0,
      errors: [],
    };

    results.push({
      name: `throughput:events=${size}`,
      warmupIterations: warmup,
      measuredIterations: measured,
      totalDurationMs: Number(totalDurationMs.toFixed(2)),
      stats,
      resources,
      failures,
      eventCount,
      batchCount: batchTotal,
      eventsPerSec: Number(eventsPerSec.toFixed(2)),
      meanPublishLatencyMs:
        publisher.count > 0 ? Number((publisher.totalMs / publisher.count).toFixed(6)) : 0,
    });
  }

  await pool.query('DELETE FROM outbox_events;');
  return results;
}
