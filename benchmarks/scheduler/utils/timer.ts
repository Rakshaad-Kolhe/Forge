/**
 * High-resolution timing, statistical analysis, and resource measurement utilities for Forge benchmarks.
 */

export interface SampleStats {
  readonly count: number;
  readonly min: number;
  readonly max: number;
  readonly mean: number;
  readonly median: number;
  readonly p95: number;
  readonly p99: number;
  readonly stdDev: number;
  readonly opsPerSec: number;
}

export interface ResourceStats {
  readonly heapUsedDeltaBytes: number;
  readonly rssDeltaBytes: number;
  readonly cpuUserMicros: number;
  readonly cpuSystemMicros: number;
}

export interface FailureStats {
  readonly total: number;
  readonly success: number;
  readonly failures: number;
  readonly conflicts: number;
  readonly timeouts: number;
  readonly errors: readonly string[];
}

export interface BenchmarkRunResult {
  readonly name: string;
  readonly warmupIterations: number;
  readonly measuredIterations: number;
  readonly totalDurationMs: number;
  readonly stats: SampleStats;
  readonly resources: ResourceStats;
  readonly failures: FailureStats;
}

export interface BenchmarkOptions<T = unknown> {
  readonly name: string;
  readonly warmupIterations?: number;
  readonly measuredIterations: number;
  readonly beforeIteration?: (iteration: number) => Promise<void> | void;
  readonly afterIteration?: (iteration: number) => Promise<void> | void;
  readonly fn: (iteration: number) => Promise<T> | T;
}

/**
 * Calculates accurate statistical distributions from a raw sample array of durations (in milliseconds).
 */
export function calculateStats(samples: readonly number[], totalDurationMs?: number): SampleStats {
  const count = samples.length;
  if (count === 0) {
    return {
      count: 0,
      min: 0,
      max: 0,
      mean: 0,
      median: 0,
      p95: 0,
      p99: 0,
      stdDev: 0,
      opsPerSec: 0,
    };
  }

  const sorted = [...samples].sort((a, b) => a - b);
  const min = sorted[0]!;
  const max = sorted[count - 1]!;
  const sum = sorted.reduce((acc, val) => acc + val, 0);
  const mean = sum / count;

  // Percentile calculation helper (nearest rank / linear interpolation)
  const getPercentile = (p: number): number => {
    if (count === 1) return sorted[0]!;
    const rank = (p / 100) * (count - 1);
    const lower = Math.floor(rank);
    const upper = Math.ceil(rank);
    const weight = rank - lower;
    return sorted[lower]! * (1 - weight) + sorted[upper]! * weight;
  };

  const median = getPercentile(50);
  const p95 = getPercentile(95);
  const p99 = getPercentile(99);

  const variance = sorted.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / count;
  const stdDev = Math.sqrt(variance);

  // Operations per second based on aggregate elapsed time
  const effectiveTotalMs =
    totalDurationMs !== undefined && totalDurationMs > 0 ? totalDurationMs : sum;
  const opsPerSec = effectiveTotalMs > 0 ? (count / effectiveTotalMs) * 1000 : 0;

  return {
    count,
    min: Number(min.toFixed(4)),
    max: Number(max.toFixed(4)),
    mean: Number(mean.toFixed(4)),
    median: Number(median.toFixed(4)),
    p95: Number(p95.toFixed(4)),
    p99: Number(p99.toFixed(4)),
    stdDev: Number(stdDev.toFixed(4)),
    opsPerSec: Number(opsPerSec.toFixed(2)),
  };
}

/**
 * Executes a controlled benchmark run with separated warmup, high-precision timing,
 * failure categorization, and resource delta tracking.
 */
export async function runBenchmark<T = unknown>(
  options: BenchmarkOptions<T>,
): Promise<BenchmarkRunResult> {
  const {
    name,
    warmupIterations = 0,
    measuredIterations,
    beforeIteration,
    afterIteration,
    fn,
  } = options;

  // 1. Warmup Phase (JIT optimization, unrecorded timing)
  for (let i = 0; i < warmupIterations; i++) {
    try {
      if (beforeIteration) await beforeIteration(i);
      await fn(i);
      if (afterIteration) await afterIteration(i);
    } catch {
      // Ignore warmup exceptions
    }
  }

  // 2. Resource Snapshot Before Measured Run
  const memBefore = process.memoryUsage();
  const cpuBefore = process.cpuUsage();
  const runStart = performance.now();

  const samples: number[] = [];
  let successCount = 0;
  let failureCount = 0;
  let conflictCount = 0;
  let timeoutCount = 0;
  const errorMessages: string[] = [];

  // 3. Measured Execution Phase
  for (let i = 0; i < measuredIterations; i++) {
    if (beforeIteration) await beforeIteration(i);

    const t0 = performance.now();
    try {
      const result = await fn(i);
      const t1 = performance.now();
      samples.push(t1 - t0);

      // Inspect if result explicitly indicates placement outcome or lease conflict
      if (
        result &&
        typeof result === 'object' &&
        'status' in result &&
        result.status === 'UNSCHEDULABLE'
      ) {
        const reason = (result as { reason?: string }).reason;
        if (reason === 'LEASE_CONFLICT') {
          conflictCount++;
        } else {
          failureCount++;
        }
      } else {
        successCount++;
      }
    } catch (err: unknown) {
      const t1 = performance.now();
      samples.push(t1 - t0);
      failureCount++;
      const message = err instanceof Error ? err.message : String(err);
      if (message.includes('timeout') || message.includes('TIMEDOUT')) {
        timeoutCount++;
      }
      if (errorMessages.length < 5) {
        errorMessages.push(message);
      }
    }

    if (afterIteration) await afterIteration(i);
  }

  const runEnd = performance.now();
  const totalDurationMs = runEnd - runStart;

  // 4. Resource Snapshot After Measured Run
  const cpuAfter = process.cpuUsage(cpuBefore);
  const memAfter = process.memoryUsage();

  const stats = calculateStats(samples, totalDurationMs);

  const resources: ResourceStats = {
    heapUsedDeltaBytes: memAfter.heapUsed - memBefore.heapUsed,
    rssDeltaBytes: memAfter.rss - memBefore.rss,
    cpuUserMicros: cpuAfter.user,
    cpuSystemMicros: cpuAfter.system,
  };

  const failures: FailureStats = {
    total: measuredIterations,
    success: successCount,
    failures: failureCount,
    conflicts: conflictCount,
    timeouts: timeoutCount,
    errors: errorMessages,
  };

  return {
    name,
    warmupIterations,
    measuredIterations,
    totalDurationMs: Number(totalDurationMs.toFixed(2)),
    stats,
    resources,
    failures,
  };
}
