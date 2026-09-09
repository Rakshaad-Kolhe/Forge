/**
 * `@forge/outbox` benchmark runner — mirrors `benchmarks/scheduler/runner.ts`.
 *
 * Runs three suites against a live PostgreSQL (no Redis, no Docker):
 *   1. transactional overhead  (`suites/overhead.bench.ts`)
 *   2. dispatcher throughput   (`suites/throughput.bench.ts`)
 *   3. claim-query EXPLAIN     (`explain.ts`)
 *
 *   npx tsx benchmarks/outbox/runner.ts                 # all three suites
 *   npx tsx benchmarks/outbox/runner.ts --explain-only  # just the EXPLAIN capture
 */
import * as os from 'node:os';
import * as path from 'node:path';
import * as fs from 'node:fs/promises';
import { execSync } from 'node:child_process';
import {
  createDatabasePool,
  resetDatabase,
  runMigrations,
  type DatabasePool,
} from '@forge/database';
import {
  formatTable,
  type EnvironmentInfo,
  type ExplainPlanResult,
} from '../scheduler/utils/reporter.js';
import type { BenchmarkRunResult } from '../scheduler/utils/timer.js';
import { createSeededRandom } from '../scheduler/utils/prng.js';
import { outboxBenchmarkConfig } from './config.js';
import { runOverheadSuite } from './suites/overhead.bench.js';
import { runThroughputSuite, type OutboxThroughputResult } from './suites/throughput.bench.js';
import { runOutboxExplain } from './explain.js';
import { resetFixtureState } from './utils/fixtures.js';

interface OutboxBenchmarkReport {
  readonly environment: EnvironmentInfo;
  readonly configuration: {
    readonly seed: number;
    readonly overheadIterations: number;
    readonly throughputIterations: number;
    readonly throughputSizes: readonly number[];
    readonly explainSeedRows: number;
  };
  readonly suites: {
    readonly overhead: readonly BenchmarkRunResult[];
    readonly overheadDeltaMs: number;
    readonly throughput: readonly OutboxThroughputResult[];
    readonly explain: readonly ExplainPlanResult[];
  };
  readonly generatedAt: string;
}

async function collectEnvironmentInfo(pool: DatabasePool): Promise<EnvironmentInfo> {
  let gitCommit = 'unknown';
  let gitBranch = 'unknown';
  try {
    gitCommit = execSync('git rev-parse HEAD', { encoding: 'utf-8' }).trim();
    gitBranch = execSync('git rev-parse --abbrev-ref HEAD', { encoding: 'utf-8' }).trim();
  } catch {
    // Git may be unavailable in a bare environment.
  }

  let postgresVersion = 'unknown';
  try {
    const res = await pool.query<{ version: string }>('SELECT version();');
    postgresVersion = res.rows[0]?.version?.split(' on ')[0] ?? 'unknown';
  } catch {
    postgresVersion = 'unavailable';
  }

  const cpus = os.cpus();
  return {
    gitCommit,
    gitBranch,
    platform: process.platform,
    arch: process.arch,
    cpuModel: cpus[0]?.model ?? 'unknown',
    cpuCores: cpus.length,
    totalMemoryBytes: os.totalmem(),
    freeMemoryBytes: os.freemem(),
    nodeVersion: process.version,
    postgresVersion,
    redisVersion: 'not-applicable',
    timestamp: new Date().toISOString(),
  };
}

function formatThroughputExtras(rows: readonly OutboxThroughputResult[]): string {
  if (rows.length === 0) {
    return '*(No results)*\n';
  }
  const headers = [
    'Batch (events)',
    'Batches/iter',
    'Events/sec',
    'Mean Batch (ms)',
    'Mean Publish (ms)',
    'RSS Δ (MB)',
  ];
  const body = rows.map((r) => [
    String(r.eventCount / r.measuredIterations),
    (r.batchCount / r.measuredIterations).toFixed(2),
    r.eventsPerSec.toLocaleString('en-US', { maximumFractionDigits: 1 }),
    r.stats.mean.toFixed(4),
    r.meanPublishLatencyMs.toFixed(4),
    (r.resources.rssDeltaBytes / (1024 * 1024)).toFixed(2),
  ]);
  const widths = headers.map((h, i) => Math.max(h.length, ...body.map((row) => row[i]!.length)));
  const line = (cells: readonly string[]): string =>
    '| ' + cells.map((c, i) => c.padEnd(widths[i]!)).join(' | ') + ' |';
  const sep = '| ' + widths.map((w) => '-'.repeat(w)).join(' | ') + ' |';
  return [line(headers), sep, ...body.map(line)].join('\n');
}

function formatReportConsole(report: OutboxBenchmarkReport): string {
  const lines: string[] = [];
  lines.push('================================================================================');
  lines.push('                     FORGE V2 OUTBOX BENCHMARK REPORT                          ');
  lines.push('================================================================================');
  lines.push(`Date:         ${report.generatedAt}`);
  lines.push(`Commit:       ${report.environment.gitCommit} (${report.environment.gitBranch})`);
  lines.push(`Platform:     ${report.environment.platform} ${report.environment.arch}`);
  lines.push(`CPU:          ${report.environment.cpuModel} (${report.environment.cpuCores} cores)`);
  lines.push(
    `RAM:          ${(report.environment.totalMemoryBytes / (1024 * 1024 * 1024)).toFixed(2)} GB`,
  );
  lines.push(`Node.js:      ${report.environment.nodeVersion}`);
  lines.push(`PostgreSQL:   ${report.environment.postgresVersion}`);
  lines.push('================================================================================\n');

  lines.push('### Transactional Overhead (withTransaction: jobs.save + jobAttempts.save)\n');
  lines.push(formatTable(report.suites.overhead));
  lines.push(
    `\nMean delta (with-outbox - baseline): ${report.suites.overheadDeltaMs.toFixed(4)} ms\n`,
  );

  lines.push('### Dispatcher Throughput (OutboxDispatcher.runOnce -> InProcessEventBus)\n');
  lines.push(formatTable(report.suites.throughput));
  lines.push('\n');
  lines.push(formatThroughputExtras(report.suites.throughput));
  lines.push('\n');

  if (report.suites.explain.length > 0) {
    lines.push('### Claim-Query Explain Plan\n');
    for (const exp of report.suites.explain) {
      lines.push(`#### ${exp.queryName}`);
      if (exp.planningTimeMs !== undefined && exp.executionTimeMs !== undefined) {
        lines.push(
          `Planning: ${exp.planningTimeMs.toFixed(3)}ms | Execution: ${exp.executionTimeMs.toFixed(3)}ms`,
        );
      }
      lines.push('```text');
      lines.push(exp.plan.join('\n'));
      lines.push('```\n');
    }
  }

  lines.push('================================================================================\n');
  return lines.join('\n');
}

async function writeReportJson(filePath: string, report: OutboxBenchmarkReport): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(report, null, 2), 'utf-8');
}

export async function runOutboxBenchmarks(explainOnly: boolean): Promise<OutboxBenchmarkReport> {
  const pool = createDatabasePool({ connectionString: outboxBenchmarkConfig.databaseUrl });

  try {
    await resetDatabase(pool);
    await runMigrations(pool);
    resetFixtureState();

    const environment = await collectEnvironmentInfo(pool);
    console.log(`Starting Outbox Benchmark Suite (Seed=${outboxBenchmarkConfig.seed})...\n`);

    let overhead: BenchmarkRunResult[] = [];
    let throughput: OutboxThroughputResult[] = [];

    if (!explainOnly) {
      console.log('-> Running Transactional Overhead suite...');
      overhead = await runOverheadSuite(pool, createSeededRandom(outboxBenchmarkConfig.seed));
      console.log(`   Completed ${overhead.length} phases.\n`);

      console.log('-> Running Dispatcher Throughput suite...');
      throughput = await runThroughputSuite(
        pool,
        createSeededRandom(outboxBenchmarkConfig.seed + 100),
      );
      console.log(`   Completed ${throughput.length} batch sizes.\n`);
    }

    console.log('-> Capturing Claim-Query EXPLAIN plan...');
    const explain = await runOutboxExplain(pool);
    console.log(`   Captured ${explain.length} plan(s).\n`);

    const baselineMean = overhead[0]?.stats.mean ?? 0;
    const withOutboxMean = overhead[1]?.stats.mean ?? 0;

    const report: OutboxBenchmarkReport = {
      environment,
      configuration: {
        seed: outboxBenchmarkConfig.seed,
        overheadIterations: outboxBenchmarkConfig.overheadIterations,
        throughputIterations: outboxBenchmarkConfig.throughputIterations,
        throughputSizes: outboxBenchmarkConfig.throughputSizes,
        explainSeedRows: outboxBenchmarkConfig.explainSeedRows,
      },
      suites: {
        overhead,
        overheadDeltaMs: Number((withOutboxMean - baselineMean).toFixed(4)),
        throughput,
        explain,
      },
      generatedAt: new Date().toISOString(),
    };

    console.log(formatReportConsole(report));

    const reportPath = path.resolve(
      process.cwd(),
      'benchmarks',
      'reports',
      'outbox-benchmark-report.json',
    );
    await writeReportJson(reportPath, report);
    console.log(`Benchmark report JSON saved to: ${reportPath}\n`);

    return report;
  } finally {
    try {
      await resetDatabase(pool);
    } catch {
      // best-effort cleanup
    }
    await pool.close();
  }
}

const isDirectRun =
  process.argv[1] &&
  (process.argv[1].endsWith('runner.ts') || process.argv[1].endsWith('runner.js'));

if (isDirectRun) {
  runOutboxBenchmarks(process.argv.includes('--explain-only'))
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('Outbox benchmark execution failed:', err);
      process.exit(1);
    });
}
