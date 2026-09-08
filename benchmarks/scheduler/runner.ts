import * as os from 'node:os';
import * as path from 'node:path';
import { execSync } from 'node:child_process';
import { createDatabasePool, runMigrations } from '@forge/database';
import { createRedisClient } from '@forge/redis';
import { benchmarkConfig } from './config.js';
import { runMicroBenchmarks } from './suites/micro.bench.js';
import { runComponentBenchmarks } from './suites/component.bench.js';
import { runSystemBenchmarks } from './suites/system.bench.js';
import { runQueryExplainPlans } from './explain.js';
import {
  formatReportConsole,
  writeJsonReport,
  type BenchmarkReport,
  type EnvironmentInfo,
} from './utils/reporter.js';

async function collectEnvironmentInfo(): Promise<EnvironmentInfo> {
  let gitCommit = 'unknown';
  let gitBranch = 'unknown';
  try {
    gitCommit = execSync('git rev-parse HEAD', { encoding: 'utf-8' }).trim();
    gitBranch = execSync('git rev-parse --abbrev-ref HEAD', { encoding: 'utf-8' }).trim();
  } catch {
    // Git commands may fail in bare environment
  }

  let postgresVersion = 'unknown';
  try {
    const pool = createDatabasePool({ connectionString: benchmarkConfig.databaseUrl });
    await runMigrations(pool);
    const res = await pool.query<{ version: string }>('SELECT version();');
    postgresVersion = res.rows[0]?.version?.split(' on ')[0] ?? 'unknown';
    await pool.close();
  } catch {
    postgresVersion = 'unavailable';
  }

  let redisVersion = 'unknown';
  try {
    const redis = createRedisClient({
      url: benchmarkConfig.redisUrl,
      connectTimeoutMillis: 3000,
      maxRetriesPerRequest: 1,
    });
    await redis.connect();
    const info = await redis.getRawClient().info('server');
    const match = info.match(/redis_version:([^\r\n]+)/);
    if (match) {
      redisVersion = match[1]!.trim();
    }
    await redis.close();
  } catch {
    redisVersion = 'unavailable';
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
    redisVersion,
    timestamp: new Date().toISOString(),
  };
}

export async function runAllBenchmarks(): Promise<BenchmarkReport> {
  console.log('Collecting environment information...');
  const environment = await collectEnvironmentInfo();

  console.log(`Starting Scheduler Benchmark Suite (Seed=${benchmarkConfig.seed})...\n`);

  console.log('-> Running Microbenchmarks...');
  const microResults = await runMicroBenchmarks();
  console.log(`   Completed ${microResults.length} microbenchmarks.\n`);

  console.log('-> Running Component Benchmarks...');
  const componentResults = await runComponentBenchmarks();
  console.log(`   Completed ${componentResults.length} component benchmarks.\n`);

  console.log('-> Running System Benchmarks...');
  const systemResults = await runSystemBenchmarks();
  console.log(`   Completed ${systemResults.length} system benchmarks.\n`);

  console.log('-> Running Query EXPLAIN Plans...');
  const explainResults = await runQueryExplainPlans();
  console.log(`   Captured ${explainResults.length} explain plans.\n`);

  const report: BenchmarkReport = {
    environment,
    configuration: {
      seed: benchmarkConfig.seed,
      warmupIterations: benchmarkConfig.warmup.micro,
      microIterations: benchmarkConfig.iterations.micro,
      componentIterations: benchmarkConfig.iterations.component,
      systemIterations: benchmarkConfig.iterations.system,
    },
    suites: {
      micro: microResults,
      component: componentResults,
      system: systemResults,
      explain: explainResults,
    },
    generatedAt: new Date().toISOString(),
  };

  const consoleOutput = formatReportConsole(report);
  console.log(consoleOutput);

  const reportPath = path.resolve(
    process.cwd(),
    'benchmarks',
    'reports',
    'scheduler-benchmark-report.json',
  );
  await writeJsonReport(reportPath, report);
  console.log(`Benchmark report JSON saved to: ${reportPath}\n`);

  return report;
}

const isDirectRun =
  process.argv[1] &&
  (process.argv[1].endsWith('runner.ts') || process.argv[1].endsWith('runner.js'));

if (isDirectRun) {
  runAllBenchmarks()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('Benchmark execution failed:', err);
      process.exit(1);
    });
}
