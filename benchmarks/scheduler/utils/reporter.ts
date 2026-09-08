import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import type { BenchmarkRunResult } from './timer.js';

export interface EnvironmentInfo {
  readonly gitCommit: string;
  readonly gitBranch: string;
  readonly platform: string;
  readonly arch: string;
  readonly cpuModel: string;
  readonly cpuCores: number;
  readonly totalMemoryBytes: number;
  readonly freeMemoryBytes: number;
  readonly nodeVersion: string;
  readonly postgresVersion: string;
  readonly redisVersion: string;
  readonly timestamp: string;
}

export interface ExplainPlanResult {
  readonly queryName: string;
  readonly sql: string;
  readonly plan: readonly string[];
  readonly executionTimeMs?: number;
  readonly planningTimeMs?: number;
}

export interface BenchmarkReport {
  readonly environment: EnvironmentInfo;
  readonly configuration: {
    readonly seed: number;
    readonly warmupIterations: number;
    readonly microIterations: number;
    readonly componentIterations: number;
    readonly systemIterations: number;
  };
  readonly suites: {
    readonly micro: readonly BenchmarkRunResult[];
    readonly component: readonly BenchmarkRunResult[];
    readonly system: readonly BenchmarkRunResult[];
    readonly explain?: readonly ExplainPlanResult[];
  };
  readonly generatedAt: string;
}

/**
 * Formats an array of BenchmarkRunResult into an aligned Markdown table string.
 */
export function formatTable(results: readonly BenchmarkRunResult[]): string {
  if (results.length === 0) {
    return '*(No results)*\n';
  }

  const headers = [
    'Benchmark Name',
    'Samples',
    'Mean (ms)',
    'Median (ms)',
    'P95 (ms)',
    'P99 (ms)',
    'Min (ms)',
    'Max (ms)',
    'Ops/Sec',
  ];

  const rows = results.map((r) => [
    r.name,
    String(r.stats.count),
    r.stats.mean.toFixed(4),
    r.stats.median.toFixed(4),
    r.stats.p95.toFixed(4),
    r.stats.p99.toFixed(4),
    r.stats.min.toFixed(4),
    r.stats.max.toFixed(4),
    r.stats.opsPerSec.toLocaleString('en-US', { maximumFractionDigits: 1 }),
  ]);

  const colWidths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((row) => (row[i] ? row[i]!.length : 0))),
  );

  const formatRow = (cells: readonly string[]): string =>
    '| ' + cells.map((cell, i) => cell.padEnd(colWidths[i]!)).join(' | ') + ' |';

  const separator = '| ' + colWidths.map((width) => '-'.repeat(width)).join(' | ') + ' |';

  return [formatRow(headers), separator, ...rows.map(formatRow)].join('\n');
}

/**
 * Formats the full report into a readable console string.
 */
export function formatReportConsole(report: BenchmarkReport): string {
  const lines: string[] = [];

  lines.push('================================================================================');
  lines.push('                   FORGE V2 SCHEDULER BENCHMARK REPORT                         ');
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
  lines.push(`Redis:        ${report.environment.redisVersion}`);
  lines.push('================================================================================\n');

  lines.push('### Microbenchmarks (In-Memory Pure Algorithms)\n');
  lines.push(formatTable(report.suites.micro));
  lines.push('\n');

  lines.push('### Component Benchmarks (Queue, DB, Leases, Placement, Aging Comparison)\n');
  lines.push(formatTable(report.suites.component));
  lines.push('\n');

  lines.push('### System Benchmarks (End-to-End Batch Scheduling & Boundary Conditions)\n');
  lines.push(formatTable(report.suites.system));
  lines.push('\n');

  if (report.suites.explain && report.suites.explain.length > 0) {
    lines.push('### Query Explain Plans\n');
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

/**
 * Serializes and writes the JSON report to the filesystem.
 */
export async function writeJsonReport(filePath: string, report: BenchmarkReport): Promise<void> {
  const dir = path.dirname(filePath);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(report, null, 2), 'utf-8');
}
