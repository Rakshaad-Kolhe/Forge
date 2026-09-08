import { createDatabasePool, type DatabasePool } from '@forge/database';
import { benchmarkConfig } from './config.js';
import type { ExplainPlanResult } from './utils/reporter.js';

export async function runQueryExplainPlans(): Promise<ExplainPlanResult[]> {
  const results: ExplainPlanResult[] = [];
  let pool: DatabasePool | null = null;

  try {
    pool = createDatabasePool({ connectionString: benchmarkConfig.databaseUrl });

    // 1. Schedulable Jobs Query
    const schedulableSql = `
      EXPLAIN (ANALYZE, BUFFERS)
      SELECT id, pipeline_run_id, step_name, command, depends_on, requirements, priority, retry_policy, next_attempt_at, status, created_at
      FROM jobs
      WHERE status = 'QUEUED'
        AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
      ORDER BY priority DESC, created_at ASC
      LIMIT 50;
    `;
    const resSchedulable = await pool.query<{ 'QUERY PLAN': string }>(schedulableSql);
    const planSchedulable = resSchedulable.rows.map((r) => r['QUERY PLAN']);
    results.push(
      parseExplainPlan('PgJobRepository.findSchedulableJobs', schedulableSql, planSchedulable),
    );

    // 2. Active Worker Lease Lookup Query
    const leaseSql = `
      EXPLAIN (ANALYZE, BUFFERS)
      SELECT id, job_id, worker_id, status, duration_ms, acquired_at, renewed_at, expires_at, created_at,
             (expires_at <= NOW()) AS is_expired
      FROM worker_leases
      WHERE job_id = '00000000-0000-0000-0000-000000000000' AND status = 'ACTIVE';
    `;
    const resLease = await pool.query<{ 'QUERY PLAN': string }>(leaseSql);
    const planLease = resLease.rows.map((r) => r['QUERY PLAN']);
    results.push(
      parseExplainPlan('PgWorkerLeaseRepository.claim (active check)', leaseSql, planLease),
    );

    // 3. Job Lock Query (FOR UPDATE)
    const lockSql = `
      EXPLAIN (ANALYZE, BUFFERS)
      SELECT id, status FROM jobs WHERE id = '00000000-0000-0000-0000-000000000000' FOR UPDATE;
    `;
    const resLock = await pool.query<{ 'QUERY PLAN': string }>(lockSql);
    const planLock = resLock.rows.map((r) => r['QUERY PLAN']);
    results.push(parseExplainPlan('PgWorkerLeaseRepository.claim (job lock)', lockSql, planLock));
  } catch (err) {
    // If DB is unreachable or explain fails, capture diagnostic note
    results.push({
      queryName: 'Explain Plan Error',
      sql: 'N/A',
      plan: [`Explain plan execution failed: ${(err as Error).message}`],
    });
  } finally {
    if (pool) {
      try {
        await pool.close();
      } catch {
        // ignore close error
      }
    }
  }

  return results;
}

function parseExplainPlan(
  queryName: string,
  sql: string,
  planLines: readonly string[],
): ExplainPlanResult {
  let planningTimeMs: number | undefined;
  let executionTimeMs: number | undefined;

  for (const line of planLines) {
    const planningMatch = line.match(/Planning Time:\s*([0-9.]+)\s*ms/i);
    if (planningMatch) {
      planningTimeMs = parseFloat(planningMatch[1]!);
    }
    const execMatch = line.match(/Execution Time:\s*([0-9.]+)\s*ms/i);
    if (execMatch) {
      executionTimeMs = parseFloat(execMatch[1]!);
    }
  }

  return {
    queryName,
    sql: sql.trim(),
    plan: planLines,
    planningTimeMs,
    executionTimeMs,
  };
}
