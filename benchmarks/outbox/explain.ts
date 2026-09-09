/**
 * Claim-query plan capture for the outbox dispatcher.
 *
 * Seeds `explainSeedRows` PENDING rows with PRNG-varied `occurred_at`, then runs
 * `EXPLAIN (ANALYZE, BUFFERS)` over the exact CTE `SELECT` that
 * `PgOutboxRepository.claimBatch` uses to pick claimable rows (`FOR UPDATE SKIP LOCKED`).
 * The captured plan text reveals whether the partial index `idx_outbox_events_claimable`
 * is used.
 */
import type { DatabasePool } from '@forge/database';
import type { ExplainPlanResult } from '../scheduler/utils/reporter.js';
import { createSeededRandom } from '../scheduler/utils/prng.js';
import { outboxBenchmarkConfig } from './config.js';
import { deterministicTimestamp, makeJobStartedInput } from './utils/fixtures.js';

const CLAIMABLE_INDEX = 'idx_outbox_events_claimable';

/**
 * The claimable-row selection from `PgOutboxRepository.claimBatch`, reproduced verbatim:
 * the `WITH claimable AS (...)` body, with `LIMIT 100` and `FOR UPDATE SKIP LOCKED`.
 * `$1` is the stale-CLAIMED cutoff (the repository binds this as `$2`).
 */
const CLAIM_SELECT_SQL = `
EXPLAIN (ANALYZE, BUFFERS)
SELECT id AS claim_id
FROM outbox_events
WHERE (status = 'PENDING' AND available_at <= NOW())
   OR (status = 'CLAIMED' AND claimed_at < $1)
ORDER BY occurred_at, id
LIMIT ${outboxBenchmarkConfig.claimQueryLimit}
FOR UPDATE SKIP LOCKED;
`;

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

async function seedPendingRows(pool: DatabasePool): Promise<void> {
  const rng = createSeededRandom(outboxBenchmarkConfig.seed + 1);
  const rows = outboxBenchmarkConfig.explainSeedRows;

  const ids: string[] = [];
  const eventIds: string[] = [];
  const occurredAts: string[] = [];
  const payloads: string[] = [];

  for (let i = 0; i < rows; i += 1) {
    const input = makeJobStartedInput(rng);
    ids.push(`bench_explain_${i}`);
    eventIds.push(input.eventId);
    occurredAts.push(deterministicTimestamp(rng, 30 * 24 * 60 * 60 * 1000));
    payloads.push(JSON.stringify(input.payload));
  }

  await pool.query('DELETE FROM outbox_events;');
  await pool.query(
    `
    INSERT INTO outbox_events (
      id, event_id, event_type, version, occurred_at,
      payload, status, delivery_attempt_count, dispatch_count, available_at, created_at
    )
    SELECT
      u.id, u.event_id, 'JobStarted', 1, u.occurred_at::timestamptz,
      u.payload::jsonb, 'PENDING', 0, 0, NOW(), NOW()
    FROM (
      SELECT
        unnest($1::text[]) AS id,
        unnest($2::text[]) AS event_id,
        unnest($3::text[]) AS occurred_at,
        unnest($4::text[]) AS payload
    ) AS u;
    `,
    [ids, eventIds, occurredAts, payloads],
  );
  await pool.query('ANALYZE outbox_events;');
}

export async function runOutboxExplain(pool: DatabasePool): Promise<ExplainPlanResult[]> {
  const results: ExplainPlanResult[] = [];

  try {
    await seedPendingRows(pool);

    const staleCutoff = deterministicTimestamp(
      createSeededRandom(outboxBenchmarkConfig.seed + 2),
      1,
    );
    const res = await pool.query<{ 'QUERY PLAN': string }>(CLAIM_SELECT_SQL, [staleCutoff]);
    const planLines = res.rows.map((r) => r['QUERY PLAN']);

    const parsed = parseExplainPlan(
      `PgOutboxRepository.claimBatch (claimable CTE SELECT, seeded ${outboxBenchmarkConfig.explainSeedRows} PENDING rows)`,
      CLAIM_SELECT_SQL,
      planLines,
    );
    const usesClaimableIndex = planLines.some((line) => line.includes(CLAIMABLE_INDEX));
    results.push({
      ...parsed,
      plan: [
        ...parsed.plan,
        '',
        `-- partial index ${CLAIMABLE_INDEX} used: ${usesClaimableIndex ? 'YES' : 'NO'}`,
      ],
    });
  } catch (err) {
    results.push({
      queryName: 'Outbox Explain Plan Error',
      sql: CLAIM_SELECT_SQL.trim(),
      plan: [`Explain plan execution failed: ${(err as Error).message}`],
    });
  }

  return results;
}
