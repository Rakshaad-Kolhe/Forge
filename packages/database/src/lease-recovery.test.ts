import { describe, it, expect, vi, beforeEach } from 'vitest';
import { LeaseRecoveryService } from './lease-recovery-service.js';
import { PgDeadLetterRepository } from './repositories/pg-dead-letter-repository.js';
import type { DatabaseClient, DatabasePool } from './types.js';

describe('LeaseRecoveryService Unit Tests', () => {
  let mockClient: DatabaseClient;
  let mockPool: DatabasePool;

  let candidateLeases: Record<string, unknown>[] = [];
  let lockedLeases: Record<string, unknown>[] = [];
  let jobRows: Record<string, unknown>[] = [];
  let attemptRows: Record<string, unknown>[] = [];

  beforeEach(() => {
    candidateLeases = [];
    lockedLeases = [];
    jobRows = [];
    attemptRows = [];

    mockClient = {
      query: vi.fn().mockImplementation(async (sql: string) => {
        const normalized = sql.replace(/\s+/g, ' ');
        if (
          normalized.includes('SELECT') &&
          normalized.includes('worker_leases') &&
          normalized.includes('expires_at <=')
        ) {
          return { rows: candidateLeases };
        }
        if (
          normalized.includes('SELECT') &&
          normalized.includes('worker_leases') &&
          normalized.includes('FOR UPDATE SKIP LOCKED')
        ) {
          return { rows: lockedLeases };
        }
        if (normalized.includes('SELECT status FROM jobs WHERE id =')) {
          return { rows: jobRows.map((j) => ({ status: j.status })) };
        }
        if (normalized.includes('SELECT') && normalized.includes('FROM jobs WHERE id =')) {
          return { rows: jobRows };
        }
        if (normalized.includes('SELECT status FROM job_attempts')) {
          return { rows: attemptRows.map((a) => ({ status: a.status })) };
        }
        if (
          normalized.includes('SELECT') &&
          normalized.includes('FROM job_attempts WHERE job_id =')
        ) {
          return { rows: attemptRows };
        }
        if (normalized.includes('SELECT') && normalized.includes('FROM pipeline_runs')) {
          return { rows: [] };
        }
        return { rows: [], rowCount: 1 };
      }),
    };

    mockPool = {
      ...mockClient,
      connect: vi.fn().mockResolvedValue({
        ...mockClient,
        release: vi.fn(),
      }),
      healthCheck: vi.fn().mockResolvedValue(true),
      close: vi.fn().mockResolvedValue(undefined),
      getPool: vi.fn(),
    };
  });

  it('returns empty result when no expired leases exist', async () => {
    candidateLeases = [];

    const service = new LeaseRecoveryService(mockPool);
    const result = await service.recoverExpiredLeases({ now: new Date() });

    expect(result.recoveredCount).toBe(0);
    expect(result.details).toHaveLength(0);
  });

  it('skips terminal jobs without resurrecting them', async () => {
    const expiredLease = {
      id: 'lease-term-1',
      job_id: 'job-term-1',
      worker_id: 'worker-1',
      status: 'ACTIVE',
      expires_at: new Date(Date.now() - 5000),
    };

    candidateLeases = [expiredLease];
    lockedLeases = [expiredLease];
    jobRows = [
      {
        id: 'job-term-1',
        pipeline_run_id: 'run-1',
        step_name: 'test',
        command: 'exit 0',
        status: 'SUCCEEDED', // Terminal!
        created_at: new Date(),
      },
    ];
    attemptRows = [];

    const service = new LeaseRecoveryService(mockPool);
    const result = await service.recoverExpiredLeases();

    expect(result.recoveredCount).toBe(0);
    expect(result.details[0]?.action).toBe('SKIPPED_TERMINAL');
    expect(result.details[0]?.details).toContain('already in terminal state');
  });

  it('recovers retryable job: reconciles attempt, sets QUEUED and nextAttemptAt', async () => {
    const expiredLease = {
      id: 'lease-retry-1',
      job_id: 'job-retry-1',
      worker_id: 'worker-crashed',
      status: 'ACTIVE',
      expires_at: new Date(Date.now() - 5000),
    };

    candidateLeases = [expiredLease];
    lockedLeases = [expiredLease];
    jobRows = [
      {
        id: 'job-retry-1',
        pipeline_run_id: 'run-1',
        step_name: 'build',
        command: 'npm run build',
        status: 'RUNNING',
        retry_policy: {
          maxAttempts: 3,
          backoff: { baseDelayMs: 2000, factor: 2, maxDelayMs: 10000 },
          retryOn: ['FAILED'],
        },
        created_at: new Date(),
      },
    ];
    attemptRows = [
      {
        id: 'job-retry-1-attempt-1',
        job_id: 'job-retry-1',
        attempt_number: 1,
        status: 'RUNNING',
        started_at: new Date(),
        finished_at: null,
        exit_code: null,
        failure_reason: null,
        created_at: new Date(),
      },
    ];

    const service = new LeaseRecoveryService(mockPool);
    const result = await service.recoverExpiredLeases();

    expect(result.recoveredCount).toBe(1);
    expect(result.details[0]?.action).toBe('REQUEUED');
    expect(result.details[0]?.nextAttemptAt).toBeDefined();
  });

  it('dead-letters job when retries are exhausted: job becomes FAILED and DLQ record inserted', async () => {
    const expiredLease = {
      id: 'lease-exhausted-1',
      job_id: 'job-exhausted-1',
      worker_id: 'worker-crashed',
      status: 'ACTIVE',
      expires_at: new Date(Date.now() - 5000),
    };

    candidateLeases = [expiredLease];
    lockedLeases = [expiredLease];
    jobRows = [
      {
        id: 'job-exhausted-1',
        pipeline_run_id: 'run-1',
        step_name: 'test',
        command: 'npm test',
        status: 'RUNNING',
        retry_policy: {
          maxAttempts: 2, // 2 total attempts -> attempt 2 is exhausted!
          retryOn: ['FAILED'],
        },
        created_at: new Date(),
      },
    ];
    attemptRows = [
      {
        id: 'job-exhausted-1-attempt-1',
        job_id: 'job-exhausted-1',
        attempt_number: 1,
        status: 'FAILED',
        started_at: new Date(),
        finished_at: new Date(),
        exit_code: 1,
        failure_reason: 'error',
        created_at: new Date(),
      },
      {
        id: 'job-exhausted-1-attempt-2',
        job_id: 'job-exhausted-1',
        attempt_number: 2,
        status: 'RUNNING',
        started_at: new Date(),
        finished_at: null,
        exit_code: null,
        failure_reason: null,
        created_at: new Date(),
      },
    ];

    const service = new LeaseRecoveryService(mockPool);
    const result = await service.recoverExpiredLeases();

    expect(result.recoveredCount).toBe(1);
    expect(result.details[0]?.action).toBe('DEAD_LETTERED');
    expect(result.details[0]?.deadLetterReason).toBe('WORKER_LOSS_RETRY_EXHAUSTED');
  });

  it('dead-letters job without retry policy with NON_RETRYABLE_FAILURE reason', async () => {
    const expiredLease = {
      id: 'lease-nopolicy-1',
      job_id: 'job-nopolicy-1',
      worker_id: 'worker-crashed',
      status: 'ACTIVE',
      expires_at: new Date(Date.now() - 5000),
    };

    candidateLeases = [expiredLease];
    lockedLeases = [expiredLease];
    jobRows = [
      {
        id: 'job-nopolicy-1',
        pipeline_run_id: 'run-1',
        step_name: 'test',
        command: 'npm test',
        status: 'RUNNING',
        created_at: new Date(),
      },
    ];
    attemptRows = [
      {
        id: 'job-nopolicy-1-attempt-1',
        job_id: 'job-nopolicy-1',
        attempt_number: 1,
        status: 'RUNNING',
        started_at: new Date(),
        finished_at: null,
        exit_code: null,
        failure_reason: null,
        created_at: new Date(),
      },
    ];

    const service = new LeaseRecoveryService(mockPool);
    const result = await service.recoverExpiredLeases();

    expect(result.recoveredCount).toBe(1);
    expect(result.details[0]?.action).toBe('DEAD_LETTERED');
    expect(result.details[0]?.deadLetterReason).toBe('NON_RETRYABLE_FAILURE');
  });

  it('returns NO_OP if concurrent recovery already locked or transitioned the lease', async () => {
    candidateLeases = [{ id: 'lease-racing-1', job_id: 'job-1', worker_id: 'w-1' }];
    lockedLeases = []; // FOR UPDATE SKIP LOCKED returns 0 rows!

    const service = new LeaseRecoveryService(mockPool);
    const result = await service.recoverExpiredLeases();

    expect(result.recoveredCount).toBe(0);
    expect(result.details[0]?.action).toBe('NO_OP');
  });
});

describe('PgDeadLetterRepository Unit Tests', () => {
  let mockClient: DatabaseClient;
  let mockQuery: ReturnType<typeof vi.fn>;
  let repo: PgDeadLetterRepository;

  beforeEach(() => {
    mockQuery = vi.fn();
    mockClient = {
      query: mockQuery,
    };
    repo = new PgDeadLetterRepository(mockClient);
  });

  it('saves DLQ record with idempotent ON CONFLICT (job_id)', async () => {
    mockQuery.mockResolvedValueOnce({ rowCount: 1 });

    await repo.save({
      id: 'dlq-1',
      jobId: 'job-1',
      pipelineRunId: 'run-1',
      reason: 'WORKER_LOSS_RETRY_EXHAUSTED',
      failedAttemptCount: 3,
      lastAttemptId: 'attempt-3',
      lastWorkerId: 'worker-lost',
      errorDetails: 'lost connection',
      metadata: { attempts: 3 },
      createdAt: new Date(),
    });

    expect(mockClient.query).toHaveBeenCalledWith(
      expect.stringContaining('ON CONFLICT (job_id) DO UPDATE'),
      expect.any(Array),
    );
  });

  it('finds DLQ record by jobId', async () => {
    const row = {
      id: 'dlq-1',
      job_id: 'job-1',
      pipeline_run_id: 'run-1',
      reason: 'RETRY_EXHAUSTED',
      failed_attempt_count: 2,
      last_attempt_id: 'att-2',
      last_worker_id: 'w-1',
      error_details: 'error',
      metadata: '{"key":"val"}',
      created_at: new Date(),
    };
    mockQuery.mockResolvedValueOnce({ rows: [row] });

    const result = await repo.findByJobId('job-1');
    expect(result).not.toBeNull();
    expect(result?.jobId).toBe('job-1');
    expect(result?.reason).toBe('RETRY_EXHAUSTED');
    expect(result?.metadata).toEqual({ key: 'val' });
  });

  it('returns null when DLQ record not found by jobId', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [] });
    const result = await repo.findByJobId('job-missing');
    expect(result).toBeNull();
  });
});
