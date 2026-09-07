import { describe, expect, it } from 'vitest';
import { RetryPolicyValidationError } from './errors.js';
import { JobAttempt } from './job-attempt.js';
import { calculateBackoffDelay, evaluateRetry, validateRetryPolicy } from './retry.js';
import { createJobAttemptId, createJobId } from './types.js';

describe('Retry Policy Validation (validateRetryPolicy)', () => {
  it('returns undefined when policy is not defined', () => {
    expect(validateRetryPolicy(undefined)).toBeUndefined();
  });

  it('validates and normalizes valid policy with defaults', () => {
    const policy = validateRetryPolicy({
      maxAttempts: 3,
    });

    expect(policy).toBeDefined();
    expect(policy?.maxAttempts).toBe(3);
    expect(policy?.backoff).toEqual({
      baseDelayMs: 1000,
      maxDelayMs: 60000,
      factor: 2,
    });
    expect(policy?.retryOn).toEqual(['FAILED', 'TIMED_OUT']);
  });

  it('preserves custom backoff and retryOn settings', () => {
    const policy = validateRetryPolicy({
      maxAttempts: 5,
      backoff: {
        baseDelayMs: 500,
        maxDelayMs: 10000,
        factor: 3,
      },
      retryOn: ['FAILED'],
    });

    expect(policy).toEqual({
      maxAttempts: 5,
      backoff: {
        baseDelayMs: 500,
        maxDelayMs: 10000,
        factor: 3,
      },
      retryOn: ['FAILED'],
    });
  });

  it('rejects invalid maxAttempts values', () => {
    expect(() => validateRetryPolicy({ maxAttempts: 0 })).toThrow(RetryPolicyValidationError);
    expect(() => validateRetryPolicy({ maxAttempts: -1 })).toThrow(RetryPolicyValidationError);
    expect(() => validateRetryPolicy({ maxAttempts: 2.5 })).toThrow(RetryPolicyValidationError);
    expect(() => validateRetryPolicy({ maxAttempts: NaN })).toThrow(RetryPolicyValidationError);
    expect(() => validateRetryPolicy({ maxAttempts: Infinity })).toThrow(
      RetryPolicyValidationError,
    );
  });

  it('rejects maxAttempts exceeding maxAttemptsCap', () => {
    expect(() => validateRetryPolicy({ maxAttempts: 15 }, { maxAttemptsCap: 10 })).toThrow(
      RetryPolicyValidationError,
    );
  });

  it('rejects invalid backoff parameters', () => {
    // baseDelay negative
    expect(() =>
      validateRetryPolicy({
        maxAttempts: 3,
        backoff: { baseDelayMs: -10, maxDelayMs: 1000 },
      }),
    ).toThrow(RetryPolicyValidationError);

    // maxDelay < baseDelay
    expect(() =>
      validateRetryPolicy({
        maxAttempts: 3,
        backoff: { baseDelayMs: 5000, maxDelayMs: 2000 },
      }),
    ).toThrow(RetryPolicyValidationError);

    // maxDelay exceeds cap
    expect(() =>
      validateRetryPolicy(
        {
          maxAttempts: 3,
          backoff: { baseDelayMs: 1000, maxDelayMs: 7200000 },
        },
        { maxBackoffMs: 3600000 },
      ),
    ).toThrow(RetryPolicyValidationError);

    // factor < 1
    expect(() =>
      validateRetryPolicy({
        maxAttempts: 3,
        backoff: { baseDelayMs: 1000, maxDelayMs: 5000, factor: 0.5 },
      }),
    ).toThrow(RetryPolicyValidationError);
  });

  it('rejects invalid retryOn conditions', () => {
    expect(() =>
      validateRetryPolicy({
        maxAttempts: 3,
        retryOn: ['UNKNOWN_STATUS' as unknown as 'FAILED'],
      }),
    ).toThrow(RetryPolicyValidationError);
  });
});

describe('Exponential Backoff Calculation (calculateBackoffDelay)', () => {
  const backoff = {
    baseDelayMs: 1000,
    maxDelayMs: 10000,
    factor: 2,
  };

  it('calculates deterministic exponential backoff delays', () => {
    // Attempt 1: 1000 * 2^0 = 1000ms
    expect(calculateBackoffDelay(1, backoff)).toBe(1000);

    // Attempt 2: 1000 * 2^1 = 2000ms
    expect(calculateBackoffDelay(2, backoff)).toBe(2000);

    // Attempt 3: 1000 * 2^2 = 4000ms
    expect(calculateBackoffDelay(3, backoff)).toBe(4000);

    // Attempt 4: 1000 * 2^3 = 8000ms
    expect(calculateBackoffDelay(4, backoff)).toBe(8000);

    // Attempt 5: 1000 * 2^4 = 16000ms -> capped at 10000ms
    expect(calculateBackoffDelay(5, backoff)).toBe(10000);
  });

  it('enforces maximum delay cap', () => {
    expect(calculateBackoffDelay(10, backoff)).toBe(10000);
  });

  it('protects against numeric overflow and huge exponents', () => {
    // Very high attempt number
    const hugeDelay = calculateBackoffDelay(100, backoff);
    expect(Number.isFinite(hugeDelay)).toBe(true);
    expect(hugeDelay).toBe(10000);
    expect(hugeDelay).toBeGreaterThanOrEqual(0);
  });

  it('handles custom backoff factor', () => {
    const custom = {
      baseDelayMs: 500,
      maxDelayMs: 50000,
      factor: 3,
    };

    expect(calculateBackoffDelay(1, custom)).toBe(500); // 500 * 3^0
    expect(calculateBackoffDelay(2, custom)).toBe(1500); // 500 * 3^1
    expect(calculateBackoffDelay(3, custom)).toBe(4500); // 500 * 3^2
  });
});

describe('Pure Retry Evaluation (evaluateRetry)', () => {
  function makeAttempt(
    attemptNumber: number,
    status: 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'TIMED_OUT' | 'CANCELLED',
  ) {
    const attempt = new JobAttempt({
      id: createJobAttemptId(`att-${attemptNumber}`),
      jobId: createJobId('job-1'),
      attemptNumber,
    });
    attempt.start();
    if (status === 'SUCCEEDED') attempt.succeed(0);
    if (status === 'FAILED') attempt.fail(1, 'Command failed');
    if (status === 'TIMED_OUT') attempt.timeout();
    if (status === 'CANCELLED') attempt.cancel();
    return attempt;
  }

  const policy = {
    maxAttempts: 3,
    backoff: {
      baseDelayMs: 1000,
      maxDelayMs: 8000,
      factor: 2,
    },
    retryOn: ['FAILED', 'TIMED_OUT'] as const,
  };

  it('returns NOT_RETRYABLE if no policy is provided', () => {
    const attempt = makeAttempt(1, 'FAILED');
    const decision = evaluateRetry(attempt, undefined);
    expect(decision.action).toBe('NOT_RETRYABLE');
    expect(decision.reason).toBe('NO_POLICY');
  });

  it('returns NOT_RETRYABLE for successful attempts', () => {
    const attempt = makeAttempt(1, 'SUCCEEDED');
    const decision = evaluateRetry(attempt, policy);
    expect(decision.action).toBe('NOT_RETRYABLE');
    expect(decision.reason).toBe('SUCCEEDED');
  });

  it('returns NOT_RETRYABLE for cancelled attempts', () => {
    const attempt = makeAttempt(1, 'CANCELLED');
    const decision = evaluateRetry(attempt, policy);
    expect(decision.action).toBe('NOT_RETRYABLE');
    expect(decision.reason).toBe('CANCELLED');
  });

  it('returns RETRY on first failure when maxAttempts is 3', () => {
    const attempt = makeAttempt(1, 'FAILED');
    const decision = evaluateRetry(attempt, policy);
    expect(decision.action).toBe('RETRY');
    if (decision.action === 'RETRY') {
      expect(decision.attemptNumber).toBe(1);
      expect(decision.nextAttemptNumber).toBe(2);
      expect(decision.delayMs).toBe(1000);
    }
  });

  it('returns RETRY on second failure with exponential backoff delay', () => {
    const attempt = makeAttempt(2, 'FAILED');
    const decision = evaluateRetry(attempt, policy);
    expect(decision.action).toBe('RETRY');
    if (decision.action === 'RETRY') {
      expect(decision.attemptNumber).toBe(2);
      expect(decision.nextAttemptNumber).toBe(3);
      expect(decision.delayMs).toBe(2000);
    }
  });

  it('returns RETRY for TIMED_OUT attempts when retryOn includes TIMED_OUT', () => {
    const attempt = makeAttempt(1, 'TIMED_OUT');
    const decision = evaluateRetry(attempt, policy);
    expect(decision.action).toBe('RETRY');
    if (decision.action === 'RETRY') {
      expect(decision.attemptNumber).toBe(1);
      expect(decision.nextAttemptNumber).toBe(2);
    }
  });

  it('returns FINAL_FAILURE when max attempts are reached (attempt 3/3)', () => {
    const attempt = makeAttempt(3, 'FAILED');
    const decision = evaluateRetry(attempt, policy);
    expect(decision.action).toBe('FINAL_FAILURE');
    if (decision.action === 'FINAL_FAILURE') {
      expect(decision.reason).toBe('MAX_ATTEMPTS_EXHAUSTED');
    }
  });

  it('returns FINAL_FAILURE when outcome is not configured in retryOn', () => {
    const failOnlyPolicy = {
      maxAttempts: 3,
      retryOn: ['FAILED'] as const,
    };
    const attempt = makeAttempt(1, 'TIMED_OUT');
    const decision = evaluateRetry(attempt, failOnlyPolicy);
    expect(decision.action).toBe('FINAL_FAILURE');
    if (decision.action === 'FINAL_FAILURE') {
      expect(decision.reason).toBe('OUTCOME_NOT_RETRYABLE');
    }
  });

  it('is completely idempotent (repeated calls produce identical decision)', () => {
    const attempt = makeAttempt(1, 'FAILED');
    const d1 = evaluateRetry(attempt, policy);
    const d2 = evaluateRetry(attempt, policy);
    const d3 = evaluateRetry(attempt, policy);
    expect(d1).toEqual(d2);
    expect(d2).toEqual(d3);
  });
});
