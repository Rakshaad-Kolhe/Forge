import {
  DEFAULT_MAX_BACKOFF_MS,
  DEFAULT_RETRY_BASE_DELAY_MS,
  MAX_JOB_ATTEMPTS_LIMIT,
  MAX_RETRY_BACKOFF_LIMIT_MS,
  type BackoffPolicy,
  type RetryCondition,
  type RetryDecision,
  type RetryPolicy,
} from '@forge/contracts';
import { RetryPolicyValidationError } from './errors.js';
import type { JobAttempt } from './job-attempt.js';

export interface RetryValidationLimits {
  readonly maxAttemptsCap?: number;
  readonly maxBackoffMs?: number;
}

const VALID_RETRY_CONDITIONS: readonly RetryCondition[] = ['FAILED', 'TIMED_OUT'];

/**
 * Validates a declared RetryPolicy against system limits and structural constraints.
 * Returns an immutable, frozen, normalized policy or undefined if no policy was declared.
 *
 * @throws {RetryPolicyValidationError} If policy constraints are violated
 */
export function validateRetryPolicy(
  policy?: RetryPolicy,
  limits?: RetryValidationLimits,
): RetryPolicy | undefined {
  if (!policy) {
    return undefined;
  }

  const issues: string[] = [];
  const maxAttemptsCap = limits?.maxAttemptsCap ?? MAX_JOB_ATTEMPTS_LIMIT;
  const maxBackoffLimit = limits?.maxBackoffMs ?? MAX_RETRY_BACKOFF_LIMIT_MS;

  // 1. Validate maxAttempts
  if (
    typeof policy.maxAttempts !== 'number' ||
    !Number.isFinite(policy.maxAttempts) ||
    !Number.isInteger(policy.maxAttempts) ||
    policy.maxAttempts < 1
  ) {
    issues.push(
      `Retry maxAttempts must be a positive integer >= 1, received: ${String(policy.maxAttempts)}`,
    );
  } else if (policy.maxAttempts > maxAttemptsCap) {
    issues.push(
      `Retry maxAttempts (${policy.maxAttempts}) exceeds maximum permitted limit of ${maxAttemptsCap}`,
    );
  }

  // 2. Validate backoff if provided
  let normalizedBackoff: BackoffPolicy = {
    baseDelayMs: DEFAULT_RETRY_BASE_DELAY_MS,
    maxDelayMs: DEFAULT_MAX_BACKOFF_MS,
    factor: 2,
  };

  if (policy.backoff) {
    const { baseDelayMs, maxDelayMs, factor } = policy.backoff;

    if (
      typeof baseDelayMs !== 'number' ||
      !Number.isFinite(baseDelayMs) ||
      !Number.isInteger(baseDelayMs) ||
      baseDelayMs < 0
    ) {
      issues.push(
        `Backoff baseDelayMs must be a non-negative integer, received: ${String(baseDelayMs)}`,
      );
    }

    if (
      typeof maxDelayMs !== 'number' ||
      !Number.isFinite(maxDelayMs) ||
      !Number.isInteger(maxDelayMs) ||
      maxDelayMs < 0
    ) {
      issues.push(
        `Backoff maxDelayMs must be a non-negative integer, received: ${String(maxDelayMs)}`,
      );
    } else if (maxDelayMs > maxBackoffLimit) {
      issues.push(
        `Backoff maxDelayMs (${maxDelayMs}) exceeds maximum permitted bound of ${maxBackoffLimit}`,
      );
    }

    if (
      typeof baseDelayMs === 'number' &&
      typeof maxDelayMs === 'number' &&
      Number.isFinite(baseDelayMs) &&
      Number.isFinite(maxDelayMs) &&
      maxDelayMs < baseDelayMs
    ) {
      issues.push(
        `Backoff maxDelayMs (${maxDelayMs}) must be greater than or equal to baseDelayMs (${baseDelayMs})`,
      );
    }

    if (factor !== undefined) {
      if (typeof factor !== 'number' || !Number.isFinite(factor) || factor < 1) {
        issues.push(`Backoff factor must be a finite number >= 1, received: ${String(factor)}`);
      }
    }

    if (issues.length === 0) {
      normalizedBackoff = {
        baseDelayMs,
        maxDelayMs,
        factor: factor ?? 2,
      };
    }
  }

  // 3. Validate retryOn conditions
  let normalizedRetryOn: readonly RetryCondition[] = ['FAILED', 'TIMED_OUT'];
  if (policy.retryOn) {
    if (!Array.isArray(policy.retryOn)) {
      issues.push('Retry retryOn must be an array of RetryCondition');
    } else {
      for (const cond of policy.retryOn) {
        if (!VALID_RETRY_CONDITIONS.includes(cond)) {
          issues.push(
            `Invalid RetryCondition "${String(cond)}". Allowed values: ${VALID_RETRY_CONDITIONS.join(', ')}`,
          );
        }
      }
      if (issues.length === 0) {
        normalizedRetryOn = Object.freeze([...new Set(policy.retryOn)]);
      }
    }
  }

  if (issues.length > 0) {
    throw new RetryPolicyValidationError(
      `Invalid retry policy: ${issues.join('; ')}`,
      Object.freeze(issues),
    );
  }

  return Object.freeze({
    maxAttempts: policy.maxAttempts,
    backoff: Object.freeze(normalizedBackoff),
    retryOn: normalizedRetryOn,
  });
}

/**
 * Pure calculation of exponential backoff delay for an attempt number.
 *
 * Formula:
 *   delay = min(maxDelayMs, baseDelayMs * (factor ^ (attemptNumber - 1)))
 *
 * Protects against numeric overflow (Infinity, NaN, large exponents) and guarantees
 * finite, non-negative, bounded return value.
 *
 * @param attemptNumber - The completed attempt number (1-indexed).
 * @param backoff - Backoff configuration parameters.
 */
export function calculateBackoffDelay(attemptNumber: number, backoff?: BackoffPolicy): number {
  const baseDelayMs = backoff?.baseDelayMs ?? DEFAULT_RETRY_BASE_DELAY_MS;
  const maxDelayMs = backoff?.maxDelayMs ?? DEFAULT_MAX_BACKOFF_MS;
  const factor = backoff?.factor ?? 2;

  if (attemptNumber <= 1) {
    return Math.min(maxDelayMs, Math.max(0, baseDelayMs));
  }

  // Exponent cap: 2^30 exceeds 10^9 ms (~11 days), avoiding any potential integer/float overflow
  const exponent = Math.min(30, Math.max(0, attemptNumber - 1));
  const multiplier = Math.pow(factor, exponent);

  if (!Number.isFinite(multiplier) || multiplier >= Number.MAX_SAFE_INTEGER / baseDelayMs) {
    return maxDelayMs;
  }

  const rawDelay = baseDelayMs * multiplier;
  if (!Number.isFinite(rawDelay)) {
    return maxDelayMs;
  }

  return Math.min(maxDelayMs, Math.max(0, Math.round(rawDelay)));
}

/**
 * Pure, deterministic evaluation of retry eligibility for a completed job attempt.
 *
 * Answers solely:
 *   "Should another attempt occur, and if so, after what delay?"
 *
 * Has zero side-effects, performs no I/O, does not modify state, and is completely idempotent.
 */
export function evaluateRetry(attempt: JobAttempt, policy?: RetryPolicy): RetryDecision {
  const attemptNumber = attempt.attemptNumber;

  if (!policy) {
    return {
      action: 'NOT_RETRYABLE',
      attemptNumber,
      reason: 'NO_POLICY',
      details: 'No retry policy configured on job',
    };
  }

  if (attempt.status === 'SUCCEEDED') {
    return {
      action: 'NOT_RETRYABLE',
      attemptNumber,
      reason: 'SUCCEEDED',
      details: 'Execution attempt succeeded',
    };
  }

  if (attempt.status === 'CANCELLED') {
    return {
      action: 'NOT_RETRYABLE',
      attemptNumber,
      reason: 'CANCELLED',
      details: 'Execution attempt was explicitly cancelled',
    };
  }

  if (attempt.status === 'PENDING' || attempt.status === 'RUNNING') {
    return {
      action: 'NOT_RETRYABLE',
      attemptNumber,
      reason: 'NO_POLICY',
      details: `Attempt is in non-terminal state "${attempt.status}"`,
    };
  }

  // The attempt is in a terminal failure state: 'FAILED' or 'TIMED_OUT'
  const retryConditions = policy.retryOn ?? ['FAILED', 'TIMED_OUT'];
  if (!retryConditions.includes(attempt.status as RetryCondition)) {
    return {
      action: 'FINAL_FAILURE',
      attemptNumber,
      reason: 'OUTCOME_NOT_RETRYABLE',
      details: `Execution outcome "${attempt.status}" is not configured as retryable in policy`,
    };
  }

  // Check attempt exhaustion
  if (attemptNumber >= policy.maxAttempts) {
    return {
      action: 'FINAL_FAILURE',
      attemptNumber,
      reason: 'MAX_ATTEMPTS_EXHAUSTED',
      details: `Maximum execution attempts reached (${attemptNumber}/${policy.maxAttempts})`,
    };
  }

  const delayMs = calculateBackoffDelay(attemptNumber, policy.backoff);
  const nextAttemptNumber = attemptNumber + 1;

  return {
    action: 'RETRY',
    attemptNumber,
    nextAttemptNumber,
    delayMs,
    reason: `Attempt ${attemptNumber} resulted in ${attempt.status}. Retrying attempt ${nextAttemptNumber} after ${delayMs}ms backoff`,
  };
}
