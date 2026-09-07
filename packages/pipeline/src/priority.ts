import { DEFAULT_JOB_PRIORITY, MAX_JOB_PRIORITY, MIN_JOB_PRIORITY } from '@forge/contracts';
import { InvalidJobPriorityError } from './errors.js';

export { DEFAULT_JOB_PRIORITY, MAX_JOB_PRIORITY, MIN_JOB_PRIORITY };

/**
 * Checks whether an input value conforms to domain job priority constraints.
 * Does not throw; returns validation boolean and descriptive issues.
 */
export function checkJobPriorityValidity(input: unknown): {
  valid: boolean;
  issues: string[];
} {
  if (input === undefined || input === null) {
    return { valid: true, issues: [] };
  }

  const issues: string[] = [];

  if (typeof input !== 'number') {
    issues.push(`Job priority must be a number, received ${typeof input}`);
    return { valid: false, issues };
  }

  if (Number.isNaN(input) || !Number.isFinite(input)) {
    issues.push('Job priority must be a finite number, received NaN or Infinity');
    return { valid: false, issues };
  }

  if (!Number.isInteger(input)) {
    issues.push(`Job priority must be an integer, received ${input}`);
    return { valid: false, issues };
  }

  if (input < MIN_JOB_PRIORITY || input > MAX_JOB_PRIORITY) {
    issues.push(
      `Job priority must be between ${MIN_JOB_PRIORITY} and ${MAX_JOB_PRIORITY}, received ${input}`,
    );
    return { valid: false, issues };
  }

  return { valid: true, issues: [] };
}

/**
 * Validates and normalizes job priority.
 *
 * Rules:
 * - Omitted (undefined / null) defaults deterministically to DEFAULT_JOB_PRIORITY (0).
 * - Must be an integer within [MIN_JOB_PRIORITY, MAX_JOB_PRIORITY].
 * - Throws InvalidJobPriorityError on invalid inputs.
 */
export function validateJobPriority(input?: unknown): number {
  if (input === undefined || input === null) {
    return DEFAULT_JOB_PRIORITY;
  }

  const check = checkJobPriorityValidity(input);
  if (!check.valid) {
    throw new InvalidJobPriorityError(
      `Invalid job priority: ${check.issues.join('; ')}`,
      check.issues,
    );
  }

  return input as number;
}
