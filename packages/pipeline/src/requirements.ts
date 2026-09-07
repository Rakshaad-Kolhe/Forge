import type { JobRequirements } from '@forge/contracts';
import { JobRequirementsValidationError } from './errors.js';

export type { JobRequirements } from '@forge/contracts';

/**
 * Permissive input format for declaring step and job execution requirements.
 */
export interface JobRequirementsInput {
  readonly executor?: string | null;
  readonly cpuCores?: number | null;
  readonly memoryBytes?: number | null;
  readonly gpuCount?: number | null;
}

/**
 * Checks whether an unknown requirements object complies with domain validity constraints.
 * Does not throw; returns validation boolean and descriptive issues.
 */
export function checkJobRequirementsValidity(input: unknown): {
  valid: boolean;
  issues: string[];
} {
  const issues: string[] = [];

  if (input === undefined || input === null) {
    return { valid: true, issues: [] };
  }

  if (typeof input !== 'object' || Array.isArray(input)) {
    return {
      valid: false,
      issues: ['Job requirements must be an object'],
    };
  }

  const record = input as Record<string, unknown>;

  if (record.executor !== undefined && record.executor !== null) {
    if (typeof record.executor !== 'string' || record.executor.trim().length === 0) {
      issues.push('Job requirement "executor" must be a non-empty string');
    }
  }

  if (record.cpuCores !== undefined && record.cpuCores !== null) {
    if (
      typeof record.cpuCores !== 'number' ||
      !Number.isFinite(record.cpuCores) ||
      record.cpuCores <= 0
    ) {
      issues.push('Job requirement "cpuCores" must be a positive finite number');
    }
  }

  if (record.memoryBytes !== undefined && record.memoryBytes !== null) {
    if (
      typeof record.memoryBytes !== 'number' ||
      !Number.isFinite(record.memoryBytes) ||
      record.memoryBytes <= 0
    ) {
      issues.push('Job requirement "memoryBytes" must be a positive finite number');
    }
  }

  if (record.gpuCount !== undefined && record.gpuCount !== null) {
    if (
      typeof record.gpuCount !== 'number' ||
      !Number.isFinite(record.gpuCount) ||
      record.gpuCount < 0
    ) {
      issues.push('Job requirement "gpuCount" must be a non-negative finite number');
    }
  }

  return {
    valid: issues.length === 0,
    issues,
  };
}

/**
 * Validates and normalizes raw job requirements input into an immutable JobRequirements object.
 * Throws JobRequirementsValidationError if any specified requirement violates domain rules.
 */
export function validateJobRequirements(input?: unknown): JobRequirements {
  if (input === undefined || input === null) {
    return Object.freeze({});
  }

  const check = checkJobRequirementsValidity(input);
  if (!check.valid) {
    throw new JobRequirementsValidationError(
      `Invalid job execution requirements: ${check.issues.join('; ')}`,
      check.issues,
    );
  }

  const record = input as Record<string, unknown>;
  const normalized: {
    executor?: string;
    cpuCores?: number;
    memoryBytes?: number;
    gpuCount?: number;
  } = {};

  if (typeof record.executor === 'string') {
    normalized.executor = record.executor.trim();
  }

  if (typeof record.cpuCores === 'number') {
    normalized.cpuCores = record.cpuCores;
  }

  if (typeof record.memoryBytes === 'number') {
    normalized.memoryBytes = record.memoryBytes;
  }

  if (typeof record.gpuCount === 'number') {
    normalized.gpuCount = record.gpuCount;
  }

  return Object.freeze(normalized);
}
