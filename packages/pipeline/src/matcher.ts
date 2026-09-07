import type { JobRequirements } from '@forge/contracts';
import { checkJobRequirementsValidity } from './requirements.js';

/**
 * Standard failure reasons explaining why a worker is ineligible for a job.
 */
export type MatchFailureReason =
  | 'EXECUTOR_UNSUPPORTED'
  | 'INSUFFICIENT_CPU'
  | 'INSUFFICIENT_MEMORY'
  | 'INSUFFICIENT_GPU'
  | 'INVALID_REQUIREMENTS';

/**
 * Explainable result of matching a job's requirements against a worker's capacity.
 */
export type WorkerMatchResult =
  | {
      readonly matched: true;
    }
  | {
      readonly matched: false;
      readonly reasons: readonly MatchFailureReason[];
    };

/**
 * Flexible worker representation compatible with WorkerMetadata, WorkerInfo,
 * WorkerRecord, or plain capability/resource descriptors.
 */
export interface WorkerCandidate {
  readonly id?: string;
  readonly workerId?: string;
  readonly capabilities?: { readonly executors?: readonly string[] } | null;
  readonly resources?: {
    readonly cpuCores?: number;
    readonly memoryBytes?: number;
    readonly gpuCount?: number;
  } | null;
  readonly worker?: {
    readonly capabilities?: { readonly executors?: readonly string[] } | null;
    readonly resources?: {
      readonly cpuCores?: number;
      readonly memoryBytes?: number;
      readonly gpuCount?: number;
    } | null;
  } | null;
  readonly executors?: readonly string[];
}

/**
 * Extracts effective capabilities and advertised hardware capacity from a worker candidate.
 */
function extractWorkerCapacity(worker: WorkerCandidate | null | undefined): {
  executors: readonly string[];
  cpuCores: number;
  memoryBytes: number;
  gpuCount: number;
} {
  if (!worker) {
    return {
      executors: [],
      cpuCores: 0,
      memoryBytes: 0,
      gpuCount: 0,
    };
  }

  const caps = worker.worker?.capabilities ?? worker.capabilities;
  const res = worker.worker?.resources ?? worker.resources;
  const executors = caps?.executors ?? worker.executors ?? [];
  const cpuCores = res?.cpuCores ?? 0;
  const memoryBytes = res?.memoryBytes ?? 0;
  const gpuCount = res?.gpuCount ?? 0;

  return { executors, cpuCores, memoryBytes, gpuCount };
}

/**
 * Pure, deterministic predicate evaluating whether a worker satisfies a job's
 * declared execution requirements.
 *
 * Checks:
 * 1. Requirements validity (invalid requirements always produce matched = false).
 * 2. Required executor presence (exact string match in worker advertised executors).
 * 3. Worker CPU cores >= requested CPU cores.
 * 4. Worker memory bytes >= requested memory bytes.
 * 5. Worker GPU count >= requested GPU count.
 */
export function matchesWorker(
  jobOrRequirements:
    { readonly requirements?: JobRequirements } | JobRequirements | undefined | null,
  worker: WorkerCandidate | undefined | null,
): WorkerMatchResult {
  // Resolve requirements from either a domain Job/Step or a raw requirements object
  const req: JobRequirements | undefined =
    jobOrRequirements && 'requirements' in jobOrRequirements
      ? jobOrRequirements.requirements
      : (jobOrRequirements as JobRequirements | undefined);

  // Validate requirements input safety
  const validity = checkJobRequirementsValidity(req);
  if (!validity.valid) {
    return {
      matched: false,
      reasons: Object.freeze(['INVALID_REQUIREMENTS']),
    };
  }

  // If requirements are empty or unspecified, any valid worker is eligible
  const effectiveReq = req ?? {};
  const reasons: MatchFailureReason[] = [];

  const { executors, cpuCores, memoryBytes, gpuCount } = extractWorkerCapacity(worker);

  // 1. Executor matching
  if (effectiveReq.executor !== undefined) {
    const hasExecutor = executors.some(
      (e) => typeof e === 'string' && e.trim() === effectiveReq.executor,
    );
    if (!hasExecutor) {
      reasons.push('EXECUTOR_UNSUPPORTED');
    }
  }

  // 2. CPU matching
  if (effectiveReq.cpuCores !== undefined) {
    if (cpuCores < effectiveReq.cpuCores) {
      reasons.push('INSUFFICIENT_CPU');
    }
  }

  // 3. Memory matching
  if (effectiveReq.memoryBytes !== undefined) {
    if (memoryBytes < effectiveReq.memoryBytes) {
      reasons.push('INSUFFICIENT_MEMORY');
    }
  }

  // 4. GPU matching
  if (effectiveReq.gpuCount !== undefined && effectiveReq.gpuCount > 0) {
    if (gpuCount < effectiveReq.gpuCount) {
      reasons.push('INSUFFICIENT_GPU');
    }
  }

  if (reasons.length === 0) {
    return { matched: true };
  }

  return {
    matched: false,
    reasons: Object.freeze(reasons),
  };
}

/**
 * Filters a collection of candidate workers to only those that satisfy the job's requirements.
 * Preserves the exact ordering of the input candidate array.
 */
export function filterEligibleWorkers<T extends WorkerCandidate>(
  jobOrRequirements:
    { readonly requirements?: JobRequirements } | JobRequirements | undefined | null,
  workers: readonly T[],
): T[] {
  if (!Array.isArray(workers) || workers.length === 0) {
    return [];
  }

  return workers.filter((worker) => {
    const result = matchesWorker(jobOrRequirements, worker);
    return result.matched;
  });
}
