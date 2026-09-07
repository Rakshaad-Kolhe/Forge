export interface ResourceRequirements {
  readonly cpuCores?: number;
  readonly memoryBytes?: number;
  readonly gpuCount?: number;
}

/**
 * Docker imposes a hard minimum container memory limit of 6MB (6,291,456 bytes).
 */
export const MIN_DOCKER_MEMORY_BYTES = 6 * 1024 * 1024;

/**
 * Maps abstract execution resource requirements into structured Docker CLI argument flags.
 *
 * @param requirements - Optional CPU, memory, and GPU requirements
 * @returns Array of Docker CLI flags (e.g. ['--cpus=2', '--memory=536870912b'])
 */
export function buildResourceArgs(requirements?: ResourceRequirements): string[] {
  if (!requirements) {
    return [];
  }

  const args: string[] = [];

  if (requirements.cpuCores !== undefined && requirements.cpuCores !== null) {
    if (requirements.cpuCores <= 0 || !Number.isFinite(requirements.cpuCores)) {
      throw new Error(
        `Invalid cpuCores requirement: must be a positive finite number, received ${requirements.cpuCores}`,
      );
    }
    args.push(`--cpus=${requirements.cpuCores}`);
  }

  if (requirements.memoryBytes !== undefined && requirements.memoryBytes !== null) {
    if (requirements.memoryBytes <= 0 || !Number.isFinite(requirements.memoryBytes)) {
      throw new Error(
        `Invalid memoryBytes requirement: must be a positive finite number, received ${requirements.memoryBytes}`,
      );
    }

    // Clamp to Docker minimum floor if smaller
    const effectiveMemory = Math.max(requirements.memoryBytes, MIN_DOCKER_MEMORY_BYTES);
    args.push(`--memory=${Math.floor(effectiveMemory)}b`);
  }

  if (requirements.gpuCount !== undefined && requirements.gpuCount !== null) {
    if (requirements.gpuCount > 0) {
      // Per PR 13 specification Section 18:
      // GPU capacity metadata is recognized in scheduling, but GPU container execution
      // requires verified NVIDIA container toolkit runtime, which is unverified/deferred here.
      throw new Error(
        `GPU execution is unverified and deferred in DockerExecutor; requested ${requirements.gpuCount} GPUs`,
      );
    }
  }

  return args;
}
