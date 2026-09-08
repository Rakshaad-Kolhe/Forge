import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

export interface WorkspaceInfo {
  /**
   * Absolute native path on the host filesystem.
   */
  readonly hostPath: string;

  /**
   * Path formatted for Docker volume bind-mounts (POSIX normalized).
   */
  readonly dockerMountPath: string;
}

/**
 * Converts a native host directory path into a format suitable for Docker volume bind mounts.
 * On Windows, converts drive-letter paths (e.g. `C:\Users\...`) to POSIX WSL mounts (`/mnt/c/Users/...`).
 */
export function toDockerBindMountPath(hostPath: string): string {
  const normalized = hostPath.replace(/\\/g, '/');

  // Match Windows drive letter: e.g. "C:" or "c:"
  const winDriveMatch = /^([A-Za-z]):/.exec(normalized);
  if (winDriveMatch && winDriveMatch[1]) {
    const driveLetter = winDriveMatch[1].toLowerCase();
    const restOfPath = normalized.slice(2);
    const cleanRest = restOfPath.startsWith('/') ? restOfPath : `/${restOfPath}`;
    return `/mnt/${driveLetter}${cleanRest}`;
  }

  return normalized;
}

/**
 * Default base directory where ephemeral execution workspaces are provisioned.
 */
export function getDefaultWorkspaceBaseDir(): string {
  return path.join(os.tmpdir(), 'forge-workspaces');
}

/**
 * Creates an isolated, unique temporary workspace directory on the host.
 *
 * @param executionId - Unique identifier for the execution attempt
 * @param baseDir - Optional base directory; defaults to os.tmpdir()/forge-workspaces
 */
export async function createWorkspace(
  executionId: string,
  baseDir?: string,
): Promise<WorkspaceInfo> {
  if (!executionId || typeof executionId !== 'string') {
    throw new Error('Invalid executionId: executionId must be a non-empty string');
  }

  // Reject explicit path traversal tokens
  if (executionId.includes('..') || executionId.includes('/') || executionId.includes('\\')) {
    throw new Error(
      `Invalid executionId: directory traversal tokens not allowed in "${executionId}"`,
    );
  }

  const sanitizedId = executionId.replace(/[^a-zA-Z0-9_-]/g, '_');
  if (!sanitizedId || sanitizedId.length === 0) {
    throw new Error('Invalid executionId: executionId yielded empty identifier after sanitization');
  }

  const parentDir = path.resolve(baseDir ?? getDefaultWorkspaceBaseDir());
  const hostPath = path.resolve(path.join(parentDir, sanitizedId));

  // Strict containment verification
  const relative = path.relative(parentDir, hostPath);
  if (relative.startsWith('..') || path.isAbsolute(relative) || relative === '') {
    throw new Error(
      `Security error: resolved workspace path "${hostPath}" escapes base directory "${parentDir}"`,
    );
  }

  await fs.mkdir(hostPath, { recursive: true });

  const dockerMountPath = toDockerBindMountPath(hostPath);
  return {
    hostPath,
    dockerMountPath,
  };
}

/**
 * Completely and recursively deletes an execution workspace directory.
 * Enforces boundary containment to ensure deletions never touch host directories outside the workspace base.
 *
 * @param workspacePath - Path to the execution workspace directory
 * @param baseDir - Optional expected parent workspace directory
 */
export async function cleanupWorkspace(workspacePath: string, baseDir?: string): Promise<void> {
  if (!workspacePath || typeof workspacePath !== 'string') {
    return;
  }

  const resolvedTarget = path.resolve(workspacePath);
  const resolvedBase = path.resolve(baseDir ?? getDefaultWorkspaceBaseDir());

  // Containment check: target must be a child subdirectory within resolvedBase
  const relative = path.relative(resolvedBase, resolvedTarget);
  if (relative.startsWith('..') || path.isAbsolute(relative) || relative === '') {
    throw new Error(
      `Security error: cannot cleanup directory "${resolvedTarget}" outside designated workspace base "${resolvedBase}"`,
    );
  }

  try {
    await fs.rm(resolvedTarget, { recursive: true, force: true });
  } catch (err) {
    // If the directory already doesn't exist, ignore
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      return;
    }
    throw err;
  }
}
