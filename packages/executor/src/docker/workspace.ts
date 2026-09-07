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
 * Creates an isolated, unique temporary workspace directory on the host.
 *
 * @param executionId - Unique identifier for the execution attempt
 * @param baseDir - Optional base directory; defaults to os.tmpdir()/forge-workspaces
 */
export async function createWorkspace(
  executionId: string,
  baseDir?: string,
): Promise<WorkspaceInfo> {
  const sanitizedId = executionId.replace(/[^a-zA-Z0-9_-]/g, '_');
  const parentDir = baseDir ?? path.join(os.tmpdir(), 'forge-workspaces');
  const hostPath = path.join(parentDir, sanitizedId);

  await fs.mkdir(hostPath, { recursive: true });

  const dockerMountPath = toDockerBindMountPath(hostPath);
  return {
    hostPath,
    dockerMountPath,
  };
}

/**
 * Completely and recursively deletes an execution workspace directory.
 */
export async function cleanupWorkspace(workspacePath: string): Promise<void> {
  try {
    await fs.rm(workspacePath, { recursive: true, force: true });
  } catch (err) {
    // If the directory already doesn't exist, ignore
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      return;
    }
    throw err;
  }
}
