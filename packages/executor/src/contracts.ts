import type {
  ExecutionContext,
  ExecutionResult,
  ExecutionStatus,
  Executor,
} from '@forge/contracts';
import type { Logger } from '@forge/logging';

export type { ExecutionContext, ExecutionResult, ExecutionStatus, Executor };

/**
 * Configuration options for initializing a DockerExecutor instance.
 */
export interface DockerExecutorOptions {
  /**
   * Default Docker image used when a job does not specify an explicit image.
   * Defaults to 'alpine:3.19'.
   */
  readonly defaultImage?: string;

  /**
   * Default execution timeout in milliseconds.
   * Defaults to 60000ms (1 minute).
   */
  readonly defaultTimeoutMs?: number;

  /**
   * Maximum allowed execution timeout in milliseconds.
   * Defaults to 1800000ms (30 minutes).
   */
  readonly maxTimeoutMs?: number;

  /**
   * Maximum output bytes captured for stdout and stderr before truncation.
   * Defaults to 1048576 (1MB).
   */
  readonly maxOutputBytes?: number;

  /**
   * Docker daemon host endpoint (e.g. 'tcp://127.0.0.1:2375').
   * If omitted, inherits from process.env.DOCKER_HOST or default socket.
   */
  readonly dockerHost?: string;

  /**
   * Unprivileged user UID:GID passed to `--user` for non-root execution.
   * Defaults to '1000:1000'.
   */
  readonly user?: string;

  /**
   * Base directory where ephemeral execution workspaces are provisioned.
   * Defaults to path.join(os.tmpdir(), 'forge-workspaces').
   */
  readonly workspaceBaseDir?: string;

  /**
   * Structured logger instance.
   */
  readonly logger?: Logger;
}
