import { spawn } from 'node:child_process';
import crypto from 'node:crypto';
import type {
  ExecutionContext,
  ExecutionResult,
  ExecutionStatus,
  Executor,
} from '@forge/contracts';
import { createLogger, type Logger } from '@forge/logging';
import type { DockerExecutorOptions } from '../contracts.js';
import { ContainerStartupError, DockerUnavailableError } from '../errors.js';
import { OutputCollector } from './output-stream.js';
import { buildResourceArgs } from './resource-mapper.js';
import { cleanupWorkspace, createWorkspace } from './workspace.js';

export class DockerExecutor implements Executor {
  public readonly name = 'docker';
  private readonly logger: Logger;
  private readonly defaultImage: string;
  private readonly defaultTimeoutMs: number;
  private readonly maxTimeoutMs: number;
  private readonly maxOutputBytes: number;
  private readonly user: string;
  private readonly dockerHost?: string;
  private readonly workspaceBaseDir?: string;

  constructor(options?: DockerExecutorOptions) {
    this.defaultImage = options?.defaultImage ?? 'alpine:3.19';
    this.defaultTimeoutMs = options?.defaultTimeoutMs ?? 60000;
    this.maxTimeoutMs = options?.maxTimeoutMs ?? 1800000;
    this.maxOutputBytes = options?.maxOutputBytes ?? 1048576;
    this.user = options?.user ?? '1000:1000';
    this.dockerHost = options?.dockerHost ?? process.env.DOCKER_HOST;
    this.workspaceBaseDir = options?.workspaceBaseDir;

    this.logger =
      options?.logger ??
      createLogger({
        service: 'worker',
        environment: 'development',
      });
  }

  /**
   * Checks if the Docker runtime and daemon are available and responsive.
   */
  public async isAvailable(): Promise<boolean> {
    return new Promise((resolve) => {
      const env = this.getSpawnEnv();
      const proc = spawn('docker', ['info'], {
        env,
        stdio: ['ignore', 'ignore', 'ignore'],
        windowsHide: true,
      });

      const timer = setTimeout(() => {
        try {
          proc.kill('SIGKILL');
        } catch {
          // ignore
        }
        resolve(false);
      }, 5000);

      proc.on('error', () => {
        clearTimeout(timer);
        resolve(false);
      });

      proc.on('close', (code) => {
        clearTimeout(timer);
        resolve(code === 0);
      });
    });
  }

  /**
   * Executes a single job attempt within an ephemeral Docker container.
   */
  public async execute(context: ExecutionContext): Promise<ExecutionResult> {
    if (!context.command || context.command.trim().length === 0) {
      throw new Error('Invalid execution context: command must be a non-empty string');
    }

    if (
      context.timeoutMs !== undefined &&
      (context.timeoutMs <= 0 || !Number.isFinite(context.timeoutMs))
    ) {
      throw new Error(
        `Invalid timeoutMs: must be a positive finite number, received ${context.timeoutMs}`,
      );
    }

    // Pre-validate environment variable names prior to provisioning ephemeral workspace
    if (context.environment) {
      for (const key of Object.keys(context.environment)) {
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
          throw new Error(`Invalid environment variable name: "${key}"`);
        }
      }
    }

    const startedAt = new Date();
    const executionId = `${context.jobId}-${context.attemptId}-${crypto.randomUUID().slice(0, 8)}`;
    const containerName = `forge-exec-${executionId.replace(/[^a-zA-Z0-9_-]/g, '_')}`;

    this.logger.info('Execution requested', {
      jobId: context.jobId,
      attemptId: context.attemptId,
      workerId: context.workerId,
      containerName,
      executionId,
    });

    // 1. Prepare isolated temporary workspace
    const workspace = await createWorkspace(executionId, this.workspaceBaseDir);

    // 2. Resolve image and timeouts
    const image = context.image ?? this.defaultImage;
    const requestedTimeout = context.timeoutMs ?? this.defaultTimeoutMs;
    const timeoutMs = Math.min(Math.max(requestedTimeout, 1000), this.maxTimeoutMs);

    // 3. Build Docker run arguments
    const dockerArgs: string[] = [
      'run',
      '--name',
      containerName,
      '--rm=false',
      '--user',
      this.user,
      '--network',
      'bridge',
      '-v',
      `${workspace.dockerMountPath}:/workspace:rw`,
      '-w',
      '/workspace',
      '--label',
      'forge.managed=true',
      '--label',
      `forge.execution_id=${executionId}`,
      '--label',
      `forge.job_id=${context.jobId}`,
      '--label',
      `forge.attempt_id=${context.attemptId}`,
    ];

    // Resource limits
    dockerArgs.push(
      ...buildResourceArgs({
        cpuCores: context.cpuCores,
        memoryBytes: context.memoryBytes,
      }),
    );

    // Environment variables
    if (context.environment) {
      for (const [key, value] of Object.entries(context.environment)) {
        dockerArgs.push('--env', `${key}=${value}`);
      }
    }

    // Image and structured command
    dockerArgs.push(image, 'sh', '-c', context.command);

    const collector = new OutputCollector(this.maxOutputBytes);
    let timedOut = false;
    let cancelled = false;
    let timeoutTimer: NodeJS.Timeout | undefined;
    let abortListener: (() => void) | undefined;
    const env = this.getSpawnEnv();

    this.logger.info('Container starting', {
      jobId: context.jobId,
      attemptId: context.attemptId,
      workerId: context.workerId,
      containerName,
      image,
    });

    try {
      const exitCode = await new Promise<number | null>((resolve, reject) => {
        const proc = spawn('docker', dockerArgs, {
          env,
          windowsHide: true,
        });

        this.logger.info('Container started', {
          jobId: context.jobId,
          attemptId: context.attemptId,
          workerId: context.workerId,
          containerName,
        });

        // Hard wall-clock timeout
        timeoutTimer = setTimeout(() => {
          timedOut = true;
          this.logger.warn('Execution timed out, stopping container', {
            containerName,
            timeoutMs,
            jobId: context.jobId,
            attemptId: context.attemptId,
          });
          void this.terminateContainer(containerName);
        }, timeoutMs);

        // Cancellation listener
        if (context.abortSignal) {
          if (context.abortSignal.aborted) {
            cancelled = true;
            this.logger.info('Execution cancelled immediately via pre-aborted signal', {
              containerName,
              jobId: context.jobId,
              attemptId: context.attemptId,
            });
            void this.terminateContainer(containerName);
          } else {
            abortListener = () => {
              cancelled = true;
              this.logger.info('Execution cancelled via abort signal, stopping container', {
                containerName,
                jobId: context.jobId,
                attemptId: context.attemptId,
              });
              void this.terminateContainer(containerName);
            };
            context.abortSignal.addEventListener('abort', abortListener);
          }
        }

        proc.stdout.on('data', (chunk) => {
          collector.pushStdout(chunk);
        });

        proc.stderr.on('data', (chunk) => {
          collector.pushStderr(chunk);
        });

        proc.on('error', (err) => {
          if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
            reject(
              new DockerUnavailableError('Docker CLI executable "docker" not found in PATH', err),
            );
          } else {
            reject(
              new ContainerStartupError(
                `Failed to spawn docker process: ${err.message}`,
                image,
                err,
              ),
            );
          }
        });

        proc.on('close', (code) => {
          resolve(code);
        });
      });

      const finishedAt = new Date();
      const durationMs = finishedAt.getTime() - startedAt.getTime();
      const stdout = collector.getStdout();
      const stderr = collector.getStderr();
      const truncated = collector.truncated;

      let status: ExecutionStatus;
      let failureReason: string | undefined;

      // Deterministic precedence: cancellation > timeout > exitCode
      if (cancelled) {
        status = 'CANCELLED';
        failureReason = 'Execution was cancelled by abort signal';
        this.logger.info('Execution cancelled', {
          jobId: context.jobId,
          attemptId: context.attemptId,
          containerName,
          durationMs,
        });
      } else if (timedOut) {
        status = 'TIMED_OUT';
        failureReason = `Execution timed out after ${timeoutMs}ms`;
        this.logger.warn('Execution failed due to timeout', {
          jobId: context.jobId,
          attemptId: context.attemptId,
          containerName,
          timeoutMs,
          durationMs,
        });
      } else if (exitCode === 0) {
        status = 'SUCCEEDED';
        this.logger.info('Execution completed successfully', {
          jobId: context.jobId,
          attemptId: context.attemptId,
          status,
          exitCode,
          durationMs,
          truncated,
        });
      } else {
        status = 'FAILED';
        failureReason = `Process exited with code ${exitCode}`;
        this.logger.warn('Execution failed', {
          jobId: context.jobId,
          attemptId: context.attemptId,
          status,
          exitCode,
          failureReason,
          durationMs,
          truncated,
        });
      }

      return {
        status,
        exitCode: timedOut || cancelled ? null : exitCode,
        startedAt,
        finishedAt,
        durationMs,
        stdout,
        stderr,
        truncated,
        failureReason,
      };
    } finally {
      if (timeoutTimer) {
        clearTimeout(timeoutTimer);
      }
      if (abortListener && context.abortSignal) {
        context.abortSignal.removeEventListener('abort', abortListener);
      }

      // Guaranteed cleanup: remove container and delete workspace
      const cleanupStart = Date.now();
      try {
        await this.cleanupContainer(containerName);
        await cleanupWorkspace(workspace.hostPath, this.workspaceBaseDir);
        this.logger.info('Cleanup completed', {
          containerName,
          workspacePath: workspace.hostPath,
          cleanupDurationMs: Date.now() - cleanupStart,
        });
      } catch (cleanupErr) {
        this.logger.error('Cleanup failed', {
          containerName,
          workspacePath: workspace.hostPath,
          error: cleanupErr instanceof Error ? cleanupErr.message : String(cleanupErr),
        });
      }
    }
  }

  /**
   * Forcibly stops a running container during timeout or cancellation.
   */
  private async terminateContainer(containerName: string): Promise<void> {
    if (!containerName.startsWith('forge-exec-')) {
      return;
    }

    return new Promise((resolve) => {
      const env = this.getSpawnEnv();
      // Send SIGTERM with a 2-second grace period before Docker issues SIGKILL
      const stopProc = spawn('docker', ['stop', '-t', '2', containerName], {
        env,
        stdio: ['ignore', 'ignore', 'ignore'],
        windowsHide: true,
      });

      const fallbackTimer = setTimeout(() => {
        // Forcibly kill container if docker stop hung
        try {
          spawn('docker', ['kill', containerName], {
            env,
            stdio: ['ignore', 'ignore', 'ignore'],
            windowsHide: true,
          });
        } catch {
          // ignore
        }
        resolve();
      }, 4000);

      stopProc.on('close', () => {
        clearTimeout(fallbackTimer);
        resolve();
      });

      stopProc.on('error', () => {
        clearTimeout(fallbackTimer);
        resolve();
      });
    });
  }

  /**
   * Forcibly removes a container after completion.
   */
  private async cleanupContainer(containerName: string): Promise<void> {
    if (!containerName.startsWith('forge-exec-')) {
      this.logger.warn('Refusing to cleanup container without forge-exec- prefix', {
        containerName,
      });
      return;
    }

    return new Promise((resolve) => {
      const env = this.getSpawnEnv();
      const rmProc = spawn('docker', ['rm', '-f', containerName], {
        env,
        stdio: ['ignore', 'ignore', 'ignore'],
        windowsHide: true,
      });

      rmProc.on('close', () => resolve());
      rmProc.on('error', (err) => {
        this.logger.warn('Failed to cleanup container', {
          containerName,
          error: err.message,
        });
        resolve();
      });
    });
  }

  private getSpawnEnv(): NodeJS.ProcessEnv {
    if (this.dockerHost) {
      return {
        ...process.env,
        DOCKER_HOST: this.dockerHost,
      };
    }
    return process.env;
  }
}
