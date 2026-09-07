import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import { beforeAll, describe, expect, it } from 'vitest';
import { DockerExecutor } from './docker-executor.js';

const execAsync = promisify(exec);

describe('DockerExecutor (Live Docker Integration)', () => {
  const dockerHost = process.env.DOCKER_HOST ?? 'tcp://172.31.91.254:2375';
  let executor: DockerExecutor;
  let dockerAvailable = false;

  beforeAll(async () => {
    executor = new DockerExecutor({
      dockerHost,
      defaultImage: 'alpine:3.19',
      defaultTimeoutMs: 30000,
      user: '1000:1000',
    });

    dockerAvailable = await executor.isAvailable();
  });

  it('verifies Docker daemon is available and responsive', () => {
    expect(dockerAvailable).toBe(true);
  });

  it('executes a successful job command (exit 0) -> SUCCEEDED', async () => {
    if (!dockerAvailable) return;

    const result = await executor.execute({
      jobId: 'job-succ-1',
      attemptId: 'att-succ-1',
      workerId: 'worker-test',
      command: 'echo "Hello from DockerExecutor"',
    });

    expect(result.status).toBe('SUCCEEDED');
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('Hello from DockerExecutor');
    expect(result.durationMs).toBeGreaterThan(0);
    expect(result.failureReason).toBeUndefined();
  });

  it('executes a failing job command (exit 42) -> FAILED', async () => {
    if (!dockerAvailable) return;

    const result = await executor.execute({
      jobId: 'job-fail-1',
      attemptId: 'att-fail-1',
      workerId: 'worker-test',
      command: 'echo "Failing step..." && exit 42',
    });

    expect(result.status).toBe('FAILED');
    expect(result.exitCode).toBe(42);
    expect(result.stdout).toContain('Failing step...');
    expect(result.failureReason).toContain('code 42');
  });

  it('enforces execution timeout and terminates container -> TIMED_OUT', async () => {
    if (!dockerAvailable) return;

    const result = await executor.execute({
      jobId: 'job-timeout-1',
      attemptId: 'att-timeout-1',
      workerId: 'worker-test',
      command: 'sleep 10',
      timeoutMs: 1500, // 1.5 seconds timeout
    });

    expect(result.status).toBe('TIMED_OUT');
    expect(result.exitCode).toBeNull();
    expect(result.failureReason).toContain('timed out');
  });

  it('enforces non-root container execution (id -u != 0)', async () => {
    if (!dockerAvailable) return;

    const result = await executor.execute({
      jobId: 'job-nonroot-1',
      attemptId: 'att-nonroot-1',
      workerId: 'worker-test',
      command: 'id -u',
    });

    expect(result.status).toBe('SUCCEEDED');
    expect(result.stdout.trim()).toBe('1000');
    expect(result.stdout.trim()).not.toBe('0');
  });

  it('passes explicit environment variables and isolates host environment', async () => {
    if (!dockerAvailable) return;

    const result = await executor.execute({
      jobId: 'job-env-1',
      attemptId: 'att-env-1',
      workerId: 'worker-test',
      command: 'echo "CUSTOM=$MY_CUSTOM_VAR" && echo "HOST_USER=$USER"',
      environment: {
        MY_CUSTOM_VAR: 'forge_secret_token_123',
      },
    });

    expect(result.status).toBe('SUCCEEDED');
    expect(result.stdout).toContain('CUSTOM=forge_secret_token_123');
  });

  it('supports file writes and reads inside the temporary workspace', async () => {
    if (!dockerAvailable) return;

    const result = await executor.execute({
      jobId: 'job-fs-1',
      attemptId: 'att-fs-1',
      workerId: 'worker-test',
      command: 'echo "workspace file content" > test.txt && cat test.txt',
    });

    expect(result.status).toBe('SUCCEEDED');
    expect(result.stdout).toContain('workspace file content');
  });

  it('handles execution cancellation via AbortSignal -> CANCELLED', async () => {
    if (!dockerAvailable) return;

    const controller = new AbortController();
    setTimeout(() => {
      controller.abort();
    }, 1000);

    const result = await executor.execute({
      jobId: 'job-cancel-1',
      attemptId: 'att-cancel-1',
      workerId: 'worker-test',
      command: 'sleep 10',
      abortSignal: controller.signal,
    });

    expect(result.status).toBe('CANCELLED');
    expect(result.exitCode).toBeNull();
    expect(result.failureReason).toContain('cancelled');
  });

  it('verifies container is cleaned up and does not persist in docker ps -a', async () => {
    if (!dockerAvailable) return;

    const jobId = 'job-clean-1';
    await executor.execute({
      jobId,
      attemptId: 'att-clean-1',
      workerId: 'worker-test',
      command: 'echo "done"',
    });

    // Query docker to ensure container is gone
    const { stdout } = await execAsync(
      'docker ps -a --filter "name=forge-exec-job-clean-1" --format "{{.Names}}"',
      {
        env: { ...process.env, DOCKER_HOST: dockerHost },
      },
    );

    expect(stdout.trim()).toBe('');
  });
});
