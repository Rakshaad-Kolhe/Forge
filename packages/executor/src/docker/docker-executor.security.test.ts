import { beforeAll, describe, expect, it } from 'vitest';
import { DockerExecutor } from './docker-executor.js';
import { cleanupWorkspace, createWorkspace } from './workspace.js';

describe('DockerExecutor Security Regressions', () => {
  const dockerHost = process.env.DOCKER_HOST ?? 'tcp://127.0.0.1:2375';
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

  it('verifies non-root execution is enforced inside running container', async () => {
    if (!dockerAvailable) return;

    const result = await executor.execute({
      jobId: 'sec-nonroot-1',
      attemptId: 'att-1',
      workerId: 'worker-sec',
      command: 'id -u && id -g',
    });

    expect(result.status).toBe('SUCCEEDED');
    expect(result.exitCode).toBe(0);
    const lines = result.stdout.trim().split('\n');
    expect(lines[0]?.trim()).toBe('1000');
    expect(lines[0]?.trim()).not.toBe('0');
  });

  it('verifies container cannot perform privileged operations or access host devices', async () => {
    if (!dockerAvailable) return;

    // Attempting to run chroot or mount inside unprivileged container should fail with EPERM (exit code non-zero)
    const result = await executor.execute({
      jobId: 'sec-priv-1',
      attemptId: 'att-1',
      workerId: 'worker-sec',
      command: 'mount -t tmpfs none /mnt 2>&1 || exit 99',
    });

    expect(result.status).toBe('FAILED');
    expect(result.exitCode).toBe(99);
  });

  it('rejects directory traversal in workspace provisioning', async () => {
    await expect(createWorkspace('../../../etc/cron.d')).rejects.toThrow(
      /directory traversal tokens/,
    );
    await expect(createWorkspace('..\\..\\Windows\\System32')).rejects.toThrow(
      /directory traversal tokens/,
    );
  });

  it('prevents cleanupWorkspace from deleting arbitrary host directories', async () => {
    await expect(cleanupWorkspace('/usr/bin')).rejects.toThrow(/Security error/);
    await expect(cleanupWorkspace('C:\\Program Files')).rejects.toThrow(/Security error/);
  });

  it('rejects malicious environment variable names containing shell metacharacters', async () => {
    const maliciousNames = [
      'FOO;rm -rf /',
      'BAR&&echo hacked',
      'ENV`whoami`',
      'SPACED NAME',
      'NAME$WITH_DOLLAR',
      'WITH"QUOTE',
    ];

    for (const badName of maliciousNames) {
      await expect(
        executor.execute({
          jobId: 'sec-env-1',
          attemptId: 'att-1',
          workerId: 'worker-sec',
          command: 'echo test',
          environment: {
            [badName]: 'malicious_payload',
          },
        }),
      ).rejects.toThrow(/Invalid environment variable name/);
    }
  });

  it('isolates host environment variables from the container execution environment', async () => {
    if (!dockerAvailable) return;

    // Set a process environment variable on the host
    process.env.FORGE_SUPER_SECRET_HOST_KEY = 'super_secret_host_value_xyz';

    try {
      const result = await executor.execute({
        jobId: 'sec-env-leak-1',
        attemptId: 'att-1',
        workerId: 'worker-sec',
        command: 'echo "LEAKED=$FORGE_SUPER_SECRET_HOST_KEY"',
      });

      expect(result.status).toBe('SUCCEEDED');
      expect(result.stdout).not.toContain('super_secret_host_value_xyz');
      expect(result.stdout.trim()).toBe('LEAKED=');
    } finally {
      delete process.env.FORGE_SUPER_SECRET_HOST_KEY;
    }
  });
});
