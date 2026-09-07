import { describe, expect, it } from 'vitest';
import { buildResourceArgs, MIN_DOCKER_MEMORY_BYTES } from './resource-mapper.js';
import { cleanupWorkspace, createWorkspace, toDockerBindMountPath } from './workspace.js';
import { OutputCollector } from './output-stream.js';
import fs from 'node:fs/promises';

describe('Resource Mapper', () => {
  it('returns empty array when requirements are undefined', () => {
    expect(buildResourceArgs(undefined)).toEqual([]);
  });

  it('maps valid cpuCores to --cpus flag', () => {
    expect(buildResourceArgs({ cpuCores: 2 })).toEqual(['--cpus=2']);
    expect(buildResourceArgs({ cpuCores: 0.5 })).toEqual(['--cpus=0.5']);
  });

  it('rejects non-positive or non-finite cpuCores', () => {
    expect(() => buildResourceArgs({ cpuCores: 0 })).toThrow(/positive finite number/);
    expect(() => buildResourceArgs({ cpuCores: -1 })).toThrow(/positive finite number/);
    expect(() => buildResourceArgs({ cpuCores: Infinity })).toThrow(/positive finite number/);
  });

  it('maps valid memoryBytes to --memory flag in bytes', () => {
    const memory = 512 * 1024 * 1024; // 512MB
    expect(buildResourceArgs({ memoryBytes: memory })).toEqual([`--memory=${memory}b`]);
  });

  it('clamps small memoryBytes to MIN_DOCKER_MEMORY_BYTES (6MB)', () => {
    const smallMemory = 1024 * 1024; // 1MB
    expect(buildResourceArgs({ memoryBytes: smallMemory })).toEqual([
      `--memory=${MIN_DOCKER_MEMORY_BYTES}b`,
    ]);
  });

  it('rejects non-positive or non-finite memoryBytes', () => {
    expect(() => buildResourceArgs({ memoryBytes: 0 })).toThrow(/positive finite number/);
    expect(() => buildResourceArgs({ memoryBytes: -1024 })).toThrow(/positive finite number/);
  });

  it('explicitly rejects unverified GPU execution when gpuCount > 0', () => {
    expect(() => buildResourceArgs({ gpuCount: 1 })).toThrow(
      /GPU execution is unverified and deferred/,
    );
  });

  it('maps combined cpu and memory requirements', () => {
    const memory = 256 * 1024 * 1024;
    expect(buildResourceArgs({ cpuCores: 4, memoryBytes: memory })).toEqual([
      '--cpus=4',
      `--memory=${memory}b`,
    ]);
  });
});

describe('Workspace Management', () => {
  it('normalizes Windows drive paths to WSL /mnt POSIX paths', () => {
    expect(toDockerBindMountPath('C:\\Users\\Rakshaad\\workspace')).toBe(
      '/mnt/c/Users/Rakshaad/workspace',
    );
    expect(toDockerBindMountPath('d:\\data\\project')).toBe('/mnt/d/data/project');
  });

  it('preserves native POSIX paths unchanged', () => {
    expect(toDockerBindMountPath('/tmp/forge/workspace-123')).toBe('/tmp/forge/workspace-123');
  });

  it('creates unique, isolated workspace directories for distinct executions', async () => {
    const ws1 = await createWorkspace('exec-1');
    const ws2 = await createWorkspace('exec-2');

    try {
      expect(ws1.hostPath).not.toBe(ws2.hostPath);
      expect(ws1.dockerMountPath).not.toBe(ws2.dockerMountPath);

      // Verify directories actually exist
      const stat1 = await fs.stat(ws1.hostPath);
      const stat2 = await fs.stat(ws2.hostPath);
      expect(stat1.isDirectory()).toBe(true);
      expect(stat2.isDirectory()).toBe(true);
    } finally {
      await cleanupWorkspace(ws1.hostPath);
      await cleanupWorkspace(ws2.hostPath);
    }
  });

  it('cleanupWorkspace recursively removes existing directory and succeeds idempotently', async () => {
    const ws = await createWorkspace('test-cleanup');
    const testFile = `${ws.hostPath}/test.txt`;
    await fs.writeFile(testFile, 'hello');

    await cleanupWorkspace(ws.hostPath);

    // Verify it is gone
    await expect(fs.stat(ws.hostPath)).rejects.toThrow();

    // Idempotent second call should not throw
    await expect(cleanupWorkspace(ws.hostPath)).resolves.not.toThrow();
  });
});

describe('Output Collector', () => {
  it('captures stdout and stderr within limits', () => {
    const collector = new OutputCollector(1024);
    collector.pushStdout('stdout line 1\n');
    collector.pushStdout('stdout line 2\n');
    collector.pushStderr('stderr line 1\n');

    expect(collector.getStdout()).toBe('stdout line 1\nstdout line 2\n');
    expect(collector.getStderr()).toBe('stderr line 1\n');
    expect(collector.truncated).toBe(false);
  });

  it('truncates output and sets truncated flag when byte limit is exceeded', () => {
    const collector = new OutputCollector(10);
    collector.pushStdout('1234567890');
    expect(collector.truncated).toBe(false);

    collector.pushStdout('extra data');
    expect(collector.truncated).toBe(true);
    expect(collector.getStdout()).toBe('1234567890');
  });
});
