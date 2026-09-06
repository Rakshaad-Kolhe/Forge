import { describe, it, expect } from 'vitest';
import { runCli, CLI_VERSION } from './index.js';

describe('CLI Shell', () => {
  it('displays help message when called with no arguments or --help', () => {
    const resNoArgs = runCli([]);
    expect(resNoArgs.exitCode).toBe(0);
    expect(resNoArgs.output).toContain('Forge CLI');
    expect(resNoArgs.output).toContain('--help');

    const resHelp = runCli(['--help']);
    expect(resHelp.exitCode).toBe(0);
    expect(resHelp.output).toContain('Forge CLI');
  });

  it('displays version when called with --version or -v', () => {
    const res = runCli(['--version']);
    expect(res.exitCode).toBe(0);
    expect(res.output).toBe(`forge version ${CLI_VERSION}`);

    const resShort = runCli(['-v']);
    expect(resShort.exitCode).toBe(0);
    expect(resShort.output).toBe(`forge version ${CLI_VERSION}`);
  });

  it('returns non-zero exit code on unrecognized commands', () => {
    const res = runCli(['run']);
    expect(res.exitCode).toBe(1);
    expect(res.output).toContain("Unknown command or option: 'run'");
  });
});
