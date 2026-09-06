#!/usr/bin/env node

export const CLI_VERSION = '0.1.0';

export interface CliResult {
  exitCode: number;
  output: string;
}

/**
 * Handles argument parsing and output for the minimal Forge CLI shell.
 */
export function runCli(args: string[]): CliResult {
  if (
    args.length === 0 ||
    args.includes('--help') ||
    args.includes('-h') ||
    args.includes('help')
  ) {
    const helpText = [
      'Forge CLI — Distributed CI/CD Orchestration Engine Shell',
      '',
      'Usage:',
      '  forge [options]',
      '  forge <command> [options]',
      '',
      'Options:',
      '  -v, --version    Show Forge CLI version',
      '  -h, --help       Show help text',
      '',
      'Note: CI/CD workflow commands (run, logs, deploy) are planned for future PRs.',
    ].join('\n');

    return { exitCode: 0, output: helpText };
  }

  if (args.includes('--version') || args.includes('-v') || args.includes('version')) {
    return { exitCode: 0, output: `forge version ${CLI_VERSION}` };
  }

  const unknown = args[0];
  return {
    exitCode: 1,
    output: `Unknown command or option: '${unknown}'. Run 'forge --help' for available options.`,
  };
}

const normalizedArgv1 = process.argv[1]?.replace(/\\/g, '/') ?? '';
const isDirectRun =
  Boolean(normalizedArgv1) &&
  (normalizedArgv1.endsWith('cli/dist/index.js') ||
    normalizedArgv1.endsWith('cli/src/index.ts') ||
    normalizedArgv1.endsWith('bin/forge'));

if (isDirectRun) {
  const result = runCli(process.argv.slice(2));
  if (result.exitCode === 0) {
    process.stdout.write(result.output + '\n');
  } else {
    process.stderr.write(result.output + '\n');
  }
  process.exit(result.exitCode);
}
