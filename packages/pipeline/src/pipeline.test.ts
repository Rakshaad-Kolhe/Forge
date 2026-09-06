import { describe, it, expect } from 'vitest';
import { Pipeline } from './pipeline.js';
import {
  DuplicateStepError,
  MissingDependencyError,
  PipelineValidationError,
  SelfDependencyError,
} from './errors.js';

describe('Pipeline Domain Model', () => {
  it('instantiates a valid pipeline definition', () => {
    const pipeline = new Pipeline({
      id: 'pipe-node-ci',
      name: 'Node CI',
      steps: [
        { name: 'install', command: 'npm ci' },
        { name: 'lint', command: 'npm run lint', dependsOn: ['install'] },
        { name: 'test', command: 'npm test', dependsOn: ['install'] },
        { name: 'build', command: 'npm run build', dependsOn: ['lint', 'test'] },
      ],
    });

    expect(pipeline.id).toBe('pipe-node-ci');
    expect(pipeline.name).toBe('Node CI');
    expect(pipeline.getSteps()).toHaveLength(4);
    expect(pipeline.getStep('install')?.command).toBe('npm ci');
    expect(pipeline.getStep('build')?.dependsOn).toEqual(['lint', 'test']);
  });

  it('rejects empty pipeline name', () => {
    expect(() => {
      new Pipeline({
        name: '   ',
        steps: [{ name: 'install', command: 'npm ci' }],
      });
    }).toThrow(PipelineValidationError);
  });

  it('rejects empty steps array', () => {
    expect(() => {
      new Pipeline({
        name: 'Empty Pipeline',
        steps: [],
      });
    }).toThrow(PipelineValidationError);
  });

  it('rejects empty step name', () => {
    expect(() => {
      new Pipeline({
        name: 'Invalid Step Name',
        steps: [{ name: '', command: 'npm ci' }],
      });
    }).toThrow(PipelineValidationError);
  });

  it('rejects empty step command', () => {
    expect(() => {
      new Pipeline({
        name: 'Invalid Step Command',
        steps: [{ name: 'test', command: '  ' }],
      });
    }).toThrow(PipelineValidationError);
  });

  it('rejects duplicate step names', () => {
    expect(() => {
      new Pipeline({
        name: 'Duplicate Steps',
        steps: [
          { name: 'test', command: 'npm test' },
          { name: 'test', command: 'npm run test:e2e' },
        ],
      });
    }).toThrow(DuplicateStepError);
  });

  it('rejects missing step dependency references', () => {
    expect(() => {
      new Pipeline({
        name: 'Missing Dep',
        steps: [{ name: 'test', command: 'npm test', dependsOn: ['missing-setup'] }],
      });
    }).toThrow(MissingDependencyError);
  });

  it('rejects self-dependencies in steps', () => {
    expect(() => {
      new Pipeline({
        name: 'Self Dep',
        steps: [{ name: 'build', command: 'npm run build', dependsOn: ['build'] }],
      });
    }).toThrow(SelfDependencyError);
  });

  it('serializes to JSON cleanly', () => {
    const pipeline = new Pipeline({
      id: 'p-1',
      name: 'Build Pipeline',
      steps: [{ name: 'build', command: 'make' }],
    });

    expect(pipeline.toJSON()).toEqual({
      id: 'p-1',
      name: 'Build Pipeline',
      steps: [{ name: 'build', command: 'make', dependsOn: [] }],
    });
  });
});
