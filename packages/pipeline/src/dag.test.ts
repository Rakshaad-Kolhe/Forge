import { describe, it, expect } from 'vitest';
import { DirectedAcyclicGraph } from './dag.js';
import { CycleDetectedError, MissingDependencyError, SelfDependencyError } from './errors.js';

describe('DirectedAcyclicGraph', () => {
  it('creates a single-node graph', () => {
    const dag = new DirectedAcyclicGraph([{ name: 'build' }]);
    expect(dag.getNodeCount()).toBe(1);
    expect(dag.getRoots()).toEqual(['build']);
    expect(dag.getLeaves()).toEqual(['build']);
    expect(dag.getTopologicalOrder()).toEqual(['build']);
  });

  it('handles linear dependency chain (A -> B -> C)', () => {
    const dag = new DirectedAcyclicGraph([
      { name: 'compile' },
      { name: 'test', dependencies: ['compile'] },
      { name: 'deploy', dependencies: ['test'] },
    ]);

    expect(dag.getNodeCount()).toBe(3);
    expect(dag.getRoots()).toEqual(['compile']);
    expect(dag.getLeaves()).toEqual(['deploy']);
    expect(dag.getDependencies('test')).toEqual(['compile']);
    expect(dag.getDependents('compile')).toEqual(['test']);
    expect(dag.getTopologicalOrder()).toEqual(['compile', 'test', 'deploy']);
  });

  it('handles diamond dependency graph with deterministic tie-breaking', () => {
    // install -> lint, test -> build
    const dag = new DirectedAcyclicGraph([
      { name: 'install' },
      { name: 'lint', dependencies: ['install'] },
      { name: 'test', dependencies: ['install'] },
      { name: 'build', dependencies: ['lint', 'test'] },
    ]);

    expect(dag.getRoots()).toEqual(['install']);
    expect(dag.getLeaves()).toEqual(['build']);
    expect(dag.getDependents('install')).toEqual(['lint', 'test']);
    expect(dag.getDependencies('build')).toEqual(['lint', 'test']);

    // Tie-breaking rule: lint comes before test alphabetically
    expect(dag.getTopologicalOrder()).toEqual(['install', 'lint', 'test', 'build']);
  });

  it('handles multiple independent roots deterministically', () => {
    const dag = new DirectedAcyclicGraph([
      { name: 'frontend-lint' },
      { name: 'backend-lint' },
      { name: 'docs' },
    ]);

    expect(dag.getRoots()).toEqual(['backend-lint', 'docs', 'frontend-lint']);
    expect(dag.getTopologicalOrder()).toEqual(['backend-lint', 'docs', 'frontend-lint']);
  });

  it('detects and rejects missing dependencies', () => {
    expect(() => {
      new DirectedAcyclicGraph([{ name: 'test', dependencies: ['non-existent'] }]);
    }).toThrow(MissingDependencyError);
  });

  it('detects and rejects self-dependencies', () => {
    expect(() => {
      new DirectedAcyclicGraph([{ name: 'build', dependencies: ['build'] }]);
    }).toThrow(SelfDependencyError);
  });

  it('detects and rejects simple 2-node cycle (A -> B -> A)', () => {
    expect(() => {
      new DirectedAcyclicGraph([
        { name: 'A', dependencies: ['B'] },
        { name: 'B', dependencies: ['A'] },
      ]);
    }).toThrow(CycleDetectedError);
  });

  it('detects and rejects complex 3-node cycle (A -> B -> C -> A)', () => {
    expect(() => {
      new DirectedAcyclicGraph([
        { name: 'A', dependencies: ['C'] },
        { name: 'B', dependencies: ['A'] },
        { name: 'C', dependencies: ['B'] },
      ]);
    }).toThrow(CycleDetectedError);
  });

  describe('Readiness Evaluation', () => {
    const dag = new DirectedAcyclicGraph([
      { name: 'install' },
      { name: 'lint', dependencies: ['install'] },
      { name: 'test', dependencies: ['install'] },
      { name: 'build', dependencies: ['lint', 'test'] },
    ]);

    it('evaluates root as ready when completed set is empty', () => {
      expect(dag.isReady('install', [])).toBe(true);
      expect(dag.isReady('lint', [])).toBe(false);
      expect(dag.isReady('test', [])).toBe(false);
      expect(dag.isReady('build', [])).toBe(false);
      expect(dag.getReadySteps([])).toEqual(['install']);
    });

    it('evaluates dependent steps ready when dependency completes', () => {
      expect(dag.isReady('install', ['install'])).toBe(false); // already completed!
      expect(dag.isReady('lint', ['install'])).toBe(true);
      expect(dag.isReady('test', ['install'])).toBe(true);
      expect(dag.isReady('build', ['install'])).toBe(false);
      expect(dag.getReadySteps(['install'])).toEqual(['lint', 'test']);
    });

    it('evaluates merge step ready only when all dependencies complete', () => {
      expect(dag.isReady('build', ['install', 'lint'])).toBe(false); // missing 'test'
      expect(dag.isReady('build', ['install', 'test'])).toBe(false); // missing 'lint'
      expect(dag.isReady('build', ['install', 'lint', 'test'])).toBe(true);
      expect(dag.getReadySteps(['install', 'lint', 'test'])).toEqual(['build']);
    });

    it('returns empty ready list when all steps are completed', () => {
      expect(dag.getReadySteps(['install', 'lint', 'test', 'build'])).toEqual([]);
    });
  });

  describe('Invariant Tests', () => {
    it('invariant: no node appears before its dependencies in topological order', () => {
      const dag = new DirectedAcyclicGraph([
        { name: 'setup' },
        { name: 'compile', dependencies: ['setup'] },
        { name: 'test-unit', dependencies: ['compile'] },
        { name: 'test-integration', dependencies: ['compile'] },
        { name: 'package', dependencies: ['test-unit', 'test-integration'] },
        { name: 'publish', dependencies: ['package'] },
      ]);

      const order = dag.getTopologicalOrder();
      const indexMap = new Map<string, number>();
      order.forEach((node, index) => indexMap.set(node, index));

      for (const node of order) {
        const deps = dag.getDependencies(node);
        for (const dep of deps) {
          expect(indexMap.get(dep)!).toBeLessThan(indexMap.get(node)!);
        }
      }
    });
  });
});
