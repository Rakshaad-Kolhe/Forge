import { CycleDetectedError, MissingDependencyError, SelfDependencyError } from './errors.js';

export interface DagNodeInput {
  name: string;
  dependencies?: readonly string[];
}

/**
 * Directed Acyclic Graph (DAG) abstraction representing step dependencies.
 * Provides deterministic graph query operations and validation.
 */
export class DirectedAcyclicGraph {
  // Step -> Set of upstream dependencies (what this step depends on)
  private readonly upstream = new Map<string, Set<string>>();
  // Step -> Set of downstream dependents (what depends on this step)
  private readonly downstream = new Map<string, Set<string>>();
  // All registered step names in the graph
  private readonly nodeNames = new Set<string>();

  /**
   * Constructs a DirectedAcyclicGraph from a list of nodes.
   *
   * @param nodes - Collection of DAG nodes with their declared dependencies
   * @throws {SelfDependencyError} If a node depends on itself
   * @throws {MissingDependencyError} If a dependency does not exist in nodes
   * @throws {CycleDetectedError} If a dependency cycle exists
   */
  constructor(nodes: readonly DagNodeInput[]) {
    // 1. Register all nodes
    for (const node of nodes) {
      this.nodeNames.add(node.name);
      this.upstream.set(node.name, new Set<string>());
      this.downstream.set(node.name, new Set<string>());
    }

    // 2. Register dependencies and validate existence and self-dependency
    for (const node of nodes) {
      const deps = node.dependencies ?? [];
      for (const dep of deps) {
        if (dep === node.name) {
          throw new SelfDependencyError(node.name);
        }
        if (!this.nodeNames.has(dep)) {
          throw new MissingDependencyError(node.name, dep);
        }
        this.upstream.get(node.name)!.add(dep);
        this.downstream.get(dep)!.add(node.name);
      }
    }

    // 3. Cycle detection
    const cycle = this.findCycle();
    if (cycle !== null) {
      throw new CycleDetectedError(cycle);
    }
  }

  /**
   * Returns total count of nodes in the graph.
   */
  public getNodeCount(): number {
    return this.nodeNames.size;
  }

  /**
   * Checks if a step exists in the graph.
   */
  public hasNode(name: string): boolean {
    return this.nodeNames.has(name);
  }

  /**
   * Returns all node names in deterministic sorted order.
   */
  public getAllNodes(): readonly string[] {
    return Array.from(this.nodeNames).sort((a, b) => a.localeCompare(b));
  }

  /**
   * Returns the upstream dependencies that the specified step directly depends on.
   */
  public getDependencies(stepName: string): readonly string[] {
    const deps = this.upstream.get(stepName);
    if (!deps) {
      return [];
    }
    return Array.from(deps).sort((a, b) => a.localeCompare(b));
  }

  /**
   * Returns the downstream dependents that directly depend on the specified step.
   */
  public getDependents(stepName: string): readonly string[] {
    const dependents = this.downstream.get(stepName);
    if (!dependents) {
      return [];
    }
    return Array.from(dependents).sort((a, b) => a.localeCompare(b));
  }

  /**
   * Returns all root steps (steps that have zero upstream dependencies).
   */
  public getRoots(): readonly string[] {
    const roots: string[] = [];
    for (const [name, deps] of this.upstream.entries()) {
      if (deps.size === 0) {
        roots.push(name);
      }
    }
    return roots.sort((a, b) => a.localeCompare(b));
  }

  /**
   * Returns all leaf steps (steps that have zero downstream dependents).
   */
  public getLeaves(): readonly string[] {
    const leaves: string[] = [];
    for (const [name, dependents] of this.downstream.entries()) {
      if (dependents.size === 0) {
        leaves.push(name);
      }
    }
    return leaves.sort((a, b) => a.localeCompare(b));
  }

  /**
   * Evaluates if a specific step is ready to execute given a set of completed steps.
   * A step is ready if:
   * 1. It is not already completed.
   * 2. All of its direct upstream dependencies are in completedStepNames.
   */
  public isReady(stepName: string, completedStepNames: Set<string> | readonly string[]): boolean {
    const completedSet =
      completedStepNames instanceof Set ? completedStepNames : new Set(completedStepNames);

    if (!this.nodeNames.has(stepName)) {
      return false;
    }

    if (completedSet.has(stepName)) {
      return false;
    }

    const deps = this.upstream.get(stepName);
    if (!deps || deps.size === 0) {
      return true;
    }

    for (const dep of deps) {
      if (!completedSet.has(dep)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns all steps that are currently ready to execute given a set of completed steps.
   * Results are returned in deterministic alphabetical order.
   */
  public getReadySteps(completedStepNames: Set<string> | readonly string[]): readonly string[] {
    const completedSet =
      completedStepNames instanceof Set ? completedStepNames : new Set(completedStepNames);

    const ready: string[] = [];
    for (const name of this.nodeNames) {
      if (this.isReady(name, completedSet)) {
        ready.push(name);
      }
    }

    return ready.sort((a, b) => a.localeCompare(b));
  }

  /**
   * Returns true if the graph contains any cycle.
   */
  public hasCycle(): boolean {
    return this.findCycle() !== null;
  }

  /**
   * Detects whether a dependency cycle exists using DFS.
   * If a cycle is detected, returns the cycle path (e.g. ['A', 'B', 'C', 'A']).
   * Otherwise returns null.
   */
  public findCycle(): readonly string[] | null {
    // 0 = unvisited, 1 = visiting (in active recursion stack), 2 = visited
    const state = new Map<string, number>();
    const parent = new Map<string, string>();

    for (const name of this.nodeNames) {
      state.set(name, 0);
    }

    const sortedNodes = Array.from(this.nodeNames).sort((a, b) => a.localeCompare(b));

    for (const startNode of sortedNodes) {
      if (state.get(startNode) === 0) {
        const cycle = this.dfsDetectCycle(startNode, state, parent);
        if (cycle !== null) {
          return cycle;
        }
      }
    }

    return null;
  }

  private dfsDetectCycle(
    u: string,
    state: Map<string, number>,
    parent: Map<string, string>,
  ): readonly string[] | null {
    state.set(u, 1);

    const dependents = Array.from(this.downstream.get(u) ?? []).sort((a, b) => a.localeCompare(b));

    for (const v of dependents) {
      if (state.get(v) === 1) {
        // Cycle found! Reconstruct cycle path from u back to v, then append v
        const path: string[] = [v];
        let curr = u;
        while (curr !== v) {
          path.push(curr);
          curr = parent.get(curr)!;
        }
        path.push(v);
        return path.reverse();
      }

      if (state.get(v) === 0) {
        parent.set(v, u);
        const cycle = this.dfsDetectCycle(v, state, parent);
        if (cycle !== null) {
          return cycle;
        }
      }
    }

    state.set(u, 2);
    return null;
  }

  /**
   * Returns a deterministic topological ordering using Kahn's algorithm.
   *
   * Tie-Breaking Rule:
   * When multiple nodes have an in-degree of 0 simultaneously, they are processed
   * in lexicographical (alphabetical) order by step name.
   */
  public getTopologicalOrder(): readonly string[] {
    const inDegree = new Map<string, number>();
    for (const name of this.nodeNames) {
      inDegree.set(name, this.upstream.get(name)!.size);
    }

    // Nodes with 0 in-degree currently available
    const available: string[] = [];
    for (const [name, deg] of inDegree.entries()) {
      if (deg === 0) {
        available.push(name);
      }
    }

    // Keep available sorted lexicographically for deterministic execution
    available.sort((a, b) => a.localeCompare(b));

    const result: string[] = [];

    while (available.length > 0) {
      // Pick the lexicographically smallest node among eligible nodes
      const node = available.shift()!;
      result.push(node);

      const dependents = Array.from(this.downstream.get(node) ?? []);
      for (const dep of dependents) {
        const newDeg = inDegree.get(dep)! - 1;
        inDegree.set(dep, newDeg);
        if (newDeg === 0) {
          available.push(dep);
          available.sort((a, b) => a.localeCompare(b));
        }
      }
    }

    return result;
  }
}
