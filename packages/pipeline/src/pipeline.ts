import { DirectedAcyclicGraph } from './dag.js';
import { DuplicateStepError, PipelineValidationError } from './errors.js';
import { PipelineStep } from './pipeline-step.js';
import {
  createPipelineId,
  type PipelineDefinition,
  type PipelineId,
  type PipelineSerialized,
} from './types.js';

/**
 * Domain model representing a validated, reusable Pipeline definition.
 */
export class Pipeline {
  public readonly id: PipelineId;
  public readonly name: string;
  private readonly stepsMap = new Map<string, PipelineStep>();
  private readonly orderedSteps: readonly PipelineStep[];
  private readonly dag: DirectedAcyclicGraph;

  constructor(definition: PipelineDefinition) {
    if (!definition.name || definition.name.trim().length === 0) {
      throw new PipelineValidationError('Pipeline name cannot be empty');
    }

    if (!definition.steps || definition.steps.length === 0) {
      throw new PipelineValidationError('Pipeline must define at least one step');
    }

    this.id = createPipelineId(
      definition.id ?? `pipe-${definition.name.toLowerCase().replace(/[^a-z0-9_-]/g, '-')}`,
    );
    this.name = definition.name.trim();

    const stepsList: PipelineStep[] = [];

    // Instantiate and check for duplicate step names
    for (const stepDef of definition.steps) {
      const step = new PipelineStep(stepDef);
      if (this.stepsMap.has(step.name)) {
        throw new DuplicateStepError(step.name);
      }
      this.stepsMap.set(step.name, step);
      stepsList.push(step);
    }

    this.orderedSteps = Object.freeze(stepsList);

    // Build and validate DAG (throws on self-deps, missing deps, or cycles)
    this.dag = new DirectedAcyclicGraph(
      this.orderedSteps.map((step) => ({
        name: step.name,
        dependencies: step.dependsOn,
      })),
    );

    Object.freeze(this);
  }

  /**
   * Retrieves a step by its name.
   */
  public getStep(name: string): PipelineStep | undefined {
    return this.stepsMap.get(name);
  }

  /**
   * Returns an immutable array of all steps in the order they were defined.
   */
  public getSteps(): readonly PipelineStep[] {
    return this.orderedSteps;
  }

  /**
   * Returns the underlying validated dependency DAG for this pipeline.
   */
  public getDag(): DirectedAcyclicGraph {
    return this.dag;
  }

  /**
   * Returns a plain-object representation for serialization/testing.
   */
  public toJSON(): PipelineSerialized {
    return {
      id: this.id,
      name: this.name,
      steps: this.orderedSteps.map((step) => step.toJSON()),
    };
  }
}
