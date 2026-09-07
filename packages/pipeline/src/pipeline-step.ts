import type { RetryPolicy } from '@forge/contracts';
import { PipelineValidationError } from './errors.js';
import { validateJobPriority } from './priority.js';
import { validateJobRequirements, type JobRequirements } from './requirements.js';
import { validateRetryPolicy } from './retry.js';
import type { PipelineStepSerialized, StepDefinition } from './types.js';

/**
 * Domain model representing a single step within a pipeline definition.
 */
export class PipelineStep {
  public readonly name: string;
  public readonly command: string;
  public readonly dependsOn: readonly string[];
  public readonly requirements: JobRequirements;
  public readonly priority: number;
  public readonly retryPolicy?: RetryPolicy;

  constructor(definition: StepDefinition) {
    if (!definition.name || definition.name.trim().length === 0) {
      throw new PipelineValidationError('Step name cannot be empty');
    }

    if (!definition.command || definition.command.trim().length === 0) {
      throw new PipelineValidationError(`Step "${definition.name}" must have a non-empty command`);
    }

    this.name = definition.name.trim();
    this.command = definition.command.trim();

    // Deduplicate and defensively copy dependency list
    const rawDeps = definition.dependsOn ?? [];
    const uniqueDeps = Array.from(new Set(rawDeps.map((d) => d.trim())));
    this.dependsOn = Object.freeze(uniqueDeps);

    // Validate and freeze execution requirements
    this.requirements = validateJobRequirements(definition.requirements);

    // Validate and store scheduling priority
    this.priority = validateJobPriority(definition.priority);

    // Validate and freeze retry policy
    this.retryPolicy = validateRetryPolicy(definition.retry);

    Object.freeze(this);
  }

  /**
   * Returns a plain-object representation for serialization/testing.
   */
  public toJSON(): PipelineStepSerialized {
    return {
      name: this.name,
      command: this.command,
      dependsOn: [...this.dependsOn],
      ...(Object.keys(this.requirements).length > 0 ? { requirements: this.requirements } : {}),
      priority: this.priority,
      ...(this.retryPolicy ? { retry: this.retryPolicy } : {}),
    };
  }
}
