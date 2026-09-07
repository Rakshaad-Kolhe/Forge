import { PipelineValidationError } from './errors.js';
import { validateJobRequirements, type JobRequirements } from './requirements.js';
import type { PipelineStepSerialized, StepDefinition } from './types.js';

/**
 * Domain model representing a single step within a pipeline definition.
 */
export class PipelineStep {
  public readonly name: string;
  public readonly command: string;
  public readonly dependsOn: readonly string[];
  public readonly requirements: JobRequirements;

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
    };
  }
}
