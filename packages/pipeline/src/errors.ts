/**
 * Base domain error for all @forge/pipeline errors.
 */
export abstract class PipelineDomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Thrown when pipeline definition validation fails.
 */
export class PipelineValidationError extends PipelineDomainError {
  public readonly issues: readonly string[];

  constructor(message: string, issues: readonly string[] = []) {
    super(message);
    this.issues = issues;
  }
}

/**
 * Thrown when duplicate step names are found in a pipeline definition.
 */
export class DuplicateStepError extends PipelineValidationError {
  public readonly stepName: string;

  constructor(stepName: string) {
    super(`Duplicate step name "${stepName}" in pipeline definition`, [
      `Step "${stepName}" is defined multiple times`,
    ]);
    this.stepName = stepName;
  }
}

/**
 * Thrown when a step depends on a non-existent step.
 */
export class MissingDependencyError extends PipelineValidationError {
  public readonly stepName: string;
  public readonly missingDependency: string;

  constructor(stepName: string, missingDependency: string) {
    super(`Step "${stepName}" depends on missing step "${missingDependency}"`, [
      `Step "${stepName}" depends on missing step "${missingDependency}"`,
    ]);
    this.stepName = stepName;
    this.missingDependency = missingDependency;
  }
}

/**
 * Thrown when a step declares a dependency on itself.
 */
export class SelfDependencyError extends PipelineValidationError {
  public readonly stepName: string;

  constructor(stepName: string) {
    super(`Step "${stepName}" cannot depend on itself`, [
      `Step "${stepName}" declares a self-dependency`,
    ]);
    this.stepName = stepName;
  }
}

/**
 * Thrown when a circular dependency cycle is detected in the pipeline DAG.
 */
export class CycleDetectedError extends PipelineValidationError {
  public readonly cyclePath: readonly string[];

  constructor(cyclePath: readonly string[]) {
    const formatted = cyclePath.join(' -> ');
    super(`Dependency cycle detected: ${formatted}`, [
      `Circular dependency found along path: ${formatted}`,
    ]);
    this.cyclePath = cyclePath;
  }
}

/**
 * Thrown when an illegal state machine transition is attempted.
 */
export class InvalidStateTransitionError extends PipelineDomainError {
  public readonly entityType: string;
  public readonly entityId: string;
  public readonly currentStatus: string;
  public readonly requestedStatus: string;

  constructor(
    entityType: string,
    entityId: string,
    currentStatus: string,
    requestedStatus: string,
    details?: string,
  ) {
    const extra = details ? ` (${details})` : '';
    super(
      `Cannot transition ${entityType} "${entityId}" from status "${currentStatus}" to "${requestedStatus}"${extra}`,
    );
    this.entityType = entityType;
    this.entityId = entityId;
    this.currentStatus = currentStatus;
    this.requestedStatus = requestedStatus;
  }
}

/**
 * Thrown when an attempt is made to add duplicate jobs for the same step.
 */
export class DuplicateJobError extends PipelineDomainError {
  public readonly stepName: string;
  public readonly pipelineRunId: string;

  constructor(stepName: string, pipelineRunId: string) {
    super(`Duplicate job for step "${stepName}" in pipeline run "${pipelineRunId}"`);
    this.stepName = stepName;
    this.pipelineRunId = pipelineRunId;
  }
}

/**
 * Thrown when job execution requirements validation fails.
 */
export class JobRequirementsValidationError extends PipelineValidationError {
  constructor(message: string, issues: readonly string[] = []) {
    super(message, issues);
  }
}

/**
 * Thrown when job scheduling priority validation fails.
 */
export class InvalidJobPriorityError extends PipelineValidationError {
  constructor(message: string, issues: readonly string[] = []) {
    super(message, issues);
  }
}

/**
 * Thrown when retry policy configuration validation fails.
 */
export class RetryPolicyValidationError extends PipelineValidationError {
  constructor(message: string, issues: readonly string[] = []) {
    super(message, issues);
  }
}
