import type { PipelineId, PipelineRun, PipelineRunId } from '@forge/pipeline';

/**
 * Repository interface for PipelineRun aggregates.
 */
export interface PipelineRunRepository {
  /**
   * Persists a pipeline run and its constituent jobs and attempts.
   * Enforces domain state machine transition rules and terminal state immutability.
   */
  save(run: PipelineRun): Promise<void>;

  /**
   * Finds a pipeline run by its unique ID, reconstructing all constituent jobs and attempts.
   */
  findById(id: PipelineRunId): Promise<PipelineRun | null>;

  /**
   * Lists pipeline runs associated with a specific pipeline ID.
   */
  findByPipelineId(pipelineId: PipelineId): Promise<PipelineRun[]>;
}
