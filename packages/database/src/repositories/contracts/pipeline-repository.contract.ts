import type { Pipeline, PipelineId } from '@forge/pipeline';

/**
 * Repository interface for Pipeline aggregates.
 */
export interface PipelineRepository {
  /**
   * Persists a new pipeline or updates an existing one.
   */
  save(pipeline: Pipeline): Promise<void>;

  /**
   * Finds a pipeline by its unique ID, fully reconstructing its domain model and DAG.
   */
  findById(id: PipelineId): Promise<Pipeline | null>;

  /**
   * Lists all pipelines ordered by creation timestamp.
   */
  list(): Promise<Pipeline[]>;

  /**
   * Deletes a pipeline by its ID.
   *
   * @returns true if a record was deleted, false if not found.
   */
  delete(id: PipelineId): Promise<boolean>;
}
