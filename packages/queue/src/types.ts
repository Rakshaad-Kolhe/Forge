/**
 * Represents the minimal, immutable job-dispatch message transported across Redis.
 * Contains stable domain references, avoiding serialization of heavy aggregates or secrets.
 */
export interface QueueMessage {
  /**
   * Globally unique identifier for this specific queued message instance.
   * Distinct from jobId and attemptNumber to permit duplicate delivery tracking.
   */
  readonly messageId: string;

  /**
   * Reference to the authoritative domain Job in PostgreSQL.
   */
  readonly jobId: string;

  /**
   * Reference to the enclosing PipelineRun in PostgreSQL.
   */
  readonly pipelineRunId: string;

  /**
   * Name of the pipeline step being dispatched.
   */
  readonly stepName: string;

  /**
   * The 1-based attempt counter for this job execution.
   */
  readonly attemptNumber: number;

  /**
   * ISO 8601 timestamp when this message was enqueued.
   */
  readonly enqueuedAt: string;
}

/**
 * Input arguments for enqueuing a job dispatch message.
 */
export interface QueueMessageInput {
  readonly jobId: string;
  readonly pipelineRunId: string;
  readonly stepName: string;
  readonly attemptNumber?: number;
  readonly customMessageId?: string;
}

/**
 * Result returned upon successful enqueue of a message.
 */
export interface EnqueueResult {
  readonly messageId: string;
  readonly enqueuedAt: string;
  readonly deduplicated?: boolean;
}

/**
 * Encapsulates a delivered message with delivery-specific tracking metadata.
 */
export interface QueueDelivery {
  /**
   * The payload message.
   */
  readonly message: QueueMessage;

  /**
   * Unique identifier for this delivery attempt.
   */
  readonly deliveryId: string;

  /**
   * ISO 8601 timestamp when this message was dequeued / delivered.
   */
  readonly deliveredAt: string;

  /**
   * ISO 8601 timestamp when the visibility lease expires, making the message recoverable.
   */
  readonly visibilityExpiresAt: string;

  /**
   * Cumulative count of delivery attempts for this message (1 on first delivery).
   */
  readonly deliveryCount: number;
}

/**
 * Options for dequeuing messages.
 */
export interface DequeueOptions {
  /**
   * Custom visibility timeout in seconds for this delivery.
   * Defaults to the queue's defaultVisibilityTimeoutSeconds.
   */
  visibilityTimeoutSeconds?: number;
}

/**
 * Options for reclaiming expired in-flight messages.
 */
export interface ReclaimOptions {
  /**
   * Maximum number of expired messages to reclaim in a single atomic invocation.
   * Default: 50.
   */
  batchSize?: number;
}

/**
 * Configuration options for initializing a JobQueue.
 */
export interface JobQueueOptions {
  /**
   * Logical queue name (e.g. 'jobs', 'ci-jobs').
   */
  queueName: string;

  /**
   * Default visibility timeout in seconds before unacknowledged messages become recoverable.
   * Default: 30 seconds.
   */
  defaultVisibilityTimeoutSeconds?: number;

  /**
   * Maximum permitted size of serialized message JSON in bytes.
   * Default: 65,536 (64 KB).
   */
  maxPayloadSizeBytes?: number;
}

/**
 * Minimal, reliable Redis-backed FIFO Job Queue interface.
 * Implements at-least-once delivery with explicit acknowledgement and recovery.
 */
export interface JobQueue {
  /**
   * The name of this queue.
   */
  readonly queueName: string;

  /**
   * Appends a job message to the FIFO queue.
   * Validates payload before insertion.
   *
   * @throws {QueueValidationError} If input fields are invalid or payload exceeds size limit.
   * @throws {QueueUnavailableError} If Redis is unreachable.
   */
  enqueue(input: QueueMessageInput): Promise<EnqueueResult>;

  /**
   * Atomically dequeues the next available ready message in strict FIFO order,
   * moving it into the in-flight state with a visibility timeout.
   *
   * @returns The delivered message and delivery metadata, or null if queue is currently empty.
   * @throws {QueueUnavailableError} If Redis is unreachable (never returns null on connection failure).
   */
  dequeue(options?: DequeueOptions): Promise<QueueDelivery | null>;

  /**
   * Explicitly acknowledges a message, permanently removing it from in-flight and storage.
   * Idempotent: repeated calls for the same message return false harmlessly.
   *
   * @returns true if the message was in-flight and removed; false if already acknowledged or not found.
   * @throws {QueueUnavailableError} If Redis is unreachable.
   */
  acknowledge(messageId: string): Promise<boolean>;

  /**
   * Returns the count of currently ready messages waiting to be dequeued.
   * Excludes in-flight, acknowledged, and recovered messages.
   *
   * @throws {QueueUnavailableError} If Redis is unreachable.
   */
  depth(): Promise<number>;

  /**
   * Returns the count of messages currently in-flight (delivered but not yet acknowledged or reclaimed).
   *
   * @throws {QueueUnavailableError} If Redis is unreachable.
   */
  inFlightCount(): Promise<number>;

  /**
   * Scans in-flight messages whose visibility timeout has expired and moves them
   * back to the ready queue for redelivery.
   *
   * @returns The number of expired messages successfully reclaimed.
   * @throws {QueueUnavailableError} If Redis is unreachable.
   */
  reclaimExpired(options?: ReclaimOptions): Promise<number>;

  /**
   * Closes the queue resources.
   */
  close(): Promise<void>;
}
