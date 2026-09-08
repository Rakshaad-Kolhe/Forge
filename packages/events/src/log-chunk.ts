/**
 * Derives ordered {@link JobLogChunkEvent}s from a completed execution's captured output.
 *
 * PR 19 captures stdout/stderr into two bounded buffers and returns them only on the final
 * `ExecutionResult` — there is no live stream. PR 20 therefore *derives* chunk events after
 * the fact so consumers get the future streaming shape (per-stream ordering, monotonic
 * sequence, truncation marker) without PR 20 having to build the log collector / WebSocket
 * path. See `docs/architecture/events.md` § JobLogChunk.
 *
 * Boundaries are byte boundaries and may bisect a multi-byte UTF-8 sequence; a consumer
 * reassembles a stream by concatenating its chunk payloads in `sequence` order.
 */
import { createForgeEvent, type CreateForgeEventOptions } from './factory.js';
import type { JobLogChunkEvent, LogStream } from './events.js';

/** Default chunk size (64 KiB) — a full 1 MiB PR 19 capture yields at most 16 chunks/stream. */
export const DEFAULT_LOG_CHUNK_BYTES = 65536;

/** Minimal shape needed from an `ExecutionResult` to derive log chunks. */
export interface LogChunkSource {
  readonly stdout: string;
  readonly stderr: string;
  readonly truncated: boolean;
}

export interface DeriveLogChunkOptions extends CreateForgeEventOptions {
  /** Max bytes per chunk payload. Defaults to {@link DEFAULT_LOG_CHUNK_BYTES}. */
  readonly chunkBytes?: number;
  /** Distinct id per derived event (defaults to a fresh generated id each). */
  readonly eventIdFactory?: () => string;
}

interface StreamPlan {
  readonly stream: LogStream;
  readonly text: string;
}

/**
 * @param source       captured output + truncation flag from the execution result.
 * @param correlation  the job/attempt the output belongs to.
 * @returns stdout chunks first (in order), then stderr chunks; `sequence` is monotonic
 *          across the whole list starting at 0; the last element has `final: true`. Empty
 *          streams contribute no chunks; entirely-empty output returns `[]`.
 */
export function deriveLogChunkEvents(
  source: LogChunkSource,
  correlation: { readonly job_id: string; readonly attempt_id: string },
  options: DeriveLogChunkOptions = {},
): JobLogChunkEvent[] {
  const chunkBytes = Math.max(1, Math.floor(options.chunkBytes ?? DEFAULT_LOG_CHUNK_BYTES));
  const plans: StreamPlan[] = [
    { stream: 'stdout', text: source.stdout },
    { stream: 'stderr', text: source.stderr },
  ];

  interface PendingChunk {
    readonly stream: LogStream;
    readonly chunk: string;
    readonly byteOffset: number;
  }
  const pending: PendingChunk[] = [];

  for (const plan of plans) {
    if (plan.text.length === 0) {
      continue;
    }
    const buffer = Buffer.from(plan.text, 'utf8');
    for (let offset = 0; offset < buffer.length; offset += chunkBytes) {
      const slice = buffer.subarray(offset, Math.min(offset + chunkBytes, buffer.length));
      pending.push({
        stream: plan.stream,
        chunk: slice.toString('utf8'),
        byteOffset: offset,
      });
    }
  }

  const nextEventId = (): string | undefined => {
    if (options.eventIdFactory) {
      return options.eventIdFactory();
    }
    return options.eventId;
  };

  return pending.map((item, index) => {
    const createOptions: { now?: Date; eventId?: string } = {};
    if (options.now) {
      createOptions.now = options.now;
    }
    const forcedId = nextEventId();
    if (forcedId !== undefined) {
      createOptions.eventId = forcedId;
    }
    return createForgeEvent(
      'JobLogChunk',
      {
        correlation: { job_id: correlation.job_id, attempt_id: correlation.attempt_id },
        payload: {
          job_id: correlation.job_id,
          attempt_id: correlation.attempt_id,
          sequence: index,
          stream: item.stream,
          chunk: item.chunk,
          byte_offset: item.byteOffset,
          truncated: source.truncated,
          final: index === pending.length - 1,
        },
      },
      createOptions,
    );
  });
}
