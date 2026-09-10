/**
 * Wire (de)serialization for {@link ForgeEvent}s crossing the Redis transport.
 *
 * The event that a downstream consumer observes must represent the **same event** the
 * producer published — `event_id`, `event_type`, `version`, `occurred_at`, and `payload`
 * are never mutated in transit. Both directions run the PR 20 schema (`parseForgeEvent`):
 * unknown additive fields are stripped (forward-compat), a wrong `version` or a malformed
 * payload is rejected. This layer does not introduce a second validation vocabulary.
 */
import { parseForgeEvent, type ForgeEvent } from '@forge/events';
import { RealtimeEventDecodeError } from './errors.js';

/**
 * Validates `event` against the PR 20 schema and returns its canonical JSON wire form.
 * @throws {RealtimeEventDecodeError} when the event does not satisfy the schema.
 */
export function serializeForgeEvent(event: ForgeEvent): string {
  try {
    return JSON.stringify(parseForgeEvent(event));
  } catch (err) {
    throw new RealtimeEventDecodeError(
      `Refusing to publish an invalid ForgeEvent: ${err instanceof Error ? err.message : String(err)}`,
      err instanceof Error ? err : undefined,
    );
  }
}

/**
 * Parses a wire payload back into a validated {@link ForgeEvent}.
 * @throws {RealtimeEventDecodeError} on malformed JSON or schema-invalid content.
 */
export function deserializeForgeEvent(raw: string): ForgeEvent {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new RealtimeEventDecodeError(
      `Realtime payload is not valid JSON: ${err instanceof Error ? err.message : String(err)}`,
      err instanceof Error ? err : undefined,
    );
  }
  try {
    return parseForgeEvent(parsed);
  } catch (err) {
    throw new RealtimeEventDecodeError(
      `Realtime payload failed ForgeEvent validation: ${err instanceof Error ? err.message : String(err)}`,
      err instanceof Error ? err : undefined,
    );
  }
}
