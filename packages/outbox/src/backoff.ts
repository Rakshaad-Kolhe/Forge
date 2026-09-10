/**
 * Deterministic exponential backoff for outbox delivery retries.
 * `min(maxMs, baseMs * 2^n)` where `n` is the number of delivery attempts already made.
 * No jitter — multiple dispatchers are de-synchronised by `FOR UPDATE SKIP LOCKED`, and
 * determinism keeps tests and benchmarks reproducible (see the project determinism rule).
 */
export function outboxBackoffMs(
  deliveryAttemptCount: number,
  baseMs: number,
  maxMs: number,
): number {
  const n = deliveryAttemptCount > 0 ? deliveryAttemptCount : 0;
  return Math.min(maxMs, baseMs * 2 ** n);
}
