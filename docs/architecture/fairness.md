# Fairness, Queue Aging & Starvation Prevention

## 1. Architectural Overview & Problem Statement

Forge V2 introduced deterministic priority scheduling in PR 11, where candidate jobs are evaluated strictly by priority descending: higher-priority jobs always sort before lower-priority jobs.

While strict priority scheduling ensures that mission-critical pipelines take precedence over background tasks, it suffers from a well-known vulnerability in distributed CI/CD engines: **starvation under sustained load**. If an organization maintains a steady influx of priority 50 jobs, a priority 10 job can remain stranded in the queue indefinitely, never receiving worker placement despite being healthy and ready to run.

PR 16 introduces **queue aging** as a deterministic, bounded fairness mechanism. The architecture deliberately does **not** replace priority scheduling; instead, it establishes the invariant:

> **Preserve priority as the primary scheduling signal while allowing sufficiently old work to gain scheduling weight so that continuously lower-priority jobs cannot starve indefinitely under sustained higher-priority load.**

---

## 2. Queue-Aging Mathematical Formula & Bounds

Effective priority is computed dynamically by the scheduler during candidate job ordering:

$$\text{effective\_priority} = \text{base\_priority} + \text{age\_bonus}$$

where the age bonus is a step-wise, monotonically non-decreasing function bounded by a configured ceiling:

$$\text{age\_bonus} = \min\left(\text{max\_age\_bonus}, \left\lfloor \frac{\text{waiting\_ms}}{\text{aging\_interval\_ms}} \right\rfloor \times \text{age\_bonus\_step}\right)$$

### Configuration Parameters

| Parameter         | Environment Variable         | Default         | Constraints                           |
| :---------------- | :--------------------------- | :-------------- | :------------------------------------ |
| `agingIntervalMs` | `FAIRNESS_AGING_INTERVAL_MS` | `60000` (1 min) | $\ge 1000$ ms                         |
| `ageBonusStep`    | `FAIRNESS_AGE_BONUS_STEP`    | `10`            | $\ge 1$                               |
| `maxAgeBonus`     | `FAIRNESS_MAX_AGE_BONUS`     | `500`           | $\le 2000$, $\ge \text{ageBonusStep}$ |

### Boundedness & Priority Band Preservation

The inclusion of `maxAgeBonus` is an essential safety constraint:

- Low-priority jobs can gradually overtake routine higher-priority jobs within a bounded priority distance.
- Low-priority jobs can **never** overpower urgent or emergency priority tiers whose base priority exceeds $\text{base\_priority} + \text{max\_age\_bonus}$.
- Example: An emergency deployment pipeline with priority `1000` will never be overtaken by a background linting job with base priority `0`, even if that background job has waited for days ($\max \text{effective\_priority} = 0 + 500 = 500 < 1000$).

---

## 3. Immutability & Persistence Model

Forge enforces a strict separation between durable domain state and transient scheduler signals:

1. **Base Priority is Immutable**: `job.priority` is committed to PostgreSQL and is **never** mutated by fairness calculations or queue aging.
2. **Zero Schema Migrations**: No `effective_priority` column is persisted. Storing effective priority in the database would create severe cache invalidation and write amplification, as thousands of queued rows would require constant database updates every minute.
3. **Dynamic Derivation from Authoritative Timestamps**: The scheduler computes effective priority in-memory using existing, indexed database timestamp columns:
   - Initial attempts: `jobs.created_at` (or `job.queuedAt` in domain memory).
   - Retried attempts: `jobs.next_attempt_at` (when retry backoff expired).

---

## 4. Retry Fairness & Age Reset Invariant

When a job fails and is rescheduled under a retry policy (PR 14/15), its waiting duration is subject to the **Retry Fairness Reset Invariant**:

- **Active Backoff Gate**: If `next_attempt_at > now`, the job is currently sleeping in backoff. Its waiting time is explicitly `0`, its age bonus is `0`, and `evaluatePlacement` rejects it with `RETRY_BACKOFF_ACTIVE`.
- **Eligibility Age Reset**: Once `next_attempt_at <= now`, the job is eligible for scheduling. Its waiting age is computed starting at `next_attempt_at`, **not** at original `created_at`.
- **Rationale**: If a job was created 4 hours ago, failed 3 times, and had a 10-minute backoff delay, calculating age from `created_at` would grant it an immediate, undeserved massive age bonus the millisecond it woke up, jumping ahead of jobs that had legitimately waited in line. Measuring from `next_attempt_at` ensures that retry attempts wait fairly alongside first-time jobs.

---

## 5. Deterministic Tie-Breaking

To prevent non-deterministic sorting oscillations across scheduling passes, Forge scheduler enforces two-level deterministic ordering:

1. **Primary**: $\text{effective\_priority}$ descending (higher values schedule first).
2. **Secondary (Tie-Breaker)**: Normalized `jobId` ascending code-point order (`idA < idB ? -1 : 1`).

```ts
export function compareFairAgingPriority<T>(
  a: T,
  b: T,
  now: Date,
  config?: Partial<QueueAgingConfig>,
): number {
  const effA = calculateEffectivePriority(a, now, config).effectivePriority;
  const effB = calculateEffectivePriority(b, now, config).effectivePriority;

  if (effA !== effB) {
    return effB - effA;
  }

  const idA = getJobId(a);
  const idB = getJobId(b);

  if (idA < idB) return -1;
  if (idA > idB) return 1;
  return 0;
}
```

This guarantees that:

- For any fixed virtual time instant `now`, the ordering is strictly deterministic and permutation-invariant.
- Shuffling the input array of candidate jobs yields identical output order.

---

## 6. Empirical Starvation Experiment Data

A controlled starvation experiment is implemented in `apps/scheduler/src/fairness.experiment.test.ts`.

### Experiment Scenario

- **Worker Capacity**: 1 worker slot.
- **Target Job**: Low-priority job ($P_{\text{low}} = 10$) queued at $t_0 = 0$.
- **Sustained Load Stream**: A continuous stream of high-priority jobs ($P_{\text{high}} = 50$) arriving at every scheduling interval ($t_i = i \times 60\,\text{s}$).
- **Queue Aging Config**: `interval = 60,000ms`, `step = 10`, `maxAgeBonus = 100`.

### Measured Results

| Metric                           | Strict Priority (`HighestPriorityFirst`)     | Queue Aging (`FairAgingPriority`) |
| :------------------------------- | :------------------------------------------- | :-------------------------------- |
| **Total Rounds Tested**          | 10 rounds                                    | 10 rounds                         |
| **Bypass Count Before Overtake** | $\infty$ (Starved indefinitely, 10 bypasses) | **5 rounds**                      |
| **Overtake Timestamp**           | Never                                        | **$t = 300,000$ ms (5 minutes)**  |
| **Scheduled Job at Round 5**     | High-priority stream job #5                  | **Target low-priority job**       |
| **Placement Outcome**            | `UNSCHEDULABLE` (queued)                     | `SCHEDULED` (placed on worker)    |

### Cycle-by-Cycle Trace Under Queue Aging

```text
Round 0 (t = 0m):  Target eff = 10, HighStream eff = 50  → High wins (Bypass 1)
Round 1 (t = 1m):  Target eff = 20, HighStream eff = 50  → High wins (Bypass 2)
Round 2 (t = 2m):  Target eff = 30, HighStream eff = 50  → High wins (Bypass 3)
Round 3 (t = 3m):  Target eff = 40, HighStream eff = 50  → High wins (Bypass 4)
Round 4 (t = 4m):  Target eff = 50, HighStream eff = 50  → Tie-break by jobId (Bypass 5)
Round 5 (t = 5m):  Target eff = 60, HighStream eff = 50  → TARGET OVERTAKES & SCHEDULES!
```

### Saturation Boundary Verification

When tested with an urgent stream job with priority `200` against the target job ($P_{\text{low}} = 10, \text{maxAgeBonus} = 100$):

- Target job effective priority reaches ceiling of $10 + 100 = 110$.
- Target job is bypassed on all 15 rounds.
- Proves empirically that queue aging cannot violate urgent priority band boundaries.

---

## 7. Hard Gates Preserved

Queue aging orders jobs; it **never** relaxes operational or execution safety gates:

1. **PR 09 Capability & Resource Matcher**: A job aged to priority 1000 still cannot be placed on a worker lacking requested CPU, RAM, or Docker capabilities.
2. **PR 10 Deterministic Worker Selection**: Once a job is chosen for placement, worker selection proceeds deterministically without alteration.
3. **PR 12 Distributed Leases**: Every placed job must acquire a valid, time-bounded distributed PostgreSQL worker lease before execution.
4. **PR 14 Active Backoff**: Inactive retry jobs cannot be scheduled regardless of age.
5. **PR 15 Worker Loss Recovery**: Expired leases and lost workers remain governed by atomic lease recovery and dead-letter queues.
