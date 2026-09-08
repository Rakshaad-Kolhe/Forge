/**
 * Mulberry32 deterministic pseudo-random number generator.
 *
 * Guarantees 100% repeatable, seedable random distributions for benchmark fixtures
 * and workloads without relying on non-deterministic Math.random().
 */
export class SeededPRNG {
  private state: number;
  private readonly initialSeed: number;

  constructor(seed: number) {
    this.initialSeed = Math.floor(seed);
    // Initialize 32-bit internal state
    this.state = this.initialSeed | 0;
  }

  public getSeed(): number {
    return this.initialSeed;
  }

  /**
   * Generates a pseudo-random floating-point number in [0, 1).
   */
  public next(): number {
    let t = (this.state += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /**
   * Alias for next() returning float in [0, 1).
   */
  public nextFloat(): number {
    return this.next();
  }

  /**
   * Generates an integer uniformly distributed in [min, max] (inclusive).
   */
  public nextInt(min: number, max: number): number {
    const low = Math.ceil(min);
    const high = Math.floor(max);
    return Math.floor(this.next() * (high - low + 1)) + low;
  }

  /**
   * Selects a random element from a non-empty array.
   */
  public nextElement<T>(array: readonly T[]): T {
    if (!array.length) {
      throw new Error('Cannot select element from empty array');
    }
    const idx = this.nextInt(0, array.length - 1);
    return array[idx]!;
  }

  /**
   * Returns a deterministically shuffled shallow copy of the input array using Fisher-Yates algorithm.
   */
  public shuffle<T>(array: readonly T[]): T[] {
    const copy = [...array];
    for (let i = copy.length - 1; i > 0; i--) {
      const j = this.nextInt(0, i);
      const temp = copy[i]!;
      copy[i] = copy[j]!;
      copy[j] = temp;
    }
    return copy;
  }
}

/**
 * Convenience factory to create a seeded PRNG.
 */
export function createSeededRandom(seed = 42): SeededPRNG {
  return new SeededPRNG(seed);
}
