/**
 * Captures process output up to a maximum byte threshold, marking truncation when exceeded.
 */
export class OutputCollector {
  private stdoutBuffer: Buffer[] = [];
  private stderrBuffer: Buffer[] = [];
  private stdoutBytes = 0;
  private stderrBytes = 0;
  private isTruncated = false;

  constructor(public readonly maxBytes: number = 1048576) {}

  /**
   * Appends a chunk to the stdout stream if within byte limits.
   */
  public pushStdout(chunk: Buffer | string): void {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    if (this.stdoutBytes + buf.length <= this.maxBytes) {
      this.stdoutBuffer.push(buf);
      this.stdoutBytes += buf.length;
    } else {
      this.isTruncated = true;
      const remaining = this.maxBytes - this.stdoutBytes;
      if (remaining > 0) {
        this.stdoutBuffer.push(buf.subarray(0, remaining));
        this.stdoutBytes += remaining;
      }
    }
  }

  /**
   * Appends a chunk to the stderr stream if within byte limits.
   */
  public pushStderr(chunk: Buffer | string): void {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    if (this.stderrBytes + buf.length <= this.maxBytes) {
      this.stderrBuffer.push(buf);
      this.stderrBytes += buf.length;
    } else {
      this.isTruncated = true;
      const remaining = this.maxBytes - this.stderrBytes;
      if (remaining > 0) {
        this.stderrBuffer.push(buf.subarray(0, remaining));
        this.stderrBytes += remaining;
      }
    }
  }

  /**
   * Returns accumulated stdout as a string.
   */
  public getStdout(): string {
    return Buffer.concat(this.stdoutBuffer).toString('utf-8');
  }

  /**
   * Returns accumulated stderr as a string.
   */
  public getStderr(): string {
    return Buffer.concat(this.stderrBuffer).toString('utf-8');
  }

  /**
   * Indicates whether output exceeded maxBytes and was truncated.
   */
  public get truncated(): boolean {
    return this.isTruncated;
  }
}
