// src/persistence/file-size-counter.ts
// Tracks the byte size of a single model's data.ndjson in memory.
// Eliminates fs.stat on the hot write path — initialized once at startup,
// then maintained purely via increment() after every append.

import * as fs from "node:fs/promises";
import { CompactionError, ValidationError } from "../errors/index.js";

// ---------------------------------------------------------------------------
// FileSizeCounter
// ---------------------------------------------------------------------------

/**
 * Tracks the current byte size of a single `data.ndjson` file in memory.
 *
 * The counter must be initialized via `initialize()` before use. Any
 * method that reads or modifies the size will throw `ValidationError`
 * if called on an uninitialized counter.
 */
export class FileSizeCounter {
  private currentSize: number = 0;
  private initialized: boolean = false;

  // -------------------------------------------------------------------------

  /**
   * Reads the actual file size from disk using `fs.stat` and sets the
   * internal `currentSize`. This is the **only** permitted `fs.stat` call
   * in the write path — it runs exactly once per model at startup.
   *
   * - Missing file (ENOENT): sets `currentSize` to `0` and marks as initialized.
   * - Existing file: sets `currentSize` to `stat.size`.
   * - Already initialized: re-initializes from disk (used after compaction).
   * - Unexpected I/O failure: throws `CompactionError` with cause in `meta`.
   */
  async initialize(filePath: string): Promise<void> {
    try {
      const stat = await fs.stat(filePath);
      this.currentSize = stat.size;
    } catch (err: unknown) {
      if (isEnoent(err)) {
        this.currentSize = 0;
      } else {
        throw new CompactionError(`Failed to stat data file at "${filePath}" during FileSizeCounter initialization.`, {
          meta: { cause: err },
        });
      }
    }
    this.initialized = true;
  }

  // -------------------------------------------------------------------------

  /**
   * Returns the current byte size, which equals the byte offset at which
   * the next record will be appended.
   *
   * Throws `ValidationError` if the counter has not been initialized.
   */
  getSize(): number {
    this.assertInitialized();
    return this.currentSize;
  }

  // -------------------------------------------------------------------------

  /**
   * Increments the internal size by `byteCount` after a successful append.
   *
   * The caller is responsible for computing the correct byte count:
   * ```
   * Buffer.byteLength(serializedLine, 'utf8')
   * ```
   * where `serializedLine` already includes the trailing `\n` newline.
   * The counter does not add the newline itself.
   *
   * Throws `ValidationError` if:
   * - The counter has not been initialized.
   * - `byteCount` is negative.
   * - `byteCount` is not a finite number (NaN, Infinity, -Infinity).
   */
  increment(byteCount: number): void {
    this.assertInitialized();
    if (!Number.isFinite(byteCount) || byteCount < 0) {
      throw new ValidationError(
        `FileSizeCounter.increment() requires a non-negative finite byteCount, got: ${String(byteCount)}`,
        { meta: { byteCount } },
      );
    }
    this.currentSize += byteCount;
  }

  // -------------------------------------------------------------------------

  /**
   * Re-initializes the counter from disk. Identical behavior to calling
   * `initialize()` on an already-initialized counter.
   *
   * Used exclusively after compaction completes — the compacted file has a
   * different size than the pre-compaction file.
   */
  async reset(filePath: string): Promise<void> {
    await this.initialize(filePath);
  }

  // -------------------------------------------------------------------------

  /**
   * Returns `true` if the counter has been initialized, `false` otherwise.
   * Always safe to call — no initialization guard.
   */
  isInitialized(): boolean {
    return this.initialized;
  }

  // -------------------------------------------------------------------------

  private assertInitialized(): void {
    if (!this.initialized) {
      throw new ValidationError("FileSizeCounter has not been initialized. Call initialize() before use.", {
        meta: {},
      });
    }
  }
}

// ---------------------------------------------------------------------------
// FileSizeCounterManager
// ---------------------------------------------------------------------------

/**
 * Manages one `FileSizeCounter` instance per model.
 * A single manager instance is held by the persistence layer for the entire
 * database lifetime.
 */
export class FileSizeCounterManager {
  private readonly counters: Map<string, FileSizeCounter> = new Map();

  // -------------------------------------------------------------------------

  /**
   * Creates (or re-initializes) a `FileSizeCounter` for the given model,
   * then calls `initialize(filePath)` on it.
   *
   * If a counter already exists for `modelName`, it is re-initialized rather
   * than replaced. This covers the post-compaction reset case.
   */
  async initializeModel(modelName: string, filePath: string): Promise<void> {
    const existing = this.counters.get(modelName);
    if (existing !== undefined) {
      await existing.initialize(filePath);
    } else {
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      this.counters.set(modelName, counter);
    }
  }

  // -------------------------------------------------------------------------

  /**
   * Returns the `FileSizeCounter` for the given model.
   *
   * Throws `ValidationError` if no counter has been registered for
   * `modelName`.
   */
  getCounter(modelName: string): FileSizeCounter {
    const counter = this.counters.get(modelName);
    if (counter === undefined) {
      throw new ValidationError(
        `No FileSizeCounter registered for model "${modelName}". Call initializeModel() first.`,
        { meta: { modelName } },
      );
    }
    return counter;
  }

  // -------------------------------------------------------------------------

  /**
   * Returns the current byte size for the given model's data file.
   * Convenience wrapper around `getCounter(modelName).getSize()`.
   */
  getSize(modelName: string): number {
    return this.getCounter(modelName).getSize();
  }

  // -------------------------------------------------------------------------

  /**
   * Increments the byte size counter for the given model.
   * Convenience wrapper around `getCounter(modelName).increment(byteCount)`.
   */
  increment(modelName: string, byteCount: number): void {
    this.getCounter(modelName).increment(byteCount);
  }

  // -------------------------------------------------------------------------

  /**
   * Re-initializes the counter for the given model from disk.
   * Used after compaction to reset the counter to the new file's actual size.
   *
   * Throws `ValidationError` if no counter exists for `modelName`.
   */
  async resetModel(modelName: string, filePath: string): Promise<void> {
    this.getCounter(modelName); // throws ValidationError if missing
    await this.getCounter(modelName).reset(filePath);
  }

  // -------------------------------------------------------------------------

  /**
   * Returns `true` if a counter has been registered for `modelName`.
   */
  hasModel(modelName: string): boolean {
    return this.counters.has(modelName);
  }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

function isEnoent(err: unknown): boolean {
  return typeof err === "object" && err !== null && "code" in err && (err as { code: unknown }).code === "ENOENT";
}
