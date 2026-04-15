// src/index-manager/index.ts

import { access } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { ValidationError } from "../errors/ValidationError.js";

export type PrimaryKey = string | number;

export type FieldValue = string | number | boolean | null;

export interface IndexManagerOptions {
  primaryKeyField: string;
  indexedFields: Set<string>;
}

export type LogicalIndex = Map<string, Map<FieldValue, Set<PrimaryKey>>>;

export type PhysicalIndex = Map<PrimaryKey, number>;

export interface IndexManager<TRecord extends Record<string, unknown>> {
  add(record: TRecord, offset: number): void;

  update(oldRecord: TRecord, newRecord: TRecord, newOffset: number): void;

  delete(id: PrimaryKey): void;

  getOffset(id: PrimaryKey): number | undefined;

  getByField(field: string, value: FieldValue): Set<PrimaryKey> | undefined;

  clear(): void;

  rebuild(filePath: string): Promise<void>;

  has(id: PrimaryKey): boolean;

  size(): number;
}

function isValidFieldValue(value: unknown): value is FieldValue {
  return value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}

function isValidPrimaryKey(value: unknown): value is PrimaryKey {
  return typeof value === "string" || typeof value === "number";
}

export class IndexManagerImpl<TRecord extends Record<string, unknown>> implements IndexManager<TRecord> {
  private logicalIndex: Map<string, Map<FieldValue, Set<PrimaryKey>>>;
  private physicalIndex: Map<PrimaryKey, number>;
  private reverseMap: Map<PrimaryKey, Map<string, FieldValue>>;
  private readonly options: IndexManagerOptions;

  constructor(options: IndexManagerOptions) {
    this.options = options;
    this.logicalIndex = new Map();
    this.physicalIndex = new Map();
    this.reverseMap = new Map();
  }

  /**
   * Registers a record in both the logical and physical index.
   * Upsert behavior — duplicate IDs overwrite previous entries.
   * Logical index is updated before the physical index.
   */
  add(record: TRecord, offset: number): void {
    const id = record[this.options.primaryKeyField];
    if (!isValidPrimaryKey(id)) {
      return;
    }

    // Logical index — update first (per spec section 12.6)
    const reverseEntry: Map<string, FieldValue> = new Map();
    for (const field of this.options.indexedFields) {
      if (field === this.options.primaryKeyField) {
        continue;
      }
      const rawValue = record[field];
      if (!isValidFieldValue(rawValue)) {
        continue;
      }
      let valueMap = this.logicalIndex.get(field);
      if (valueMap === undefined) {
        valueMap = new Map();
        this.logicalIndex.set(field, valueMap);
      }
      let idSet = valueMap.get(rawValue);
      if (idSet === undefined) {
        idSet = new Set();
        valueMap.set(rawValue, idSet);
      }
      idSet.add(id);
      reverseEntry.set(field, rawValue);
    }

    // Reverse map — update after logical, before physical
    this.reverseMap.set(id, reverseEntry);

    // Physical index — update last
    this.physicalIndex.set(id, offset);
  }

  /**
   * Transitions a record from its old state to its new state in all indexes.
   * Removes old logical index entries, adds new ones, updates the reverse map
   * and physical index. Order: logical → reverse map → physical.
   */
  update(oldRecord: TRecord, newRecord: TRecord, newOffset: number): void {
    const id = newRecord[this.options.primaryKeyField];
    if (!isValidPrimaryKey(id)) {
      return;
    }

    // Logical index — update first
    const newReverseEntry: Map<string, FieldValue> = new Map();
    for (const field of this.options.indexedFields) {
      if (field === this.options.primaryKeyField) {
        continue;
      }
      const oldValue = oldRecord[field];
      const newValue = newRecord[field];

      // Remove id from old logical index entry
      if (isValidFieldValue(oldValue)) {
        const valueMap = this.logicalIndex.get(field);
        if (valueMap !== undefined) {
          const idSet = valueMap.get(oldValue);
          if (idSet !== undefined) {
            idSet.delete(id);
            if (idSet.size === 0) {
              valueMap.delete(oldValue);
            }
          }
        }
      }

      // Add id to new logical index entry
      if (isValidFieldValue(newValue)) {
        let valueMap = this.logicalIndex.get(field);
        if (valueMap === undefined) {
          valueMap = new Map();
          this.logicalIndex.set(field, valueMap);
        }
        let idSet = valueMap.get(newValue);
        if (idSet === undefined) {
          idSet = new Set();
          valueMap.set(newValue, idSet);
        }
        idSet.add(id);
        newReverseEntry.set(field, newValue);
      }
    }

    // Reverse map — update after logical
    this.reverseMap.set(id, newReverseEntry);

    // Physical index — update last
    this.physicalIndex.set(id, newOffset);
  }

  /**
   * Removes a record from all indexes. No-op if the id does not exist.
   * Uses the reverse map to locate and clean up logical index entries.
   * Order: logical → reverse map → physical.
   */
  delete(id: PrimaryKey): void {
    if (!this.physicalIndex.has(id)) {
      return;
    }

    // Logical index — clean up first using reverse map
    const reverseEntry = this.reverseMap.get(id);
    if (reverseEntry !== undefined) {
      for (const [field, value] of reverseEntry) {
        const valueMap = this.logicalIndex.get(field);
        if (valueMap !== undefined) {
          const idSet = valueMap.get(value);
          if (idSet !== undefined) {
            idSet.delete(id);
            if (idSet.size === 0) {
              valueMap.delete(value);
            }
          }
        }
      }
    }

    // Reverse map — remove after logical
    this.reverseMap.delete(id);

    // Physical index — remove last
    this.physicalIndex.delete(id);
  }

  /**
   * Returns the byte offset of the record's line in data.ndjson,
   * or undefined if the primary key is not in the physical index.
   */
  getOffset(id: PrimaryKey): number | undefined {
    return this.physicalIndex.get(id);
  }

  /**
   * Returns a defensive copy of the Set of primary keys that have
   * the given value for the given indexed field.
   * Returns undefined if the field is not indexed, or if no records
   * have that value (never returns an empty Set).
   */
  getByField(field: string, value: FieldValue): Set<PrimaryKey> | undefined {
    if (!this.options.indexedFields.has(field)) {
      return undefined;
    }
    const idSet = this.logicalIndex.get(field)?.get(value);
    if (idSet === undefined || idSet.size === 0) {
      return undefined;
    }
    return new Set(idSet);
  }

  /**
   * Resets all internal indexes to empty state.
   * Uses .clear() on each Map — does not reassign fields.
   */
  clear(): void {
    this.logicalIndex.clear();
    this.reverseMap.clear();
    this.physicalIndex.clear();
  }

  /**
   * Rebuilds all indexes from scratch by streaming data.ndjson line by line.
   * Resolves immediately if the file does not exist.
   * Uses last-occurrence-wins semantics via upsert in add() and no-op delete().
   */
  rebuild(filePath: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this.clear();

      // Resolve immediately if file does not exist
      access(filePath)
        .then(() => {
          const rl = createInterface({
            input: createReadStream(filePath),
            crlfDelay: Infinity,
          });

          let currentOffset = 0;
          // Track lines as [lineContent, startOffset] so we can detect the last line
          // and handle malformed-final-line vs malformed-mid-file differently.
          const lines: Array<{ line: string; offset: number }> = [];

          rl.on("line", (line: string) => {
            lines.push({ line, offset: currentOffset });
            currentOffset += Buffer.byteLength(line, "utf8") + 1;
          });

          rl.on("error", (err: unknown) => {
            reject(err instanceof Error ? err : new Error(String(err)));
          });

          rl.on("close", () => {
            try {
              for (let i = 0; i < lines.length; i++) {
                const entry = lines[i];
                if (entry === undefined) {
                  continue;
                }
                const { line, offset } = entry;
                const isLastLine = i === lines.length - 1;

                let parsed: unknown;
                try {
                  parsed = JSON.parse(line);
                } catch {
                  if (isLastLine) {
                    console.warn(
                      `[OrionDB] Malformed final line in ${filePath} (line ${i + 1}) — discarding. ` +
                        `This may indicate an incomplete write. Content: ${line}`,
                    );
                    continue;
                  }
                  throw new ValidationError(`Malformed NDJSON on line ${i + 1} in ${filePath}: ${line}`, {
                    meta: { lineNumber: i + 1, content: line },
                  });
                }

                if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
                  if (isLastLine) {
                    console.warn(
                      `[OrionDB] Non-object record on final line in ${filePath} (line ${i + 1}) — discarding.`,
                    );
                    continue;
                  }
                  throw new ValidationError(`Non-object record on line ${i + 1} in ${filePath}: ${line}`, {
                    meta: { lineNumber: i + 1, content: line },
                  });
                }

                const record = parsed as Record<string, unknown>;
                const id = record[this.options.primaryKeyField];

                if (!isValidPrimaryKey(id)) {
                  continue;
                }

                if (record["_deleted"] === true) {
                  this.delete(id);
                } else {
                  this.add(record as TRecord, offset);
                }
              }
              resolve();
            } catch (err: unknown) {
              reject(err instanceof Error ? err : new Error(String(err)));
            }
          });
        })
        .catch((err: unknown) => {
          // File does not exist — resolve without error
          const isNotFound =
            err !== null &&
            typeof err === "object" &&
            "code" in err &&
            (err as NodeJS.ErrnoException).code === "ENOENT";
          if (isNotFound) {
            resolve();
          } else {
            reject(err instanceof Error ? err : new Error(String(err)));
          }
        });
    });
  }

  /**
   * Returns true if the given primary key exists in the physical index.
   */
  has(id: PrimaryKey): boolean {
    return this.physicalIndex.has(id);
  }

  /**
   * Returns the number of active (non-deleted) records tracked
   * in the physical index.
   */
  size(): number {
    return this.physicalIndex.size;
  }
}
