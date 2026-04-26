// src/persistence/reader.ts
// Implements the indexed read path: O(1) direct lookup by primary key or unique field.
// Also implements the full scan engine for findMany() and findFirst().

import { createReadStream } from "node:fs";
import * as fs from "node:fs/promises";
import { createInterface } from "node:readline";
import { CompactionError, QueryError, RecordNotFoundError, ValidationError } from "../errors/index.js";
import type { FieldValue, PrimaryKey } from "../index-manager/index.js";
import { READ_BUFFER_SIZE } from "./types.js";
import type {
  FilterFn,
  FindFirstArgs,
  FindManyArgs,
  FindUniqueArgs,
  FindUniqueOrThrowArgs,
  ModelReaderContext,
  RawRecord,
  ScanOptions,
  ScanResult,
  SelectClause,
} from "./types.js";
import { stripSystemFields } from "./writer.js";

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/**
 * Reads exactly one NDJSON record line from `filePath` at the given byte `offset`.
 * Opens the file with `fs.open` + `fs.read` for O(1) random access — never streams.
 * Always closes the file handle in a `finally` block.
 *
 * @throws RecordNotFoundError  if the file does not exist (ENOENT)
 * @throws ValidationError      if the line cannot be parsed as JSON or is not a plain object
 * @throws CompactionError      for any other I/O failure
 */
export async function readRecordAtOffset(filePath: string, offset: number, modelName: string): Promise<RawRecord> {
  let fileHandle: fs.FileHandle | undefined;
  try {
    fileHandle = await fs.open(filePath, "r");
  } catch (err: unknown) {
    if (isEnoent(err)) {
      throw new RecordNotFoundError(`Data file not found at "${filePath}".`, {
        model: modelName,
        meta: { offset, filePath },
      });
    }
    throw new CompactionError(`Failed to open data file at "${filePath}".`, {
      model: modelName,
      meta: { cause: err, offset },
    });
  }

  let bytesRead: number;
  const buffer = Buffer.allocUnsafe(READ_BUFFER_SIZE);

  try {
    const result = await fileHandle.read(buffer, 0, READ_BUFFER_SIZE, offset);
    bytesRead = result.bytesRead;
  } catch (err: unknown) {
    throw new CompactionError(`Failed to read from data file at "${filePath}".`, {
      model: modelName,
      meta: { cause: err, offset },
    });
  } finally {
    await fileHandle.close();
  }

  // Find the end of the line within only the bytes that were actually read.
  // Searching the full uninitialized buffer would hit arbitrary bytes past bytesRead.
  const newlineIndex = buffer.subarray(0, bytesRead).indexOf("\n");
  const lineLength = newlineIndex === -1 ? bytesRead : newlineIndex;
  const line = buffer.subarray(0, lineLength).toString("utf8");

  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    throw new ValidationError(`Failed to parse record at offset ${offset} in "${filePath}".`, {
      model: modelName,
      meta: { offset, raw: line },
    });
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new ValidationError(`Record at offset ${offset} in "${filePath}" is not a plain object.`, {
      model: modelName,
      meta: { offset, reason: "record at offset is not a plain object" },
    });
  }

  return parsed as RawRecord;
}

/**
 * Resolves a `where` clause to a single primary key using the in-memory index.
 * Checks the primary key field before unique fields. Returns `undefined` when
 * no matching record exists in the index.
 *
 * @throws QueryError if the where clause contains no recognized PK or unique field
 * @throws QueryError if the PK value is not a string or number
 * @throws QueryError if a unique field value is not a valid scalar (FieldValue)
 * @throws QueryError if a unique field matches multiple records (data integrity violation)
 */
function resolveWhereToId(where: Record<string, unknown>, ctx: ModelReaderContext): PrimaryKey | undefined {
  const pkField = ctx.schema.primaryKeyField;

  // 1 — PK field takes priority over unique fields
  if (pkField in where) {
    const value = where[pkField];
    if (typeof value !== "string" && typeof value !== "number") {
      throw new QueryError(`Primary key field '${pkField}' on model '${ctx.modelName}' must be a string or number.`, {
        model: ctx.modelName,
        field: pkField,
        meta: { field: pkField, value, reason: "primary key must be a string or number" },
      });
    }
    return value;
  }

  // 2 — Unique field check (Set iteration order)
  for (const fieldName of ctx.schema.uniqueFields) {
    if (!(fieldName in where)) continue;

    const value = where[fieldName];
    if (!isFieldValue(value)) {
      throw new QueryError(
        `Unique field '${fieldName}' on model '${ctx.modelName}' where clause must be a scalar value.`,
        {
          model: ctx.modelName,
          field: fieldName,
          meta: { field: fieldName, value, reason: "unique field value must be a scalar" },
        },
      );
    }

    const matches = ctx.indexManager.getByField(fieldName, value);
    if (matches === undefined || matches.size === 0) {
      return undefined;
    }

    if (matches.size > 1) {
      throw new QueryError(
        `Unique field '${fieldName}' matched ${matches.size} records in '${ctx.modelName}' — index integrity violation.`,
        {
          model: ctx.modelName,
          field: fieldName,
          meta: {
            field: fieldName,
            value,
            matchCount: matches.size,
            reason: "unique field matched multiple records — index integrity violation",
          },
        },
      );
    }

    // Exactly one match — extract and return its PK
    const [id] = matches;
    return id;
  }

  // 3 — No recognized field found in where
  throw new QueryError(
    `findUnique on model '${ctx.modelName}' where clause must reference the primary key or a unique field.`,
    {
      model: ctx.modelName,
      meta: { where, reason: "findUnique where clause must reference the primary key or a unique field" },
    },
  );
}

/**
 * Performs the two-phase O(1) index lookup and direct file read for a known primary key.
 * Returns `null` when the record is absent from the physical index or is tombstoned.
 *
 * @throws ValidationError  if the record at the offset is malformed
 * @throws CompactionError  for unexpected I/O errors
 */
async function lookupByPrimaryKey(id: PrimaryKey, ctx: ModelReaderContext): Promise<RawRecord | null> {
  const offset = ctx.indexManager.getOffset(id);
  if (offset === undefined) return null;

  const rawRecord = await readRecordAtOffset(ctx.paths.dataFile, offset, ctx.modelName);

  // Defensive tombstone check — deleted records should not be in the physical index,
  // but guard anyway against stale index entries.
  if (rawRecord["_deleted"] === true) return null;

  return rawRecord;
}

/**
 * Filters a stripped record to only the fields marked `true` in the `select` clause.
 * Returns the record unchanged when `select` is `undefined` or has no entries.
 * Returns a new object — does not mutate input.
 */
function applySelect(record: RawRecord, select: SelectClause | undefined): RawRecord {
  if (select === undefined) return record;

  const result: RawRecord = {};
  for (const [key, include] of Object.entries(select)) {
    if (include && key in record) {
      result[key] = record[key];
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// Type guards
// ---------------------------------------------------------------------------

/**
 * Returns true if `value` is a valid `FieldValue` (string | number | boolean | null).
 */
function isFieldValue(value: unknown): value is FieldValue {
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null;
}

/**
 * Returns true if `err` is a Node.js ENOENT error.
 */
function isEnoent(err: unknown): boolean {
  return (
    typeof err === "object" && err !== null && "code" in err && (err as Record<string, unknown>)["code"] === "ENOENT"
  );
}

// ---------------------------------------------------------------------------
// Main exports
// ---------------------------------------------------------------------------

/**
 * Returns the single record matching the `where` clause with system fields stripped,
 * or `null` if not found. Uses the in-memory index for O(1) lookup — no scanning.
 *
 * Returns `null` when:
 * - The where clause value is not present in the index
 * - The record has been deleted (tombstoned)
 *
 * @throws QueryError      if the where clause references no PK or unique field,
 *                         or if field values have wrong types
 * @throws ValidationError if the data on disk is malformed at the resolved offset
 * @throws CompactionError if the data file is missing or an unexpected I/O error occurs
 */
export async function findUnique(ctx: ModelReaderContext, args: FindUniqueArgs): Promise<RawRecord | null> {
  try {
    const id = resolveWhereToId(args.where, ctx);
    if (id === undefined) return null;

    const rawRecord = await lookupByPrimaryKey(id, ctx);
    if (rawRecord === null) return null;

    const stripped = stripSystemFields(rawRecord);
    return applySelect(stripped, args.select);
  } catch (err: unknown) {
    if (err instanceof QueryError) throw err;
    if (err instanceof ValidationError) throw err;
    // A missing data file during a read indicates structural corruption, not a missing record
    if (err instanceof RecordNotFoundError) {
      throw new CompactionError(
        `Data file missing during findUnique() on model '${ctx.modelName}' — this indicates structural corruption.`,
        { model: ctx.modelName, meta: { cause: err, reason: "data file missing during read" } },
      );
    }
    throw new CompactionError(`Unexpected error during findUnique() on model '${ctx.modelName}'.`, {
      model: ctx.modelName,
      meta: { cause: err },
    });
  }
}

/**
 * Same as `findUnique` but throws `RecordNotFoundError` instead of returning `null`.
 * Inherits `select` clause behavior from `findUnique`.
 *
 * @throws RecordNotFoundError if no matching record is found
 * @throws QueryError          if the where clause references no PK or unique field
 * @throws ValidationError     if data on disk is malformed at the resolved offset
 * @throws CompactionError     if the data file is missing or an unexpected I/O error occurs
 */
export async function findUniqueOrThrow(ctx: ModelReaderContext, args: FindUniqueOrThrowArgs): Promise<RawRecord> {
  const record = await findUnique(ctx, args);
  if (record === null) {
    throw new RecordNotFoundError(`No record found for the given where clause on model '${ctx.modelName}'.`, {
      model: ctx.modelName,
      meta: { where: args.where },
    });
  }
  return record;
}

// ---------------------------------------------------------------------------
// Full scan engine
// ---------------------------------------------------------------------------

/**
 * Returns a FilterFn that always returns true. Used when no compiledFilter is provided.
 */
const buildPassthroughFilter = (): FilterFn => (): boolean => true;

/**
 * Streams data.ndjson line by line and returns matched records.
 * Uses node:readline for streaming — never reads the entire file into memory.
 * Resolves with empty result if the file does not exist.
 * Skips deleted records. Applies filter, skip, and take with early exit.
 *
 * When `options.pkField` and `options.isLive` are provided, the scan tracks
 * each line's byte offset and skips lines that are not the canonical (latest)
 * version of the record. This handles both deletions and superseded update lines
 * in the append-only log without a second pass.
 */
async function scanRecords(filePath: string, options: ScanOptions): Promise<ScanResult> {
  try {
    await fs.access(filePath);
  } catch {
    return { records: [], scannedCount: 0, matchedCount: 0 };
  }

  // take: 0 means collect nothing — no scan needed
  if (options.take === 0) {
    return { records: [], scannedCount: 0, matchedCount: 0 };
  }

  return new Promise<ScanResult>((resolve, reject) => {
    let scannedCount = 0;
    let matchedCount = 0;
    const results: RawRecord[] = [];
    const skip = options.skip ?? 0;
    let skippedSoFar = 0;
    // Tracks the byte offset of the current line's start for isLive dedup.
    let currentByteOffset = 0;
    // Guards against processing buffered lines after early exit (rl.close() is async).
    let done = false;

    const fileStream = createReadStream(filePath, { encoding: "utf8" });
    const rl = createInterface({ input: fileStream, crlfDelay: Infinity });

    rl.on("close", () => {
      fileStream.destroy();
      resolve({ records: results, scannedCount, matchedCount });
    });

    rl.on("error", (err: unknown) => {
      reject(
        new CompactionError("Scan failed on readline interface.", {
          meta: { cause: err, filePath },
        }),
      );
    });

    fileStream.on("error", (err: unknown) => {
      reject(
        new CompactionError("Scan failed on file stream.", {
          meta: { cause: err, filePath },
        }),
      );
    });

    rl.on("line", (line: string) => {
      // Record the start offset of this line before advancing.
      const lineStartOffset = currentByteOffset;
      // +1 for the \n newline character (NDJSON always uses \n, not \r\n)
      currentByteOffset += Buffer.byteLength(line, "utf8") + 1;

      // Skip buffered lines that arrived after early exit was signalled.
      if (done) return;

      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        console.warn(`[OrionDB] scanRecords: skipping malformed line (parse error): ${line}`);
        return;
      }

      if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        console.warn(`[OrionDB] scanRecords: skipping non-object line: ${line}`);
        return;
      }

      const record = parsed as RawRecord;

      if (record["_deleted"] === true) return;

      // Deduplication: skip superseded versions of the same record.
      // When `isLive` is provided, only the line whose byte offset matches the
      // physical index entry for this record's ID is canonical.
      if (options.isLive !== undefined && options.pkField !== undefined) {
        const id = record[options.pkField];
        if (!options.isLive(id, lineStartOffset)) return;
      }

      scannedCount++;

      if (options.filter !== undefined && !options.filter(record)) return;

      matchedCount++;

      if (skippedSoFar < skip) {
        skippedSoFar++;
        return;
      }

      results.push(record);

      if (options.take !== undefined && results.length >= options.take) {
        done = true;
        rl.close();
      }
    });
  });
}

/**
 * Returns all records matching the optional compiledFilter, with system fields
 * stripped and select applied. Uses full scan — O(n) over data.ndjson.
 * Pass compiledFilter from the query engine for filtered queries.
 */
export async function findMany(
  ctx: ModelReaderContext,
  args: FindManyArgs,
  compiledFilter?: FilterFn,
): Promise<RawRecord[]> {
  try {
    const filter = compiledFilter ?? buildPassthroughFilter();
    // Supply pkField and isLive so the scan engine can skip superseded update lines
    // and lines for deleted records without re-reading the physical index on each line.
    const pkField = ctx.schema.primaryKeyField;
    const isLive = (id: unknown, offset: number): boolean =>
      ctx.indexManager.getOffset(id as string | number) === offset;
    const scanResult = await scanRecords(ctx.paths.dataFile, {
      filter,
      take: args.take,
      skip: args.skip,
      orderBy: args.orderBy,
      pkField,
      isLive,
    });
    return scanResult.records.map((r) => applySelect(stripSystemFields(r), args.select));
  } catch (err: unknown) {
    if (err instanceof CompactionError) throw err;
    throw new CompactionError(`Unexpected error during findMany() on model '${ctx.modelName}'.`, {
      model: ctx.modelName,
      meta: { cause: err },
    });
  }
}

/**
 * Returns the first matching record or `null`. Delegates to findMany with take: 1
 * for O(k) early exit — does not re-implement scan logic.
 */
export async function findFirst(
  ctx: ModelReaderContext,
  args: FindFirstArgs,
  compiledFilter?: FilterFn,
): Promise<RawRecord | null> {
  const records = await findMany(ctx, { ...args, take: 1 }, compiledFilter);
  return records[0] ?? null;
}
