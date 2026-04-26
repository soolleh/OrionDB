// src/persistence/types.ts — all persistence-related type definitions

/**
 * Byte Offset Model
 *
 * All byte offsets in OrionDB refer to the zero-based byte position
 * of the START of a record's line within data.ndjson.
 *
 * Offset 0 = first byte of the file = start of the first record line.
 *
 * After writing a line of N bytes (including the trailing \n),
 * the next record's offset will be currentOffset + N.
 *
 * The FileSizeCounter tracks this value in memory so fs.stat
 * is never called on the hot write path.
 */

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const DATA_FILENAME = "data.ndjson" as const;
export const META_FILENAME = "meta.json" as const;
export const DB_META_FILENAME = "_meta.json" as const;
export const SCHEMA_FILENAME = "_schema.json" as const;
export const ORIONDB_VERSION = "0.2.0" as const;
export const DATABASE_META_VERSION = 1 as const;
export const NEWLINE = "\n" as const;

/**
 * Maximum byte length of a single NDJSON line read in one `fs.read()` call.
 * 64 KiB is sufficient for any reasonable record in Phase 1.
 */
export const READ_BUFFER_SIZE = 65_536 as const;

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

/**
 * Represents the contents of `<ModelName>/meta.json`.
 * Tracks record and tombstone counts for a single model.
 */
export interface ModelMeta {
  modelName: string;
  recordCount: number;
  tombstoneCount: number;
  totalLines: number;
  lastCompactedAt: string | null;
  createdAt: string;
}

/**
 * Represents the contents of `_meta.json` at the database root.
 */
export interface DatabaseMeta {
  version: number;
  oriondbVersion: string;
  createdAt: string;
  location: string;
}

/**
 * Resolved filesystem paths for a single model.
 */
export interface ModelPaths {
  modelDir: string;
  dataFile: string;
  metaFile: string;
}

/**
 * Resolved filesystem paths for the database root.
 */
export interface DatabasePaths {
  dbLocation: string;
  schemaFile: string;
  metaFile: string;
}

// ---------------------------------------------------------------------------
// Writer types
// ---------------------------------------------------------------------------

import type { ParsedModelDefinition } from "../schema/index.js";
import type { IndexManager } from "../index-manager/index.js";
import type { FileSizeCounter } from "./file-size-counter.js";

/**
 * Arguments for the create() write operation.
 */
export interface CreateArgs<TData extends Record<string, unknown>> {
  data: TData;
}

/**
 * Result returned from a write operation — the written record with system
 * fields stripped.
 */
export interface WriteResult<TRecord extends Record<string, unknown>> {
  record: TRecord;
}

/**
 * Groups all dependencies required by a writer operation for a single model.
 */
export interface ModelWriterContext {
  modelName: string;
  paths: ModelPaths;
  schema: ParsedModelDefinition;
  indexManager: IndexManager<Record<string, unknown>>;
  counter: FileSizeCounter;
  /** Fraction (0–1) at which auto-compaction is triggered. Defaults to 0.30. */
  autoCompactThreshold?: number;
}

/**
 * Arguments for the createMany() write operation.
 */
export interface CreateManyArgs<TData extends Record<string, unknown>> {
  data: TData[];
}

/**
 * Result returned from a createMany() operation.
 */
export interface CreateManyResult {
  count: number;
}

/**
 * Arguments for the update() write operation.
 */
export interface UpdateArgs {
  where: Record<string, unknown>;
  data: Record<string, unknown>;
}

/**
 * Arguments for the updateMany() write operation.
 */
export interface UpdateManyArgs {
  where?: WhereClause;
  data: Record<string, unknown>;
}

/**
 * Result returned from an updateMany() operation.
 */
export interface UpdateManyResult {
  count: number;
}

// ---------------------------------------------------------------------------
// Reader types
// ---------------------------------------------------------------------------

/**
 * Untyped record shape as read from disk, before system field stripping.
 */
export type RawRecord = Record<string, unknown>;

/**
 * A map of field names to inclusion flags for narrowing the returned record shape.
 * Only fields set to `true` are included in the result.
 */
export type SelectClause = Record<string, boolean>;

/**
 * Arguments for findUnique().
 */
export interface FindUniqueArgs {
  where: Record<string, unknown>;
  select?: SelectClause;
}

/**
 * Arguments for findUniqueOrThrow().
 */
export interface FindUniqueOrThrowArgs {
  where: Record<string, unknown>;
  select?: SelectClause;
}

/**
 * Groups all dependencies required by a reader operation for a single model.
 * Does not include FileSizeCounter — readers never write and never call fs.stat.
 */
export interface ModelReaderContext {
  modelName: string;
  paths: ModelPaths;
  schema: ParsedModelDefinition;
  indexManager: IndexManager<Record<string, unknown>>;
}

// ---------------------------------------------------------------------------
// Full scan types
// ---------------------------------------------------------------------------

/**
 * A raw where clause as passed by the caller.
 * Operator evaluation is delegated to the query engine via FilterFn.
 */
export type WhereClause = Record<string, unknown>;

/**
 * A compiled predicate for the scan engine.
 * Returns `true` if the record should be included in results.
 */
export type FilterFn = (record: RawRecord) => boolean;

/**
 * Sort direction for a single field.
 */
export type OrderByDirection = "asc" | "desc";

/**
 * A map of field names to sort direction.
 * Applied after the scan completes — not inside the scan engine.
 */
export type OrderByClause = {
  [field: string]: OrderByDirection;
};

/**
 * Options controlling a full scan pass over data.ndjson.
 */
export interface ScanOptions {
  /** Pre-compiled predicate — `undefined` means match all records. */
  filter?: FilterFn;
  /** Maximum number of records to collect. `undefined` means no limit. */
  take?: number;
  /** Number of matched records to skip before collecting. `undefined` means 0. */
  skip?: number;
  /** Sort clause — stored in result but not applied by the scan engine. */
  orderBy?: OrderByClause;
  /**
   * Primary key field name — must be provided when `isLive` is provided.
   * Used to extract the record ID for the offset deduplication check.
   */
  pkField?: string;
  /**
   * Optional live-record check for append-only log deduplication.
   * When provided, the scan tracks each line's byte offset and calls this
   * function to determine whether the line is the current canonical version
   * of the record (i.e. its offset matches the physical index entry).
   * Returns `true` to include the record, `false` to skip it (old version or deleted).
   */
  isLive?: (id: unknown, lineStartOffset: number) => boolean;
}

/**
 * Result returned from `scanRecords`.
 */
export interface ScanResult {
  records: RawRecord[];
  /** Total non-deleted lines scanned (for diagnostics). */
  scannedCount: number;
  /** Records matched by filter before skip/take. */
  matchedCount: number;
}

/**
 * Arguments for findMany().
 */
export interface FindManyArgs {
  where?: WhereClause;
  select?: SelectClause;
  take?: number;
  skip?: number;
  orderBy?: OrderByClause;
}

/**
 * Arguments for findFirst().
 */
export interface FindFirstArgs {
  where?: WhereClause;
  select?: SelectClause;
  orderBy?: OrderByClause;
}

// ---------------------------------------------------------------------------
// Delete types
// ---------------------------------------------------------------------------

/**
 * Arguments for deleteRecord().
 */
export interface DeleteArgs {
  where: Record<string, unknown>;
}

/**
 * Arguments for deleteMany().
 */
export interface DeleteManyArgs {
  where?: WhereClause;
}

/**
 * Result returned from a deleteMany() operation.
 */
export interface DeleteManyResult {
  count: number;
}
