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
