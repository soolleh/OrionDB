// src/persistence/initializer.ts — file layout bootstrapping for database and model directories

import * as fs from "node:fs/promises";
import * as path from "node:path";
import { CompactionError } from "../errors/index.js";
import {
  DATA_FILENAME,
  META_FILENAME,
  DB_META_FILENAME,
  SCHEMA_FILENAME,
  ORIONDB_VERSION,
  DATABASE_META_VERSION,
} from "./types.js";
import type { DatabasePaths, ModelPaths, ModelMeta, DatabaseMeta } from "./types.js";

// ---------------------------------------------------------------------------
// resolveDatabasePaths
// ---------------------------------------------------------------------------

/**
 * Synchronous, pure. Resolves all root-level database paths from the given
 * location string. Returns a DatabasePaths object. No I/O.
 */
export function resolveDatabasePaths(location: string): DatabasePaths {
  const dbLocation = path.resolve(location);
  return {
    dbLocation,
    schemaFile: path.join(dbLocation, SCHEMA_FILENAME),
    metaFile: path.join(dbLocation, DB_META_FILENAME),
  };
}

// ---------------------------------------------------------------------------
// resolveModelPaths
// ---------------------------------------------------------------------------

/**
 * Synchronous, pure. Resolves all model-level filesystem paths.
 * Model name is case-preserved as the directory name. No I/O.
 */
export function resolveModelPaths(dbLocation: string, modelName: string): ModelPaths {
  const modelDir = path.join(dbLocation, modelName);
  return {
    modelDir,
    dataFile: path.join(modelDir, DATA_FILENAME),
    metaFile: path.join(modelDir, META_FILENAME),
  };
}

// ---------------------------------------------------------------------------
// initializeDatabaseDirectory
// ---------------------------------------------------------------------------

/**
 * Ensures the database root directory exists and _meta.json is present.
 * Never overwrites an existing _meta.json.
 * Never touches _schema.json.
 * Throws CompactionError on any I/O failure.
 */
export async function initializeDatabaseDirectory(paths: DatabasePaths): Promise<void> {
  try {
    await fs.mkdir(paths.dbLocation, { recursive: true });
  } catch (err: unknown) {
    throw new CompactionError(`Failed to create database directory at "${paths.dbLocation}".`, {
      meta: { cause: err },
    });
  }

  // Check if _meta.json already exists — never overwrite
  const metaExists = await fileExists(paths.metaFile);
  if (metaExists) return;

  const meta: DatabaseMeta = {
    version: DATABASE_META_VERSION,
    oriondbVersion: ORIONDB_VERSION,
    createdAt: new Date().toISOString(),
    location: paths.dbLocation,
  };

  try {
    await fs.writeFile(paths.metaFile, JSON.stringify(meta, null, 2), "utf8");
  } catch (err: unknown) {
    throw new CompactionError(`Failed to write database meta file at "${paths.metaFile}".`, { meta: { cause: err } });
  }
}

// ---------------------------------------------------------------------------
// initializeModelDirectory
// ---------------------------------------------------------------------------

/**
 * Ensures a single model's directory and files exist.
 * Returns the current ModelMeta — freshly created or read from existing meta.json.
 * Never overwrites an existing meta.json.
 * Throws CompactionError on any I/O failure.
 */
export async function initializeModelDirectory(paths: ModelPaths, modelName: string): Promise<ModelMeta> {
  try {
    await fs.mkdir(paths.modelDir, { recursive: true });
  } catch (err: unknown) {
    throw new CompactionError(`Failed to create model directory at "${paths.modelDir}".`, {
      meta: { cause: err },
    });
  }

  // Create data.ndjson if absent
  const dataExists = await fileExists(paths.dataFile);
  if (!dataExists) {
    try {
      await fs.writeFile(paths.dataFile, "", "utf8");
    } catch (err: unknown) {
      throw new CompactionError(`Failed to create data file at "${paths.dataFile}".`, {
        meta: { cause: err },
      });
    }
  }

  // If meta.json exists, read and return it
  const metaExists = await fileExists(paths.metaFile);
  if (metaExists) {
    return readModelMeta(paths);
  }

  // Create a fresh meta.json
  const meta: ModelMeta = {
    modelName,
    recordCount: 0,
    tombstoneCount: 0,
    totalLines: 0,
    lastCompactedAt: null,
    createdAt: new Date().toISOString(),
  };

  try {
    await fs.writeFile(paths.metaFile, JSON.stringify(meta, null, 2), "utf8");
  } catch (err: unknown) {
    throw new CompactionError(`Failed to write model meta file at "${paths.metaFile}".`, {
      meta: { cause: err },
    });
  }

  return meta;
}

// ---------------------------------------------------------------------------
// initializeAllModelDirectories
// ---------------------------------------------------------------------------

/**
 * Runs initializeModelDirectory for every model name sequentially.
 * Returns a Map<string, ModelMeta> keyed by model name.
 * Propagates any failure immediately — does not continue on error.
 */
export async function initializeAllModelDirectories(
  dbLocation: string,
  modelNames: string[],
): Promise<Map<string, ModelMeta>> {
  const result = new Map<string, ModelMeta>();

  for (const modelName of modelNames) {
    const paths = resolveModelPaths(dbLocation, modelName);
    const meta = await initializeModelDirectory(paths, modelName);
    result.set(modelName, meta);
  }

  return result;
}

// ---------------------------------------------------------------------------
// updateModelMeta
// ---------------------------------------------------------------------------

/**
 * Reads the existing meta.json for a model, merges updates, recomputes
 * totalLines, writes it back, and returns the final ModelMeta.
 * Throws CompactionError on any I/O failure.
 */
export async function updateModelMeta(
  paths: ModelPaths,
  updates: Partial<Omit<ModelMeta, "modelName" | "createdAt">>,
): Promise<ModelMeta> {
  const current = await readModelMeta(paths);

  const merged: ModelMeta = {
    ...current,
    ...updates,
    // Always recompute totalLines from the final recordCount + tombstoneCount
    totalLines: (updates.recordCount ?? current.recordCount) + (updates.tombstoneCount ?? current.tombstoneCount),
  };

  try {
    await fs.writeFile(paths.metaFile, JSON.stringify(merged, null, 2), "utf8");
  } catch (err: unknown) {
    throw new CompactionError(`Failed to update model meta file at "${paths.metaFile}".`, {
      meta: { cause: err },
    });
  }

  return merged;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/**
 * Returns true if the file at the given path is accessible, false otherwise.
 * Never throws — access errors are treated as absence.
 */
async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

/**
 * Reads and parses a ModelMeta JSON file from `paths.metaFile`.
 * Throws CompactionError if the file cannot be read or is not a plain object.
 */
export async function readModelMeta(paths: ModelPaths): Promise<ModelMeta> {
  const metaFilePath = paths.metaFile;
  let raw: string;
  try {
    raw = await fs.readFile(metaFilePath, "utf8");
  } catch (err: unknown) {
    throw new CompactionError(`Failed to read model meta file at "${metaFilePath}".`, {
      meta: { cause: err },
    });
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err: unknown) {
    throw new CompactionError(`Failed to parse model meta file at "${metaFilePath}".`, {
      meta: { cause: err },
    });
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new CompactionError(`Model meta file at "${metaFilePath}" is not a valid object.`, { meta: { parsed } });
  }

  return parsed as ModelMeta;
}

// ---------------------------------------------------------------------------
// shouldAutoCompact
// ---------------------------------------------------------------------------

/**
 * Returns `true` when the dead-line ratio for `modelName` equals or exceeds
 * `threshold`. Returns `false` when the file is empty, below threshold, or
 * when the meta file cannot be read (fail-safe — never triggers on error).
 *
 * @param paths - Resolved model paths (only `metaFile` is read).
 * @param threshold - Fraction (0–1) at which compaction should trigger.
 *   Defaults to 0.30 when `undefined`.
 */
export async function shouldAutoCompact(paths: ModelPaths, threshold: number | undefined): Promise<boolean> {
  const effectiveThreshold = threshold ?? 0.3;
  if (effectiveThreshold <= 0) return false;
  try {
    const meta = await readModelMeta(paths);
    if (meta.totalLines === 0) return false;
    const deadLines = meta.totalLines - meta.recordCount;
    const ratio = deadLines / meta.totalLines;
    return ratio >= effectiveThreshold;
  } catch {
    return false;
  }
}
