// src/persistence/writer.ts
// Implements the write path: create, createMany, update, updateMany,
// deleteRecord, deleteMany

import * as fs from "node:fs/promises";
import {
  CompactionError,
  OrionDBError,
  RecordNotFoundError,
  UniqueConstraintError,
  ValidationError,
} from "../errors/index.js";
import type { IndexManager, PrimaryKey } from "../index-manager/index.js";
import { SYSTEM_FIELDS } from "../schema/index.js";
import type { ParsedModelDefinition } from "../schema/index.js";
import { updateModelMeta } from "./initializer.js";
import { findMany, findUnique, readRecordAtOffset } from "./reader.js";
import { NEWLINE } from "./types.js";
import type {
  ModelMeta,
  ModelPaths,
  ModelReaderContext,
  ModelWriterContext,
  CreateArgs,
  CreateManyArgs,
  CreateManyResult,
  UpdateArgs,
  UpdateManyArgs,
  UpdateManyResult,
  DeleteArgs,
  DeleteManyArgs,
  DeleteManyResult,
  FilterFn,
} from "./types.js";

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/**
 * Applies schema-defined defaults to fields absent from `data`.
 * Returns a new object — does not mutate the input.
 * Handles both static values and factory function defaults.
 */
function applyDefaults(data: Record<string, unknown>, schema: ParsedModelDefinition): Record<string, unknown> {
  const result: Record<string, unknown> = { ...data };

  for (const [fieldName, field] of schema.fields) {
    if (field.type === "relation") continue;
    if (!(fieldName in result) && field.hasDefault) {
      result[fieldName] =
        typeof field.defaultValue === "function" ? (field.defaultValue as () => unknown)() : field.defaultValue;
    }
  }

  return result;
}

/**
 * Validates that all required scalar fields are present and non-null/undefined
 * in the data after defaults have been applied.
 * Throws `ValidationError` for any violation.
 */
function validateRequiredFields(data: Record<string, unknown>, schema: ParsedModelDefinition, modelName: string): void {
  for (const [fieldName, field] of schema.fields) {
    if (field.type === "relation") continue;
    if (!field.required) continue;

    const value = data[fieldName];
    if (!(fieldName in data) || value === null || value === undefined) {
      throw new ValidationError(`Required field '${fieldName}' is missing on model '${modelName}'.`, {
        model: modelName,
        field: fieldName,
        meta: { reason: "required field missing" },
      });
    }
  }
}

/**
 * Validates that each field value in `data` matches its declared schema type.
 * Only validates fields that are present in `data`.
 * Skips relation fields. Throws `ValidationError` on type mismatch.
 */
function validateFieldTypes(data: Record<string, unknown>, schema: ParsedModelDefinition, modelName: string): void {
  for (const [fieldName, field] of schema.fields) {
    if (field.type === "relation") continue;
    if (!(fieldName in data)) continue;

    const value = data[fieldName];

    let valid: boolean;
    switch (field.type) {
      case "string":
        valid = typeof value === "string";
        break;
      case "number":
        valid = typeof value === "number" && isFinite(value);
        break;
      case "boolean":
        valid = typeof value === "boolean";
        break;
      case "date":
        valid = value instanceof Date || (typeof value === "string" && !isNaN(new Date(value).getTime()));
        break;
      case "json":
        valid = typeof value === "object" && value !== null && !Array.isArray(value);
        break;
      case "enum":
        valid = typeof value === "string" && (field.enumValues?.includes(value) ?? false);
        break;
    }

    if (!valid) {
      throw new ValidationError(
        `Field '${fieldName}' on model '${modelName}' has an invalid value for type '${field.type}'.`,
        {
          model: modelName,
          field: fieldName,
          meta: { expected: field.type, received: typeof value },
        },
      );
    }
  }
}

/**
 * Checks that no unique constraint is violated by the given data.
 * Also checks primary key uniqueness.
 * Pass `excludeId` on the update path to allow a record to be updated
 * without triggering a conflict against itself.
 * Throws `UniqueConstraintError` on any violation.
 */
function checkUniqueConstraints(
  data: Record<string, unknown>,
  schema: ParsedModelDefinition,
  indexManager: IndexManager<Record<string, unknown>>,
  modelName: string,
  excludeId?: PrimaryKey,
): void {
  // Check primary key uniqueness
  const pkValue = data[schema.primaryKeyField];
  if (isPrimaryKey(pkValue)) {
    if (indexManager.has(pkValue) && pkValue !== excludeId) {
      throw new UniqueConstraintError(
        `A record with ${schema.primaryKeyField} '${String(pkValue)}' already exists in '${modelName}'.`,
        { model: modelName, field: schema.primaryKeyField, meta: { value: pkValue } },
      );
    }
  }

  // Check all unique field constraints
  for (const fieldName of schema.uniqueFields) {
    const value = data[fieldName];
    if (value === undefined) continue;

    const matches = indexManager.getByField(fieldName, value as string | number | boolean | null);
    if (matches !== undefined && matches.size > 0) {
      const isOnlySelf = excludeId !== undefined && matches.size === 1 && matches.has(excludeId);
      if (!isOnlySelf) {
        throw new UniqueConstraintError(
          `A record with ${fieldName} '${JSON.stringify(value)}' already exists in '${modelName}'.`,
          { model: modelName, field: fieldName, meta: { value } },
        );
      }
    }
  }
}

/**
 * Serializes a record as a single NDJSON line including the trailing newline.
 * Pure function — no I/O, no side effects.
 */
function serializeRecord(record: Record<string, unknown>): string {
  return JSON.stringify(record) + NEWLINE;
}

/**
 * Attaches the three system fields to a record before writing.
 * System fields are appended last in the returned object.
 * Returns a new object — does not mutate the input.
 *
 * @param data - User field values (with defaults already applied).
 * @param existing - If provided, preserves the original `_createdAt` (for updates).
 */
function attachSystemFields(data: Record<string, unknown>, existing?: { _createdAt: string }): Record<string, unknown> {
  const now = new Date().toISOString();
  return {
    ...data,
    _deleted: false,
    _createdAt: existing !== undefined ? existing._createdAt : now,
    _updatedAt: now,
  };
}

/**
 * Returns a shallow copy of `record` with all three system fields removed.
 * Does not mutate the input.
 * Exported for use by reader.ts — single implementation shared across the module.
 */
export function stripSystemFields(record: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = { ...record };
  for (const field of SYSTEM_FIELDS) {
    delete result[field];
  }
  return result;
}

/**
 * Reads and parses `meta.json` for a model.
 * Throws `CompactionError` on any read or parse failure.
 */
async function readModelMeta(paths: ModelPaths): Promise<ModelMeta> {
  let raw: string;
  try {
    raw = await fs.readFile(paths.metaFile, "utf8");
  } catch (err: unknown) {
    throw new CompactionError(`Failed to read model meta file at "${paths.metaFile}".`, {
      meta: { cause: err },
    });
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err: unknown) {
    throw new CompactionError(`Failed to parse model meta file at "${paths.metaFile}".`, {
      meta: { cause: err },
    });
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new CompactionError(`Model meta file at "${paths.metaFile}" is not a valid object.`, { meta: { parsed } });
  }

  return parsed as ModelMeta;
}

// ---------------------------------------------------------------------------
// create()
// ---------------------------------------------------------------------------

/**
 * Writes a single new record to the model's data.ndjson file.
 *
 * Follows the full write path from oriondb-dev-guide.md §14.2:
 * apply defaults → validate required → validate types → check uniqueness →
 * attach system fields → serialize → get offset → append → update indexes
 * → increment counter → update meta → return stripped record.
 *
 * Throws typed OrionDBError subclasses for all validation failures.
 * Wraps unexpected errors in CompactionError.
 */
export async function create(
  ctx: ModelWriterContext,
  args: CreateArgs<Record<string, unknown>>,
): Promise<Record<string, unknown>> {
  try {
    // Step 1 — Apply defaults
    const dataWithDefaults = applyDefaults(args.data, ctx.schema);

    // Step 2 — Validate required fields
    validateRequiredFields(dataWithDefaults, ctx.schema, ctx.modelName);

    // Step 3 — Validate field types
    validateFieldTypes(dataWithDefaults, ctx.schema, ctx.modelName);

    // Step 4 — Check unique constraints
    checkUniqueConstraints(dataWithDefaults, ctx.schema, ctx.indexManager, ctx.modelName);

    // Step 5 — Attach system fields (always last in object)
    const recordToWrite = attachSystemFields(dataWithDefaults);

    // Step 6 — Serialize
    const serializedLine = serializeRecord(recordToWrite);

    // Step 7 — Get current offset from in-memory counter (never fs.stat)
    const currentOffset = ctx.counter.getSize();

    // Step 8 — Append to file
    await fs.appendFile(ctx.paths.dataFile, serializedLine);

    // Step 9 — Update indexes via indexManager.add() (handles logical + reverse + physical)
    const pkValue = recordToWrite[ctx.schema.primaryKeyField];
    if (!isPrimaryKey(pkValue)) {
      throw new ValidationError(
        `Primary key field '${ctx.schema.primaryKeyField}' has an invalid value on model '${ctx.modelName}'.`,
        { model: ctx.modelName, field: ctx.schema.primaryKeyField, meta: { value: pkValue } },
      );
    }
    ctx.indexManager.add(recordToWrite, currentOffset);

    // Step 10 — Increment counter by exact byte count of the written line
    ctx.counter.increment(Buffer.byteLength(serializedLine, "utf8"));

    // Step 11 — Update meta.json record count
    const currentMeta = await readModelMeta(ctx.paths);
    await updateModelMeta(ctx.paths, {
      recordCount: currentMeta.recordCount + 1,
    });

    // Step 12 — Return record with system fields stripped
    return stripSystemFields(recordToWrite);
  } catch (err: unknown) {
    if (err instanceof OrionDBError) throw err;
    throw new CompactionError(`Unexpected error during create() on model '${ctx.modelName}'.`, {
      meta: { cause: err },
    });
  }
}

// ---------------------------------------------------------------------------
// checkBatchUniqueness
// ---------------------------------------------------------------------------

/**
 * Checks that no two records in the same batch share a value on a unique field
 * (including the primary key field).
 *
 * Runs after per-record validation has passed, before any writes.
 * Only detects within-batch duplicates — conflicts against existing index
 * entries are caught by checkUniqueConstraints per record.
 *
 * Throws `UniqueConstraintError` with `meta: { value, batchIndex: [i, j] }`
 * for the first duplicate pair found.
 */
function checkBatchUniqueness(
  records: Record<string, unknown>[],
  schema: ParsedModelDefinition,
  modelName: string,
): void {
  // Build the set of fields to check: PK + all uniqueFields
  const fieldsToCheck = new Set<string>(schema.uniqueFields);
  fieldsToCheck.add(schema.primaryKeyField);

  for (const fieldName of fieldsToCheck) {
    // Map from serialized value → first index where it appeared
    const seen = new Map<string, number>();

    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      if (record === undefined) continue;
      const value = record[fieldName];
      if (value === undefined) continue;

      // Use JSON.stringify as a stable, collision-resistant key
      const key = JSON.stringify(value);
      const firstIndex = seen.get(key);

      if (firstIndex !== undefined) {
        throw new UniqueConstraintError(
          `Duplicate value for '${fieldName}' at batch indexes ${firstIndex} and ${i} in '${modelName}'.`,
          {
            model: modelName,
            field: fieldName,
            meta: { value, batchIndex: [firstIndex, i] },
          },
        );
      }

      seen.set(key, i);
    }
  }
}

// ---------------------------------------------------------------------------
// createMany()
// ---------------------------------------------------------------------------

/**
 * Writes multiple new records to the model's data.ndjson file.
 *
 * Validates ALL records before writing ANY. If any record fails validation,
 * the entire operation is aborted with no writes (Phase 1 batch atomicity).
 *
 * After all validation passes, records are written sequentially in input order.
 * Index updates happen immediately after each appendFile call. Meta is updated
 * exactly once after all writes complete.
 *
 * Returns `{ count: N }` where N is the number of records written.
 * Throws typed OrionDBError subclasses for validation failures.
 * Wraps unexpected errors in CompactionError.
 */
export async function createMany(
  ctx: ModelWriterContext,
  args: CreateManyArgs<Record<string, unknown>>,
): Promise<CreateManyResult> {
  try {
    // Step 1 — Guard empty input
    if (args.data.length === 0) {
      return { count: 0 };
    }

    // Step 2 — Validation pass (all records, no writes)
    const validatedRecords: Record<string, unknown>[] = [];

    for (let i = 0; i < args.data.length; i++) {
      const record = args.data[i];
      if (record === undefined) continue;

      // 2a — Apply defaults
      const dataWithDefaults = applyDefaults(record, ctx.schema);
      // 2b — Validate required fields
      try {
        validateRequiredFields(dataWithDefaults, ctx.schema, ctx.modelName);
      } catch (e: unknown) {
        if (e instanceof ValidationError) {
          throw new ValidationError(e.message, {
            model: ctx.modelName,
            field: e.field,
            meta: { batchIndex: i, originalError: e.message },
          });
        }
        throw e;
      }

      // 2c — Validate field types
      try {
        validateFieldTypes(dataWithDefaults, ctx.schema, ctx.modelName);
      } catch (e: unknown) {
        if (e instanceof ValidationError) {
          throw new ValidationError(e.message, {
            model: ctx.modelName,
            field: e.field,
            meta: { batchIndex: i, originalError: e.message },
          });
        }
        throw e;
      }

      // 2d — Check unique constraints against existing index
      try {
        checkUniqueConstraints(dataWithDefaults, ctx.schema, ctx.indexManager, ctx.modelName);
      } catch (e: unknown) {
        if (e instanceof UniqueConstraintError) {
          throw new UniqueConstraintError(e.message, {
            model: ctx.modelName,
            field: e.field,
            meta: { batchIndex: i, originalError: e.message },
          });
        }
        throw e;
      }

      // 2e — Collect validated record
      validatedRecords.push(dataWithDefaults);
    }

    // 2f — Check within-batch uniqueness
    checkBatchUniqueness(validatedRecords, ctx.schema, ctx.modelName);

    // Step 3 — Write pass (all records, sequentially, in input order)
    for (let i = 0; i < validatedRecords.length; i++) {
      const dataWithDefaults = validatedRecords[i];
      if (dataWithDefaults === undefined) continue;

      // 3a — Attach system fields
      const recordToWrite = attachSystemFields(dataWithDefaults);

      // 3b — Serialize
      const serializedLine = serializeRecord(recordToWrite);

      // 3c — Get current offset (in-memory, never fs.stat)
      const currentOffset = ctx.counter.getSize();

      // 3d — Append to file
      try {
        await fs.appendFile(ctx.paths.dataFile, serializedLine);
      } catch (err: unknown) {
        throw new CompactionError(
          `Write failed mid-batch during createMany() on model '${ctx.modelName}' (record ${i}).`,
          {
            model: ctx.modelName,
            meta: { cause: err, writtenCount: i, totalCount: validatedRecords.length },
          },
        );
      }

      // 3e — Validate PK
      const pkValue = recordToWrite[ctx.schema.primaryKeyField];
      if (!isPrimaryKey(pkValue)) {
        throw new ValidationError(
          `Primary key field '${ctx.schema.primaryKeyField}' has an invalid value on model '${ctx.modelName}'.`,
          { model: ctx.modelName, field: ctx.schema.primaryKeyField, meta: { value: pkValue } },
        );
      }

      // 3f — Update index immediately after write
      ctx.indexManager.add(recordToWrite, currentOffset);

      // 3g — Increment counter immediately after write
      ctx.counter.increment(Buffer.byteLength(serializedLine, "utf8"));
    }

    // Step 4 — Update meta.json exactly once
    const existingMeta = await readModelMeta(ctx.paths);
    await updateModelMeta(ctx.paths, {
      recordCount: existingMeta.recordCount + validatedRecords.length,
    });

    // Step 5 — Return count
    return { count: validatedRecords.length };
  } catch (err: unknown) {
    if (err instanceof OrionDBError) throw err;
    throw new CompactionError(`Unexpected error during createMany() on model '${ctx.modelName}'.`, {
      meta: { cause: err },
    });
  }
}

// ---------------------------------------------------------------------------
// update()
// ---------------------------------------------------------------------------

/**
 * Updates a single record matching `args.where` with the data in `args.data`.
 * Appends a new record version to data.ndjson. Preserves `_createdAt`.
 * The superseded original line is counted as a tombstone for compaction purposes.
 *
 * Relation fields and system fields in `args.data` are silently ignored.
 * The primary key field may not be changed — throws `ValidationError` on mutation attempt.
 *
 * @throws RecordNotFoundError   if no matching record exists
 * @throws ValidationError       if data contains an invalid type or PK mutation attempt
 * @throws UniqueConstraintError if update introduces a unique constraint violation
 * @throws CompactionError       on unexpected I/O error
 */
export async function update(ctx: ModelWriterContext, args: UpdateArgs): Promise<Record<string, unknown>> {
  try {
    const readerCtx: ModelReaderContext = {
      modelName: ctx.modelName,
      paths: ctx.paths,
      schema: ctx.schema,
      indexManager: ctx.indexManager,
    };

    // Step 1 — Find existing record (stripped, no system fields)
    const existing = await findUnique(readerCtx, { where: args.where });
    if (existing === null) {
      throw new RecordNotFoundError(`No record found for the given where clause on model '${ctx.modelName}'.`, {
        model: ctx.modelName,
        meta: { where: args.where },
      });
    }

    // Step 2 — PK mutation guard
    const pkField = ctx.schema.primaryKeyField;
    const existingPk = existing[pkField];
    const attemptedPk = args.data[pkField];
    if (attemptedPk !== undefined && attemptedPk !== existingPk) {
      throw new ValidationError(`Cannot change primary key field '${pkField}' on model '${ctx.modelName}'.`, {
        model: ctx.modelName,
        field: pkField,
        meta: { existingPk, attemptedPk },
      });
    }

    if (!isPrimaryKeyValue(existingPk)) {
      throw new ValidationError(`Primary key field '${pkField}' has an invalid type on model '${ctx.modelName}'.`, {
        model: ctx.modelName,
        field: pkField,
        meta: { value: existingPk },
      });
    }
    const pkValue = existingPk;

    // Step 3 — Read raw record from disk at its known offset
    const offset = ctx.indexManager.getOffset(pkValue);
    if (offset === undefined) {
      throw new RecordNotFoundError(
        `Record with ${pkField} '${String(pkValue)}' is not in the physical index on model '${ctx.modelName}'.`,
        { model: ctx.modelName, meta: { pkValue } },
      );
    }
    const rawRecord = await readRecordAtOffset(ctx.paths.dataFile, offset, ctx.modelName);

    // Step 4 — Build merged data (skip system fields and relation fields from args.data)
    const userDataFiltered: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(args.data)) {
      if (SYSTEM_FIELDS.includes(key)) continue;
      const fieldDef = ctx.schema.fields.get(key);
      if (fieldDef?.type === "relation") continue;
      userDataFiltered[key] = value;
    }
    const mergedData = { ...stripSystemFields(rawRecord), ...userDataFiltered };

    // Step 5 — Validate merged record
    validateFieldTypes(mergedData, ctx.schema, ctx.modelName);
    checkUniqueConstraints(mergedData, ctx.schema, ctx.indexManager, ctx.modelName, pkValue);

    // Step 6 — Re-attach system fields, preserving _createdAt from original
    const rawCreatedAt = rawRecord["_createdAt"];
    const recordToWrite = attachSystemFields(mergedData, {
      _createdAt: typeof rawCreatedAt === "string" ? rawCreatedAt : new Date().toISOString(),
    });

    // Step 7 — Serialize
    const serializedLine = serializeRecord(recordToWrite);

    // Step 8 — Get current offset from in-memory counter (never fs.stat)
    const currentOffset = ctx.counter.getSize();

    // Step 9 — Append new record version
    await fs.appendFile(ctx.paths.dataFile, serializedLine);

    // Step 10 — Update all three index structures atomically
    ctx.indexManager.update(rawRecord, recordToWrite, currentOffset);

    // Step 11 — Increment counter
    ctx.counter.increment(Buffer.byteLength(serializedLine, "utf8"));

    // Step 12 — Update meta: old line becomes superseded (tombstone)
    const existingMeta = await readModelMeta(ctx.paths);
    await updateModelMeta(ctx.paths, {
      tombstoneCount: existingMeta.tombstoneCount + 1,
    });

    // Auto-compact threshold check (compaction triggered in prompt 5.8)
    const updatedMeta = await readModelMeta(ctx.paths);
    if (shouldAutoCompact(updatedMeta, ctx.autoCompactThreshold ?? 0.3)) {
      // compaction will be triggered here in prompt 5.8
    }

    // Step 13 — Return the updated record with system fields stripped
    return stripSystemFields(recordToWrite);
  } catch (err: unknown) {
    if (err instanceof OrionDBError) throw err;
    throw new CompactionError(`Unexpected error during update() on model '${ctx.modelName}'.`, {
      model: ctx.modelName,
      meta: { cause: err },
    });
  }
}

// ---------------------------------------------------------------------------
// updateMany()
// ---------------------------------------------------------------------------

/**
 * Updates all records matching `args.where` with the data in `args.data`.
 * Validates ALL records before writing ANY (batch atomicity).
 * Meta is updated exactly once after all writes.
 * Returns `{ count: N }` where N is the number of records updated.
 *
 * @throws ValidationError       if any record in the batch fails type validation
 * @throws UniqueConstraintError if any record introduces a unique constraint collision
 * @throws CompactionError       on mid-batch write failure (includes writtenCount in meta)
 */
export async function updateMany(
  ctx: ModelWriterContext,
  args: UpdateManyArgs,
  compiledFilter?: FilterFn,
): Promise<UpdateManyResult> {
  try {
    const readerCtx: ModelReaderContext = {
      modelName: ctx.modelName,
      paths: ctx.paths,
      schema: ctx.schema,
      indexManager: ctx.indexManager,
    };

    // Step 1 — Find all matching records
    const matching = await findMany(readerCtx, { where: args.where }, compiledFilter);
    if (matching.length === 0) {
      return { count: 0 };
    }

    const pkField = ctx.schema.primaryKeyField;

    // Step 2 — Validation pass: buildmerged records, validate, prepare write entries
    type ToUpdateEntry = {
      rawRecord: Record<string, unknown>;
      recordToWrite: Record<string, unknown>;
      pkValue: PrimaryKey;
    };
    const toUpdate: ToUpdateEntry[] = [];

    for (let i = 0; i < matching.length; i++) {
      const existing = matching[i];
      if (existing === undefined) continue;

      // PK mutation guard
      const existingPk = existing[pkField];
      const attemptedPk = args.data[pkField];
      if (attemptedPk !== undefined && attemptedPk !== existingPk) {
        throw new ValidationError(`Cannot change primary key field '${pkField}' on model '${ctx.modelName}'.`, {
          model: ctx.modelName,
          field: pkField,
          meta: { existingPk, attemptedPk, batchIndex: i },
        });
      }

      if (!isPrimaryKeyValue(existingPk)) continue;
      const pkValue = existingPk;

      const offset = ctx.indexManager.getOffset(pkValue);
      if (offset === undefined) continue; // deleted between scan and update — skip

      const rawRecord = await readRecordAtOffset(ctx.paths.dataFile, offset, ctx.modelName);

      // Merge
      const userDataFiltered: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(args.data)) {
        if (SYSTEM_FIELDS.includes(key)) continue;
        const fieldDef = ctx.schema.fields.get(key);
        if (fieldDef?.type === "relation") continue;
        userDataFiltered[key] = value;
      }
      const mergedData = { ...stripSystemFields(rawRecord), ...userDataFiltered };

      // Validate merged
      try {
        validateFieldTypes(mergedData, ctx.schema, ctx.modelName);
      } catch (e: unknown) {
        if (e instanceof ValidationError) {
          throw new ValidationError(e.message, {
            model: ctx.modelName,
            field: e.field,
            meta: { batchIndex: i, originalError: e.message },
          });
        }
        throw e;
      }

      try {
        checkUniqueConstraints(mergedData, ctx.schema, ctx.indexManager, ctx.modelName, pkValue);
      } catch (e: unknown) {
        if (e instanceof UniqueConstraintError) {
          throw new UniqueConstraintError(e.message, {
            model: ctx.modelName,
            field: e.field,
            meta: { batchIndex: i, originalError: e.message },
          });
        }
        throw e;
      }

      const rawCreatedAt = rawRecord["_createdAt"];
      const recordToWrite = attachSystemFields(mergedData, {
        _createdAt: typeof rawCreatedAt === "string" ? rawCreatedAt : new Date().toISOString(),
      });

      toUpdate.push({ rawRecord, recordToWrite, pkValue });
    }

    // Check for within-batch unique field conflicts introduced by the updates
    checkBatchUniqueness(
      toUpdate.map((e) => stripSystemFields(e.recordToWrite)),
      ctx.schema,
      ctx.modelName,
    );

    if (toUpdate.length === 0) {
      return { count: 0 };
    }

    // Step 3 — Write pass: append new version of each record sequentially
    let writtenCount = 0;
    for (const { rawRecord, recordToWrite } of toUpdate) {
      const serializedLine = serializeRecord(recordToWrite);
      const currentOffset = ctx.counter.getSize();

      try {
        await fs.appendFile(ctx.paths.dataFile, serializedLine);
      } catch (err: unknown) {
        throw new CompactionError(
          `Write failed mid-batch during updateMany() on model '${ctx.modelName}' (record ${writtenCount}).`,
          {
            model: ctx.modelName,
            meta: { cause: err, writtenCount, totalCount: toUpdate.length },
          },
        );
      }

      ctx.indexManager.update(rawRecord, recordToWrite, currentOffset);
      ctx.counter.increment(Buffer.byteLength(serializedLine, "utf8"));
      writtenCount++;
    }

    // Step 4 — Update meta once after all writes
    const existingMeta = await readModelMeta(ctx.paths);
    await updateModelMeta(ctx.paths, {
      tombstoneCount: existingMeta.tombstoneCount + toUpdate.length,
    });

    // Auto-compact threshold check (compaction triggered in prompt 5.8)
    const updatedMeta = await readModelMeta(ctx.paths);
    if (shouldAutoCompact(updatedMeta, ctx.autoCompactThreshold ?? 0.3)) {
      // compaction will be triggered here in prompt 5.8
    }

    return { count: toUpdate.length };
  } catch (err: unknown) {
    if (err instanceof OrionDBError) throw err;
    throw new CompactionError(`Unexpected error during updateMany() on model '${ctx.modelName}'.`, {
      model: ctx.modelName,
      meta: { cause: err },
    });
  }
}

// ---------------------------------------------------------------------------
// Private type guards
// ---------------------------------------------------------------------------

function isPrimaryKeyValue(value: unknown): value is PrimaryKey {
  return typeof value === "string" || typeof value === "number";
}

/** @alias isPrimaryKeyValue for backward compatibility within this file */
function isPrimaryKey(value: unknown): value is PrimaryKey {
  return typeof value === "string" || typeof value === "number";
}

// ---------------------------------------------------------------------------
// buildTombstone (private)
// ---------------------------------------------------------------------------

/**
 * Constructs a tombstone from an existing raw record (system fields present).
 * Sets `_deleted` to `true`, updates `_updatedAt`, preserves `_createdAt`.
 * System fields appear last, consistent with `attachSystemFields` convention.
 * Returns a new object — never mutates the input.
 */
const buildTombstone = (rawRecord: Record<string, unknown>): Record<string, unknown> => {
  // Destructure to separate user fields from system fields so we can
  // rewrite system fields at the end of the object.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { _deleted: _prevDeleted, _createdAt, _updatedAt: _prevUpdatedAt, ...userFields } = rawRecord;
  return {
    ...userFields,
    _deleted: true,
    _createdAt,
    _updatedAt: new Date().toISOString(),
  };
};

// ---------------------------------------------------------------------------
// shouldAutoCompact (private)
// ---------------------------------------------------------------------------

/**
 * Returns `true` when the tombstone ratio meets or exceeds `threshold`.
 * Returns `false` when `totalLines` is 0 to avoid division by zero.
 * `threshold` is a fraction between 0 and 1 (e.g. 0.30 for 30%).
 */
const shouldAutoCompact = (meta: ModelMeta, threshold: number): boolean => {
  if (meta.totalLines === 0) return false;
  return meta.tombstoneCount / meta.totalLines >= threshold;
};

// ---------------------------------------------------------------------------
// deleteRecord()
// ---------------------------------------------------------------------------

/**
 * Deletes a single record matching `args.where`.
 * Appends a tombstone line, removes the record from all indexes, updates meta.
 * Returns the pre-deletion record state with system fields stripped.
 *
 * @throws RecordNotFoundError  if no matching record exists
 * @throws QueryError           if the where clause does not reference PK or unique field
 * @throws ValidationError      if the PK value has an unexpected type
 * @throws CompactionError      if an unexpected I/O error occurs
 */
export async function deleteRecord(ctx: ModelWriterContext, args: DeleteArgs): Promise<Record<string, unknown>> {
  try {
    const readerCtx: ModelReaderContext = {
      modelName: ctx.modelName,
      paths: ctx.paths,
      schema: ctx.schema,
      indexManager: ctx.indexManager,
    };

    // Step 1 — Find existing record (returns stripped record or null)
    const existing = await findUnique(readerCtx, { where: args.where });
    if (existing === null) {
      throw new RecordNotFoundError(`No record found for the given where clause on model '${ctx.modelName}'.`, {
        model: ctx.modelName,
        meta: { where: args.where },
      });
    }

    // Step 2 — Resolve PK and read raw record from disk
    const pkValue = existing[ctx.schema.primaryKeyField];
    if (!isPrimaryKey(pkValue)) {
      throw new ValidationError(
        `Primary key field '${ctx.schema.primaryKeyField}' has an invalid type on model '${ctx.modelName}'.`,
        { model: ctx.modelName, field: ctx.schema.primaryKeyField, meta: { value: pkValue } },
      );
    }

    const offset = ctx.indexManager.getOffset(pkValue);
    if (offset === undefined) {
      // Defensive: should not occur if findUnique succeeded
      throw new RecordNotFoundError(
        `Record with ${ctx.schema.primaryKeyField} '${String(pkValue)}' is not in the physical index on model '${ctx.modelName}'.`,
        { model: ctx.modelName, meta: { pkValue } },
      );
    }

    const rawRecord = await readRecordAtOffset(ctx.paths.dataFile, offset, ctx.modelName);

    // Step 3 — Build tombstone
    const tombstone = buildTombstone(rawRecord);

    // Step 4 — Serialize
    const serializedLine = serializeRecord(tombstone);

    // Step 5-6 — Append tombstone (tombstone is written at ctx.counter.getSize())
    await fs.appendFile(ctx.paths.dataFile, serializedLine);

    // Step 7 — Remove from all index structures (logical → reverse map → physical)
    ctx.indexManager.delete(pkValue);

    // Step 8 — Increment file size counter
    ctx.counter.increment(Buffer.byteLength(serializedLine, "utf8"));

    // Step 9 — Update meta.json
    const existingMeta = await readModelMeta(ctx.paths);
    await updateModelMeta(ctx.paths, {
      recordCount: existingMeta.recordCount - 1,
      tombstoneCount: existingMeta.tombstoneCount + 1,
    });

    // Auto-compact threshold check (compaction triggered in prompt 5.8)
    const updatedMeta = await readModelMeta(ctx.paths);
    if (shouldAutoCompact(updatedMeta, ctx.autoCompactThreshold ?? 0.3)) {
      // compaction will be triggered here in prompt 5.8
    }

    // Step 10 — Return pre-deletion record without system fields
    return stripSystemFields(rawRecord);
  } catch (err: unknown) {
    if (err instanceof OrionDBError) throw err;
    throw new CompactionError(`Unexpected error during deleteRecord() on model '${ctx.modelName}'.`, {
      model: ctx.modelName,
      meta: { cause: err },
    });
  }
}

// ---------------------------------------------------------------------------
// deleteMany()
// ---------------------------------------------------------------------------

/**
 * Deletes all records matching the optional `args.where` clause.
 * Pass `compiledFilter` from the query engine for operator-based filtering.
 * Tombstones are written sequentially. Meta is updated once after all writes.
 * Returns `{ count: N }` where N is the number of records deleted.
 *
 * @throws CompactionError  on mid-batch write failure (includes `deletedCount` in meta)
 */
export async function deleteMany(
  ctx: ModelWriterContext,
  args: DeleteManyArgs,
  compiledFilter?: FilterFn,
): Promise<DeleteManyResult> {
  try {
    const readerCtx: ModelReaderContext = {
      modelName: ctx.modelName,
      paths: ctx.paths,
      schema: ctx.schema,
      indexManager: ctx.indexManager,
    };

    // Step 1 — Find all matching records (returns stripped records)
    const matching = await findMany(readerCtx, { where: args.where }, compiledFilter);
    if (matching.length === 0) {
      return { count: 0 };
    }

    // Step 2 — Collect raw records and resolved PKs
    type ToDeleteEntry = { rawRecord: Record<string, unknown>; pkValue: PrimaryKey };
    const toDelete: ToDeleteEntry[] = [];

    for (const record of matching) {
      const pkValue = record[ctx.schema.primaryKeyField];
      if (!isPrimaryKey(pkValue)) continue; // defensive — skip records with invalid PK type

      const offset = ctx.indexManager.getOffset(pkValue);
      if (offset === undefined) continue; // already deleted between scan and delete — skip

      const rawRecord = await readRecordAtOffset(ctx.paths.dataFile, offset, ctx.modelName);
      toDelete.push({ rawRecord, pkValue });
    }

    if (toDelete.length === 0) {
      return { count: 0 };
    }

    // Step 3 — Write pass: one tombstone per record, sequentially
    let writtenCount = 0;
    for (const { rawRecord, pkValue } of toDelete) {
      // a. Build tombstone
      const tombstone = buildTombstone(rawRecord);

      // b. Serialize
      const serializedLine = serializeRecord(tombstone);

      // c-d. Append tombstone
      try {
        await fs.appendFile(ctx.paths.dataFile, serializedLine);
      } catch (err: unknown) {
        throw new CompactionError(
          `Write failed mid-batch during deleteMany() on model '${ctx.modelName}' (tombstone ${writtenCount}).`,
          {
            model: ctx.modelName,
            meta: { cause: err, deletedCount: writtenCount, totalCount: toDelete.length },
          },
        );
      }

      // e. Remove from all index structures
      ctx.indexManager.delete(pkValue);

      // f. Increment file size counter
      ctx.counter.increment(Buffer.byteLength(serializedLine, "utf8"));

      writtenCount++;
    }

    // Step 4 — Update meta.json once after all writes
    const existingMeta = await readModelMeta(ctx.paths);
    await updateModelMeta(ctx.paths, {
      recordCount: existingMeta.recordCount - toDelete.length,
      tombstoneCount: existingMeta.tombstoneCount + toDelete.length,
    });

    // Auto-compact threshold check (compaction triggered in prompt 5.8)
    const updatedMeta = await readModelMeta(ctx.paths);
    if (shouldAutoCompact(updatedMeta, ctx.autoCompactThreshold ?? 0.3)) {
      // compaction will be triggered here in prompt 5.8
    }

    // Step 5 — Return result
    return { count: toDelete.length };
  } catch (err: unknown) {
    if (err instanceof OrionDBError) throw err;
    throw new CompactionError(`Unexpected error during deleteMany() on model '${ctx.modelName}'.`, {
      model: ctx.modelName,
      meta: { cause: err },
    });
  }
}
