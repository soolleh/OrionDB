// src/persistence/writer.ts
// Implements the write path: create, createMany, update, updateMany
// createMany, update, updateMany are implemented in prompt 4.4

import * as fs from "node:fs/promises";
import { CompactionError, OrionDBError, UniqueConstraintError, ValidationError } from "../errors/index.js";
import type { IndexManager, PrimaryKey } from "../index-manager/index.js";
import { SYSTEM_FIELDS } from "../schema/index.js";
import type { ParsedModelDefinition } from "../schema/index.js";
import { updateModelMeta } from "./initializer.js";
import { NEWLINE } from "./types.js";
import type {
  ModelMeta,
  ModelPaths,
  ModelWriterContext,
  CreateArgs,
  CreateManyArgs,
  CreateManyResult,
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
 */
function stripSystemFields(record: Record<string, unknown>): Record<string, unknown> {
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
// Private type guards
// ---------------------------------------------------------------------------

function isPrimaryKey(value: unknown): value is PrimaryKey {
  return typeof value === "string" || typeof value === "number";
}
