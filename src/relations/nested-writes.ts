// src/relations/nested-writes.ts
//
// Nested write operations — nested create and connect logic.
// FK injection for one-to-many / one-to-one (FK on related side) happens
// inside executeNestedWrites. FK injection for many-to-one / one-to-one
// (FK on declaring side) is handled at the caller level via
// resolveConnectForeignKey before the parent write.

import type { ParsedModelDefinition } from "../schema/index.js";
import { RelationError } from "../errors/index.js";
import { buildRelationDescriptor, isNestedCreate, isNestedConnect } from "./types.js";
import type { NestedCreateInput, NestedConnectInput, NestedWriteOperation } from "./types.js";

// ---------------------------------------------------------------------------
// ExecuteContext
// ---------------------------------------------------------------------------

/**
 * Injected execution dependencies for `executeNestedWrites`.
 * Keeps nested writes testable without file I/O.
 */
interface ExecuteContext {
  /**
   * Creates a single record in the given model.
   * Delegates to the persistence layer's `create` at runtime.
   */
  createRecord: (modelName: string, data: Record<string, unknown>) => Promise<Record<string, unknown>>;

  /**
   * Updates a record identified by `where` in the given model.
   * Delegates to the persistence layer's `update` at runtime.
   */
  updateRecord: (
    modelName: string,
    where: Record<string, unknown>,
    data: Record<string, unknown>,
  ) => Promise<Record<string, unknown>>;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/**
 * Normalises a create/connect value to an array regardless of
 * whether the user passed a single object or an array.
 */
const toRecordArray = (value: Record<string, unknown> | Record<string, unknown>[]): Record<string, unknown>[] =>
  Array.isArray(value) ? value : [value];

/**
 * Asserts that every element in `records` is a plain, non-array
 * object. Throws `RelationError` for the first invalid entry.
 */
const assertPlainObjects = (records: unknown[], field: string, modelName: string): void => {
  for (const record of records) {
    if (typeof record !== "object" || record === null || Array.isArray(record)) {
      throw new RelationError(
        `Nested write entry for field '${field}' on model '${modelName}' must be a plain object.`,
        {
          model: modelName,
          field,
          meta: { reason: "nested record must be a plain object", received: typeof record },
        },
      );
    }
  }
};

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

/**
 * Scans a parent data object for relation fields that contain
 * nested write operations (`create` or `connect`).
 *
 * Returns:
 * - `cleanData` — a shallow copy of `data` with all relation
 *   field entries removed, safe to pass to the parent writer.
 * - `operations` — ordered list of `NestedWriteOperation` objects
 *   ready for execution after the parent write.
 *
 * `foreignKeyValue` in every returned operation is `null` at
 * extraction time because the parent PK is not yet known.
 * `executeNestedWrites` receives the actual PK and applies it.
 *
 * For `many-to-one` `connect`, the FK must be injected into the
 * parent data before the parent write. Use `resolveConnectForeignKey`
 * to extract the FK value and set it on `cleanData` at the call site.
 *
 * @throws `RelationError` if a relation field's value is neither
 *   a `create` nor a `connect` input.
 */
export const extractNestedWrites = (
  data: Record<string, unknown>,
  schema: ParsedModelDefinition,
  allSchemas: Map<string, ParsedModelDefinition>,
): { cleanData: Record<string, unknown>; operations: NestedWriteOperation[] } => {
  const operations: NestedWriteOperation[] = [];

  for (const [field, relationField] of schema.relationFields) {
    const fieldValue = data[field];
    if (fieldValue === undefined || fieldValue === null) continue;
    if (typeof fieldValue !== "object" || Array.isArray(fieldValue)) {
      throw new RelationError(
        `Relation field '${field}' on model '${schema.name}' must contain a create or connect object.`,
        {
          model: schema.name,
          field,
          meta: { reason: "relation field data must contain create or connect" },
        },
      );
    }

    const nestedValue = fieldValue as Record<string, unknown>;
    const hasCreate = isNestedCreate(nestedValue);
    const hasConnect = isNestedConnect(nestedValue);

    if (!hasCreate && !hasConnect) {
      throw new RelationError(
        `Relation field '${field}' on model '${schema.name}' must contain a create or connect object.`,
        {
          model: schema.name,
          field,
          meta: { reason: "relation field data must contain create or connect" },
        },
      );
    }

    const descriptor = buildRelationDescriptor(field, relationField, schema.name, allSchemas);

    // --- create ---
    if (hasCreate) {
      const createValue = (nestedValue as NestedCreateInput).create;
      const records = toRecordArray(createValue);
      assertPlainObjects(records, field, schema.name);
      operations.push({
        parentField: field,
        relatedModel: descriptor.relatedModel,
        foreignKey: descriptor.foreignKey,
        foreignKeyValue: null,
        relationType: descriptor.relationType,
        records,
      });
    }

    // --- connect ---
    if (hasConnect) {
      const connectValue = (nestedValue as NestedConnectInput).connect;
      const connectEntries = toRecordArray(connectValue);
      assertPlainObjects(connectEntries, field, schema.name);
      operations.push({
        parentField: field,
        relatedModel: descriptor.relatedModel,
        foreignKey: descriptor.foreignKey,
        foreignKeyValue: null,
        relationType: descriptor.relationType,
        records: connectEntries.map((where) => ({
          ...where,
          _nestedOp: "connect",
        })),
      });
    }
  }

  // Build cleanData: shallow copy with all relation fields stripped
  const cleanData = { ...data };
  for (const field of schema.relationFields.keys()) {
    if (field in cleanData) {
      delete cleanData[field];
    }
  }

  return { cleanData, operations };
};

/**
 * Executes all nested write operations using the parent record's
 * PK as the FK value.
 *
 * Ordering guarantee: all `create` operations in a batch execute
 * before any `connect` operations within the same parent write.
 *
 * FK injection strategy:
 * - `one-to-many` / `one-to-one` (FK on related): FK is injected
 *   into the child record here before calling `execute.createRecord`.
 * - `many-to-one` nested create: not supported — throws `RelationError`.
 * - `one-to-many` connect: calls `execute.updateRecord` to update the
 *   related record's FK.
 * - `many-to-one` / `one-to-one` (FK on declaring side) connect:
 *   FK was already set on the parent data before the parent write.
 *   No-op here.
 *
 * @throws `RelationError` for `many-to-one` nested creates (unsupported).
 */
export const executeNestedWrites = async (
  operations: NestedWriteOperation[],
  parentPk: unknown,
  execute: ExecuteContext,
): Promise<void> => {
  const isConnect = (op: NestedWriteOperation): boolean => op.records[0]?.["_nestedOp"] === "connect";

  const createOps = operations.filter((op) => !isConnect(op));
  const connectOps = operations.filter((op) => isConnect(op));

  // --- create pass ---
  for (const op of createOps) {
    if (op.relationType === "many-to-one") {
      throw new RelationError(
        `Nested create on many-to-one relation '${op.parentField}' is not supported. Create the parent record first.`,
        {
          field: op.parentField,
          meta: {
            relatedModel: op.relatedModel,
            reason: "create the parent record first",
          },
        },
      );
    }
    // one-to-many and one-to-one (FK on related): inject parent PK as FK
    for (const record of op.records) {
      const cleanRecord = { ...record };
      delete cleanRecord["_nestedOp"];
      const recordWithFk = { ...cleanRecord, [op.foreignKey]: parentPk };
      await execute.createRecord(op.relatedModel, recordWithFk);
    }
  }

  // --- connect pass ---
  for (const op of connectOps) {
    for (const connectEntry of op.records) {
      const where = { ...connectEntry };
      delete where["_nestedOp"];

      if (op.relationType === "one-to-many") {
        // Update the related record's FK to point to this parent
        await execute.updateRecord(op.relatedModel, where, { [op.foreignKey]: parentPk });
      }
      // many-to-one / one-to-one (FK on declaring side):
      // FK was already set on the parent data before execution.
      // No further action needed.
    }
  }
};

/**
 * Extracts the FK field name and value from a `many-to-one` connect
 * `where` clause so the caller can inject it into the parent's
 * `cleanData` before the parent write.
 *
 * The connect `where` clause must include the primary key of the
 * related model.
 *
 * @throws `RelationError` if the related model schema is not found.
 * @throws `RelationError` if the connect `where` clause does not
 *   include the related model's primary key.
 */
export const resolveConnectForeignKey = (
  operation: NestedWriteOperation,
  where: Record<string, unknown>,
  allSchemas: Map<string, ParsedModelDefinition>,
): { field: string; value: unknown } => {
  const relatedSchema = allSchemas.get(operation.relatedModel);
  if (relatedSchema === undefined) {
    throw new RelationError(`Schema not found for model '${operation.relatedModel}'.`, {
      meta: { relatedModel: operation.relatedModel },
    });
  }
  const pkField = relatedSchema.primaryKeyField;
  const pkValue = where[pkField];
  if (pkValue === undefined || pkValue === null) {
    throw new RelationError("connect where clause must include the primary key of the related model.", {
      meta: { relatedModel: operation.relatedModel, where },
    });
  }
  return { field: operation.foreignKey, value: pkValue };
};
