// src/relations/resolver.ts
//
// Batched include resolver — N+1 prevention for relation resolution.
// Executes exactly ONE ctx.findMany call per relation field,
// never one per parent record.

import type { ParsedModelDefinition } from "../schema/index.js";
import { RelationError } from "../errors/index.js";
import { buildRelationDescriptor, isIncludeObject } from "./types.js";
import type {
  IncludeClause,
  IncludeResult,
  IncludeValue,
  RelationDescriptor,
  RelationResolverContext,
} from "./types.js";

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/**
 * Converts a primary key value (unknown) to its string representation
 * for use as a Map key. Returns `undefined` for non-primitive values
 * that cannot safely be stringified (objects, symbols, etc.).
 */
const pkToString = (pk: unknown): string | undefined => {
  if (typeof pk === "string" || typeof pk === "number") return String(pk);
  return undefined;
};

/**
 * Collects all unique non-null, non-undefined values of `fkField`
 * from `parentRecords` into a Set in a single pass.
 */
const collectForeignKeyValues = (parentRecords: Record<string, unknown>[], fkField: string): Set<unknown> => {
  const values = new Set<unknown>();
  for (const record of parentRecords) {
    const val = record[fkField];
    if (val !== null && val !== undefined) {
      values.add(val);
    }
  }
  return values;
};

/**
 * Groups a flat array of records into a Map keyed by the value
 * of `field`. Records with null or undefined for `field` are
 * silently skipped — they cannot be attached to a parent.
 */
const groupByField = (records: Record<string, unknown>[], field: string): Map<unknown, Record<string, unknown>[]> => {
  const grouped = new Map<unknown, Record<string, unknown>[]>();
  for (const record of records) {
    const key = record[field];
    if (key === null || key === undefined) continue;
    const group = grouped.get(key) ?? [];
    group.push(record);
    grouped.set(key, group);
  }
  return grouped;
};

/**
 * Applies the `select` projection to a set of resolved records.
 * Phase 1: only `select` is honored — `where`, `take`, `skip`,
 * and `orderBy` on included relations are silently ignored.
 */
const applySelectToRecords = (
  records: Record<string, unknown>[],
  includeValue: IncludeValue,
): Record<string, unknown>[] => {
  if (!isIncludeObject(includeValue)) return records;
  if (!includeValue.select) return records;
  const select = includeValue.select;
  return records.map((record) => {
    const result: Record<string, unknown> = {};
    for (const [key, enabled] of Object.entries(select)) {
      if (enabled) result[key] = record[key];
    }
    return result;
  });
};

/**
 * Resolves a single relation field using the batched algorithm.
 * Returns a Map from "grouping key" to array of related records.
 *
 * Strategy A (`ownerSide: 'related'`):
 *   Map key = FK value on the related record (e.g. `post.authorId`)
 *   At attach time: parent PK is used to look up related records.
 *
 * Strategy B (`ownerSide: 'declaring'`):
 *   Map key = PK of the related record (e.g. `user.id`)
 *   At attach time: parent FK value is used to look up related record.
 */
const resolveOneRelation = async (
  descriptor: RelationDescriptor,
  parentRecords: Record<string, unknown>[],
  includeValue: IncludeValue,
  ctx: RelationResolverContext,
  parentSchema: ParsedModelDefinition,
): Promise<Map<unknown, Record<string, unknown>[]>> => {
  if (descriptor.ownerSide === "related") {
    // Strategy A: FK is on the related model.
    // Collect parent PKs and scan related model for matching FK values.
    const parentPkValues = collectForeignKeyValues(parentRecords, parentSchema.primaryKeyField);

    const relatedRecords = await ctx.findMany(descriptor.relatedModel, (record) =>
      parentPkValues.has(record[descriptor.foreignKey]),
    );

    const projected = applySelectToRecords(relatedRecords, includeValue);
    return groupByField(projected, descriptor.foreignKey);
  } else {
    // Strategy B: FK is on the declaring (parent) model.
    // Collect FK values from parents and scan related model for matching PKs.
    const fkValues = collectForeignKeyValues(parentRecords, descriptor.foreignKey);

    const relatedSchema = ctx.allSchemas.get(descriptor.relatedModel);
    if (relatedSchema === undefined) {
      throw new RelationError(
        `Related model '${descriptor.relatedModel}' schema not found during relation resolution.`,
        {
          model: ctx.modelName,
          field: descriptor.field,
          meta: { relatedModel: descriptor.relatedModel, reason: "related model schema not found" },
        },
      );
    }

    const relatedPkField = relatedSchema.primaryKeyField;

    const relatedRecords = await ctx.findMany(descriptor.relatedModel, (record) =>
      fkValues.has(record[relatedPkField]),
    );

    const projected = applySelectToRecords(relatedRecords, includeValue);
    return groupByField(projected, relatedPkField);
  }
};

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

/**
 * Resolves all include clauses for a set of parent records using
 * batched N+1-safe resolution.
 *
 * Executes exactly ONE `ctx.findMany` call per relation field,
 * regardless of how many parent records are in the result set.
 *
 * Returns an `IncludeResult` Map keyed by parent PK (as string),
 * where each value is a plain object mapping relation field names
 * to their resolved values:
 * - `one-to-many` → array of related records (empty array for no matches)
 * - `one-to-one` / `many-to-one` → single record or `null`
 *
 * @throws `RelationError` if an include field is not a declared
 *   relation on the model.
 */
export const resolveIncludes = async (
  ctx: RelationResolverContext,
  parentRecords: Record<string, unknown>[],
  include: IncludeClause,
): Promise<IncludeResult> => {
  if (parentRecords.length === 0) return new Map();

  const result: IncludeResult = new Map();
  const pkField = ctx.schema.primaryKeyField;

  // Initialize an empty aggregation object for every parent record
  for (const record of parentRecords) {
    const pkStr = pkToString(record[pkField]);
    if (pkStr !== undefined) {
      result.set(pkStr, {});
    }
  }

  for (const [field, includeValue] of Object.entries(include)) {
    const relationField = ctx.schema.relationFields.get(field);
    if (relationField === undefined) {
      throw new RelationError(`Relation field '${field}' not found on model '${ctx.modelName}'.`, {
        model: ctx.modelName,
        field,
        meta: { reason: "field is not a declared relation" },
      });
    }

    const descriptor = buildRelationDescriptor(field, relationField, ctx.modelName, ctx.allSchemas);

    // ONE scan of the related model for all parent records combined
    const groupedMap = await resolveOneRelation(descriptor, parentRecords, includeValue, ctx, ctx.schema);

    // Distribute resolved records back to each parent entry
    for (const record of parentRecords) {
      const pkStr = pkToString(record[pkField]);
      if (pkStr === undefined) continue;
      const entry = result.get(pkStr) ?? {};

      if (descriptor.ownerSide === "related") {
        // Strategy A: look up by parent PK value (raw, for Map key equality)
        const pk = record[pkField];
        const relatedRecords = pk !== null && pk !== undefined ? (groupedMap.get(pk) ?? []) : [];
        if (descriptor.relationType === "one-to-many") {
          entry[field] = relatedRecords;
        } else {
          // one-to-one where FK is on the related side
          entry[field] = relatedRecords[0] ?? null;
        }
      } else {
        // Strategy B: look up by FK value carried on the parent record
        const fkValue = record[descriptor.foreignKey];
        const relatedRecords = fkValue !== null && fkValue !== undefined ? (groupedMap.get(fkValue) ?? []) : [];
        // many-to-one / one-to-one where FK is on the declaring side
        entry[field] = relatedRecords[0] ?? null;
      }

      result.set(pkStr, entry);
    }
  }

  return result;
};

/**
 * Merges resolved include data back onto parent records.
 *
 * Returns a new array of records with included relation fields
 * attached via object spread. Does not mutate any input record
 * or the input array.
 *
 * Records with no PK or no entry in `includeResult` are
 * returned as the original reference (unchanged).
 */
export const attachIncludes = (
  parentRecords: Record<string, unknown>[],
  includeResult: IncludeResult,
  pkField: string,
): Record<string, unknown>[] =>
  parentRecords.map((record) => {
    const pkStr = pkToString(record[pkField]);
    if (pkStr === undefined) return record;
    const resolved = includeResult.get(pkStr);
    if (resolved === undefined) return record;
    return { ...record, ...resolved };
  });
