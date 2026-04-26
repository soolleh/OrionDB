// src/relations/types.ts
//
// All relation resolver type definitions for OrionDB.
// Only types, constants, type guards, and the pure
// `buildRelationDescriptor` factory are permitted here.

import type { ParsedModelDefinition, ParsedRelationField } from "../schema/index.js";
import { RelationError } from "../errors/index.js";

// ---------------------------------------------------------------------------
// RelationType
// ---------------------------------------------------------------------------

/**
 * The cardinality type of a relation.
 * Defined independently here — the relations module owns its
 * own type surface and does not import from the schema module
 * for this type.
 */
export type RelationType = "one-to-one" | "one-to-many" | "many-to-one";

// ---------------------------------------------------------------------------
// ResolvedRelation
// ---------------------------------------------------------------------------

/**
 * Represents a single resolved relation attachment — the result
 * of resolving one `include` clause for one field.
 *
 * The resolver always returns arrays. The caller is responsible
 * for cardinality coercion:
 * - `one-to-many`: use `records` as-is
 * - `one-to-one` / `many-to-one`: use `records[0] ?? null`
 */
export interface ResolvedRelation {
  /** The relation field name on the parent record. */
  field: string;
  relationType: RelationType;
  /**
   * Resolved related records.
   * - Array (possibly empty) for `one-to-many`.
   * - Single-element array (or empty) for `one-to-one` / `many-to-one`.
   */
  records: Record<string, unknown>[];
}

// ---------------------------------------------------------------------------
// IncludeClause / IncludeValue
// ---------------------------------------------------------------------------

/**
 * The value for a single relation field in an `include` clause.
 *
 * `true` — include all scalar fields, no filtering.
 *
 * Object form — Phase 1 supports `select` only.
 * `where`, `take`, `skip`, and `orderBy` are silently ignored
 * in Phase 1 and are reserved for Phase 2.
 */
export type IncludeValue =
  | true
  | {
      select?: Record<string, boolean>;
      /** Phase 2 — silently ignored in Phase 1. */
      where?: Record<string, unknown>;
      /** Phase 2 — silently ignored in Phase 1. */
      take?: number;
      /** Phase 2 — silently ignored in Phase 1. */
      skip?: number;
      /** Phase 2 — silently ignored in Phase 1. */
      orderBy?: Record<string, "asc" | "desc"> | Record<string, "asc" | "desc">[];
    };

/**
 * The raw `include` argument as passed by the caller.
 * Maps relation field names to their include configuration.
 */
export type IncludeClause = {
  [relationField: string]: IncludeValue;
};

// ---------------------------------------------------------------------------
// RelationDescriptor
// ---------------------------------------------------------------------------

/**
 * Describes a relation that needs to be resolved, built from a
 * `ParsedModelDefinition` at resolve time.
 *
 * `ownerSide` identifies which model holds the foreign key:
 * - `'declaring'` — FK is on the model declaring this relation
 *   (many-to-one: child holds FK to parent)
 * - `'related'` — FK is on the related model
 *   (one-to-many: related records hold FK pointing back)
 */
export interface RelationDescriptor {
  /** Field name on the declaring model. */
  field: string;
  /** Target model name. */
  relatedModel: string;
  /** Foreign key field name. */
  foreignKey: string;
  relationType: RelationType;
  /** Which model holds the foreign key. */
  ownerSide: "declaring" | "related";
}

// ---------------------------------------------------------------------------
// RelationResolverContext
// ---------------------------------------------------------------------------

/**
 * Context passed to resolver functions, grouping all
 * dependencies needed to resolve relations for a single model.
 */
export interface RelationResolverContext {
  modelName: string;
  schema: ParsedModelDefinition;
  /** Full map of all model schemas — needed for related model lookups. */
  allSchemas: Map<string, ParsedModelDefinition>;
  /**
   * Injected async function used to fetch related records.
   * The resolver never imports from the persistence layer
   * directly, ensuring testability without file I/O.
   */
  findMany: FindManyForResolver;
}

// ---------------------------------------------------------------------------
// FindManyForResolver
// ---------------------------------------------------------------------------

/**
 * The injected `findMany` signature used by the resolver.
 * Accepts a model name and a compiled filter predicate.
 * The runtime implementation delegates to the persistence
 * layer's `findMany` with an appropriate `ModelReaderContext`.
 */
export type FindManyForResolver = (
  modelName: string,
  filter: (record: Record<string, unknown>) => boolean,
) => Promise<Record<string, unknown>[]>;

// ---------------------------------------------------------------------------
// IncludeResult
// ---------------------------------------------------------------------------

/**
 * The result of resolving all `include` clauses for a set of
 * parent records.
 *
 * Keyed by the parent record's primary key value (as string).
 * Each entry maps relation field names to their resolved value:
 * - `Record<string, unknown>[]` for `one-to-many`
 * - `Record<string, unknown> | null` for `one-to-one` / `many-to-one`
 */
export type IncludeResult = Map<
  string, // parent PK value (as string)
  Record<string, unknown> // relation field → resolved value
>;

// ---------------------------------------------------------------------------
// NestedCreateInput
// ---------------------------------------------------------------------------

/**
 * Input shape for a nested create operation.
 *
 * @example
 * ```ts
 * db.user.create({
 *   data: {
 *     name: 'Alice',
 *     posts: { create: [{ title: 'Post 1' }] }
 *   }
 * })
 * ```
 */
export interface NestedCreateInput {
  create: Record<string, unknown> | Record<string, unknown>[];
}

// ---------------------------------------------------------------------------
// NestedWriteOperation
// ---------------------------------------------------------------------------

/**
 * Describes a single pending nested write extracted from a
 * parent `create` or `update` data object.
 */
export interface NestedWriteOperation {
  /** Relation field name on the parent model. */
  parentField: string;
  /** Target model to write into. */
  relatedModel: string;
  /** FK field name on the related record. */
  foreignKey: string;
  /** The PK value of the parent record (set as FK on child). */
  foreignKeyValue: unknown;
  relationType: RelationType;
  /** Records to create in the related model. */
  records: Record<string, unknown>[];
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Named constant for the `true` shorthand in include clauses.
 * Documents intent at usage sites.
 */
export const INCLUDE_ALL = true as const;

/** Type alias for the `INCLUDE_ALL` constant. */
export type IncludeAll = typeof INCLUDE_ALL;

// ---------------------------------------------------------------------------
// Type guards
// ---------------------------------------------------------------------------

/**
 * Returns `true` when the include value is the `true` shorthand,
 * meaning include all fields with no filtering.
 */
export const isIncludeAll = (value: IncludeValue): value is IncludeAll => value === true;

/**
 * Returns `true` when the include value is the object form.
 * In Phase 1, only the `select` key is honored.
 */
export const isIncludeObject = (value: IncludeValue): value is Exclude<IncludeValue, true> =>
  typeof value === "object" && value !== null;

// ---------------------------------------------------------------------------
// buildRelationDescriptor
// ---------------------------------------------------------------------------

/**
 * Pure factory that constructs a `RelationDescriptor` from a
 * parsed relation field definition and the declaring model's
 * schema.
 *
 * Determines `ownerSide` by:
 * - `many-to-one` → `'declaring'` (FK is on the declaring model)
 * - `one-to-many` → `'related'` (FK is on the related model)
 * - `one-to-one` → checks whether `foreignKey` is a scalar field
 *   on the declaring model; `'declaring'` if yes, `'related'` if no
 *
 * @throws `RelationError` if the related model schema is not found
 *   in `allSchemas`.
 */
export function buildRelationDescriptor(
  field: string,
  relationField: ParsedRelationField,
  declaringModelName: string,
  allSchemas: Map<string, ParsedModelDefinition>,
): RelationDescriptor {
  const relatedModel = relationField.model;
  const foreignKey = relationField.foreignKey;
  const relationType = relationField.relation as RelationType;

  const relatedSchema = allSchemas.get(relatedModel);
  if (relatedSchema === undefined) {
    throw new RelationError(
      `Related model '${relatedModel}' schema not found when building descriptor for field '${field}' on '${declaringModelName}'.`,
      {
        model: declaringModelName,
        field,
        meta: { relatedModel, reason: "related model schema not found" },
      },
    );
  }

  let ownerSide: "declaring" | "related";

  if (relationType === "many-to-one") {
    ownerSide = "declaring";
  } else if (relationType === "one-to-many") {
    ownerSide = "related";
  } else {
    // one-to-one: check if the FK lives on the declaring model
    const declaringSchema = allSchemas.get(declaringModelName);
    const declaringHasFK =
      declaringSchema !== undefined &&
      declaringSchema.fields.has(foreignKey) &&
      declaringSchema.fields.get(foreignKey)?.type !== "relation";
    ownerSide = declaringHasFK ? "declaring" : "related";
  }

  return { field, relatedModel, foreignKey, relationType, ownerSide };
}
