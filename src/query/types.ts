// src/query/types.ts
// All query-related type definitions, constants, and type-guard helpers.
// No implemented logic beyond the two type-guard helpers at the bottom.

// ---------------------------------------------------------------------------
// Scalar filter interfaces
// ---------------------------------------------------------------------------

/** Filter operators for string fields. */
export interface StringFilter {
  equals?: string;
  not?: string | StringFilter;
  in?: string[];
  notIn?: string[];
  contains?: string;
  startsWith?: string;
  endsWith?: string;
  lt?: string;
  lte?: string;
  gt?: string;
  gte?: string;
}

/** Filter operators for number fields. */
export interface NumberFilter {
  equals?: number;
  not?: number | NumberFilter;
  in?: number[];
  notIn?: number[];
  lt?: number;
  lte?: number;
  gt?: number;
  gte?: number;
}

/** Filter operators for boolean fields. */
export interface BooleanFilter {
  equals?: boolean;
  not?: boolean;
}

/** Filter operators for date fields. Accepts `Date` objects or ISO 8601 strings. */
export interface DateFilter {
  equals?: Date | string;
  not?: Date | string | DateFilter;
  lt?: Date | string;
  lte?: Date | string;
  gt?: Date | string;
  gte?: Date | string;
}

/** Filter operators for enum fields. */
export interface EnumFilter {
  equals?: string;
  not?: string | EnumFilter;
  in?: string[];
  notIn?: string[];
}

// ---------------------------------------------------------------------------
// WhereInput
// ---------------------------------------------------------------------------

/**
 * The top-level `where` clause accepted by all query operations.
 *
 * Named logical-operator keys (`AND`, `OR`, `NOT`) are handled explicitly.
 * The index signature accommodates arbitrary field names while listing every
 * valid value shape — no `any` used.
 *
 * `undefined` is included in the index signature value union to satisfy
 * `exactOptionalPropertyTypes`.
 */
export type WhereInput = {
  AND?: WhereInput | WhereInput[];
  OR?: WhereInput[];
  NOT?: WhereInput | WhereInput[];
  [field: string]:
    | string
    | number
    | boolean
    | null
    | StringFilter
    | NumberFilter
    | BooleanFilter
    | DateFilter
    | EnumFilter
    | WhereInput
    | WhereInput[]
    | undefined;
};

// ---------------------------------------------------------------------------
// OrderByInput
// ---------------------------------------------------------------------------

/** Direction for a single field sort. */
export type OrderByDirection = "asc" | "desc";

/**
 * Ordering clause for query operations.
 *
 * - Single-field: `{ name: 'asc' }`
 * - Multi-field: `[{ name: 'asc' }, { age: 'desc' }]`
 *
 * Elements in the array are applied in order — the first element is the
 * primary sort, subsequent elements are tiebreakers.
 */
export type OrderByInput = { [field: string]: OrderByDirection } | { [field: string]: OrderByDirection }[];

// ---------------------------------------------------------------------------
// SelectInput
// ---------------------------------------------------------------------------

/**
 * Field selection clause. Set a field to `true` to include it in the result,
 * `false` (or omit) to exclude it.
 */
export type SelectInput = {
  [field: string]: boolean;
};

// ---------------------------------------------------------------------------
// PaginationInput
// ---------------------------------------------------------------------------

/** Skip/take pagination parameters. */
export interface PaginationInput {
  skip?: number;
  take?: number;
}

// ---------------------------------------------------------------------------
// Find operation input shapes
// ---------------------------------------------------------------------------

/**
 * Full argument shape for `findMany` at the query layer.
 * Richer than the persistence-layer `FindManyArgs` because it carries the
 * fully-typed `WhereInput` rather than an opaque `WhereClause`.
 */
export interface FindManyInput {
  where?: WhereInput;
  select?: SelectInput;
  orderBy?: OrderByInput;
  skip?: number;
  take?: number;
}

/** Argument shape for `findFirst` at the query layer. */
export interface FindFirstInput {
  where?: WhereInput;
  select?: SelectInput;
  orderBy?: OrderByInput;
}

/** Argument shape for `findUnique` at the query layer. */
export interface FindUniqueInput {
  where: WhereInput;
  select?: SelectInput;
}

// ---------------------------------------------------------------------------
// Aggregate types
// ---------------------------------------------------------------------------

/** Input for aggregate operations (`_count`, `_avg`, `_sum`, `_min`, `_max`). */
export interface AggregateInput {
  where?: WhereInput;
  _count?: boolean | { [field: string]: boolean };
  _avg?: { [field: string]: boolean };
  _sum?: { [field: string]: boolean };
  _min?: { [field: string]: boolean };
  _max?: { [field: string]: boolean };
}

/** Result shape returned from aggregate operations. */
export interface AggregateResult {
  _count?: number | { [field: string]: number };
  _avg?: { [field: string]: number | null };
  _sum?: { [field: string]: number | null };
  _min?: { [field: string]: number | string | null };
  _max?: { [field: string]: number | string | null };
}

// ---------------------------------------------------------------------------
// GroupBy types
// ---------------------------------------------------------------------------

/** Input for `groupBy` operations. */
export interface GroupByInput {
  by: string[];
  where?: WhereInput;
  _count?: boolean | { [field: string]: boolean };
  _avg?: { [field: string]: boolean };
  _sum?: { [field: string]: boolean };
  _min?: { [field: string]: boolean };
  _max?: { [field: string]: boolean };
  orderBy?: OrderByInput;
  skip?: number;
  take?: number;
}

/** A single row in a `groupBy` result. */
export interface GroupByResult {
  [field: string]: unknown;
  _count?: number | { [field: string]: number };
  _avg?: { [field: string]: number | null };
  _sum?: { [field: string]: number | null };
  _min?: { [field: string]: number | string | null };
  _max?: { [field: string]: number | string | null };
}

// ---------------------------------------------------------------------------
// Compiled function types
// ---------------------------------------------------------------------------

/**
 * Internal representation of a compiled `WhereInput`.
 * The filter compiler in `filter.ts` takes a `WhereInput` and returns this
 * predicate. Shares the same function signature as `FilterFn` in the
 * persistence layer — both are `(record: Record<string, unknown>) => boolean`.
 */
export type CompiledFilter = (record: Record<string, unknown>) => boolean;

/**
 * The output of the sort compiler. A standard comparator function for use
 * with `Array.prototype.sort`.
 */
export type CompiledSort = (a: Record<string, unknown>, b: Record<string, unknown>) => number;

// ---------------------------------------------------------------------------
// Query constants
// ---------------------------------------------------------------------------

/** The three logical combinator keys recognised at the top level of a `WhereInput`. */
export const LOGICAL_OPERATORS = ["AND", "OR", "NOT"] as const;

/** Union of the three logical operator key strings. */
export type LogicalOperator = (typeof LOGICAL_OPERATORS)[number];

/** All scalar comparison operators supported in field-level filters. */
export const SCALAR_OPERATORS = [
  "equals",
  "not",
  "in",
  "notIn",
  "contains",
  "startsWith",
  "endsWith",
  "lt",
  "lte",
  "gt",
  "gte",
] as const;

/** Union of all supported scalar operator key strings. */
export type ScalarOperator = (typeof SCALAR_OPERATORS)[number];

// ---------------------------------------------------------------------------
// Type-guard helpers
// ---------------------------------------------------------------------------

/**
 * Returns `true` when `key` is one of the logical operator strings
 * (`'AND'`, `'OR'`, `'NOT'`).
 */
export function isLogicalOperator(key: string): key is LogicalOperator {
  return (LOGICAL_OPERATORS as ReadonlyArray<string>).includes(key);
}

/**
 * Returns `true` when `key` is one of the recognised scalar operator strings
 * (e.g. `'equals'`, `'contains'`, `'gt'`).
 */
export function isScalarOperator(key: string): key is ScalarOperator {
  return (SCALAR_OPERATORS as ReadonlyArray<string>).includes(key);
}
