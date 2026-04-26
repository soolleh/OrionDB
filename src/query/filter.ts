/**
 * Filter compiler — where clause evaluation and operator matching.
 *
 * Design:
 * - Structural errors (unknown operators, invalid clause shapes) throw
 *   `QueryError` at compile time (when `compileFilter` is called).
 * - Type mismatches between operators and record values return `false` at
 *   evaluation time rather than throwing — a record that cannot satisfy a
 *   condition is treated as not matching, avoiding mid-scan crashes.
 */

import { QueryError } from "../errors/index.js";
import type { CompiledFilter, WhereInput } from "./types.js";

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/**
 * Returns `true` if `value` is a non-null, non-array plain object.
 * Implemented locally — not imported from another module.
 */
const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * Returns a numeric timestamp for comparison.
 * - `Date` instance  → `value.getTime()`
 * - ISO string       → `new Date(value).getTime()`
 * - Anything else    → `NaN`
 *
 * Called once at compile time so each comparison operator pays the
 * conversion cost only once, not once per evaluated record.
 */
const normalizeDate = (value: unknown): number => {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "string") return new Date(value).getTime();
  return NaN;
};

// ---------------------------------------------------------------------------
// Scalar operator compiler
// ---------------------------------------------------------------------------

/**
 * Produces a `CompiledFilter` for a single operator applied to a single field.
 * Throws `QueryError` for unknown operators or invalid clause shapes (compile time).
 * Returns `false` at evaluation time for type mismatches rather than throwing.
 */
const compileOperator = (field: string, operator: string, value: unknown): CompiledFilter => {
  switch (operator) {
    case "equals": {
      // Normalise Date values to ISO strings at compile time.
      const expected = value instanceof Date ? value.toISOString() : value;
      return (record) => record[field] === expected;
    }

    case "not": {
      // Recurse when value is a nested filter object; otherwise negate equality.
      if (isPlainObject(value)) {
        const inner = compileFieldFilter(field, value);
        return (record) => !inner(record);
      }
      const expected = value instanceof Date ? value.toISOString() : value;
      return (record) => record[field] !== expected;
    }

    case "in": {
      if (!Array.isArray(value)) {
        throw new QueryError(`'in' operator requires an array value for field '${field}'.`, {
          field,
          meta: { field, operator, reason: "'in' operator requires an array value" },
        });
      }
      const set = new Set(value);
      return (record) => set.has(record[field]);
    }

    case "notIn": {
      if (!Array.isArray(value)) {
        throw new QueryError(`'notIn' operator requires an array value for field '${field}'.`, {
          field,
          meta: { field, operator, reason: "'notIn' operator requires an array value" },
        });
      }
      const set = new Set(value);
      return (record) => !set.has(record[field]);
    }

    case "contains": {
      if (typeof value !== "string") {
        throw new QueryError(`'contains' operator requires a string value for field '${field}'.`, {
          field,
          meta: { field, operator, reason: "'contains' operator requires a string value" },
        });
      }
      return (record) => {
        const actual = record[field];
        return typeof actual === "string" && actual.includes(value);
      };
    }

    case "startsWith": {
      if (typeof value !== "string") {
        throw new QueryError(`'startsWith' operator requires a string value for field '${field}'.`, {
          field,
          meta: { field, operator, reason: "'startsWith' operator requires a string value" },
        });
      }
      return (record) => {
        const actual = record[field];
        return typeof actual === "string" && actual.startsWith(value);
      };
    }

    case "endsWith": {
      if (typeof value !== "string") {
        throw new QueryError(`'endsWith' operator requires a string value for field '${field}'.`, {
          field,
          meta: { field, operator, reason: "'endsWith' operator requires a string value" },
        });
      }
      return (record) => {
        const actual = record[field];
        return typeof actual === "string" && actual.endsWith(value);
      };
    }

    case "lt":
    case "lte":
    case "gt":
    case "gte": {
      // Determine at compile time whether we're doing numeric or date comparison.
      if (typeof value === "number") {
        const op = operator;
        return (record) => {
          const actual = record[field];
          if (typeof actual !== "number") return false;
          if (op === "lt") return actual < value;
          if (op === "lte") return actual <= value;
          if (op === "gt") return actual > value;
          return actual >= value;
        };
      }
      // Date comparison — normalise value at compile time.
      const compiledMs = normalizeDate(value);
      if (isNaN(compiledMs)) {
        throw new QueryError(`'${operator}' operator requires a number or date value for field '${field}'.`, {
          field,
          meta: {
            field,
            operator,
            value,
            reason: "comparison operators require number or date values",
          },
        });
      }
      const op = operator;
      return (record) => {
        const actualMs = normalizeDate(record[field]);
        if (isNaN(actualMs)) return false;
        if (op === "lt") return actualMs < compiledMs;
        if (op === "lte") return actualMs <= compiledMs;
        if (op === "gt") return actualMs > compiledMs;
        return actualMs >= compiledMs;
      };
    }

    default:
      throw new QueryError(`Unknown operator: ${operator}`, {
        meta: { field, operator, reason: "unknown scalar operator" },
      });
  }
};

// ---------------------------------------------------------------------------
// Field-level filter compiler
// ---------------------------------------------------------------------------

/**
 * Compiles a condition for a single field.
 *
 * - `null` condition  → equals null check
 * - Primitive value   → equals check
 * - Plain object      → compile each key as a scalar operator, combine with AND
 * - Anything else     → throw `QueryError`
 */
const compileFieldFilter = (field: string, condition: unknown): CompiledFilter => {
  if (condition === null) {
    return (record) => record[field] === null;
  }

  if (typeof condition === "string" || typeof condition === "number" || typeof condition === "boolean") {
    return (record) => record[field] === condition;
  }

  if (isPlainObject(condition)) {
    const operatorFilters = Object.entries(condition).map(([op, val]) => compileOperator(field, op, val));
    // Short-circuit AND: every operator sub-filter must pass.
    return (record) => operatorFilters.every((f) => f(record));
  }

  throw new QueryError(`Invalid condition for field '${field}'.`, {
    field,
    meta: { field, condition, reason: "field condition must be a scalar, null, or a filter object" },
  });
};

// ---------------------------------------------------------------------------
// Logical operator compilers
// ---------------------------------------------------------------------------

/**
 * Compiles an `AND` clause.
 * Accepts a single `WhereInput` or an array of `WhereInput`.
 * Combines sub-filters with `every` (short-circuit AND).
 */
const compileAnd = (value: unknown): CompiledFilter => {
  if (Array.isArray(value)) {
    const filters = value.map((v) => compileFilter(v as WhereInput));
    return (record) => filters.every((f) => f(record));
  }
  if (isPlainObject(value)) {
    return compileFilter(value as WhereInput);
  }
  throw new QueryError("Invalid AND clause.", {
    meta: { reason: "AND value must be a WhereInput or WhereInput[]" },
  });
};

/**
 * Compiles an `OR` clause.
 * Accepts an array of `WhereInput` only.
 * An empty array matches nothing (`() => false`).
 * Combines sub-filters with `some` (short-circuit OR).
 */
const compileOr = (value: unknown): CompiledFilter => {
  if (!Array.isArray(value)) {
    throw new QueryError("Invalid OR clause.", {
      meta: { reason: "OR value must be a WhereInput[]" },
    });
  }
  if (value.length === 0) return () => false;
  const filters = value.map((v) => compileFilter(v as WhereInput));
  return (record) => filters.some((f) => f(record));
};

/**
 * Compiles a `NOT` clause.
 * Accepts a single `WhereInput` or an array of `WhereInput`.
 * Array form: compile each element with AND, then negate.
 */
const compileNot = (value: unknown): CompiledFilter => {
  if (Array.isArray(value)) {
    const filters = value.map((v) => compileFilter(v as WhereInput));
    const combined: CompiledFilter = (record) => filters.every((f) => f(record));
    return (record) => !combined(record);
  }
  if (isPlainObject(value)) {
    const inner = compileFilter(value as WhereInput);
    return (record) => !inner(record);
  }
  throw new QueryError("Invalid NOT clause.", {
    meta: { reason: "NOT value must be a WhereInput or WhereInput[]" },
  });
};

// ---------------------------------------------------------------------------
// Key dispatcher
// ---------------------------------------------------------------------------

/**
 * Dispatches a single `WhereInput` key to the appropriate compiler.
 * Logical operator keys delegate to `compileAnd` / `compileOr` / `compileNot`.
 * All other keys are treated as field names and compiled with `compileFieldFilter`.
 */
const compileKey = (key: string, value: unknown): CompiledFilter => {
  if (key === "AND") return compileAnd(value);
  if (key === "OR") return compileOr(value);
  if (key === "NOT") return compileNot(value);
  return compileFieldFilter(key, value);
};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/**
 * Compiles a `WhereInput` into a `CompiledFilter` predicate function.
 *
 * - `undefined` where or empty object → passthrough `() => true`
 * - Top-level keys are combined with implicit AND (every key must match)
 * - Logical operators (`AND`, `OR`, `NOT`) recurse into sub-clauses
 * - Field-level operators are compiled at call time — evaluation is O(1)
 *   per record
 *
 * Structural errors (unknown operators, invalid clause shapes) throw
 * `QueryError` at compile time. Type mismatches between operators and
 * record values return `false` at evaluation time rather than throwing —
 * a record that cannot satisfy a condition is treated as not matching.
 */
export const compileFilter = (where: WhereInput | undefined): CompiledFilter => {
  if (where === undefined) return () => true;

  const entries = Object.entries(where);
  if (entries.length === 0) return () => true;

  const filters = entries.map(([key, value]) => compileKey(key, value));
  // Short-circuit AND: all top-level conditions must pass.
  return (record) => filters.every((f) => f(record));
};
