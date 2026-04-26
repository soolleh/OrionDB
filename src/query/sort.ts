import { QueryError } from "../errors/index.js";
import type { RawRecord } from "../persistence/index.js";
import type { CompiledSort, OrderByDirection, OrderByInput } from "./types.js";

const normalizeToMs = (value: unknown): number => {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "string") return new Date(value).getTime();
  return NaN;
};

const compareValues = (a: unknown, b: unknown, direction: OrderByDirection): number => {
  const aIsNil = a === null || a === undefined;
  const bIsNil = b === null || b === undefined;
  if (aIsNil && bIsNil) return 0;
  if (aIsNil) return 1;
  if (bIsNil) return -1;
  if (typeof a === "number" && typeof b === "number") return direction === "asc" ? a - b : b - a;
  if (typeof a === "string" && typeof b === "string")
    return direction === "asc" ? a.localeCompare(b) : b.localeCompare(a);
  if (typeof a === "boolean" && typeof b === "boolean") {
    const na = a ? 1 : 0;
    const nb = b ? 1 : 0;
    return direction === "asc" ? na - nb : nb - na;
  }
  const msA = normalizeToMs(a);
  const msB = normalizeToMs(b);
  if (!isNaN(msA) && !isNaN(msB)) return direction === "asc" ? msA - msB : msB - msA;
  return 0;
};

const normalizeOrderBy = (orderBy: OrderByInput): Array<{ field: string; direction: OrderByDirection }> => {
  const entries = Array.isArray(orderBy) ? orderBy : [orderBy];
  return entries.flatMap((entry) => Object.entries(entry).map(([field, direction]) => ({ field, direction })));
};

const compileSingleFieldSort =
  (field: string, direction: OrderByDirection): CompiledSort =>
  (a, b) =>
    compareValues(a[field], b[field], direction);

export const compileSort = (orderBy: OrderByInput | undefined): CompiledSort | undefined => {
  if (orderBy === undefined) return undefined;
  if (Array.isArray(orderBy) && orderBy.length === 0) return undefined;
  const pairs = normalizeOrderBy(orderBy);
  if (pairs.length === 0) return undefined;
  for (const { field, direction } of pairs) {
    if (direction !== "asc" && direction !== "desc") {
      throw new QueryError(`Invalid orderBy direction '${String(direction)}' for field '${field}'.`, {
        meta: { field, direction, reason: "orderBy direction must be asc or desc" },
      });
    }
  }
  const comparators = pairs.map(({ field, direction }) => compileSingleFieldSort(field, direction));
  return (a, b) => {
    for (const comparator of comparators) {
      const result = comparator(a, b);
      if (result !== 0) return result;
    }
    return 0;
  };
};

export const applySort = (records: RawRecord[], compiledSort: CompiledSort | undefined): RawRecord[] => {
  if (compiledSort === undefined) return records;
  return [...records].sort(compiledSort);
};
