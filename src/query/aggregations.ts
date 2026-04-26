import { CompactionError } from "../errors/index.js";
import { QueryError } from "../errors/index.js";
import { findMany } from "../persistence/index.js";
import type { ModelReaderContext, RawRecord } from "../persistence/index.js";
import { applyPagination } from "./pagination.js";
import { applySort, compileSort } from "./sort.js";
import type {
  AggregateInput,
  AggregateResult,
  CompiledFilter,
  GroupByInput,
  GroupByResult,
  WhereInput,
} from "./types.js";

/** Extracts all finite numeric values for a field across records. */
const extractNumericValues = (records: RawRecord[], field: string): number[] =>
  records.reduce<number[]>((acc, record) => {
    const val = record[field];
    if (typeof val === "number" && isFinite(val)) acc.push(val);
    return acc;
  }, []);

/** Counts records where the field is present and not null/undefined. */
const computeFieldCount = (records: RawRecord[], field: string): number =>
  records.filter((r) => r[field] !== null && r[field] !== undefined).length;

/** Computes all requested aggregations over a filtered record set. */
const computeAggregations = (
  records: RawRecord[],
  input: Pick<AggregateInput, "_count" | "_avg" | "_sum" | "_min" | "_max">,
): AggregateResult => {
  const result: AggregateResult = {};

  if (input._count !== undefined) {
    if (input._count === true) {
      result._count = records.length;
    } else {
      const countObj: { [field: string]: number } = {};
      for (const [field, enabled] of Object.entries(input._count)) {
        if (enabled) countObj[field] = computeFieldCount(records, field);
      }
      result._count = countObj;
    }
  }

  if (input._avg !== undefined) {
    const avgObj: { [field: string]: number | null } = {};
    for (const [field, enabled] of Object.entries(input._avg)) {
      if (enabled) {
        const values = extractNumericValues(records, field);
        avgObj[field] = values.length === 0 ? null : values.reduce((sum, v) => sum + v, 0) / values.length;
      }
    }
    result._avg = avgObj;
  }

  if (input._sum !== undefined) {
    const sumObj: { [field: string]: number | null } = {};
    for (const [field, enabled] of Object.entries(input._sum)) {
      if (enabled) {
        const values = extractNumericValues(records, field);
        sumObj[field] = values.length === 0 ? null : values.reduce((sum, v) => sum + v, 0);
      }
    }
    result._sum = sumObj;
  }

  if (input._min !== undefined) {
    const minObj: { [field: string]: number | string | null } = {};
    for (const [field, enabled] of Object.entries(input._min)) {
      if (enabled) {
        const numericValues = extractNumericValues(records, field);
        if (numericValues.length > 0) {
          minObj[field] = Math.min(...numericValues);
        } else {
          const stringValues = records.map((r) => r[field]).filter((v): v is string => typeof v === "string");
          minObj[field] = stringValues.length > 0 ? stringValues.reduce((min, v) => (v < min ? v : min)) : null;
        }
      }
    }
    result._min = minObj;
  }

  if (input._max !== undefined) {
    const maxObj: { [field: string]: number | string | null } = {};
    for (const [field, enabled] of Object.entries(input._max)) {
      if (enabled) {
        const numericValues = extractNumericValues(records, field);
        if (numericValues.length > 0) {
          maxObj[field] = Math.max(...numericValues);
        } else {
          const stringValues = records.map((r) => r[field]).filter((v): v is string => typeof v === "string");
          maxObj[field] = stringValues.length > 0 ? stringValues.reduce((max, v) => (v > max ? v : max)) : null;
        }
      }
    }
    result._max = maxObj;
  }

  return result;
};

/** Builds a deterministic composite group key from a record's `by` field values. */
const buildGroupKey = (record: RawRecord, byFields: string[]): string =>
  byFields.map((field) => JSON.stringify(record[field] ?? null)).join("::");

/**
 * Returns the number of records matching the `where` clause.
 */
export const count = async (
  ctx: ModelReaderContext,
  input: { where?: WhereInput },
  compiledFilter?: CompiledFilter,
): Promise<number> => {
  try {
    const filter = compiledFilter ?? (() => true);
    const records = await findMany(ctx, { where: input.where }, filter);
    return records.length;
  } catch (error) {
    if (error instanceof QueryError || error instanceof CompactionError) throw error;
    throw new CompactionError("Unexpected error during count.", { meta: { cause: error } });
  }
};

/**
 * Computes aggregations (`_count`, `_avg`, `_sum`, `_min`, `_max`) over
 * records matching the `where` clause.
 */
export const aggregate = async (
  ctx: ModelReaderContext,
  input: AggregateInput,
  compiledFilter?: CompiledFilter,
): Promise<AggregateResult> => {
  try {
    const filter = compiledFilter ?? (() => true);
    const records = await findMany(ctx, { where: input.where }, filter);
    return computeAggregations(records, {
      _count: input._count,
      _avg: input._avg,
      _sum: input._sum,
      _min: input._min,
      _max: input._max,
    });
  } catch (error) {
    if (error instanceof QueryError || error instanceof CompactionError) throw error;
    throw new CompactionError("Unexpected error during aggregate.", { meta: { cause: error } });
  }
};

/**
 * Groups records by one or more fields and optionally applies aggregations
 * and pagination per group.
 */
export const groupBy = async (
  ctx: ModelReaderContext,
  input: GroupByInput,
  compiledFilter?: CompiledFilter,
): Promise<GroupByResult[]> => {
  if (!Array.isArray(input.by) || input.by.length === 0) {
    throw new QueryError("groupBy requires at least one field in by array.", {
      meta: { reason: "groupBy requires at least one field in by array" },
    });
  }

  try {
    const filter = compiledFilter ?? (() => true);
    const records = await findMany(ctx, { where: input.where }, filter);

    const groups = new Map<string, RawRecord[]>();
    for (const record of records) {
      const key = buildGroupKey(record, input.by);
      const group = groups.get(key) ?? [];
      group.push(record);
      groups.set(key, group);
    }

    const results: GroupByResult[] = [];
    for (const [, groupRecords] of groups) {
      const groupResult: GroupByResult = {};
      for (const field of input.by) {
        groupResult[field] = groupRecords[0]?.[field] ?? null;
      }
      const agg = computeAggregations(groupRecords, {
        _count: input._count,
        _avg: input._avg,
        _sum: input._sum,
        _min: input._min,
        _max: input._max,
      });
      if (agg._count !== undefined) groupResult._count = agg._count;
      if (agg._avg !== undefined) groupResult._avg = agg._avg;
      if (agg._sum !== undefined) groupResult._sum = agg._sum;
      if (agg._min !== undefined) groupResult._min = agg._min;
      if (agg._max !== undefined) groupResult._max = agg._max;
      results.push(groupResult);
    }

    if (input.orderBy !== undefined) {
      const compiledSort = compileSort(input.orderBy);
      const sorted = applySort(results as RawRecord[], compiledSort);
      return applyPagination(sorted, input.skip, input.take) as GroupByResult[];
    }

    return applyPagination(results as RawRecord[], input.skip, input.take) as GroupByResult[];
  } catch (error) {
    if (error instanceof QueryError || error instanceof CompactionError) throw error;
    throw new CompactionError("Unexpected error during groupBy.", { meta: { cause: error } });
  }
};
