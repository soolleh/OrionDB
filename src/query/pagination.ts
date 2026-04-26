import { QueryError } from "../errors/index.js";
import type { RawRecord } from "../persistence/index.js";
import type { OrderByInput } from "./types.js";

/**
 * Validates skip and take arguments, throwing QueryError for invalid values.
 */
const validatePaginationArgs = (skip: number | undefined, take: number | undefined): void => {
  if (skip !== undefined) {
    if (skip < 0) {
      throw new QueryError("skip must be a non-negative integer.", {
        meta: { skip, reason: "skip must be a non-negative integer" },
      });
    }
    if (!Number.isInteger(skip)) {
      throw new QueryError("skip must be an integer.", {
        meta: { skip, reason: "skip must be an integer" },
      });
    }
  }
  if (take !== undefined) {
    if (take < 0) {
      throw new QueryError("take must be a non-negative integer.", {
        meta: { take, reason: "take must be a non-negative integer" },
      });
    }
    if (!Number.isInteger(take)) {
      throw new QueryError("take must be an integer.", {
        meta: { take, reason: "take must be an integer" },
      });
    }
  }
};

/**
 * Returns true when orderBy is absent or an empty array.
 */
const isOrderByAbsent = (orderBy: OrderByInput | undefined): boolean => {
  if (orderBy === undefined) return true;
  if (Array.isArray(orderBy) && orderBy.length === 0) return true;
  return false;
};

/**
 * Applies skip and take to an already-filtered, already-sorted array of records.
 * Returns the original array reference when both skip and take are undefined.
 */
export const applyPagination = (
  records: RawRecord[],
  skip: number | undefined,
  take: number | undefined,
): RawRecord[] => {
  validatePaginationArgs(skip, take);
  if (skip === undefined && take === undefined) return records;
  const effectiveSkip = skip ?? 0;
  if (take === undefined) return records.slice(effectiveSkip);
  return records.slice(effectiveSkip, effectiveSkip + take);
};

/**
 * Determines whether skip/take should be pushed to the scan engine (early-exit
 * optimization) or deferred to post-sort pagination.
 */
export const buildPaginationStrategy = (
  orderBy: OrderByInput | undefined,
  skip: number | undefined,
  take: number | undefined,
): {
  scanSkip: number | undefined;
  scanTake: number | undefined;
  postSkip: number | undefined;
  postTake: number | undefined;
} => {
  if (isOrderByAbsent(orderBy)) {
    return { scanSkip: skip, scanTake: take, postSkip: undefined, postTake: undefined };
  }
  return { scanSkip: undefined, scanTake: undefined, postSkip: skip, postTake: take };
};

/**
 * Computes page metadata given the total matched count and pagination args.
 * Pure function — no validation, no I/O.
 */
export const getPageInfo = (
  totalMatchedCount: number,
  skip: number | undefined,
  take: number | undefined,
): {
  totalCount: number;
  returnedCount: number;
  hasNextPage: boolean;
  hasPreviousPage: boolean;
} => {
  const effectiveSkip = skip ?? 0;
  const effectiveTake = take ?? totalMatchedCount;
  const returnedCount = Math.max(0, Math.min(effectiveTake, totalMatchedCount - effectiveSkip));
  const hasNextPage = take !== undefined && effectiveSkip + effectiveTake < totalMatchedCount;
  const hasPreviousPage = effectiveSkip > 0;
  return { totalCount: totalMatchedCount, returnedCount, hasNextPage, hasPreviousPage };
};
