// src/client/model-client.ts
//
// ModelClient — per-model CRUD and query orchestration.
// Implemented in prompts 8.2 (reads), 8.3 (writes),
// 8.4 (relations + nested writes).

import {
  findUnique as persistenceFindUnique,
  findUniqueOrThrow as persistenceFindUniqueOrThrow,
  findFirst as persistenceFindFirst,
  findMany as persistenceFindMany,
} from "../persistence/index.js";
import type { ModelReaderContext, FindManyArgs } from "../persistence/index.js";
import {
  compileFilter,
  compileSort,
  applySort,
  applyPagination,
  buildPaginationStrategy,
  count as queryCount,
  aggregate as queryAggregate,
  groupBy as queryGroupBy,
} from "../query/index.js";
import type { SelectInput, AggregateResult, GroupByResult } from "../query/index.js";
import { QueryError } from "../errors/index.js";
import type {
  ModelClientConfig,
  ModelClientMethods,
  FindUniqueClientInput,
  FindFirstClientInput,
  FindManyClientInput,
  CountInput,
  AggregateClientInput,
  GroupByClientInput,
} from "./types.js";

// ---------------------------------------------------------------------------
// assertConnected
// ---------------------------------------------------------------------------

/**
 * Throws a `QueryError` if the connection getter returns `false`.
 * Accepts a live getter \u2014 never a snapshot \u2014 so it always reflects the
 * current `ClientContext.isConnected` value at call time.
 *
 * In practice, the Proxy in `oriondb.ts` already blocks model property
 * access before `$connect()` resolves. This guard is a defence-in-depth
 * safety net for paths that bypass the Proxy (e.g. tests, direct internal
 * calls).
 */
export function assertConnected(isConnected: () => boolean, modelName: string, method: string): void {
  if (!isConnected()) {
    throw new QueryError(`Cannot call ${method}() on '${modelName}' \u2014 call await db.$connect() first.`, {
      model: modelName,
      meta: { model: modelName, method, reason: "not connected" },
    });
  }
}

// ---------------------------------------------------------------------------
// createModelClient
// ---------------------------------------------------------------------------

/**
 * Creates a ModelClient instance for a single model.
 * All methods are tracked through `config.operationTracker` so that
 * `$disconnect()` can drain them before clearing state.
 */
export const createModelClient = (config: ModelClientConfig): ModelClientMethods => {
  const { isConnectedGetter, modelName, operationTracker } = config;

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Builds the persistence reader context from this client's config. */
  const buildReaderCtx = (): ModelReaderContext => ({
    modelName: config.modelName,
    paths: config.paths,
    schema: config.schema,
    indexManager: config.indexManager,
  });

  /**
   * Projects a single record down to the fields listed in `select`.
   * Returns the original record reference when `select` is undefined.
   */
  const applySelect = (record: Record<string, unknown>, select: SelectInput | undefined): Record<string, unknown> => {
    if (!select) return record;
    const result: Record<string, unknown> = {};
    for (const [key, enabled] of Object.entries(select)) {
      if (enabled) result[key] = record[key];
    }
    return result;
  };

  /**
   * Projects an array of records down to the fields listed in `select`.
   * Returns the original array reference when `select` is undefined.
   */
  const applySelectMany = (
    records: Record<string, unknown>[],
    select: SelectInput | undefined,
  ): Record<string, unknown>[] => {
    if (!select) return records;
    return records.map((r) => applySelect(r, select));
  };

  /**
   * Translates `FindManyClientInput` into the `FindManyArgs` shape expected by
   * the persistence layer, routing scan-level vs post-sort pagination according
   * to the supplied strategy.
   */
  const buildFindManyArgs = (
    input: FindManyClientInput,
    strategy: ReturnType<typeof buildPaginationStrategy>,
  ): FindManyArgs => ({
    where: input.where,
    skip: strategy.scanSkip,
    take: strategy.scanTake,
  });

  // ---------------------------------------------------------------------------
  // Read methods
  // ---------------------------------------------------------------------------

  /**
   * Returns the unique record matching `where`, or `null` if not found.
   * `where` must target a primary key or unique field.
   */
  const findUnique = (input: FindUniqueClientInput): Promise<Record<string, unknown> | null> => {
    assertConnected(isConnectedGetter, modelName, "findUnique");
    return operationTracker.track(
      (async () => {
        const readerCtx = buildReaderCtx();
        const record = await persistenceFindUnique(readerCtx, {
          where: input.where,
          select: input.select,
        });
        if (record === null) return null;
        // TODO: include — wired in prompt 8.4
        return applySelect(record, input.select);
      })(),
    );
  };

  /**
   * Returns the unique record matching `where`.
   * Throws `RecordNotFoundError` when not found.
   */
  const findUniqueOrThrow = (input: FindUniqueClientInput): Promise<Record<string, unknown>> => {
    assertConnected(isConnectedGetter, modelName, "findUniqueOrThrow");
    return operationTracker.track(
      (async () => {
        const readerCtx = buildReaderCtx();
        const record = await persistenceFindUniqueOrThrow(readerCtx, {
          where: input.where,
          select: input.select,
        });
        // TODO: include — wired in prompt 8.4
        return applySelect(record, input.select);
      })(),
    );
  };

  /**
   * Returns the first record matching `where`, or `null` if none match.
   *
   * When `orderBy` is present the full result set must be scanned and sorted
   * before the first element can be determined. When absent, the persistence
   * layer uses an early-exit scan (take: 1).
   */
  const findFirst = (input: FindFirstClientInput): Promise<Record<string, unknown> | null> => {
    assertConnected(isConnectedGetter, modelName, "findFirst");
    return operationTracker.track(
      (async () => {
        const readerCtx = buildReaderCtx();

        const compiledFilter = input.where ? compileFilter(input.where) : undefined;
        const compiledSort = input.orderBy ? compileSort(input.orderBy) : undefined;

        if (compiledSort) {
          // orderBy present — must full-scan, sort, then take first
          const records = await persistenceFindMany(readerCtx, { where: input.where }, compiledFilter);
          const sorted = applySort(records, compiledSort);
          const first = sorted[0] ?? null;
          if (first === null) return null;
          // TODO: include — wired in prompt 8.4
          return applySelect(first, input.select);
        }

        // No orderBy — delegate to persistence early-exit (take: 1)
        const record = await persistenceFindFirst(
          readerCtx,
          { where: input.where, select: input.select },
          compiledFilter,
        );
        if (record === null) return null;
        // TODO: include — wired in prompt 8.4
        return applySelect(record, input.select);
      })(),
    );
  };

  /**
   * Returns all records matching the provided filters, sorted and paginated
   * according to `orderBy`, `skip`, and `take`.
   *
   * Two-phase pagination strategy:
   * - No `orderBy`: scan-level skip/take (early exit in persistence layer).
   * - With `orderBy`: full scan → sort → post-sort skip/take.
   */
  const findMany = (input: FindManyClientInput): Promise<Record<string, unknown>[]> => {
    assertConnected(isConnectedGetter, modelName, "findMany");
    return operationTracker.track(
      (async () => {
        const readerCtx = buildReaderCtx();

        const compiledFilter = input.where ? compileFilter(input.where) : undefined;
        const compiledSort = input.orderBy ? compileSort(input.orderBy) : undefined;

        const strategy = buildPaginationStrategy(input.orderBy, input.skip, input.take);

        const records = await persistenceFindMany(readerCtx, buildFindManyArgs(input, strategy), compiledFilter);

        // applySort is a no-op when compiledSort is undefined
        const sorted = applySort(records, compiledSort);

        // applyPagination is a no-op when both postSkip and postTake are undefined
        const paginated = applyPagination(sorted, strategy.postSkip, strategy.postTake);

        // TODO: include — wired in prompt 8.4

        return applySelectMany(paginated, input.select);
      })(),
    );
  };

  /**
   * Returns the number of records matching `where`.
   * When called with no arguments, returns total non-deleted record count.
   */
  const count = (input?: CountInput): Promise<number> => {
    assertConnected(isConnectedGetter, modelName, "count");
    return operationTracker.track(
      (async () => {
        const readerCtx = buildReaderCtx();
        const compiledFilter = input?.where ? compileFilter(input.where) : undefined;
        return queryCount(readerCtx, { where: input?.where }, compiledFilter);
      })(),
    );
  };

  /**
   * Computes aggregations (`_count`, `_avg`, `_sum`, `_min`, `_max`) over
   * records matching `where`.
   */
  const aggregate = (input: AggregateClientInput): Promise<AggregateResult> => {
    assertConnected(isConnectedGetter, modelName, "aggregate");
    return operationTracker.track(
      (async () => {
        const readerCtx = buildReaderCtx();
        const compiledFilter = input.where ? compileFilter(input.where) : undefined;
        return queryAggregate(
          readerCtx,
          {
            where: input.where,
            _count: input._count,
            _avg: input._avg,
            _sum: input._sum,
            _min: input._min,
            _max: input._max,
          },
          compiledFilter,
        );
      })(),
    );
  };

  /**
   * Groups records by one or more fields and computes per-group aggregations.
   */
  const groupBy = (input: GroupByClientInput): Promise<GroupByResult[]> => {
    assertConnected(isConnectedGetter, modelName, "groupBy");
    return operationTracker.track(
      (async () => {
        const readerCtx = buildReaderCtx();
        const compiledFilter = input.where ? compileFilter(input.where) : undefined;
        return queryGroupBy(
          readerCtx,
          {
            by: input.by,
            where: input.where,
            _count: input._count,
            _avg: input._avg,
            _sum: input._sum,
            _min: input._min,
            _max: input._max,
            orderBy: input.orderBy,
            skip: input.skip,
            take: input.take,
          },
          compiledFilter,
        );
      })(),
    );
  };

  // ---------------------------------------------------------------------------
  // Write method stubs — implemented in prompt 8.3
  // ---------------------------------------------------------------------------

  const notImplemented = (): never => {
    throw new Error("Write methods implemented in prompt 8.3");
  };

  return {
    findUnique,
    findUniqueOrThrow,
    findFirst,
    findMany,
    count,
    aggregate,
    groupBy,
    create: notImplemented,
    createMany: notImplemented,
    update: notImplemented,
    updateMany: notImplemented,
    delete: notImplemented,
    deleteMany: notImplemented,
    upsert: notImplemented,
  };
};
