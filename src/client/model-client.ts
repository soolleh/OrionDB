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
  create as persistenceCreate,
  createMany as persistenceCreateMany,
  update as persistenceUpdate,
  updateMany as persistenceUpdateMany,
  deleteRecord as persistenceDelete,
  deleteMany as persistenceDeleteMany,
} from "../persistence/index.js";
import type { ModelReaderContext, ModelWriterContext, FindManyArgs } from "../persistence/index.js";
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
import { QueryError, ValidationError } from "../errors/index.js";
import type {
  ModelClientConfig,
  ModelClientMethods,
  FindUniqueClientInput,
  FindFirstClientInput,
  FindManyClientInput,
  CountInput,
  AggregateClientInput,
  GroupByClientInput,
  CreateInput,
  CreateManyInput,
  UpdateInput,
  UpdateManyInput,
  DeleteInput,
  DeleteManyInput,
  UpsertInput,
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

  /** Builds the persistence writer context from this client's config. */
  const buildWriterCtx = (): ModelWriterContext => ({
    modelName: config.modelName,
    paths: config.paths,
    schema: config.schema,
    indexManager: config.indexManager,
    counter: config.counter,
    autoCompactThreshold: config.autoCompactThreshold,
  });

  /**
   * Removes relation fields from a data object before passing to the
   * persistence layer. Persistence has no knowledge of relations and
   * would reject them as unknown fields.
   *
   * Phase 1 stub — replaced by `extractNestedWrites` in prompt 8.4.
   */
  const stripRelationFields = (data: Record<string, unknown>): Record<string, unknown> => {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(data)) {
      if (!config.schema.relationFields.has(key)) {
        result[key] = value;
      }
    }
    return result;
  };

  /**
   * When `config.strict` is `true`, throws `ValidationError` for any field
   * in `data` that is not declared in the schema (scalar or relation).
   * Relation fields are explicitly permitted — they will be processed as
   * nested writes in prompt 8.4.
   * No-op when `config.strict` is falsy.
   */
  const assertStrictMode = (data: Record<string, unknown>, operation: "create" | "update"): void => {
    if (!config.strict) return;
    for (const key of Object.keys(data)) {
      if (!config.schema.fields.has(key) && !config.schema.relationFields.has(key)) {
        throw new ValidationError(`Unknown field '${key}' on model '${config.modelName}' in strict mode`, {
          model: config.modelName,
          field: key,
          meta: { operation, reason: "strict mode — unknown field" },
        });
      }
    }
  };

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
  // Write methods
  // ---------------------------------------------------------------------------

  /**
   * Creates a single record. Validates fields, applies defaults, checks unique
   * constraints, writes to disk, updates indexes, and returns the new record.
   */
  const create = (input: CreateInput): Promise<Record<string, unknown>> => {
    assertConnected(isConnectedGetter, modelName, "create");
    return operationTracker.track(
      (async () => {
        // Strict mode before strip — relation fields are valid in strict mode
        assertStrictMode(input.data, "create");

        // TODO: nested writes — wired in prompt 8.4
        const cleanData = stripRelationFields(input.data);

        const writerCtx = buildWriterCtx();
        const record = await persistenceCreate(writerCtx, { data: cleanData });

        // TODO: auto-compact trigger — wired in prompt 13.5
        config.logger?.debug(`[${modelName}] create complete`);

        // TODO: include — wired in prompt 8.4

        return applySelect(record, input.select);
      })(),
    );
  };

  /**
   * Creates multiple records. Validates all records before writing any
   * (fail-fast, all-or-nothing validation). Returns `{ count: N }`.
   *
   * Note: `skipDuplicates` is a Phase 2 feature and is silently ignored
   * in this implementation.
   */
  const createMany = (input: CreateManyInput): Promise<{ count: number }> => {
    assertConnected(isConnectedGetter, modelName, "createMany");
    return operationTracker.track(
      (async () => {
        const writerCtx = buildWriterCtx();

        // Strict mode on all records first — validate before any writes
        for (const record of input.data) {
          assertStrictMode(record, "create");
        }

        // TODO: nested writes — wired in prompt 8.4
        const cleanRecords = input.data.map(stripRelationFields);

        const result = await persistenceCreateMany(writerCtx, { data: cleanRecords });

        // TODO: auto-compact trigger — wired in prompt 13.5
        config.logger?.debug(`[${modelName}] createMany complete`, { count: result.count });

        return result;
      })(),
    );
  };

  /**
   * Updates the single record identified by `where`. Throws `RecordNotFoundError`
   * when no matching record exists. Returns the updated record.
   */
  const update = (input: UpdateInput): Promise<Record<string, unknown>> => {
    assertConnected(isConnectedGetter, modelName, "update");
    return operationTracker.track(
      (async () => {
        // Strict mode on raw data before strip
        assertStrictMode(input.data, "update");

        // TODO: nested writes — wired in prompt 8.4
        const cleanData = stripRelationFields(input.data);

        const writerCtx = buildWriterCtx();
        // persistenceUpdate handles RecordNotFoundError internally with
        // model + meta.where context already set correctly
        const record = await persistenceUpdate(writerCtx, {
          where: input.where,
          data: cleanData,
        });

        // TODO: auto-compact trigger — wired in prompt 13.5
        config.logger?.debug(`[${modelName}] update complete`);

        // TODO: include — wired in prompt 8.4

        return applySelect(record, input.select);
      })(),
    );
  };

  /**
   * Updates all records matching `where`. When `where` is omitted, all records
   * are updated. Returns `{ count: N }` where N is the number of records updated.
   */
  const updateMany = (input: UpdateManyInput): Promise<{ count: number }> => {
    assertConnected(isConnectedGetter, modelName, "updateMany");
    return operationTracker.track(
      (async () => {
        assertStrictMode(input.data, "update");

        // TODO: nested writes — wired in prompt 8.4
        const cleanData = stripRelationFields(input.data);

        const writerCtx = buildWriterCtx();
        const compiledFilter = input.where ? compileFilter(input.where) : undefined;

        const result = await persistenceUpdateMany(writerCtx, { where: input.where, data: cleanData }, compiledFilter);

        // TODO: auto-compact trigger — wired in prompt 13.5
        config.logger?.debug(`[${modelName}] updateMany complete`, { count: result.count });

        return result;
      })(),
    );
  };

  /**
   * Deletes the single record identified by `where`.
   * Throws `RecordNotFoundError` when `where` matches nothing.
   * Returns the pre-deletion record state with system fields stripped.
   */
  const deleteRecord = (input: DeleteInput): Promise<Record<string, unknown>> => {
    assertConnected(isConnectedGetter, modelName, "delete");
    return operationTracker.track(
      (async () => {
        const writerCtx = buildWriterCtx();
        const record = await persistenceDelete(writerCtx, { where: input.where });

        // TODO: auto-compact trigger — wired in prompt 13.5
        config.logger?.debug(`[${modelName}] delete complete`);

        return record;
      })(),
    );
  };

  /**
   * Deletes all records matching `where`.
   * When `where` is omitted, all records are deleted.
   * Returns `{ count: 0 }` when no records match — never throws for an empty match.
   */
  const deleteManyRecords = (input: DeleteManyInput): Promise<{ count: number }> => {
    assertConnected(isConnectedGetter, modelName, "deleteMany");
    return operationTracker.track(
      (async () => {
        const writerCtx = buildWriterCtx();
        const compiledFilter = input.where ? compileFilter(input.where) : undefined;

        const result = await persistenceDeleteMany(writerCtx, { where: input.where }, compiledFilter);

        // TODO: auto-compact trigger — wired in prompt 13.5
        config.logger?.debug(`[${modelName}] deleteMany complete`, { count: result.count });

        return result;
      })(),
    );
  };

  /**
   * Finds the record identified by `where`.
   * - If it exists: updates it with `input.update` and returns the updated record.
   * - If it does not exist: creates it with `input.create` and returns the new record.
   *
   * Note: upsert is a find-then-write operation, not atomic in Phase 1.
   * Concurrent upserts on the same record may race.
   */
  const upsert = (input: UpsertInput): Promise<Record<string, unknown>> => {
    assertConnected(isConnectedGetter, modelName, "upsert");
    return operationTracker.track(
      (async () => {
        const readerCtx = buildReaderCtx();
        const existing = await persistenceFindUnique(readerCtx, { where: input.where });

        if (existing !== null) {
          // Record exists — update path
          assertStrictMode(input.update, "update");
          const cleanUpdate = stripRelationFields(input.update);
          const writerCtx = buildWriterCtx();
          const updated = await persistenceUpdate(writerCtx, {
            where: input.where,
            data: cleanUpdate,
          });

          // TODO: auto-compact trigger — wired in prompt 13.5
          // TODO: include — wired in prompt 8.4

          return applySelect(updated, input.select);
        }

        // Record does not exist — create path
        assertStrictMode(input.create, "create");
        const cleanCreate = stripRelationFields(input.create);
        const writerCtx = buildWriterCtx();
        const created = await persistenceCreate(writerCtx, { data: cleanCreate });

        // TODO: auto-compact trigger — wired in prompt 13.5
        // TODO: include — wired in prompt 8.4

        return applySelect(created, input.select);
      })(),
    );
  };

  return {
    findUnique,
    findUniqueOrThrow,
    findFirst,
    findMany,
    count,
    aggregate,
    groupBy,
    create,
    createMany,
    update,
    updateMany,
    delete: deleteRecord,
    deleteMany: deleteManyRecords,
    upsert,
  };
};
