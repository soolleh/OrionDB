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
import { shouldAutoCompact } from "../persistence/index.js";
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
import { QueryError, ValidationError, RelationError } from "../errors/index.js";
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
import {
  resolveIncludes,
  attachIncludes,
  extractNestedWrites,
  executeNestedWrites,
  resolveConnectForeignKey,
} from "../relations/index.js";
import type {
  RelationResolverContext,
  FindManyForResolver,
  IncludeClause,
  NestedWriteOperation,
} from "../relations/index.js";
import type { ParsedModelDefinition } from "../schema/index.js";

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
  // Include and nested write helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds an injected `findMany` function for the relation resolver.
   * Looks up per-model reader context from the cross-model registries.
   */
  const buildFindManyForResolver = (): FindManyForResolver => {
    return async (relatedModelName: string, filter: (record: Record<string, unknown>) => boolean) => {
      const paths = config.allPaths.get(relatedModelName);
      const schema = config.allSchemas.get(relatedModelName);
      const indexManager = config.allIndexManagers.get(relatedModelName);

      if (!paths || !schema || !indexManager) {
        throw new RelationError(`Cannot resolve relation — model '${relatedModelName}' not found in registry`, {
          model: config.modelName,
          meta: { relatedModel: relatedModelName, reason: "model not in allSchemas registry" },
        });
      }

      const readerCtx: ModelReaderContext = {
        modelName: relatedModelName,
        paths,
        schema,
        indexManager,
      };

      return persistenceFindMany(readerCtx, {}, filter);
    };
  };

  /**
   * Builds the `RelationResolverContext` used by `resolveIncludes`.
   */
  const buildResolverCtx = (): RelationResolverContext => ({
    modelName: config.modelName,
    schema: config.schema,
    allSchemas: config.allSchemas,
    findMany: buildFindManyForResolver(),
  });

  /**
   * Resolves `include` clauses for a single record and merges the
   * resolved relations back onto the record.
   * Returns the original reference when `include` is `undefined`.
   */
  const resolveAndAttach = async (
    record: Record<string, unknown>,
    include: IncludeClause | undefined,
  ): Promise<Record<string, unknown>> => {
    if (!include) return record;
    const resolverCtx = buildResolverCtx();
    const pkField = config.schema.primaryKeyField;
    const includeResult = await resolveIncludes(resolverCtx, [record], include);
    const [attached] = attachIncludes([record], includeResult, pkField);
    return attached ?? record;
  };

  /**
   * Resolves `include` clauses for an array of records and merges the
   * resolved relations back onto each record.
   * Returns the original array reference when `include` is `undefined` or
   * the array is empty.
   */
  const resolveAndAttachMany = async (
    records: Record<string, unknown>[],
    include: IncludeClause | undefined,
  ): Promise<Record<string, unknown>[]> => {
    if (!include || records.length === 0) return records;
    const resolverCtx = buildResolverCtx();
    const pkField = config.schema.primaryKeyField;
    const includeResult = await resolveIncludes(resolverCtx, records, include);
    return attachIncludes(records, includeResult, pkField);
  };

  /**
   * Builds the execution context injected into `executeNestedWrites`.
   * Constructs per-model writer contexts from the cross-model registries.
   */
  const buildExecuteContext = () => ({
    createRecord: async (nestedModelName: string, data: Record<string, unknown>) => {
      const paths = config.allPaths.get(nestedModelName);
      const schema = config.allSchemas.get(nestedModelName);
      const indexManager = config.allIndexManagers.get(nestedModelName);
      const counter = config.allCounters.get(nestedModelName);

      if (!paths || !schema || !indexManager || !counter) {
        throw new RelationError(`Cannot nested-create — model '${nestedModelName}' not found in registry`, {
          model: config.modelName,
          meta: { relatedModel: nestedModelName },
        });
      }

      const writerCtx: ModelWriterContext = {
        modelName: nestedModelName,
        paths,
        schema,
        indexManager,
        counter,
      };

      return persistenceCreate(writerCtx, { data });
    },

    updateRecord: async (nestedModelName: string, where: Record<string, unknown>, data: Record<string, unknown>) => {
      const paths = config.allPaths.get(nestedModelName);
      const schema = config.allSchemas.get(nestedModelName);
      const indexManager = config.allIndexManagers.get(nestedModelName);
      const counter = config.allCounters.get(nestedModelName);

      if (!paths || !schema || !indexManager || !counter) {
        throw new RelationError(`Cannot nested-update — model '${nestedModelName}' not found in registry`, {
          model: config.modelName,
          meta: { relatedModel: nestedModelName },
        });
      }

      const writerCtx: ModelWriterContext = {
        modelName: nestedModelName,
        paths,
        schema,
        indexManager,
        counter,
      };

      return persistenceUpdate(writerCtx, { where, data });
    },
  });

  /**
   * For `many-to-one` connect operations, injects the FK into the parent's
   * `cleanData` before the parent write. No-op for other relation types.
   */
  const injectConnectForeignKeys = (
    cleanData: Record<string, unknown>,
    operations: NestedWriteOperation[],
    allSchemas: Map<string, ParsedModelDefinition>,
  ): Record<string, unknown> => {
    let data = { ...cleanData };

    for (const op of operations) {
      if (op.relationType !== "many-to-one") continue;

      const connectRecords = op.records.filter((r) => r["_nestedOp"] === "connect");
      for (const connectRecord of connectRecords) {
        const where = { ...connectRecord };
        delete where["_nestedOp"];
        const { field, value } = resolveConnectForeignKey(op, where, allSchemas);
        data = { ...data, [field]: value };
      }
    }

    return data;
  };

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
        });
        if (record === null) return null;
        const withIncludes = await resolveAndAttach(record, input.include);
        return applySelect(withIncludes, input.select);
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
        });
        const withIncludes = await resolveAndAttach(record, input.include);
        return applySelect(withIncludes, input.select);
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
          const withIncludes = await resolveAndAttach(first, input.include);
          return applySelect(withIncludes, input.select);
        }

        // No orderBy — delegate to persistence early-exit (take: 1)
        const record = await persistenceFindFirst(readerCtx, { where: input.where }, compiledFilter);
        if (record === null) return null;
        const withIncludes = await resolveAndAttach(record, input.include);
        return applySelect(withIncludes, input.select);
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

        const withIncludes = await resolveAndAttachMany(paginated, input.include);
        return applySelectMany(withIncludes, input.select);
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
   *
   * Note: nested write execution is non-atomic. If the parent write succeeds
   * but a nested child write fails, the parent record exists in the database
   * with no children. There is no transaction rollback in Phase 1.
   */
  const create = (input: CreateInput): Promise<Record<string, unknown>> => {
    assertConnected(isConnectedGetter, modelName, "create");
    return operationTracker.track(
      (async () => {
        assertStrictMode(input.data, "create");

        const { cleanData, operations } = extractNestedWrites(input.data, config.schema, config.allSchemas);

        const finalData = injectConnectForeignKeys(cleanData, operations, config.allSchemas);

        const writerCtx = buildWriterCtx();
        const record = await persistenceCreate(writerCtx, { data: finalData });

        const parentPk = record[config.schema.primaryKeyField];
        await executeNestedWrites(operations, parentPk, buildExecuteContext());

        if (config.onShouldCompact) {
          const doCompact = await shouldAutoCompact(config.paths, config.autoCompactThreshold);
          if (doCompact) config.onShouldCompact(config.modelName);
        }
        config.logger?.debug(`[${modelName}] create complete`);

        const withIncludes = await resolveAndAttach(record, input.include);
        return applySelect(withIncludes, input.select);
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

        // createMany: strip relation fields, no nested write execution
        // Nested writes in bulk creates are Phase 2
        const cleanRecords = input.data.map((d) => extractNestedWrites(d, config.schema, config.allSchemas).cleanData);

        const result = await persistenceCreateMany(writerCtx, { data: cleanRecords });

        if (config.onShouldCompact) {
          const doCompact = await shouldAutoCompact(config.paths, config.autoCompactThreshold);
          if (doCompact) config.onShouldCompact(config.modelName);
        }
        config.logger?.debug(`[${modelName}] createMany complete`, { count: result.count });

        return result;
      })(),
    );
  };

  /**
   * Updates the single record identified by `where`. Throws `RecordNotFoundError`
   * when no matching record exists. Returns the updated record.
   *
   * Note: nested write execution is non-atomic. If the parent write succeeds
   * but a nested child write fails, the parent record exists in the database
   * with no children. There is no transaction rollback in Phase 1.
   */
  const update = (input: UpdateInput): Promise<Record<string, unknown>> => {
    assertConnected(isConnectedGetter, modelName, "update");
    return operationTracker.track(
      (async () => {
        assertStrictMode(input.data, "update");

        const { cleanData, operations } = extractNestedWrites(input.data, config.schema, config.allSchemas);

        const writerCtx = buildWriterCtx();
        // persistenceUpdate handles RecordNotFoundError internally with
        // model + meta.where context already set correctly
        const record = await persistenceUpdate(writerCtx, {
          where: input.where,
          data: cleanData,
        });

        const parentPk = record[config.schema.primaryKeyField];
        await executeNestedWrites(operations, parentPk, buildExecuteContext());

        if (config.onShouldCompact) {
          const doCompact = await shouldAutoCompact(config.paths, config.autoCompactThreshold);
          if (doCompact) config.onShouldCompact(config.modelName);
        }
        config.logger?.debug(`[${modelName}] update complete`);

        const withIncludes = await resolveAndAttach(record, input.include);
        return applySelect(withIncludes, input.select);
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

        // updateMany: strip relation fields, no nested write execution
        // Nested writes in bulk updates are Phase 2
        const { cleanData } = extractNestedWrites(input.data, config.schema, config.allSchemas);

        const writerCtx = buildWriterCtx();
        const compiledFilter = input.where ? compileFilter(input.where) : undefined;

        const result = await persistenceUpdateMany(writerCtx, { where: input.where, data: cleanData }, compiledFilter);

        if (config.onShouldCompact) {
          const doCompact = await shouldAutoCompact(config.paths, config.autoCompactThreshold);
          if (doCompact) config.onShouldCompact(config.modelName);
        }
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

        if (config.onShouldCompact) {
          const doCompact = await shouldAutoCompact(config.paths, config.autoCompactThreshold);
          if (doCompact) config.onShouldCompact(config.modelName);
        }
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

        if (config.onShouldCompact) {
          const doCompact = await shouldAutoCompact(config.paths, config.autoCompactThreshold);
          if (doCompact) config.onShouldCompact(config.modelName);
        }
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
   *
   * Note: nested write execution is non-atomic. If the parent write succeeds
   * but a nested child write fails, the parent record exists in the database
   * with no children. There is no transaction rollback in Phase 1.
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
          const { cleanData: cleanUpdate, operations: updateOps } = extractNestedWrites(
            input.update,
            config.schema,
            config.allSchemas,
          );
          const writerCtx = buildWriterCtx();
          const updated = await persistenceUpdate(writerCtx, {
            where: input.where,
            data: cleanUpdate,
          });
          const parentPk = updated[config.schema.primaryKeyField];
          await executeNestedWrites(updateOps, parentPk, buildExecuteContext());

          const withIncludes = await resolveAndAttach(updated, input.include);
          return applySelect(withIncludes, input.select);
        }

        // Record does not exist — create path
        assertStrictMode(input.create, "create");
        const { cleanData: cleanCreate, operations: createOps } = extractNestedWrites(
          input.create,
          config.schema,
          config.allSchemas,
        );
        const finalCreate = injectConnectForeignKeys(cleanCreate, createOps, config.allSchemas);
        const writerCtx = buildWriterCtx();
        const created = await persistenceCreate(writerCtx, { data: finalCreate });
        const parentPk = created[config.schema.primaryKeyField];
        await executeNestedWrites(createOps, parentPk, buildExecuteContext());

        const withIncludes = await resolveAndAttach(created, input.include);
        return applySelect(withIncludes, input.select);
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
