// src/client/oriondb.ts
//
// OrionDB — top-level database instance factory.
// Returns a Proxy immediately (no I/O at construction).
// All I/O happens inside $connect().

import {
  resolveDatabasePaths,
  resolveModelPaths,
  initializeDatabaseDirectory,
  initializeModelDirectory,
  FileSizeCounter,
} from "../persistence/index.js";
import { parseModelSchema, readSchemaFile, writeSchemaFile, runStartupSchemaValidation } from "../schema/index.js";
import type { ParsedModelDefinition } from "../schema/index.js";
import { IndexManagerImpl } from "../index-manager/index.js";
import { QueryError } from "../errors/index.js";
import type { OrionDBConfig, OrionDB, ClientContext, ConnectOptions, StartupResult } from "./types.js";
import { createModelClient } from "./model-client.js";
import { createLogger } from "./logger.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_MISMATCH_STRATEGY = "block" as const;
const DEFAULT_LOG_LEVEL = "warn" as const;

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/**
 * Initializes a single model: creates its directory and data file if absent,
 * builds the in-memory index from disk, and initializes the file-size counter.
 * Stores all artefacts into `ctx` before returning the startup diagnostics.
 */
async function initializeModel(
  modelName: string,
  parsed: ParsedModelDefinition,
  ctx: ClientContext,
): Promise<StartupResult> {
  const paths = resolveModelPaths(ctx.config.dbLocation, modelName);
  const meta = await initializeModelDirectory(paths, modelName);

  const indexManager = new IndexManagerImpl<Record<string, unknown>>({
    primaryKeyField: parsed.primaryKeyField,
    indexedFields: new Set(parsed.indexedFields),
  });

  await indexManager.rebuild(paths.dataFile);

  const counter = new FileSizeCounter();
  await counter.initialize(paths.dataFile);

  ctx.allPaths.set(modelName, paths);
  ctx.allSchemas.set(modelName, parsed);
  ctx.allIndexManagers.set(modelName, indexManager);
  ctx.allCounters.set(modelName, counter);

  return {
    modelName,
    recordCount: meta.recordCount,
    indexedCount: indexManager.size(),
    dataFileSizeBytes: counter.getSize(),
  };
}

/**
 * Attaches a model client to `target` for both the original model name and
 * its lowercase variant (e.g. `target.User` and `target.user`).
 */
function attachModelClient(modelName: string, ctx: ClientContext, target: Record<string, unknown>): void {
  const paths = ctx.allPaths.get(modelName);
  const schema = ctx.allSchemas.get(modelName);
  const indexManager = ctx.allIndexManagers.get(modelName);
  const counter = ctx.allCounters.get(modelName);

  if (!paths || !schema || !indexManager || !counter) return;

  const client = createModelClient({
    modelName,
    paths,
    schema,
    indexManager,
    counter,
    allSchemas: ctx.allSchemas,
    allPaths: ctx.allPaths,
    allIndexManagers: ctx.allIndexManagers,
    allCounters: ctx.allCounters,
    autoCompactThreshold: ctx.config.autoCompactThreshold,
  });

  target[modelName] = client;
  const lower = modelName.toLowerCase();
  if (lower !== modelName) {
    target[lower] = client;
  }
}

/**
 * Removes all model client entries added by `attachModelClient` from `target`.
 */
function detachAllModelClients(ctx: ClientContext, target: Record<string, unknown>): void {
  for (const modelName of ctx.allSchemas.keys()) {
    delete target[modelName];
    const lower = modelName.toLowerCase();
    if (lower !== modelName) {
      delete target[lower];
    }
  }
}

// ---------------------------------------------------------------------------
// createOrionDB
// ---------------------------------------------------------------------------

/**
 * Creates an OrionDB instance backed by a JavaScript `Proxy`.
 *
 * No I/O is performed at this point. Call `$connect()` to initialize
 * the database, build in-memory indexes, and unlock model client access.
 *
 * ```ts
 * const db = createOrionDB({ dbLocation: './mydb', schema: { User: { ... } } })
 * await db.$connect()
 * const user = await db.user.create({ data: { name: 'Alice' } })
 * ```
 */
export const createOrionDB = (config: OrionDBConfig): OrionDB => {
  const logger = createLogger(config.logLevel ?? DEFAULT_LOG_LEVEL);

  const ctx: ClientContext = {
    config,
    // resolveDatabasePaths is pure (no I/O) — safe to call at construction time
    dbPaths: resolveDatabasePaths(config.dbLocation),
    logger,
    allSchemas: new Map(),
    allPaths: new Map(),
    allIndexManagers: new Map(),
    allCounters: new Map(),
    isConnected: false,
  };

  // Proxy target — model clients are attached here after $connect
  const target: Record<string, unknown> = {};

  // -------------------------------------------------------------------------
  // $connect
  // -------------------------------------------------------------------------

  const $connect = async (options?: ConnectOptions): Promise<void> => {
    if (ctx.isConnected && options?.force !== true) return;

    if (ctx.isConnected && options?.force === true) {
      detachAllModelClients(ctx, target);
      ctx.allSchemas.clear();
      ctx.allPaths.clear();
      ctx.allIndexManagers.clear();
      ctx.allCounters.clear();
      ctx.isConnected = false;
    }

    // Parse all model schemas from config
    const parsedSchemas = new Map<string, ParsedModelDefinition>();
    const schemaEntries = Object.entries(config.schema ?? {});

    for (const [modelName, schemaInput] of schemaEntries) {
      const parsed = parseModelSchema(modelName, schemaInput);
      parsedSchemas.set(modelName, parsed);
    }

    if (parsedSchemas.size === 0) {
      logger.warn("$connect called with no model schemas defined — database will be empty.");
    }

    // Read on-disk schema and run mismatch / relationship validation
    const diskSchema = await readSchemaFile(ctx.dbPaths.schemaFile);
    const strategy = config.schemaMismatchStrategy ?? DEFAULT_MISMATCH_STRATEGY;
    runStartupSchemaValidation(parsedSchemas, diskSchema, strategy);

    // Ensure database root directory and _meta.json exist
    await initializeDatabaseDirectory(ctx.dbPaths);

    // Initialize all model directories and rebuild indexes in parallel
    const entries = [...parsedSchemas.entries()];
    const results = await Promise.all(entries.map(([modelName, parsed]) => initializeModel(modelName, parsed, ctx)));

    // Persist the current code schema to disk (overwrite / create _schema.json)
    await writeSchemaFile(ctx.dbPaths.schemaFile, ctx.allSchemas);

    ctx.isConnected = true;

    // Attach model clients to the proxy target
    for (const modelName of ctx.allSchemas.keys()) {
      attachModelClient(modelName, ctx, target);
    }

    // Log per-model startup diagnostics at info level
    for (const result of results) {
      logger.info(`Model "${result.modelName}" ready`, {
        recordCount: result.recordCount,
        indexedCount: result.indexedCount,
        dataFileSizeBytes: result.dataFileSizeBytes,
      });
    }
  };

  // -------------------------------------------------------------------------
  // $disconnect
  // -------------------------------------------------------------------------

  const $disconnect = (): Promise<void> => {
    if (!ctx.isConnected) return Promise.resolve();

    detachAllModelClients(ctx, target);

    ctx.allSchemas.clear();
    ctx.allPaths.clear();
    ctx.allIndexManagers.clear();
    ctx.allCounters.clear();
    ctx.isConnected = false;

    logger.info("OrionDB disconnected.");

    return Promise.resolve();
  };

  // -------------------------------------------------------------------------
  // $compact
  // -------------------------------------------------------------------------

  const $compact = (_modelName?: string): Promise<void> => {
    if (!ctx.isConnected) {
      return Promise.reject(
        new QueryError("Cannot compact before calling $connect().", {
          meta: { isConnected: false },
        }),
      );
    }
    // Phase 1 stub — compaction is implemented in the compaction feature branch
    return Promise.reject(
      new QueryError("$compact is not yet implemented in Phase 1.", {
        meta: { phase: 1 },
      }),
    );
  };

  // -------------------------------------------------------------------------
  // $rebuildIndexes
  // -------------------------------------------------------------------------

  const $rebuildIndexes = async (modelName?: string): Promise<void> => {
    if (!ctx.isConnected) {
      throw new QueryError("Cannot rebuild indexes before calling $connect().", {
        meta: { isConnected: false },
      });
    }

    const modelNames = modelName !== undefined ? [modelName] : [...ctx.allSchemas.keys()];

    await Promise.all(
      modelNames.map(async (name) => {
        const indexManager = ctx.allIndexManagers.get(name);
        const paths = ctx.allPaths.get(name);
        const counter = ctx.allCounters.get(name);

        if (!indexManager || !paths || !counter) {
          throw new QueryError(`No registered model found with name "${name}".`, {
            model: name,
            meta: { name },
          });
        }

        await indexManager.rebuild(paths.dataFile);
        await counter.initialize(paths.dataFile);

        logger.info(`Indexes rebuilt for model "${name}".`, {
          indexedCount: indexManager.size(),
        });
      }),
    );
  };

  // -------------------------------------------------------------------------
  // Proxy
  // -------------------------------------------------------------------------

  return new Proxy(target as OrionDB, {
    get(proxyTarget, prop) {
      if (typeof prop === "symbol") {
        return Reflect.get(proxyTarget, prop) as unknown;
      }

      if (prop === "$connect") return $connect;
      if (prop === "$disconnect") return $disconnect;
      if (prop === "$compact") return $compact;
      if (prop === "$rebuildIndexes") return $rebuildIndexes;

      if (!ctx.isConnected) {
        throw new QueryError(`Cannot access "${prop}" before calling $connect().`, {
          meta: { prop },
        });
      }

      return Reflect.get(proxyTarget, prop) as unknown;
    },

    set(proxyTarget, prop, value) {
      return Reflect.set(proxyTarget, prop, value);
    },
  });
};
