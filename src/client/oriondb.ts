// src/client/oriondb.ts
//
// OrionDB — top-level database instance factory.
// Returns a Proxy immediately (no I/O at construction).
// All I/O happens inside $connect().

import { readFile, writeFile, rename, unlink, stat } from "node:fs/promises";
import {
  resolveDatabasePaths,
  resolveModelPaths,
  initializeDatabaseDirectory,
  initializeModelDirectory,
  updateModelMeta,
  readModelMeta,
  FileSizeCounter,
} from "../persistence/index.js";
import { parseModelSchema, readSchemaFile, writeSchemaFile, runStartupSchemaValidation } from "../schema/index.js";
import type { ParsedModelDefinition } from "../schema/index.js";
import { IndexManagerImpl } from "../index-manager/index.js";
import { OrionDBError, QueryError, CompactionError } from "../errors/index.js";
import type {
  OrionDBConfig,
  OrionDB,
  ClientContext,
  ConnectOptions,
  DisconnectOptions,
  StartupResult,
  ModelClientMethods,
  CompactOptions,
  CompactionResult,
} from "./types.js";
import { createModelClient } from "./model-client.js";
import { createLogger } from "./logger.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_MISMATCH_STRATEGY = "block" as const;
const DEFAULT_LOG_LEVEL = "warn" as const;
const DEFAULT_DISCONNECT_TIMEOUT_MS = 5_000 as const;

// ---------------------------------------------------------------------------
// computeCompactionRatio
// ---------------------------------------------------------------------------

/**
 * Pure function — no I/O.
 * Returns the fraction of dead lines (stale versions + tombstones) relative
 * to total lines in the model's `data.ndjson`.
 *
 * `deadLines = totalLines − recordCount` (live records).
 * Returns `0` for empty models.
 */
const computeCompactionRatio = (meta: { totalLines: number; recordCount: number }): number => {
  if (meta.totalLines === 0) return 0;
  const deadLines = meta.totalLines - meta.recordCount;
  return deadLines / meta.totalLines;
};

// ---------------------------------------------------------------------------
// OperationTracker
// ---------------------------------------------------------------------------

/**
 * Tracks in-flight async operations so that `$disconnect` can wait for
 * them to settle before clearing state.
 *
 * `track()` registers a promise and removes it when it settles.
 * `drain()` waits for all registered promises via `Promise.allSettled`
 * so that individual rejections never abort the drain.
 */
class OperationTracker {
  private readonly pending = new Set<Promise<unknown>>();

  /**
   * Registers `operation` in the pending set and returns it unchanged.
   * The operation is removed automatically when it resolves or rejects.
   */
  track<T>(operation: Promise<T>): Promise<T> {
    this.pending.add(operation);
    // Use .then(onFulfilled, onRejected) with the same cleanup for both branches.
    // Unlike .finally(), this creates a promise that always RESOLVES (both handlers
    // return undefined), preventing an unhandled-rejection on the cleanup chain
    // while still removing the operation from the pending set.
    operation.then(
      () => this.pending.delete(operation),
      () => this.pending.delete(operation),
    );
    return operation;
  }

  /**
   * Waits for all in-flight operations to settle.
   * Uses `Promise.allSettled` so rejections do not abort the drain.
   */
  async drain(): Promise<void> {
    if (this.pending.size === 0) return;
    await Promise.allSettled([...this.pending]);
  }

  /** Number of currently tracked (pending) operations. */
  get size(): number {
    return this.pending.size;
  }
}

// ---------------------------------------------------------------------------
// invokeHook
// ---------------------------------------------------------------------------

/**
 * Invokes a lifecycle hook, awaiting its result if it returns a Promise.
 * If the hook throws, the error is forwarded to `onError` (synchronously,
 * never awaited) and then swallowed so it never propagates to the caller.
 */
async function invokeHook(
  hook: (() => void | Promise<void>) | undefined,
  onError: ((err: unknown) => void) | undefined,
  logger: ClientContext["logger"],
  hookName: string,
): Promise<void> {
  if (!hook) return;
  try {
    await hook();
  } catch (err) {
    logger.warn(`Lifecycle hook '${hookName}' threw`, { error: String(err) });
    if (onError) {
      try {
        onError(err);
      } catch {
        // swallow errors thrown by onError itself
      }
    }
  }
}

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

  // Clean up any leftover compaction temp file from a previous crashed run.
  // This is best-effort — a missing file is the normal case.
  const tempFile = `${paths.dataFile}.compact.tmp`;
  try {
    await unlink(tempFile);
    ctx.logger.warn(`Removed leftover compaction temp file for '${modelName}'`, { tempFile });
  } catch {
    // File does not exist — normal case, ignore
  }

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
 * Converts a PascalCase or any model name to camelCase for use as
 * the `modelClients` Map key and the proxy property name.
 *
 * Examples: `User` → `user`, `BlogPost` → `blogPost`.
 */
function toCamelCaseKey(modelName: string): string {
  return modelName.charAt(0).toLowerCase() + modelName.slice(1);
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

  /**
   * Map of camelCase model name → model client instance.
   * Populated during `$connect`, cleared during `$disconnect`.
   * The proxy `get` trap reads exclusively from this Map.
   */
  const modelClients = new Map<string, ModelClientMethods>();

  /** In-flight operation tracker — shared across all model clients. */
  const operationTracker = new OperationTracker();

  // -------------------------------------------------------------------------
  // $connect
  // -------------------------------------------------------------------------

  const $connect = async (options?: ConnectOptions): Promise<void> => {
    // Idempotency guard: skip if already connected unless force-reconnecting.
    // After $disconnect sets isConnected = false, this guard does NOT fire —
    // allowing a full reconnect without passing force.
    if (ctx.isConnected && !options?.force) return;

    // Clear any stale state from a previous connect (force-reconnect case
    // or defensive cleanup before first-time connect where maps are empty).
    modelClients.clear();
    ctx.allSchemas.clear();
    ctx.allPaths.clear();
    ctx.allIndexManagers.clear();
    ctx.allCounters.clear();
    ctx.isConnected = false;

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

    // Populate modelClients with camelCase keys
    for (const [modelName, schema] of ctx.allSchemas) {
      const clientKey = toCamelCaseKey(modelName);
      const paths = ctx.allPaths.get(modelName);
      const indexManager = ctx.allIndexManagers.get(modelName);
      const counter = ctx.allCounters.get(modelName);

      if (!paths || !indexManager || !counter) continue;

      modelClients.set(
        clientKey,
        createModelClient({
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
          isConnectedGetter: () => ctx.isConnected,
          operationTracker,
          strict: ctx.config.strict,
          logger,
          onShouldCompact: (name) => {
            // Fire-and-forget auto-compact — never blocks the write return path
            compactModel(name, ctx, { force: false })
              .then((result) => {
                if (result !== null) {
                  ctx.logger.info("Auto-compact complete", { ...result });
                }
              })
              .catch((err: unknown) => {
                ctx.logger.warn("Auto-compact failed", { modelName: name, error: String(err) });
                if (ctx.config.hooks?.onError) {
                  ctx.config.hooks.onError(err as Error);
                }
              });
          },
        }),
      );
    }

    // Log per-model startup diagnostics at info level
    for (const result of results) {
      logger.info(`Model "${result.modelName}" ready`, {
        recordCount: result.recordCount,
        indexedCount: result.indexedCount,
        dataFileSizeBytes: result.dataFileSizeBytes,
      });
    }

    // Invoke onConnect lifecycle hook
    await invokeHook(config.hooks?.onConnect, config.hooks?.onError, logger, "onConnect");
  };

  // -------------------------------------------------------------------------
  // $disconnect
  // -------------------------------------------------------------------------

  const $disconnect = async (options?: DisconnectOptions): Promise<void> => {
    // Step 1 — Idempotency guard
    if (!ctx.isConnected) {
      logger.debug("$disconnect called on already-disconnected instance");
      return;
    }

    // Step 2 — Mark as disconnecting immediately.
    // Setting isConnected = false stops the proxy from accepting new model
    // access and causes assertConnected to throw in any model method called
    // after this point. New work is rejected before draining existing work.
    ctx.isConnected = false;

    // Step 3 — Drain in-flight operations
    const timeout = options?.timeout ?? DEFAULT_DISCONNECT_TIMEOUT_MS;
    const force = options?.force ?? false;

    if (!force) {
      if (operationTracker.size > 0) {
        logger.info(`Waiting for ${operationTracker.size} in-flight operations to complete`);
      }

      const drainPromise = operationTracker.drain();

      if (timeout > 0) {
        const timeoutPromise = new Promise<void>((_resolve, reject) =>
          setTimeout(
            () =>
              reject(
                new OrionDBError(
                  `$disconnect timed out after ${timeout}ms waiting for in-flight operations`,
                  "ORIONDB_DISCONNECT_TIMEOUT",
                  { meta: { timeout, pendingCount: operationTracker.size } },
                ),
              ),
            timeout,
          ),
        );
        try {
          await Promise.race([drainPromise, timeoutPromise]);
        } catch {
          logger.warn("$disconnect drain timed out — forcing disconnect", {
            timeout,
            pendingCount: operationTracker.size,
          });
          // Continue with cleanup despite timeout
        }
      } else {
        // timeout === 0: drain without ceiling
        await drainPromise;
      }
    }

    // Step 4 — Flush meta.json for all models after drain
    for (const [modelName, paths] of ctx.allPaths) {
      try {
        const indexManager = ctx.allIndexManagers.get(modelName);
        if (!indexManager) continue;
        await updateModelMeta(paths, { recordCount: indexManager.size() });
      } catch (err) {
        logger.warn(`Failed to flush meta for model '${modelName}'`, { error: String(err) });
        // Do not rethrow — meta flush failure must not abort disconnect
      }
    }

    // Step 5 — Clear all in-memory state
    modelClients.clear();
    ctx.allIndexManagers.clear();
    ctx.allCounters.clear();
    ctx.allPaths.clear();
    ctx.allSchemas.clear();

    // Step 6 — Log completion
    logger.info("$disconnect complete");

    // Step 7 — Invoke onDisconnect lifecycle hook
    await invokeHook(config.hooks?.onDisconnect, config.hooks?.onError, logger, "onDisconnect");
  };

  // -------------------------------------------------------------------------
  // compactModel / runCompaction (inner closures — access modelClients)
  // -------------------------------------------------------------------------

  // Per-model compaction lock — prevents concurrent compactions of the same model
  // (e.g. auto-compact racing against an explicit $compact call).
  const compactingModels = new Set<string>();

  /**
   * Core compaction algorithm for a single model.
   *
   * 1. Reads all lines from `data.ndjson` into memory.
   * 2. Keeps only the latest version of each record (last-occurrence-wins).
   * 3. Discards tombstones and stale versions.
   * 4. Writes live records to a temp file then renames atomically.
   * 5. Rebuilds the in-memory index from the new file.
   * 6. Updates `meta.json` and the `FileSizeCounter`.
   * 7. Re-attaches the model client with the fresh index manager.
   *
   * **Write safety:** If the process crashes mid-write, the original
   * `data.ndjson` is untouched. The temp file is cleaned up on next
   * `$connect`.
   *
   * **Index consistency:** The index is fully rebuilt from the new file
   * after the atomic rename. All three index structures are replaced.
   *
   * **Concurrent access (Phase 1):** Compaction is not safe to run
   * concurrently with writes to the same model. Auto-compact fires only
   * from write methods which run serially through `operationTracker`.
   * `$disconnect` drains in-flight operations before any external call.
   */
  async function runCompaction(
    name: string,
    paths: import("../persistence/index.js").ModelPaths,
    ctx: ClientContext,
  ): Promise<Omit<CompactionResult, "modelName" | "durationMs">> {
    const dataFile = paths.dataFile;

    // Step 1 — Read all lines
    const content = await readFile(dataFile, "utf8");
    const lines = content.split("\n").filter((line) => line.trim().length > 0);

    // Step 2 — Build latest-version map (last-occurrence-wins)
    const schema = ctx.allSchemas.get(name);
    if (!schema) {
      throw new CompactionError(`Schema not found for model '${name}' during compaction`, {
        meta: { modelName: name },
      });
    }
    const pkField = schema.primaryKeyField;
    const latestVersions = new Map<string, Record<string, unknown>>();
    let parseErrors = 0;

    for (const line of lines) {
      try {
        const record = JSON.parse(line) as Record<string, unknown>;
        const pk = record[pkField];
        if (pk === null || pk === undefined) continue;
        if (typeof pk !== "string" && typeof pk !== "number") continue;
        // Always overwrite — later lines are newer versions
        latestVersions.set(String(pk), record);
      } catch {
        parseErrors++;
        ctx.logger.warn(`Skipping malformed line during compaction of '${name}'`);
      }
    }

    // Step 3 — Filter to live records
    const liveRecords: Record<string, unknown>[] = [];
    let tombstonesRemoved = 0;

    for (const record of latestVersions.values()) {
      if (record["_deleted"] === true) {
        tombstonesRemoved++;
      } else {
        liveRecords.push(record);
      }
    }

    // Stale lines = total parsed lines minus the one-per-PK latest set
    const staleLinesRemoved = lines.length - parseErrors - latestVersions.size;

    // Step 4 — Write to temp file
    const tempFile = `${dataFile}.compact.tmp`;
    const newContent = liveRecords.map((r) => JSON.stringify(r)).join("\n");
    const finalContent = newContent.length > 0 ? newContent + "\n" : "";

    await writeFile(tempFile, finalContent, "utf8");

    // Step 5 — Atomic rename
    try {
      await rename(tempFile, dataFile);
    } catch (err) {
      // Best-effort cleanup of temp file before rethrowing
      try {
        await unlink(tempFile);
      } catch {
        /* ignore */
      }
      throw new CompactionError(`Compaction failed during file rename for model '${name}'`, {
        meta: { modelName: name, tempFile, dataFile, cause: err },
      });
    }

    // Step 6 — Rebuild in-memory index from the compacted file
    const freshIndex = new IndexManagerImpl<Record<string, unknown>>({
      primaryKeyField: schema.primaryKeyField,
      indexedFields: new Set(schema.indexedFields),
    });
    await freshIndex.rebuild(dataFile);
    ctx.allIndexManagers.set(name, freshIndex);

    // Step 7 — Update meta.json
    await updateModelMeta(paths, {
      recordCount: liveRecords.length,
      tombstoneCount: 0,
      lastCompactedAt: new Date().toISOString(),
    });

    // Step 8 — Update FileSizeCounter
    const counter = ctx.allCounters.get(name);
    if (counter) {
      await counter.initialize(dataFile);
    } else {
      const freshCounter = new FileSizeCounter();
      await freshCounter.initialize(dataFile);
      ctx.allCounters.set(name, freshCounter);
    }

    // Step 9 — Re-attach model client with fresh index manager and counter
    const freshCounter = ctx.allCounters.get(name);
    if (paths && schema && freshCounter) {
      const freshClient = createModelClient({
        modelName: name,
        paths,
        schema,
        indexManager: ctx.allIndexManagers.get(name) ?? freshIndex,
        counter: freshCounter,
        allSchemas: ctx.allSchemas,
        allPaths: ctx.allPaths,
        allIndexManagers: ctx.allIndexManagers,
        allCounters: ctx.allCounters,
        autoCompactThreshold: ctx.config.autoCompactThreshold,
        isConnectedGetter: () => ctx.isConnected,
        operationTracker,
        strict: ctx.config.strict,
        logger,
        onShouldCompact: (n) => {
          compactModel(n, ctx, { force: false })
            .then((result) => {
              if (result !== null) {
                ctx.logger.info("Auto-compact complete", { ...result });
              }
            })
            .catch((err: unknown) => {
              ctx.logger.warn("Auto-compact failed", { modelName: n, error: String(err) });
              if (ctx.config.hooks?.onError) {
                ctx.config.hooks.onError(err as Error);
              }
            });
        },
      });
      const clientKey = toCamelCaseKey(name);
      modelClients.set(clientKey, freshClient);
    }

    // Step 10 — Collect file stats and return
    const fileStat = await stat(dataFile);

    return {
      linesBeforeCompaction: lines.length,
      linesAfterCompaction: liveRecords.length,
      recordsRetained: liveRecords.length,
      tombstonesRemoved,
      staleLinesRemoved,
      newFileSizeBytes: fileStat.size,
    };
  }

  /**
   * Compacts a single model. Checks the dead-line ratio against the threshold
   * and skips if below threshold (unless `force` is set). Supports `dryRun`.
   *
   * Returns `null` when skipped (below threshold, `force` not set).
   * Returns the full `CompactionResult` when compaction runs.
   */
  async function compactModel(
    name: string,
    ctx: ClientContext,
    options: CompactOptions | undefined,
  ): Promise<CompactionResult | null> {
    const paths = ctx.allPaths.get(name);
    if (!paths) {
      throw new CompactionError(`Paths not found for model '${name}' during compaction`, {
        meta: { modelName: name },
      });
    }

    // Lock guard — skip if a compaction is already in progress for this model.
    if (compactingModels.has(name)) {
      ctx.logger.debug(`Skipping compaction for '${name}' — already in progress`);
      return null;
    }
    compactingModels.add(name);

    try {
      // Step 1 — Read current meta
      const meta = await readModelMeta(paths);

      // Step 2 — Check threshold
      const ratio = computeCompactionRatio(meta);
      const threshold = ctx.config.autoCompactThreshold ?? 0.3;
      const force = options?.force ?? false;
      const dryRun = options?.dryRun ?? false;

      if (!force && ratio < threshold) {
        ctx.logger.debug(
          `Skipping compaction for '${name}' \u2014 ratio ${ratio.toFixed(3)} below threshold ${threshold}`,
        );
        return null;
      }

      // Step 3 — Dry run path
      if (dryRun) {
        const liveLines = meta.recordCount;
        const deadLines = meta.totalLines - liveLines;
        ctx.logger.info(`[dry-run] Would compact '${name}'`, {
          linesBeforeCompaction: meta.totalLines,
          linesAfterCompaction: liveLines,
          tombstonesRemoved: meta.tombstoneCount,
          staleLinesRemoved: deadLines - meta.tombstoneCount,
        });
        return {
          modelName: name,
          linesBeforeCompaction: meta.totalLines,
          linesAfterCompaction: liveLines,
          recordsRetained: liveLines,
          tombstonesRemoved: meta.tombstoneCount,
          staleLinesRemoved: deadLines - meta.tombstoneCount,
          durationMs: 0,
          newFileSizeBytes: 0,
        };
      }

      // Step 4 — Run compaction
      const startTime = Date.now();
      ctx.logger.info(`Compacting model '${name}'`, {
        ratio: ratio.toFixed(3),
        totalLines: meta.totalLines,
        liveRecords: meta.recordCount,
      });

      const result = await runCompaction(name, paths, ctx);
      const durationMs = Date.now() - startTime;

      ctx.logger.info(`Compaction complete for '${name}'`, { durationMs, ...result });

      return { modelName: name, durationMs, ...result };
    } finally {
      compactingModels.delete(name);
    }
  }

  // -------------------------------------------------------------------------
  // $compact
  // -------------------------------------------------------------------------

  /**
   * Compacts one or all models, removing tombstones and stale record versions
   * from `data.ndjson` and rebuilding in-memory indexes.
   *
   * **Write safety:** Writes to a `.compact.tmp` file first, then atomically
   * renames. The original file is untouched if the process crashes mid-write.
   *
   * **No data loss:** Only records that are `_deleted: true` in their latest
   * version are removed. Records that were updated retain their latest version.
   *
   * **Index consistency:** Indexes are fully rebuilt from the compacted file
   * after the atomic rename completes.
   *
   * **Concurrent access (Phase 1):** Safe to call from outside the process
   * only when `$disconnect` has been awaited first.
   *
   * @param modelName - Compact a single named model; omit to compact all.
   * @param options - `force` bypasses the threshold; `dryRun` computes result
   *   without writing.
   * @returns Array of `CompactionResult` — one per model actually compacted.
   *   Empty when below threshold and `force` is not set.
   * @throws `OrionDBError` when called before `$connect()`.
   * @throws `OrionDBError` when `modelName` is not registered.
   * @throws `CompactionError` on file-rename failure.
   */
  const $compact = async (modelName?: string, options?: CompactOptions): Promise<CompactionResult[]> => {
    // Step 1 — Connection guard
    if (!ctx.isConnected) {
      throw new OrionDBError("Cannot call $compact() before $connect()", "ORIONDB_NOT_CONNECTED", {
        meta: { reason: "not connected" },
      });
    }

    // Step 2 — Validate and resolve models to compact
    if (modelName !== undefined && !ctx.allSchemas.has(modelName)) {
      throw new OrionDBError(`Cannot compact \u2014 model '${modelName}' not found`, "ORIONDB_MODEL_NOT_FOUND", {
        meta: { modelName, knownModels: [...ctx.allSchemas.keys()] },
      });
    }

    const modelsToCompact = modelName !== undefined ? [modelName] : [...ctx.allSchemas.keys()];

    // Step 3 — Compact sequentially (parallel risks concurrent file writes)
    const results: CompactionResult[] = [];
    for (const name of modelsToCompact) {
      const result = await compactModel(name, ctx, options);
      if (result !== null) results.push(result);
    }

    return results;
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

  /** All known admin property names — used in error messages and `has` / `ownKeys`. */
  const ADMIN_PROPS = ["$connect", "$disconnect", "$compact", "$rebuildIndexes", "$isConnected"] as const;

  const handler: ProxyHandler<Record<string, unknown>> = {
    /**
     * Intercepts all property reads on the `OrionDB` instance.
     * Routes admin methods, `$isConnected`, and model client lookups.
     * Throws `QueryError` for unknown properties or pre-connect model access.
     */
    get(_target: Record<string, unknown>, prop: string | symbol): unknown {
      // Symbol access (e.g. Symbol.toPrimitive, Symbol.iterator) — never throw
      if (typeof prop === "symbol") {
        return (_target as Record<symbol, unknown>)[prop];
      }

      // ── Admin methods ──────────────────────────────────────────────────────
      if (prop === "$connect") return $connect;
      if (prop === "$disconnect") return $disconnect;
      if (prop === "$compact") return $compact;
      if (prop === "$rebuildIndexes") return $rebuildIndexes;

      // $isConnected — read-only diagnostic, accessible before $connect()
      if (prop === "$isConnected") return ctx.isConnected;

      // Unknown $-prefixed property
      if (prop.startsWith("$")) {
        throw new QueryError(`Unknown admin property '${prop}' on OrionDB instance.`, {
          meta: { property: prop, known: [...ADMIN_PROPS] },
        });
      }

      // ── Connection guard ──────────────────────────────────────────────────
      if (!ctx.isConnected) {
        throw new QueryError(`Cannot access db.${prop} — call await db.$connect() first.`, {
          meta: { property: prop, reason: "not connected" },
        });
      }

      // ── Model client lookup ────────────────────────────────────────────────
      const client = modelClients.get(prop);
      if (client !== undefined) return client;

      // ── Unknown model ────────────────────────────────────────────────────────
      const knownModels = [...modelClients.keys()];
      throw new QueryError(`No model named '${prop}' found in schema.`, {
        meta: {
          property: prop,
          knownModels,
          hint:
            knownModels.length === 0
              ? "No models registered — was $connect() awaited?"
              : `Available models: ${knownModels.join(", ")}`,
        },
      });
    },

    /**
     * Throws `QueryError` for any property assignment.
     * `OrionDB` instances are immutable from the caller's perspective.
     */
    set(_target: Record<string, unknown>, prop: string | symbol): boolean {
      throw new QueryError(`OrionDB instances are immutable — cannot set property '${String(prop)}'.`, {
        meta: { property: String(prop) },
      });
    },

    /**
     * Supports the `in` operator for model name and admin prop checks.
     * Example: `'user' in db` returns `true` after `$connect()`.
     */
    has(_target: Record<string, unknown>, prop: string | symbol): boolean {
      if (typeof prop === "symbol") return false;
      if ((ADMIN_PROPS as readonly string[]).includes(prop)) return true;
      return modelClients.has(prop);
    },

    /**
     * Supports `Object.keys(db)` and spread. Returns all admin prop names
     * plus every registered model name.
     */
    ownKeys(): string[] {
      return [...ADMIN_PROPS, ...modelClients.keys()];
    },

    /**
     * Returns a property descriptor for every key exposed by `ownKeys`,
     * and `undefined` for unknown keys.
     * Required alongside `ownKeys` to satisfy the Proxy invariants for
     * `Object.keys()` / `Object.getOwnPropertyNames()` to work correctly.
     */
    getOwnPropertyDescriptor(_target: Record<string, unknown>, prop: string | symbol): PropertyDescriptor | undefined {
      if (typeof prop === "symbol") return undefined;
      const known = new Set<string>([...ADMIN_PROPS, ...modelClients.keys()]);
      if (!known.has(prop)) return undefined;
      return { configurable: true, enumerable: true, writable: false };
    },
  };

  // The Proxy handler dynamically provides all OrionDB methods and model
  // clients at runtime. The cast is safe because the handler implements
  // the full OrionDB contract.
  return new Proxy({} as Record<string, unknown>, handler) as unknown as OrionDB;
};
