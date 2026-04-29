// src/client/types.ts
//
// All client-layer type definitions for OrionDB.
// Types and interfaces only — zero runtime logic.

import type {
  ModelPaths,
  DatabasePaths,
  FileSizeCounter,
  ModelWriterContext,
  ModelReaderContext,
} from "../persistence/index.js";
import type { ParsedModelDefinition, SchemaMismatchStrategy, SchemaDefinition } from "../schema/index.js";
import type { IndexManager } from "../index-manager/index.js";
import type { IncludeClause } from "../relations/index.js";
import type { WhereInput, OrderByInput, SelectInput, AggregateResult, GroupByResult } from "../query/index.js";
import type { Logger, LogLevel } from "./logger.js";

// ---------------------------------------------------------------------------
// CompactionResult
// ---------------------------------------------------------------------------

/**
 * Result of a single model's compaction pass.
 * Returned as one element in the `CompactionResult[]` array from `$compact`.
 */
export interface CompactionResult {
  /** Name of the compacted model. */
  modelName: string;
  /** Total lines in `data.ndjson` before compaction. */
  linesBeforeCompaction: number;
  /** Total lines in `data.ndjson` after compaction (= `recordsRetained`). */
  linesAfterCompaction: number;
  /** Number of live records retained in the compacted file. */
  recordsRetained: number;
  /** Number of tombstone lines (`_deleted: true`) removed. */
  tombstonesRemoved: number;
  /** Number of stale-version lines (superseded updates) removed. */
  staleLinesRemoved: number;
  /** Wall-clock duration of the compaction operation in milliseconds. */
  durationMs: number;
  /** Size of `data.ndjson` after compaction, in bytes. */
  newFileSizeBytes: number;
}

// ---------------------------------------------------------------------------
// CompactOptions
// ---------------------------------------------------------------------------

/**
 * Options controlling the behaviour of `$compact`.
 */
export interface CompactOptions {
  /**
   * When `true`, compact even if the model is below the auto-compact
   * threshold. Default: `false`.
   */
  force?: boolean;
  /**
   * When `true`, compute what would be compacted but do not write
   * anything to disk. Returns `durationMs: 0` and `newFileSizeBytes: 0`.
   * Default: `false`.
   */
  dryRun?: boolean;
}

// ---------------------------------------------------------------------------
// OrionDBConfig
// ---------------------------------------------------------------------------

/**
 * Top-level configuration for an OrionDB instance.
 */
export interface OrionDBConfig {
  /** Folder path where the database files are stored. */
  dbLocation: string;
  /**
   * Model schemas, keyed by model name.
   * Parsed and validated during `$connect`.
   */
  schema?: SchemaDefinition;
  /**
   * How to handle a mismatch between the code-defined schema and the
   * schema snapshot on disk. Default: `'block'`.
   */
  schemaMismatchStrategy?: SchemaMismatchStrategy;
  /**
   * When `true`, auto-compaction runs after each delete operation.
   * Default: `true`.
   */
  autoCompact?: boolean;
  /**
   * Fraction of tombstone lines that triggers auto-compaction.
   * Default: 0.30 (30%).
   */
  autoCompactThreshold?: number;
  /**
   * When `true`, passing unknown fields in `create` or `update`
   * data throws a `ValidationError` instead of silently ignoring them.
   * Default: `false`.
   */
  strict?: boolean;
  /**
   * Minimum log level for internal OrionDB messages.
   * Default: `'warn'`.
   */
  logLevel?: LogLevel;
  /**
   * Optional lifecycle hooks invoked on connect and disconnect.
   * Hook failures are swallowed — they never cause `$connect` or
   * `$disconnect` to fail.
   */
  hooks?: LifecycleHooks;
}

// ---------------------------------------------------------------------------
// ModelClientConfig
// ---------------------------------------------------------------------------

/**
 * Configuration for a single model client instance.
 * Carries the per-model context plus all cross-model registries
 * needed to resolve relations and perform nested writes.
 */
export interface ModelClientConfig {
  /** The name of the model this client manages. */
  modelName: string;
  /** Resolved file-system paths for this model. */
  paths: ModelPaths;
  /** Parsed schema definition for this model. */
  schema: ParsedModelDefinition;
  /** In-memory index manager for this model. */
  indexManager: IndexManager<Record<string, unknown>>;
  /** In-memory file-size counter for this model's data file. */
  counter: FileSizeCounter;
  /** Full schema registry — all models. */
  allSchemas: Map<string, ParsedModelDefinition>;
  /** Full path registry — all models. */
  allPaths: Map<string, ModelPaths>;
  /** Full index manager registry — all models. */
  allIndexManagers: Map<string, IndexManager<Record<string, unknown>>>;
  /** Full file-size counter registry — all models. */
  allCounters: Map<string, FileSizeCounter>;
  /**
   * Auto-compaction threshold (fraction) to propagate from the
   * top-level config. Default: 0.30.
   */
  autoCompactThreshold?: number;
  /**
   * Getter that returns the current connection state from the shared
   * `ClientContext`. Always returns the live value — never a snapshot.
   */
  isConnectedGetter: () => boolean;
  /**
   * Shared operation tracker used to drain in-flight async operations
   * before `$disconnect` clears state.
   */
  operationTracker: OperationTrackerInterface;
  /**
   * When `true`, unknown fields in `create` or `update` data throw
   * `ValidationError` instead of being silently ignored.
   * Mirrors `OrionDBConfig.strict`. Default: `false`.
   */
  strict?: boolean;
  /**
   * Logger scoped to this OrionDB instance. Used for auto-compact
   * warnings and other diagnostic messages.
   */
  logger?: Logger;
  /**
   * Fire-and-forget callback invoked when the auto-compact threshold is
   * exceeded after a write. Signals the `OrionDB` instance to schedule
   * compaction asynchronously without blocking the write return path.
   * Never assigned directly by callers — wired by `createOrionDB` during
   * `$connect`.
   */
  onShouldCompact?: (modelName: string) => void;
}

// ---------------------------------------------------------------------------
// Input types — writes
// ---------------------------------------------------------------------------

/**
 * Input for `create()`. Creates a single record.
 */
export interface CreateInput {
  /** Field values for the new record. */
  data: Record<string, unknown>;
  /** Relation fields to resolve and attach to the returned record. */
  include?: IncludeClause;
  /** Scalar fields to include in the returned record. */
  select?: SelectInput;
}

/**
 * Input for `createMany()`. Creates multiple records.
 *
 * `skipDuplicates` is a Phase 2 feature — accepted in the API
 * but silently ignored by the Phase 1 implementation.
 */
export interface CreateManyInput {
  /** Array of field value objects for the new records. */
  data: Record<string, unknown>[];
  /**
   * Phase 2 — silently ignored in Phase 1.
   * When `true`, records whose unique fields collide with existing
   * records are skipped rather than throwing.
   */
  skipDuplicates?: boolean;
}

/**
 * Input for `update()`. Updates a single record identified by `where`.
 */
export interface UpdateInput {
  /** Filter identifying the single record to update. */
  where: WhereInput;
  /** Fields to update on the matched record. */
  data: Record<string, unknown>;
  /** Relation fields to resolve and attach to the returned record. */
  include?: IncludeClause;
  /** Scalar fields to include in the returned record. */
  select?: SelectInput;
}

/**
 * Input for `updateMany()`. Updates all records matching `where`.
 */
export interface UpdateManyInput {
  /** Filter identifying the records to update. Omit to update all. */
  where?: WhereInput;
  /** Fields to update on every matched record. */
  data: Record<string, unknown>;
}

/**
 * Input for `delete()`. Deletes a single record identified by `where`.
 */
export interface DeleteInput {
  /** Filter identifying the single record to delete. */
  where: WhereInput;
}

/**
 * Input for `deleteMany()`. Deletes all records matching `where`.
 */
export interface DeleteManyInput {
  /** Filter identifying the records to delete. Omit to delete all. */
  where?: WhereInput;
}

// ---------------------------------------------------------------------------
// Input types — reads
// ---------------------------------------------------------------------------

/**
 * Input for `findUnique()` and `findUniqueOrThrow()`.
 * `where` must target a primary key or unique field.
 */
export interface FindUniqueClientInput {
  /** Filter identifying the unique record. */
  where: WhereInput;
  /** Relation fields to resolve and attach to the returned record. */
  include?: IncludeClause;
  /** Scalar fields to include in the returned record. */
  select?: SelectInput;
}

/**
 * Input for `findFirst()`. Returns the first record matching `where`.
 */
export interface FindFirstClientInput {
  /** Filter applied to candidate records. */
  where?: WhereInput;
  /** Relation fields to resolve and attach to the returned record. */
  include?: IncludeClause;
  /** Scalar fields to include in the returned record. */
  select?: SelectInput;
  /** Sort order applied before taking the first match. */
  orderBy?: OrderByInput;
}

/**
 * Input for `findMany()`. Returns all records matching the filters.
 */
export interface FindManyClientInput {
  /** Filter applied to candidate records. */
  where?: WhereInput;
  /** Relation fields to resolve and attach to the returned records. */
  include?: IncludeClause;
  /** Scalar fields to include in the returned records. */
  select?: SelectInput;
  /** Sort order for the result set. */
  orderBy?: OrderByInput;
  /** Number of records to skip (offset). */
  skip?: number;
  /** Maximum number of records to return. */
  take?: number;
}

// ---------------------------------------------------------------------------
// Input types — count / aggregate
// ---------------------------------------------------------------------------

/**
 * Input for `count()`. Returns the number of matching records.
 */
export interface CountInput {
  /** Filter applied to candidate records. */
  where?: WhereInput;
}

/**
 * Input for `aggregate()`. Computes aggregations over matching records.
 */
export interface AggregateClientInput {
  /** Filter applied to candidate records. */
  where?: WhereInput;
  /** Count of records or per-field counts. */
  _count?: boolean | Record<string, boolean>;
  /** Average value per numeric field. */
  _avg?: Record<string, boolean>;
  /** Sum per numeric field. */
  _sum?: Record<string, boolean>;
  /** Minimum value per field. */
  _min?: Record<string, boolean>;
  /** Maximum value per field. */
  _max?: Record<string, boolean>;
}

/**
 * Input for `groupBy()`. Groups records by one or more fields
 * and computes per-group aggregations.
 */
export interface GroupByClientInput {
  /** Fields to group by. Must be non-empty. */
  by: string[];
  /** Filter applied before grouping. */
  where?: WhereInput;
  /** Count of records in each group. */
  _count?: boolean | Record<string, boolean>;
  /** Average value per numeric field within each group. */
  _avg?: Record<string, boolean>;
  /** Sum per numeric field within each group. */
  _sum?: Record<string, boolean>;
  /** Minimum value per field within each group. */
  _min?: Record<string, boolean>;
  /** Maximum value per field within each group. */
  _max?: Record<string, boolean>;
  /** Sort order applied to the group results. */
  orderBy?: OrderByInput;
  /** Number of groups to skip. */
  skip?: number;
  /** Maximum number of groups to return. */
  take?: number;
}

// ---------------------------------------------------------------------------
// Upsert
// ---------------------------------------------------------------------------

/**
 * Input for `upsert()`.
 * Attempts to update a record matching `where`; if none exists,
 * creates a new record using `create`.
 */
export interface UpsertInput {
  /** Filter identifying the record to update. */
  where: WhereInput;
  /** Data for the new record if no match is found. */
  create: Record<string, unknown>;
  /** Fields to update on the matched record. */
  update: Record<string, unknown>;
  /** Relation fields to resolve and attach to the returned record. */
  include?: IncludeClause;
  /** Scalar fields to include in the returned record. */
  select?: SelectInput;
}

// ---------------------------------------------------------------------------
// ModelClientMethods
// ---------------------------------------------------------------------------

/**
 * Full interface for a model client.
 * All methods are async and return Promises.
 */
export interface ModelClientMethods {
  create(input: CreateInput): Promise<Record<string, unknown>>;
  createMany(input: CreateManyInput): Promise<{ count: number }>;
  findUnique(input: FindUniqueClientInput): Promise<Record<string, unknown> | null>;
  findUniqueOrThrow(input: FindUniqueClientInput): Promise<Record<string, unknown>>;
  findFirst(input: FindFirstClientInput): Promise<Record<string, unknown> | null>;
  findMany(input: FindManyClientInput): Promise<Record<string, unknown>[]>;
  update(input: UpdateInput): Promise<Record<string, unknown>>;
  updateMany(input: UpdateManyInput): Promise<{ count: number }>;
  delete(input: DeleteInput): Promise<Record<string, unknown>>;
  deleteMany(input: DeleteManyInput): Promise<{ count: number }>;
  upsert(input: UpsertInput): Promise<Record<string, unknown>>;
  count(input?: CountInput): Promise<number>;
  aggregate(input: AggregateClientInput): Promise<AggregateResult>;
  groupBy(input: GroupByClientInput): Promise<GroupByResult[]>;
}

// ---------------------------------------------------------------------------
// OrionDBInstance / ModelRegistry / OrionDB
// ---------------------------------------------------------------------------

/**
 * A dynamic map of lowercased model names to their model client.
 */
export type ModelRegistry = {
  [modelName: string]: ModelClientMethods;
};

/**
 * Administrative methods on the top-level OrionDB instance.
 * All method names are prefixed with `$` to distinguish them
 * from model client accessors.
 */
export interface OrionDBInstance {
  /** Initializes all model directories and rebuilds in-memory indexes. */
  $connect(options?: ConnectOptions): Promise<void>;
  /** Flushes pending operations and releases file handles. */
  $disconnect(options?: DisconnectOptions): Promise<void>;
  /**
   * Triggers manual compaction.
   * @param modelName - Compact a single model; omit to compact all models.
   * @param options - Compaction options (`force`, `dryRun`).
   * @returns Array of `CompactionResult` — one per model actually compacted.
   *   Empty when the model is below threshold and `force` is not set.
   */
  $compact(modelName?: string, options?: CompactOptions): Promise<CompactionResult[]>;
  /**
   * Forces a full index rebuild from the NDJSON data file.
   * @param modelName - Rebuild for a single model; omit to rebuild all.
   */
  $rebuildIndexes(modelName?: string): Promise<void>;
  /**
   * Read-only `true` after `$connect()` resolves, `false` otherwise.
   * Accessible before `$connect()` — no connection guard applied.
   */
  readonly $isConnected: boolean;
}

/**
 * The top-level OrionDB type.
 * Merges administrative methods with the dynamic model registry:
 *
 * ```ts
 * await db.$connect()
 * await db.user.create({ data: { name: 'Alice' } })
 * ```
 */
export type OrionDB = OrionDBInstance & ModelRegistry;

// ---------------------------------------------------------------------------
// ClientContext
// ---------------------------------------------------------------------------

/**
 * Internal context assembled at `$connect` time and shared
 * across all model client method invocations.
 */
export interface ClientContext {
  /** Top-level database configuration. */
  config: OrionDBConfig;
  /** Resolved file-system paths for the database root. */
  dbPaths: DatabasePaths;
  /** Internal logger scoped to this instance. */
  logger: Logger;
  /** Parsed schema definitions for all models. */
  allSchemas: Map<string, ParsedModelDefinition>;
  /** Resolved file-system paths for all models. */
  allPaths: Map<string, ModelPaths>;
  /** In-memory index managers for all models. */
  allIndexManagers: Map<string, IndexManager<Record<string, unknown>>>;
  /** In-memory file-size counters for all models. */
  allCounters: Map<string, FileSizeCounter>;
  /** `true` after `$connect()` has completed successfully. */
  isConnected: boolean;
}

// ---------------------------------------------------------------------------
// StrictModeViolation
// ---------------------------------------------------------------------------

/**
 * Payload used as the `meta` of a `ValidationError` when
 * `config.strict === true` and an unknown field is passed
 * to `create` or `update`.
 */
export interface StrictModeViolation {
  /** Model on which the violation occurred. */
  model: string;
  /** The unknown field name. */
  field: string;
  /** The operation that triggered the violation. */
  operation: "create" | "update";
}

// ---------------------------------------------------------------------------
// ContextBuilder
// ---------------------------------------------------------------------------

/**
 * Factory type for constructing per-operation context objects
 * from the shared `ClientContext`.
 *
 * The implementation lives in `model-client.ts` and is exported
 * as a named constant.
 */
export type ContextBuilder = {
  writerCtx: (modelName: string, clientCtx: ClientContext) => ModelWriterContext;
  readerCtx: (modelName: string, clientCtx: ClientContext) => ModelReaderContext;
};

// ---------------------------------------------------------------------------
// StartupResult
// ---------------------------------------------------------------------------

/**
 * Diagnostic data returned per model after a successful `$connect` or
 * `$rebuildIndexes` call.
 */
export interface StartupResult {
  /** The model that was initialized. */
  modelName: string;
  /** Record count read from `meta.json` at startup. */
  recordCount: number;
  /** Number of live (non-deleted) records indexed in memory. */
  indexedCount: number;
  /** Current byte size of `data.ndjson` after initialization. */
  dataFileSizeBytes: number;
}

// ---------------------------------------------------------------------------
// ConnectOptions
// ---------------------------------------------------------------------------

/**
 * Optional arguments accepted by `$connect()`.
 */
export interface ConnectOptions {
  /**
   * When `true`, forces a full re-initialization even if `$connect()`
   * has already been called. Default: `false`.
   */
  force?: boolean;
}

// ---------------------------------------------------------------------------
// DisconnectOptions
// ---------------------------------------------------------------------------

/**
 * Optional arguments accepted by `$disconnect()`.
 */
export interface DisconnectOptions {
  /**
   * When `true`, skips draining in-flight operations and clears state
   * immediately. Default: `false`.
   */
  force?: boolean;
  /**
   * Milliseconds to wait for in-flight operations before forcing
   * disconnect. Default: `5000`. Use `0` for no ceiling.
   */
  timeout?: number;
}

// ---------------------------------------------------------------------------
// LifecycleHooks
// ---------------------------------------------------------------------------

/**
 * Optional hooks called at connection lifecycle events.
 * Hook failures are never propagated — they are passed to `onError` if
 * provided, then swallowed.
 */
export interface LifecycleHooks {
  /** Called at the end of a successful `$connect()`. */
  onConnect?: () => void | Promise<void>;
  /** Called at the end of `$disconnect()`, after state is fully cleared. */
  onDisconnect?: () => void | Promise<void>;
  /**
   * Called synchronously (never awaited) whenever a lifecycle hook throws.
   * Use this for error reporting or telemetry.
   */
  onError?: (error: unknown) => void;
}

// ---------------------------------------------------------------------------
// OperationTrackerInterface
// ---------------------------------------------------------------------------

/**
 * Structural interface for the private `OperationTracker` class.
 * Used as the type of `ModelClientConfig.operationTracker` so the
 * class itself does not need to be exported.
 */
export interface OperationTrackerInterface {
  /** Registers `operation` for drain tracking and returns it unchanged. */
  track<T>(operation: Promise<T>): Promise<T>;
  /**
   * Waits for all registered operations to settle (resolve or reject).
   * Returns immediately when there are no pending operations.
   */
  drain(): Promise<void>;
  /** Number of currently tracked (pending) operations. */
  readonly size: number;
}
