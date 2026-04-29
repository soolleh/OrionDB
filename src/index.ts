/**
 * OrionDB — append-only NDJSON embedded database.
 *
 * Public API surface. Import everything from this file.
 * Do not import from internal module paths.
 *
 * @example
 * ```ts
 * import { createOrionDB } from 'oriondb'
 *
 * const db = createOrionDB({
 *   dbLocation: './data',
 *   schema: { User: { id: { type: 'string', primary: true } } },
 * })
 *
 * await db.$connect()
 * const user = await db.user.create({ data: { name: 'Alice' } })
 * await db.$disconnect()
 * ```
 */

// ── Entry point ─────────────────────────────────────────────
export { createOrionDB } from "./client/index.js";

// ── Top-level instance types ─────────────────────────────────────
export type {
  OrionDBConfig,
  OrionDB,
  OrionDBInstance,
  ModelRegistry,
  ModelClientMethods,
  ConnectOptions,
  DisconnectOptions,
  CompactOptions,
  CompactionResult,
  LifecycleHooks,
  StartupResult,
} from "./client/index.js";

// ── Method input types ───────────────────────────────────────────
export type {
  CreateInput,
  CreateManyInput,
  UpdateInput,
  UpdateManyInput,
  DeleteInput,
  DeleteManyInput,
  FindUniqueClientInput,
  FindFirstClientInput,
  FindManyClientInput,
  CountInput,
  AggregateClientInput,
  GroupByClientInput,
  UpsertInput,
} from "./client/index.js";

// ── Schema definition types ──────────────────────────────────────
export type { SchemaDefinition, ModelDefinition, FieldDefinition, RelationDefinition } from "./schema/index.js";

// ── Query clause types ───────────────────────────────────────────
export type { WhereInput, OrderByInput, SelectInput, AggregateResult, GroupByResult } from "./query/index.js";

// ── Include clause types ─────────────────────────────────────────
export type { IncludeClause, IncludeValue } from "./relations/index.js";

// ── Error classes ───────────────────────────────────────────────
export {
  OrionDBError,
  ValidationError,
  UniqueConstraintError,
  RecordNotFoundError,
  RelationError,
  CompactionError,
  SchemaError,
  QueryError,
} from "./errors/index.js";
