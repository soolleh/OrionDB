// src/client/index.ts — barrel re-exports only

export type {
  SchemaDefinition,
  OrionDBConfig,
  ModelClientConfig,
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
  ModelClientMethods,
  ModelRegistry,
  OrionDBInstance,
  OrionDB,
  ClientContext,
  StrictModeViolation,
  ContextBuilder,
  StartupResult,
  ConnectOptions,
  DisconnectOptions,
  LifecycleHooks,
  OperationTrackerInterface,
} from "./types.js";

export { createModelClient, assertConnected } from "./model-client.js";

export { createOrionDB } from "./oriondb.js";

export type { Logger, LogLevel } from "./logger.js";
export { createLogger } from "./logger.js";
