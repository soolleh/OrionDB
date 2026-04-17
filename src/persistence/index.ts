// src/persistence/index.ts — barrel re-exports only

export type {
  ModelMeta,
  DatabaseMeta,
  ModelPaths,
  DatabasePaths,
  CreateArgs,
  WriteResult,
  ModelWriterContext,
  CreateManyArgs,
  CreateManyResult,
} from "./types.js";

export {
  DATA_FILENAME,
  META_FILENAME,
  DB_META_FILENAME,
  SCHEMA_FILENAME,
  ORIONDB_VERSION,
  DATABASE_META_VERSION,
  NEWLINE,
} from "./types.js";

export {
  resolveDatabasePaths,
  resolveModelPaths,
  initializeDatabaseDirectory,
  initializeModelDirectory,
  initializeAllModelDirectories,
  updateModelMeta,
} from "./initializer.js";

export { FileSizeCounter, FileSizeCounterManager } from "./file-size-counter.js";

export { create, createMany } from "./writer.js";
