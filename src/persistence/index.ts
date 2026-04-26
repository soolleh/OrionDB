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
  UpdateArgs,
  UpdateManyArgs,
  UpdateManyResult,
  RawRecord,
  SelectClause,
  FindUniqueArgs,
  FindUniqueOrThrowArgs,
  ModelReaderContext,
  WhereClause,
  FilterFn,
  OrderByDirection,
  OrderByClause,
  ScanOptions,
  ScanResult,
  FindManyArgs,
  FindFirstArgs,
  DeleteArgs,
  DeleteManyArgs,
  DeleteManyResult,
} from "./types.js";

export {
  DATA_FILENAME,
  META_FILENAME,
  DB_META_FILENAME,
  SCHEMA_FILENAME,
  ORIONDB_VERSION,
  DATABASE_META_VERSION,
  NEWLINE,
  READ_BUFFER_SIZE,
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

export { create, createMany, update, updateMany, stripSystemFields, deleteRecord, deleteMany } from "./writer.js";

export { readRecordAtOffset, findUnique, findUniqueOrThrow, findMany, findFirst } from "./reader.js";
