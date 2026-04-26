// src/client/model-client.ts
//
// ModelClient — per-model CRUD and query orchestration.
// Implemented in prompts 8.2 (reads), 8.3 (writes),
// 8.4 (relations + nested writes).

import type { ModelClientConfig, ModelClientMethods } from "./types.js";

/**
 * Creates a ModelClient instance for a single model.
 * Stub — not yet implemented.
 */
export const createModelClient = (_config: ModelClientConfig): ModelClientMethods => {
  const notImplemented = (): never => {
    throw new Error("ModelClient not yet implemented");
  };
  return {
    create: notImplemented,
    createMany: notImplemented,
    findUnique: notImplemented,
    findUniqueOrThrow: notImplemented,
    findFirst: notImplemented,
    findMany: notImplemented,
    update: notImplemented,
    updateMany: notImplemented,
    delete: notImplemented,
    deleteMany: notImplemented,
    upsert: notImplemented,
    count: notImplemented,
    aggregate: notImplemented,
    groupBy: notImplemented,
  };
};
