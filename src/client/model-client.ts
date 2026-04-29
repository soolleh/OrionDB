// src/client/model-client.ts
//
// ModelClient — per-model CRUD and query orchestration.
// Implemented in prompts 8.2 (reads), 8.3 (writes),
// 8.4 (relations + nested writes).

import { QueryError } from "../errors/index.js";
import type { ModelClientConfig, ModelClientMethods } from "./types.js";

// ---------------------------------------------------------------------------
// assertConnected
// ---------------------------------------------------------------------------

/**
 * Throws an `OrionDBError` if `isConnected` is `false`.
 * Used as a guard at the top of every model client method to
 * ensure callers cannot invoke operations against a disconnected database.
 *
 * In practice, the proxy in `oriondb.ts` already blocks access to model
 * clients before `$connect()` resolves. This guard provides an additional
 * safety net for paths that bypass the proxy (e.g., tests, internals).
 */
export function assertConnected(isConnected: boolean, modelName: string, method: string): void {
  if (!isConnected) {
    throw new QueryError(`Cannot call "${method}" on model "${modelName}" before calling $connect().`, {
      model: modelName,
      meta: { method },
    });
  }
}

// ---------------------------------------------------------------------------
// createModelClient
// ---------------------------------------------------------------------------

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
