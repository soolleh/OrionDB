import { OrionDBError, OrionDBErrorOptions } from "./OrionDBError.js";

export class CompactionError extends OrionDBError {
  constructor(message: string, options?: OrionDBErrorOptions) {
    super(message, "COMPACTION_ERROR", options);
  }
}
