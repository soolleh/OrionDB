import { OrionDBError, OrionDBErrorOptions } from "./OrionDBError.js";

export class QueryError extends OrionDBError {
  constructor(message: string, options?: OrionDBErrorOptions) {
    super(message, "INVALID_QUERY", options);
  }
}
