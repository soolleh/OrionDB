import { OrionDBError, OrionDBErrorOptions } from "./OrionDBError.js";

export class RelationError extends OrionDBError {
  constructor(message: string, options?: OrionDBErrorOptions) {
    super(message, "RELATION_ERROR", options);
  }
}
