import { OrionDBError, OrionDBErrorOptions } from "./OrionDBError.js";

export class RecordNotFoundError extends OrionDBError {
  constructor(message: string, options?: OrionDBErrorOptions) {
    super(message, "RECORD_NOT_FOUND", options);
  }
}
