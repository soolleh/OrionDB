import { OrionDBError, OrionDBErrorOptions } from "./OrionDBError.js";

export class ValidationError extends OrionDBError {
  constructor(message: string, options?: OrionDBErrorOptions) {
    super(message, "VALIDATION_ERROR", options);
  }
}
