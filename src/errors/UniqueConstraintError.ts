import { OrionDBError, OrionDBErrorOptions } from "./OrionDBError.js";

export class UniqueConstraintError extends OrionDBError {
  constructor(message: string, options?: OrionDBErrorOptions) {
    super(message, "UNIQUE_CONSTRAINT_VIOLATION", options);
  }
}
