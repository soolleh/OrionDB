import { OrionDBErrorOptions } from "./OrionDBError.js";
import { SchemaError } from "./SchemaError.js";

export class SchemaValidationError extends SchemaError {
  constructor(message: string, options?: OrionDBErrorOptions) {
    super(message, "SCHEMA_VALIDATION_ERROR", options);
  }
}
