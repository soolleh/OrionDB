import { OrionDBErrorOptions } from "./OrionDBError.js";
import { SchemaError } from "./SchemaError.js";

export class SchemaMismatchError extends SchemaError {
  constructor(message: string, options?: OrionDBErrorOptions) {
    super(message, "SCHEMA_MISMATCH", options);
  }
}
