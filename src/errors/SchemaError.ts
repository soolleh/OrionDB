import { OrionDBError, OrionDBErrorOptions } from "./OrionDBError.js";

export abstract class SchemaError extends OrionDBError {
  constructor(message: string, code: string, options?: OrionDBErrorOptions) {
    super(message, code, options);
  }
}
