export interface OrionDBErrorOptions {
  model?: string;
  field?: string;
  meta?: unknown;
}

export class OrionDBError extends Error {
  code: string;
  model?: string;
  field?: string;
  meta?: unknown;

  constructor(message: string, code: string, options?: OrionDBErrorOptions) {
    super(message);

    this.name = this.constructor.name;
    this.code = code;

    const errorCtor = Error as { captureStackTrace?: (target: object, fn: object) => void };
    if (errorCtor.captureStackTrace) {
      errorCtor.captureStackTrace(this, this.constructor);
    }

    if (options) {
      this.model = options.model;
      this.field = options.field;
      this.meta = options.meta;
    }
  }
}
