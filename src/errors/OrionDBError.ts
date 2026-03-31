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

    if ((Error as any).captureStackTrace) {
      (Error as any).captureStackTrace(this, this.constructor);
    }

    if (options) {
      this.model = options.model;
      this.field = options.field;
      this.meta = options.meta;
    }
  }
}
