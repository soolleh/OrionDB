import {
  CompactionError,
  OrionDBError,
  QueryError,
  RecordNotFoundError,
  RelationError,
  SchemaError,
  SchemaMismatchError,
  SchemaValidationError,
  UniqueConstraintError,
  ValidationError,
} from "../../../src/errors/index.js";

describe("ValidationError", () => {
  it("should have correct code", () => {
    const error = new ValidationError("name is required", {
      model: "User",
      field: "name",
    });

    expect(error.code).toBe("VALIDATION_ERROR");
  });

  it("should be instance of OrionDBError", () => {
    const error = new ValidationError("name is required");
    expect(error).toBeInstanceOf(OrionDBError);
  });
});

describe("CompactionError", () => {
  it("should have correct code", () => {
    const error = new CompactionError("compaction failed");
    expect(error.code).toBe("COMPACTION_ERROR");
  });

  it("should have correct name", () => {
    const error = new CompactionError("compaction failed");
    expect(error.name).toBe("CompactionError");
  });

  it("should be instance of OrionDBError", () => {
    const error = new CompactionError("compaction failed");
    expect(error).toBeInstanceOf(OrionDBError);
  });

  it("should set model and field from options", () => {
    const error = new CompactionError("compaction failed", {
      model: "User",
      field: "email",
    });
    expect(error.model).toBe("User");
    expect(error.field).toBe("email");
  });

  it("should have a stack trace", () => {
    const error = new CompactionError("compaction failed");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("UniqueConstraintError", () => {
  it("should have correct code", () => {
    const error = new UniqueConstraintError("unique constraint failed");
    expect(error.code).toBe("UNIQUE_CONSTRAINT_VIOLATION");
  });

  it("should have correct name", () => {
    const error = new UniqueConstraintError("unique constraint failed");
    expect(error.name).toBe("UniqueConstraintError");
  });

  it("should be instance of OrionDBError", () => {
    const error = new UniqueConstraintError("unique constraint failed");
    expect(error).toBeInstanceOf(OrionDBError);
  });

  it("should set model and field from options", () => {
    const error = new UniqueConstraintError("unique constraint failed", {
      model: "User",
      field: "email",
    });
    expect(error.model).toBe("User");
    expect(error.field).toBe("email");
  });

  it("should have a stack trace", () => {
    const error = new UniqueConstraintError("unique constraint failed");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("OrionDBError", () => {
  it("should have correct code", () => {
    const error = new OrionDBError("base error", "BASE_ERROR");
    expect(error.code).toBe("BASE_ERROR");
  });

  it("should have correct name", () => {
    const error = new OrionDBError("base error", "BASE_ERROR");
    expect(error.name).toBe("OrionDBError");
  });

  it("should set model and field from options", () => {
    const error = new OrionDBError("base error", "BASE_ERROR", {
      model: "User",
      field: "email",
    });
    expect(error.model).toBe("User");
    expect(error.field).toBe("email");
  });

  it("should set meta from options", () => {
    const error = new OrionDBError("base error", "BASE_ERROR", {
      meta: { extra: "info" },
    });
    expect(error.meta).toEqual({ extra: "info" });
  });

  it("should have a stack trace", () => {
    const error = new OrionDBError("base error", "BASE_ERROR");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("ValidationError", () => {
  it("should have correct code", () => {
    const error = new ValidationError("name is required");
    expect(error.code).toBe("VALIDATION_ERROR");
  });

  it("should have correct name", () => {
    const error = new ValidationError("name is required");
    expect(error.name).toBe("ValidationError");
  });

  it("should be instance of OrionDBError", () => {
    const error = new ValidationError("name is required");
    expect(error).toBeInstanceOf(OrionDBError);
  });

  it("should set model and field from options", () => {
    const error = new ValidationError("name is required", {
      model: "User",
      field: "name",
    });
    expect(error.model).toBe("User");
    expect(error.field).toBe("name");
  });

  it("should have a stack trace", () => {
    const error = new ValidationError("name is required");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("UniqueConstraintError", () => {
  it("should have correct code", () => {
    const error = new UniqueConstraintError("unique constraint failed");
    expect(error.code).toBe("UNIQUE_CONSTRAINT_VIOLATION");
  });

  it("should have correct name", () => {
    const error = new UniqueConstraintError("unique constraint failed");
    expect(error.name).toBe("UniqueConstraintError");
  });

  it("should be instance of OrionDBError", () => {
    const error = new UniqueConstraintError("unique constraint failed");
    expect(error).toBeInstanceOf(OrionDBError);
  });

  it("should set model and field from options", () => {
    const error = new UniqueConstraintError("unique constraint failed", {
      model: "User",
      field: "email",
    });
    expect(error.model).toBe("User");
    expect(error.field).toBe("email");
  });

  it("should have a stack trace", () => {
    const error = new UniqueConstraintError("unique constraint failed");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("RecordNotFoundError", () => {
  it("should have correct code", () => {
    const error = new RecordNotFoundError("record not found");
    expect(error.code).toBe("RECORD_NOT_FOUND");
  });

  it("should have correct name", () => {
    const error = new RecordNotFoundError("record not found");
    expect(error.name).toBe("RecordNotFoundError");
  });

  it("should be instance of OrionDBError", () => {
    const error = new RecordNotFoundError("record not found");
    expect(error).toBeInstanceOf(OrionDBError);
  });

  it("should set model and field from options", () => {
    const error = new RecordNotFoundError("record not found", {
      model: "User",
      field: "id",
    });
    expect(error.model).toBe("User");
    expect(error.field).toBe("id");
  });

  it("should have a stack trace", () => {
    const error = new RecordNotFoundError("record not found");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("QueryError", () => {
  it("should have correct code", () => {
    const error = new QueryError("invalid query");
    expect(error.code).toBe("INVALID_QUERY");
  });

  it("should have correct name", () => {
    const error = new QueryError("invalid query");
    expect(error.name).toBe("QueryError");
  });

  it("should be instance of OrionDBError", () => {
    const error = new QueryError("invalid query");
    expect(error).toBeInstanceOf(OrionDBError);
  });

  it("should set model and field from options", () => {
    const error = new QueryError("invalid query", {
      model: "User",
      field: "email",
    });
    expect(error.model).toBe("User");
    expect(error.field).toBe("email");
  });

  it("should have a stack trace", () => {
    const error = new QueryError("invalid query");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("SchemaError", () => {
  it("should not be instantiable directly", () => {
    // SchemaError is abstract — TypeScript prevents direct instantiation
    // This test documents that intent
    expect(SchemaMismatchError.prototype).toBeInstanceOf(SchemaError);
    expect(SchemaValidationError.prototype).toBeInstanceOf(SchemaError);
  });
});

describe("SchemaMismatchError", () => {
  it("should have correct code", () => {
    const error = new SchemaMismatchError("schema mismatch");
    expect(error.code).toBe("SCHEMA_MISMATCH");
  });

  it("should have correct name", () => {
    const error = new SchemaMismatchError("schema mismatch");
    expect(error.name).toBe("SchemaMismatchError");
  });

  it("should be instance of OrionDBError and SchemaError", () => {
    const error = new SchemaMismatchError("schema mismatch");
    expect(error).toBeInstanceOf(OrionDBError);
    expect(error).toBeInstanceOf(SchemaError);
  });

  it("should set meta from options", () => {
    const error = new SchemaMismatchError("schema mismatch", {
      meta: { diff: { added: ["name"], removed: [] } },
    });
    expect(error.meta).toEqual({ diff: { added: ["name"], removed: [] } });
  });

  it("should have a stack trace", () => {
    const error = new SchemaMismatchError("schema mismatch");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("SchemaValidationError", () => {
  it("should have correct code", () => {
    const error = new SchemaValidationError("schema validation failed");
    expect(error.code).toBe("SCHEMA_VALIDATION_ERROR");
  });

  it("should have correct name", () => {
    const error = new SchemaValidationError("schema validation failed");
    expect(error.name).toBe("SchemaValidationError");
  });

  it("should be instance of OrionDBError and SchemaError", () => {
    const error = new SchemaValidationError("schema validation failed");
    expect(error).toBeInstanceOf(OrionDBError);
    expect(error).toBeInstanceOf(SchemaError);
  });

  it("should set model from options", () => {
    const error = new SchemaValidationError("schema validation failed", {
      model: "User",
    });
    expect(error.model).toBe("User");
  });

  it("should have a stack trace", () => {
    const error = new SchemaValidationError("schema validation failed");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("RelationError", () => {
  it("should have correct code", () => {
    const error = new RelationError("relation error");
    expect(error.code).toBe("RELATION_ERROR");
  });

  it("should have correct name", () => {
    const error = new RelationError("relation error");
    expect(error.name).toBe("RelationError");
  });

  it("should be instance of OrionDBError", () => {
    const error = new RelationError("relation error");
    expect(error).toBeInstanceOf(OrionDBError);
  });

  it("should set model and field from options", () => {
    const error = new RelationError("relation error", {
      model: "Post",
      field: "authorId",
    });
    expect(error.model).toBe("Post");
    expect(error.field).toBe("authorId");
  });

  it("should have a stack trace", () => {
    const error = new RelationError("relation error");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});

describe("CompactionError", () => {
  it("should have correct code", () => {
    const error = new CompactionError("compaction failed");
    expect(error.code).toBe("COMPACTION_ERROR");
  });

  it("should have correct name", () => {
    const error = new CompactionError("compaction failed");
    expect(error.name).toBe("CompactionError");
  });

  it("should be instance of OrionDBError", () => {
    const error = new CompactionError("compaction failed");
    expect(error).toBeInstanceOf(OrionDBError);
  });

  it("should set model and field from options", () => {
    const error = new CompactionError("compaction failed", {
      model: "User",
      field: "email",
    });
    expect(error.model).toBe("User");
    expect(error.field).toBe("email");
  });

  it("should have a stack trace", () => {
    const error = new CompactionError("compaction failed");
    expect(error.stack).toBeDefined();
    expect(typeof error.stack).toBe("string");
  });
});
