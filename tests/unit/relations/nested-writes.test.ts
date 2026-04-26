// tests/unit/relations/nested-writes.test.ts

import { describe, it, expect, vi } from "vitest";
import { extractNestedWrites, executeNestedWrites, resolveConnectForeignKey } from "../../../src/relations/index.js";
import type { NestedWriteOperation } from "../../../src/relations/index.js";
import type { ParsedModelDefinition, ParsedRelationField, ParsedField } from "../../../src/schema/index.js";
import { RelationError } from "../../../src/errors/index.js";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeScalarField(name: string): ParsedField {
  return {
    name,
    type: "string",
    primary: false,
    unique: false,
    required: false,
    hasDefault: false,
    defaultValue: undefined,
    enumValues: undefined,
  };
}

function makeRelField(
  name: string,
  model: string,
  foreignKey: string,
  relation: "many-to-one" | "one-to-many" | "one-to-one",
): ParsedRelationField {
  return { name, type: "relation", model, foreignKey, relation };
}

function makeSchema(
  name: string,
  scalarNames: string[] = ["id", "name"],
  relationEntries: [string, ParsedRelationField][] = [],
  primaryKeyField = "id",
): ParsedModelDefinition {
  const fields = new Map<string, ParsedField>();
  for (const fieldName of scalarNames) {
    fields.set(fieldName, makeScalarField(fieldName));
  }
  return {
    name,
    fields,
    primaryKeyField,
    uniqueFields: new Set<string>(),
    indexedFields: new Set<string>([primaryKeyField]),
    relationFields: new Map(relationEntries),
  };
}

/** Creates a mock execute context compatible with the unexported ExecuteContext interface. */
function makeMockExecute() {
  return {
    createRecord: vi.fn(
      (_modelName: string, data: Record<string, unknown>): Promise<Record<string, unknown>> =>
        Promise.resolve({ ...data }),
    ),
    updateRecord: vi.fn(
      (
        _modelName: string,
        _where: Record<string, unknown>,
        data: Record<string, unknown>,
      ): Promise<Record<string, unknown>> => Promise.resolve({ ...data }),
    ),
  };
}

// ---------------------------------------------------------------------------
// extractNestedWrites
// ---------------------------------------------------------------------------

describe("extractNestedWrites()", () => {
  it("returns empty operations and unchanged cleanData when no relation fields exist on schema", () => {
    const schema = makeSchema("User", ["id", "name"]);
    const allSchemas = new Map([["User", schema]]);
    const data = { id: "u1", name: "Alice" };

    const { cleanData, operations } = extractNestedWrites(data, schema, allSchemas);

    expect(cleanData).toEqual({ id: "u1", name: "Alice" });
    expect(operations).toHaveLength(0);
  });

  it("returns empty operations when relation field value is absent from data", () => {
    const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
    const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
    const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
    const allSchemas = new Map([
      ["User", userSchema],
      ["Post", postSchema],
    ]);

    const { cleanData, operations } = extractNestedWrites({ id: "u1", name: "Alice" }, userSchema, allSchemas);

    expect(cleanData).toEqual({ id: "u1", name: "Alice" });
    expect(operations).toHaveLength(0);
  });

  it("returns empty operations when relation field is explicitly null", () => {
    const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
    const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
    const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
    const allSchemas = new Map([
      ["User", userSchema],
      ["Post", postSchema],
    ]);

    const { operations } = extractNestedWrites({ id: "u1", name: "Alice", posts: null }, userSchema, allSchemas);

    expect(operations).toHaveLength(0);
  });

  describe("nested create", () => {
    it("extracts a one-to-many nested create operation", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const data = { id: "u1", name: "Alice", posts: { create: { id: "p1", title: "Post 1" } } };
      const { cleanData, operations } = extractNestedWrites(data, userSchema, allSchemas);

      expect(cleanData).toEqual({ id: "u1", name: "Alice" });
      expect(cleanData).not.toHaveProperty("posts");
      expect(operations).toHaveLength(1);
      expect(operations).toMatchObject([
        {
          relatedModel: "Post",
          foreignKey: "authorId",
          relationType: "one-to-many",
          records: [{ id: "p1", title: "Post 1" }],
        },
      ]);
    });

    it("sets foreignKeyValue to null at extraction time", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const { operations } = extractNestedWrites({ id: "u1", posts: { create: { id: "p1" } } }, userSchema, allSchemas);

      expect(operations).toMatchObject([{ foreignKeyValue: null }]);
    });

    it("normalises a single create object into an array of records", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const { operations } = extractNestedWrites({ id: "u1", posts: { create: { id: "p1" } } }, userSchema, allSchemas);

      expect(operations).toMatchObject([{ records: [{ id: "p1" }] }]);
    });

    it("supports an array of create records", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const { operations } = extractNestedWrites(
        { id: "u1", posts: { create: [{ id: "p1" }, { id: "p2" }] } },
        userSchema,
        allSchemas,
      );

      expect(operations).toMatchObject([{ records: [{ id: "p1" }, { id: "p2" }] }]);
    });

    it("records the parentField name from the schema relation", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const { operations } = extractNestedWrites({ id: "u1", posts: { create: { id: "p1" } } }, userSchema, allSchemas);

      expect(operations).toMatchObject([{ parentField: "posts" }]);
    });
  });

  describe("nested connect", () => {
    it("extracts a connect operation with _nestedOp marker", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const { operations } = extractNestedWrites(
        { id: "u1", posts: { connect: { id: "p1" } } },
        userSchema,
        allSchemas,
      );

      expect(operations).toMatchObject([{ records: [{ id: "p1", _nestedOp: "connect" }] }]);
    });

    it("normalises a single connect where clause into an array", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const { operations } = extractNestedWrites(
        { id: "u1", posts: { connect: { id: "p1" } } },
        userSchema,
        allSchemas,
      );

      expect(operations).toHaveLength(1);
      expect(operations[0]?.records).toHaveLength(1);
    });

    it("supports an array of connect where clauses", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const { operations } = extractNestedWrites(
        { id: "u1", posts: { connect: [{ id: "p1" }, { id: "p2" }] } },
        userSchema,
        allSchemas,
      );

      expect(operations[0]?.records).toHaveLength(2);
    });
  });

  describe("mixed create + connect on the same field", () => {
    it("produces two operations when both create and connect are provided", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const { operations } = extractNestedWrites(
        { id: "u1", posts: { create: { id: "p1" }, connect: { id: "p2" } } },
        userSchema,
        allSchemas,
      );

      expect(operations).toHaveLength(2);
      const createOp = operations.find((op) => !op.records.some((r) => r["_nestedOp"] === "connect"));
      const connectOp = operations.find((op) => op.records.some((r) => r["_nestedOp"] === "connect"));
      expect(createOp).toBeDefined();
      expect(connectOp).toBeDefined();
    });
  });

  describe("cleanData — relation field removal", () => {
    it("removes multiple relation fields from cleanData", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const commentRelField = makeRelField("comments", "Comment", "userId", "one-to-many");
      const userSchema = makeSchema(
        "User",
        ["id", "name"],
        [
          ["posts", postRelField],
          ["comments", commentRelField],
        ],
      );
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const commentSchema = makeSchema("Comment", ["id", "userId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
        ["Comment", commentSchema],
      ]);

      const { cleanData } = extractNestedWrites(
        {
          id: "u1",
          name: "Alice",
          posts: { create: { id: "p1" } },
          comments: { create: { id: "c1" } },
        },
        userSchema,
        allSchemas,
      );

      expect(cleanData).toEqual({ id: "u1", name: "Alice" });
    });

    it("does not mutate the original data object", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const data = { id: "u1", name: "Alice", posts: { create: { id: "p1" } } };
      extractNestedWrites(data, userSchema, allSchemas);

      expect(data).toHaveProperty("posts");
    });
  });

  describe("error cases", () => {
    it("throws RelationError when relation field value is an array", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      expect(() => extractNestedWrites({ id: "u1", posts: [] }, userSchema, allSchemas)).toThrow(RelationError);
    });

    it("throws RelationError when relation field value is a string", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      expect(() => extractNestedWrites({ id: "u1", posts: "invalid" }, userSchema, allSchemas)).toThrow(RelationError);
    });

    it("throws RelationError when relation object has neither create nor connect key", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      expect(() =>
        extractNestedWrites({ id: "u1", posts: { update: {} } } as Record<string, unknown>, userSchema, allSchemas),
      ).toThrow(RelationError);
    });

    it("throws RelationError when a create array contains a non-object element", () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      expect(() =>
        extractNestedWrites(
          { id: "u1", posts: { create: ["not-a-plain-object"] } } as Record<string, unknown>,
          userSchema,
          allSchemas,
        ),
      ).toThrow(RelationError);
    });
  });
});

// ---------------------------------------------------------------------------
// executeNestedWrites
// ---------------------------------------------------------------------------

describe("executeNestedWrites()", () => {
  it("does nothing and makes no calls when operations is empty", async () => {
    const execute = makeMockExecute();
    await executeNestedWrites([], "u1", execute);
    expect(execute.createRecord).not.toHaveBeenCalled();
    expect(execute.updateRecord).not.toHaveBeenCalled();
  });

  describe("create operations", () => {
    it("calls createRecord with FK injected for one-to-many", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "posts",
        relatedModel: "Post",
        foreignKey: "authorId",
        foreignKeyValue: null,
        relationType: "one-to-many",
        records: [{ id: "p1", title: "Hello" }],
      };

      await executeNestedWrites([op], "u1", execute);

      expect(execute.createRecord).toHaveBeenCalledOnce();
      expect(execute.createRecord).toHaveBeenCalledWith("Post", { id: "p1", title: "Hello", authorId: "u1" });
    });

    it("calls createRecord once per record in the operation", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "posts",
        relatedModel: "Post",
        foreignKey: "authorId",
        foreignKeyValue: null,
        relationType: "one-to-many",
        records: [{ id: "p1" }, { id: "p2" }, { id: "p3" }],
      };

      await executeNestedWrites([op], "u1", execute);

      expect(execute.createRecord).toHaveBeenCalledTimes(3);
    });

    it("uses the parentPk value as the FK on the child record", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "comments",
        relatedModel: "Comment",
        foreignKey: "postId",
        foreignKeyValue: null,
        relationType: "one-to-many",
        records: [{ body: "Nice!" }],
      };

      await executeNestedWrites([op], "post-99", execute);

      expect(execute.createRecord).toHaveBeenCalledWith("Comment", { body: "Nice!", postId: "post-99" });
    });

    it("calls createRecord with FK for one-to-one (FK on related side)", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "profile",
        relatedModel: "Profile",
        foreignKey: "userId",
        foreignKeyValue: null,
        relationType: "one-to-one",
        records: [{ bio: "Engineer" }],
      };

      await executeNestedWrites([op], "u1", execute);

      expect(execute.createRecord).toHaveBeenCalledWith("Profile", { bio: "Engineer", userId: "u1" });
    });

    it("throws RelationError for a many-to-one nested create", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "author",
        relatedModel: "User",
        foreignKey: "authorId",
        foreignKeyValue: null,
        relationType: "many-to-one",
        records: [{ id: "u1", name: "Alice" }],
      };

      await expect(executeNestedWrites([op], "p1", execute)).rejects.toThrow(RelationError);
    });

    it("error message for many-to-one create identifies the field", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "author",
        relatedModel: "User",
        foreignKey: "authorId",
        foreignKeyValue: null,
        relationType: "many-to-one",
        records: [{ id: "u1" }],
      };

      await expect(executeNestedWrites([op], "p1", execute)).rejects.toThrow(/author/);
    });
  });

  describe("connect operations", () => {
    it("calls updateRecord for one-to-many connect, setting FK on the related record", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "posts",
        relatedModel: "Post",
        foreignKey: "authorId",
        foreignKeyValue: null,
        relationType: "one-to-many",
        records: [{ id: "p1", _nestedOp: "connect" }],
      };

      await executeNestedWrites([op], "u1", execute);

      expect(execute.updateRecord).toHaveBeenCalledOnce();
      expect(execute.updateRecord).toHaveBeenCalledWith("Post", { id: "p1" }, { authorId: "u1" });
    });

    it("strips _nestedOp from the where clause before calling updateRecord", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "posts",
        relatedModel: "Post",
        foreignKey: "authorId",
        foreignKeyValue: null,
        relationType: "one-to-many",
        records: [{ id: "p1", _nestedOp: "connect" }],
      };

      await executeNestedWrites([op], "u1", execute);

      const whereArg = (execute.updateRecord.mock.calls[0] as unknown[])[1] as Record<string, unknown>;
      expect(whereArg).not.toHaveProperty("_nestedOp");
    });

    it("is a no-op for many-to-one connect", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "author",
        relatedModel: "User",
        foreignKey: "authorId",
        foreignKeyValue: null,
        relationType: "many-to-one",
        records: [{ id: "u1", _nestedOp: "connect" }],
      };

      await executeNestedWrites([op], "p1", execute);

      expect(execute.updateRecord).not.toHaveBeenCalled();
      expect(execute.createRecord).not.toHaveBeenCalled();
    });

    it("calls updateRecord once per connect entry", async () => {
      const execute = makeMockExecute();
      const op: NestedWriteOperation = {
        parentField: "posts",
        relatedModel: "Post",
        foreignKey: "authorId",
        foreignKeyValue: null,
        relationType: "one-to-many",
        records: [
          { id: "p1", _nestedOp: "connect" },
          { id: "p2", _nestedOp: "connect" },
          { id: "p3", _nestedOp: "connect" },
        ],
      };

      await executeNestedWrites([op], "u1", execute);

      expect(execute.updateRecord).toHaveBeenCalledTimes(3);
    });
  });

  describe("ordering guarantee — creates run before connects", () => {
    it("executes create ops before connect ops regardless of input order", async () => {
      const callOrder: string[] = [];
      const execute = {
        createRecord: vi.fn((): Promise<Record<string, unknown>> => {
          callOrder.push("create");
          return Promise.resolve({});
        }),
        updateRecord: vi.fn((): Promise<Record<string, unknown>> => {
          callOrder.push("connect");
          return Promise.resolve({});
        }),
      };

      const connectOp: NestedWriteOperation = {
        parentField: "comments",
        relatedModel: "Comment",
        foreignKey: "postId",
        foreignKeyValue: null,
        relationType: "one-to-many",
        records: [{ id: "c1", _nestedOp: "connect" }],
      };
      const createOp: NestedWriteOperation = {
        parentField: "tags",
        relatedModel: "Tag",
        foreignKey: "postId",
        foreignKeyValue: null,
        relationType: "one-to-many",
        records: [{ id: "t1" }],
      };

      // connect is passed BEFORE create — implementation must still run creates first
      await executeNestedWrites([connectOp, createOp], "post-1", execute);

      expect(callOrder).toEqual(["create", "connect"]);
    });
  });
});

// ---------------------------------------------------------------------------
// resolveConnectForeignKey
// ---------------------------------------------------------------------------

describe("resolveConnectForeignKey()", () => {
  function makeOperation(relatedModel: string, foreignKey: string): NestedWriteOperation {
    return {
      parentField: "author",
      relatedModel,
      foreignKey,
      foreignKeyValue: null,
      relationType: "many-to-one",
      records: [],
    };
  }

  it("returns the FK field name and PK value from the where clause", () => {
    const userSchema = makeSchema("User", ["id", "name"]);
    const allSchemas = new Map([["User", userSchema]]);

    const op = makeOperation("User", "authorId");
    const result = resolveConnectForeignKey(op, { id: "u1" }, allSchemas);

    expect(result.field).toBe("authorId");
    expect(result.value).toBe("u1");
  });

  it("returns the correct FK value when PK is numeric", () => {
    const userSchema = makeSchema("User", ["id", "name"]);
    const allSchemas = new Map([["User", userSchema]]);

    const op = makeOperation("User", "authorId");
    const result = resolveConnectForeignKey(op, { id: 42 }, allSchemas);

    expect(result.value).toBe(42);
  });

  it("throws RelationError when the related model schema is not found", () => {
    const allSchemas = new Map<string, ParsedModelDefinition>(); // empty
    const op = makeOperation("NonExistentModel", "authorId");

    expect(() => resolveConnectForeignKey(op, { id: "u1" }, allSchemas)).toThrow(RelationError);
  });

  it("error message identifies the missing related model", () => {
    const allSchemas = new Map<string, ParsedModelDefinition>();
    const op = makeOperation("MissingModel", "someFK");

    expect(() => resolveConnectForeignKey(op, { id: "x" }, allSchemas)).toThrow(/MissingModel/);
  });

  it("throws RelationError when where clause does not include the related model's primary key", () => {
    const userSchema = makeSchema("User", ["id", "name"]);
    const allSchemas = new Map([["User", userSchema]]);

    const op = makeOperation("User", "authorId");
    // where has 'email', not 'id'
    expect(() => resolveConnectForeignKey(op, { email: "alice@test.com" }, allSchemas)).toThrow(RelationError);
  });

  it("throws RelationError when the PK value is null in the where clause", () => {
    const userSchema = makeSchema("User", ["id", "name"]);
    const allSchemas = new Map([["User", userSchema]]);

    const op = makeOperation("User", "authorId");
    expect(() => resolveConnectForeignKey(op, { id: null }, allSchemas)).toThrow(RelationError);
  });

  it("throws RelationError when the PK value is undefined in the where clause", () => {
    const userSchema = makeSchema("User", ["id", "name"]);
    const allSchemas = new Map([["User", userSchema]]);

    const op = makeOperation("User", "authorId");
    expect(() => resolveConnectForeignKey(op, { id: undefined }, allSchemas)).toThrow(RelationError);
  });
});
