// tests/unit/relations/resolver.test.ts

import { describe, it, expect, vi } from "vitest";
import { resolveIncludes, attachIncludes } from "../../../src/relations/index.js";
import type { RelationResolverContext, FindManyForResolver, IncludeResult } from "../../../src/relations/index.js";
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
  scalarNames: string[],
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

function makeContext(
  modelName: string,
  schema: ParsedModelDefinition,
  allSchemas: Map<string, ParsedModelDefinition>,
  findMany: FindManyForResolver,
): RelationResolverContext {
  return { modelName, schema, allSchemas, findMany };
}

// ---------------------------------------------------------------------------
// resolveIncludes
// ---------------------------------------------------------------------------

describe("resolveIncludes()", () => {
  it("returns an empty Map when parentRecords is empty", async () => {
    const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));
    const userSchema = makeSchema("User", ["id", "name"]);
    const allSchemas = new Map([["User", userSchema]]);
    const ctx = makeContext("User", userSchema, allSchemas, findMany);

    const result = await resolveIncludes(ctx, [], { posts: true });

    expect(result.size).toBe(0);
    expect(findMany).not.toHaveBeenCalled();
  });

  describe("one-to-many include", () => {
    it("groups related records by FK and attaches arrays to each parent", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const allPosts: Record<string, unknown>[] = [
        { id: "p1", authorId: "u1", title: "Post A" },
        { id: "p2", authorId: "u1", title: "Post B" },
        { id: "p3", authorId: "u2", title: "Post C" },
      ];
      const findMany: FindManyForResolver = vi.fn(
        (_modelName: string, filter: (r: Record<string, unknown>) => boolean) =>
          Promise.resolve(allPosts.filter(filter)),
      );

      const parentRecords = [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
      ];

      const ctx = makeContext("User", userSchema, allSchemas, findMany);
      const result = await resolveIncludes(ctx, parentRecords, { posts: true });

      expect(result.get("u1")).toEqual({
        posts: [
          { id: "p1", authorId: "u1", title: "Post A" },
          { id: "p2", authorId: "u1", title: "Post B" },
        ],
      });
      expect(result.get("u2")).toEqual({
        posts: [{ id: "p3", authorId: "u2", title: "Post C" }],
      });
    });

    it("calls findMany exactly once for all parents (N+1 prevention)", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));

      const parentRecords = [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
        { id: "u3", name: "Charlie" },
      ];

      const ctx = makeContext("User", userSchema, allSchemas, findMany);
      await resolveIncludes(ctx, parentRecords, { posts: true });

      expect(findMany).toHaveBeenCalledTimes(1);
    });

    it("attaches an empty array for parents with no related records", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));
      const ctx = makeContext("User", userSchema, allSchemas, findMany);

      const result = await resolveIncludes(ctx, [{ id: "u1", name: "Alice" }], { posts: true });

      expect(result.get("u1")).toEqual({ posts: [] });
    });
  });

  describe("many-to-one include", () => {
    it("attaches a single record (not an array) for each parent", async () => {
      const authorRelField = makeRelField("author", "User", "authorId", "many-to-one");
      const postSchema = makeSchema("Post", ["id", "authorId", "title"], [["author", authorRelField]]);
      const userSchema = makeSchema("User", ["id", "name"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const allUsers: Record<string, unknown>[] = [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
      ];
      const findMany: FindManyForResolver = vi.fn(
        (_modelName: string, filter: (r: Record<string, unknown>) => boolean) =>
          Promise.resolve(allUsers.filter(filter)),
      );

      const parentRecords = [
        { id: "p1", authorId: "u1", title: "Post A" },
        { id: "p2", authorId: "u2", title: "Post B" },
      ];

      const ctx = makeContext("Post", postSchema, allSchemas, findMany);
      const result = await resolveIncludes(ctx, parentRecords, { author: true });

      expect(result.get("p1")).toEqual({ author: { id: "u1", name: "Alice" } });
      expect(result.get("p2")).toEqual({ author: { id: "u2", name: "Bob" } });
    });

    it("returns null when the related record is not found (many-to-one)", async () => {
      const authorRelField = makeRelField("author", "User", "authorId", "many-to-one");
      const postSchema = makeSchema("Post", ["id", "authorId", "title"], [["author", authorRelField]]);
      const userSchema = makeSchema("User", ["id", "name"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));
      const ctx = makeContext("Post", postSchema, allSchemas, findMany);

      const result = await resolveIncludes(ctx, [{ id: "p1", authorId: "missing-user", title: "Post" }], {
        author: true,
      });

      expect(result.get("p1")).toEqual({ author: null });
    });

    it("calls findMany exactly once for a many-to-one include", async () => {
      const authorRelField = makeRelField("author", "User", "authorId", "many-to-one");
      const postSchema = makeSchema("Post", ["id", "authorId", "title"], [["author", authorRelField]]);
      const userSchema = makeSchema("User", ["id", "name"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));
      const ctx = makeContext("Post", postSchema, allSchemas, findMany);

      await resolveIncludes(
        ctx,
        [
          { id: "p1", authorId: "u1" },
          { id: "p2", authorId: "u2" },
        ],
        { author: true },
      );

      expect(findMany).toHaveBeenCalledTimes(1);
    });
  });

  describe("select projection", () => {
    it("applies select to included one-to-many records — only selected fields returned", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      // FK (authorId) must be in the select so the groupByField step can match records to parents
      const findMany: FindManyForResolver = vi.fn(() =>
        Promise.resolve([{ id: "p1", authorId: "u1", title: "Post A" }]),
      );
      const ctx = makeContext("User", userSchema, allSchemas, findMany);

      const result = await resolveIncludes(ctx, [{ id: "u1", name: "Alice" }], {
        posts: { select: { authorId: true, title: true } },
      });

      expect(result.get("u1")?.["posts"]).toEqual([{ authorId: "u1", title: "Post A" }]);
    });

    it("excludes un-selected fields from projected records", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      // Include FK + title; verify that 'id' is excluded
      const findMany: FindManyForResolver = vi.fn(() =>
        Promise.resolve([{ id: "p1", authorId: "u1", title: "Post A" }]),
      );
      const ctx = makeContext("User", userSchema, allSchemas, findMany);

      const result = await resolveIncludes(ctx, [{ id: "u1", name: "Alice" }], {
        posts: { select: { authorId: true, title: true } },
      });

      const posts = result.get("u1")?.["posts"] as Record<string, unknown>[] | undefined;
      const firstPost = Array.isArray(posts) ? posts[0] : undefined;
      expect(firstPost).toBeDefined();
      expect(firstPost?.["id"]).toBeUndefined();
    });

    it("returns all fields when include value is INCLUDE_ALL (true)", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
      ]);

      const findMany: FindManyForResolver = vi.fn(() =>
        Promise.resolve([{ id: "p1", authorId: "u1", title: "Post A" }]),
      );
      const ctx = makeContext("User", userSchema, allSchemas, findMany);

      const result = await resolveIncludes(ctx, [{ id: "u1", name: "Alice" }], { posts: true });

      expect(result.get("u1")?.["posts"]).toEqual([{ id: "p1", authorId: "u1", title: "Post A" }]);
    });
  });

  describe("multiple include fields", () => {
    it("executes one findMany call per relation field for multiple includes", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const profileRelField = makeRelField("profile", "Profile", "userId", "one-to-one");
      const userSchema = makeSchema(
        "User",
        ["id", "name"],
        [
          ["posts", postRelField],
          ["profile", profileRelField],
        ],
      );
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const profileSchema = makeSchema("Profile", ["id", "userId", "bio"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
        ["Profile", profileSchema],
      ]);

      const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));
      const ctx = makeContext("User", userSchema, allSchemas, findMany);

      await resolveIncludes(ctx, [{ id: "u1", name: "Alice" }], { posts: true, profile: true });

      // One call per relation field — never per parent record
      expect(findMany).toHaveBeenCalledTimes(2);
    });

    it("resolves multiple includes independently and combines into one entry per parent", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const profileRelField = makeRelField("profile", "Profile", "userId", "one-to-one");
      const userSchema = makeSchema(
        "User",
        ["id", "name"],
        [
          ["posts", postRelField],
          ["profile", profileRelField],
        ],
      );
      const postSchema = makeSchema("Post", ["id", "authorId", "title"]);
      const profileSchema = makeSchema("Profile", ["id", "userId", "bio"]);
      const allSchemas = new Map([
        ["User", userSchema],
        ["Post", postSchema],
        ["Profile", profileSchema],
      ]);

      const posts: Record<string, unknown>[] = [{ id: "p1", authorId: "u1", title: "Hello" }];
      const profiles: Record<string, unknown>[] = [{ id: "pr1", userId: "u1", bio: "dev" }];
      const findMany: FindManyForResolver = vi.fn(
        (modelName: string, filter: (r: Record<string, unknown>) => boolean) => {
          if (modelName === "Post") return Promise.resolve(posts.filter(filter));
          if (modelName === "Profile") return Promise.resolve(profiles.filter(filter));
          return Promise.resolve([]);
        },
      );

      const ctx = makeContext("User", userSchema, allSchemas, findMany);
      const result = await resolveIncludes(ctx, [{ id: "u1", name: "Alice" }], {
        posts: true,
        profile: true,
      });

      const entry = result.get("u1");
      expect(entry?.["posts"]).toEqual([{ id: "p1", authorId: "u1", title: "Hello" }]);
      expect(entry?.["profile"]).toEqual({ id: "pr1", userId: "u1", bio: "dev" });
    });
  });

  describe("error cases", () => {
    it("throws RelationError for an unknown relation field in the include clause", async () => {
      const userSchema = makeSchema("User", ["id", "name"]);
      const allSchemas = new Map([["User", userSchema]]);
      const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));
      const ctx = makeContext("User", userSchema, allSchemas, findMany);

      await expect(resolveIncludes(ctx, [{ id: "u1", name: "Alice" }], { unknownField: true })).rejects.toThrow(
        RelationError,
      );
    });

    it("error message identifies the unknown field", async () => {
      const userSchema = makeSchema("User", ["id", "name"]);
      const allSchemas = new Map([["User", userSchema]]);
      const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));
      const ctx = makeContext("User", userSchema, allSchemas, findMany);

      await expect(resolveIncludes(ctx, [{ id: "u1" }], { nonExistentRelation: true })).rejects.toThrow(
        /nonExistentRelation/,
      );
    });

    it("throws RelationError when the related model schema is missing during resolution", async () => {
      const postRelField = makeRelField("posts", "Post", "authorId", "one-to-many");
      const userSchema = makeSchema("User", ["id", "name"], [["posts", postRelField]]);
      // Post schema is deliberately absent
      const allSchemas = new Map([["User", userSchema]]);
      const findMany = vi.fn((): Promise<Record<string, unknown>[]> => Promise.resolve([]));
      const ctx = makeContext("User", userSchema, allSchemas, findMany);

      await expect(resolveIncludes(ctx, [{ id: "u1" }], { posts: true })).rejects.toThrow(RelationError);
    });
  });
});

// ---------------------------------------------------------------------------
// attachIncludes
// ---------------------------------------------------------------------------

describe("attachIncludes()", () => {
  it("returns a new array with include data merged into each record", () => {
    const includeResult: IncludeResult = new Map([
      ["u1", { posts: [{ id: "p1", title: "Hello" }] }],
      ["u2", { posts: [] }],
    ]);

    const parents = [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ];

    const result = attachIncludes(parents, includeResult, "id");

    expect(result[0]).toEqual({ id: "u1", name: "Alice", posts: [{ id: "p1", title: "Hello" }] });
    expect(result[1]).toEqual({ id: "u2", name: "Bob", posts: [] });
  });

  it("does not mutate the original record objects", () => {
    const parent = { id: "u1", name: "Alice" };
    const includeResult: IncludeResult = new Map([["u1", { posts: [] }]]);

    attachIncludes([parent], includeResult, "id");

    expect(parent).toEqual({ id: "u1", name: "Alice" });
    expect(Object.keys(parent)).not.toContain("posts");
  });

  it("does not mutate the input array", () => {
    const parents = [{ id: "u1", name: "Alice" }];
    const includeResult: IncludeResult = new Map([["u1", { posts: [] }]]);

    const result = attachIncludes(parents, includeResult, "id");

    expect(result).not.toBe(parents);
  });

  it("returns the original record reference when no entry exists in includeResult", () => {
    const parent = { id: "u1", name: "Alice" };
    const includeResult: IncludeResult = new Map(); // empty

    const result = attachIncludes([parent], includeResult, "id");

    expect(result[0]).toBe(parent);
  });

  it("returns the original record reference when PK cannot be stringified (non-primitive)", () => {
    // record has no 'id' field — pkToString(undefined) returns undefined
    const parent: Record<string, unknown> = { name: "NoId" };
    const includeResult: IncludeResult = new Map([["undefined", { posts: [] }]]);

    const result = attachIncludes([parent], includeResult, "id");

    expect(result[0]).toBe(parent);
  });

  it("handles numeric PK values", () => {
    const includeResult: IncludeResult = new Map([["42", { tags: ["ts", "node"] }]]);
    const parent = { id: 42, name: "Post" };

    const result = attachIncludes([parent], includeResult, "id");

    expect(result[0]).toEqual({ id: 42, name: "Post", tags: ["ts", "node"] });
  });

  it("processes multiple parents without cross-contamination", () => {
    const includeResult: IncludeResult = new Map([
      ["u1", { posts: [{ id: "p1" }] }],
      ["u2", { posts: [{ id: "p2" }, { id: "p3" }] }],
    ]);

    const parents = [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ];

    const result = attachIncludes(parents, includeResult, "id");

    expect(result[0]?.["posts"]).toEqual([{ id: "p1" }]);
    expect(result[1]?.["posts"]).toEqual([{ id: "p2" }, { id: "p3" }]);
  });
});
