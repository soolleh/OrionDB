import { SchemaValidationError, SchemaMismatchError } from "../../../src/errors/index.js";
import {
  validateRelationships,
  runStartupSchemaValidation,
  parseModelSchema,
  serializeSchema,
} from "../../../src/schema/index.js";
import type { ParsedModelDefinition, PersistedSchema } from "../../../src/schema/index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a ParsedModelDefinition without any relation fields. */
function makeScalarModel(name: string, extra: Parameters<typeof parseModelSchema>[1] = {}): ParsedModelDefinition {
  return parseModelSchema(name, {
    id: { type: "string", primary: true },
    name: { type: "string" },
    ...extra,
  });
}

/**
 * Build a bidirectional User <-> Post schema where:
 *   - User.posts  : one-to-many → Post (FK: 'authorId' on Post)
 *   - Post.author : many-to-one → User (FK: 'authorId' on Post)
 * This satisfies C3 (bidirectional requirement).
 */
function makeBidirectionalModels(): Map<string, ParsedModelDefinition> {
  const user = parseModelSchema("User", {
    id: { type: "string", primary: true },
    name: { type: "string" },
    posts: {
      type: "relation",
      model: "Post",
      foreignKey: "authorId",
      relation: "one-to-many",
    },
  });

  const post = parseModelSchema("Post", {
    id: { type: "string", primary: true },
    title: { type: "string" },
    authorId: { type: "string" },
    author: {
      type: "relation",
      model: "User",
      foreignKey: "authorId",
      relation: "many-to-one",
    },
  });

  return new Map([
    ["User", user],
    ["Post", post],
  ]);
}

/** Models with a valid one-to-one bidirectional relation. */
function makeOneToOneModels(): Map<string, ParsedModelDefinition> {
  const user = parseModelSchema("User", {
    id: { type: "string", primary: true },
    name: { type: "string" },
    profile: {
      type: "relation",
      model: "Profile",
      foreignKey: "userId",
      relation: "one-to-one",
    },
  });

  const profile = parseModelSchema("Profile", {
    id: { type: "string", primary: true },
    userId: { type: "string" },
    bio: { type: "string" },
    user: {
      type: "relation",
      model: "User",
      foreignKey: "userId",
      relation: "one-to-one",
    },
  });

  return new Map([
    ["User", user],
    ["Profile", profile],
  ]);
}

// ---------------------------------------------------------------------------
// validateRelationships()
// ---------------------------------------------------------------------------

describe("validateRelationships()", () => {
  // ─── No relations — should never throw ─────────────────────────────────

  describe("models with no relation fields", () => {
    it("does not throw for an empty models map", () => {
      expect(() => validateRelationships(new Map())).not.toThrow();
    });

    it("does not throw for a single model with no relations", () => {
      const models = new Map([["User", makeScalarModel("User")]]);

      expect(() => validateRelationships(models)).not.toThrow();
    });

    it("does not throw for multiple models with no relations", () => {
      const models = new Map([
        ["User", makeScalarModel("User")],
        ["Post", makeScalarModel("Post")],
      ]);

      expect(() => validateRelationships(models)).not.toThrow();
    });
  });

  // ─── Rule 1: referenced model must exist ────────────────────────────────

  describe("Rule 1 — referenced model must exist", () => {
    it("throws SchemaValidationError when referenced model does not exist", () => {
      // Post.author references 'User', but User is not in the map
      // Pass 1 catches this before Pass 2 runs for this field
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        author: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "many-to-one",
        },
      });
      const models = new Map([["Post", post]]);

      expect(() => validateRelationships(models)).toThrow(SchemaValidationError);
    });

    it("error from missing model includes the missing model name in meta", () => {
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        author: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "many-to-one",
        },
      });
      const models = new Map([["Post", post]]);

      try {
        validateRelationships(models);
        expect.fail("Expected SchemaValidationError to be thrown");
      } catch (error) {
        expect(error).toBeInstanceOf(SchemaValidationError);
        const e = error as SchemaValidationError;
        const meta = e.meta as Record<string, unknown>;
        expect(meta["missingModel"]).toBe("User");
      }
    });
  });

  // ─── Rule 4: no self-referential relations ───────────────────────────────

  describe("Rule 4 — no self-referential relations", () => {
    it("throws SchemaValidationError for a self-referential relation", () => {
      const category = parseModelSchema("Category", {
        id: { type: "string", primary: true },
        parentId: { type: "string" },
        parent: {
          type: "relation",
          model: "Category", // references itself
          foreignKey: "parentId",
          relation: "many-to-one",
        },
      });
      const models = new Map([["Category", category]]);

      expect(() => validateRelationships(models)).toThrow(SchemaValidationError);
    });

    it("error from self-reference includes declaring model in meta", () => {
      const category = parseModelSchema("Category", {
        id: { type: "string", primary: true },
        parentId: { type: "string" },
        parent: {
          type: "relation",
          model: "Category",
          foreignKey: "parentId",
          relation: "many-to-one",
        },
      });
      const models = new Map([["Category", category]]);

      try {
        validateRelationships(models);
        expect.fail("Expected SchemaValidationError to be thrown");
      } catch (error) {
        expect(error).toBeInstanceOf(SchemaValidationError);
        const e = error as SchemaValidationError;
        const meta = e.meta as Record<string, unknown>;
        expect(meta["declaringModel"]).toBe("Category");
        expect(meta["referencedModel"]).toBe("Category");
      }
    });
  });

  // ─── Rule 2: foreign key field existence ────────────────────────────────

  describe("Rule 2 — foreign key must exist on the correct model", () => {
    it("throws SchemaValidationError when one-to-many FK is missing on child (referenced) model", () => {
      // User.posts one-to-many Post with FK 'authorId' — but Post has NO authorId field
      // Post must also have a back-relation (C3), so we declare it with a valid FK scenario
      // We put the FK on User (wrong side) to trigger Rule 2
      const user = parseModelSchema("User", {
        id: { type: "string", primary: true },
        authorId: { type: "string" }, // FK on wrong side
        posts: {
          type: "relation",
          model: "Post",
          foreignKey: "authorId",
          relation: "one-to-many",
        },
      });
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        title: { type: "string" },
        // NO authorId here — but back-relation needed for C3
        user: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "many-to-one",
        },
      });
      const models = new Map([
        ["User", user],
        ["Post", post],
      ]);

      // Rule 2: one-to-many FK must be on Post, but Post has no authorId scalar field
      expect(() => validateRelationships(models)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError when many-to-one FK is missing on declaring model", () => {
      // Post.author many-to-one User — FK 'authorId' must be on Post, but it is not
      const user = parseModelSchema("User", {
        id: { type: "string", primary: true },
        name: { type: "string" },
        posts: {
          type: "relation",
          model: "Post",
          foreignKey: "authorId",
          relation: "one-to-many",
        },
      });
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        title: { type: "string" },
        // No 'authorId' scalar field here — FK is declared but missing
        author: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "many-to-one",
        },
      });
      const models = new Map([
        ["User", user],
        ["Post", post],
      ]);

      expect(() => validateRelationships(models)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError when one-to-one FK exists on neither side", () => {
      const user = parseModelSchema("User", {
        id: { type: "string", primary: true },
        name: { type: "string" },
        profile: {
          type: "relation",
          model: "Profile",
          foreignKey: "userId", // missing from both
          relation: "one-to-one",
        },
      });
      const profile = parseModelSchema("Profile", {
        id: { type: "string", primary: true },
        bio: { type: "string" },
        // No 'userId' scalar field
        user: {
          type: "relation",
          model: "User",
          foreignKey: "userId",
          relation: "one-to-one",
        },
      });
      const models = new Map([
        ["User", user],
        ["Profile", profile],
      ]);

      expect(() => validateRelationships(models)).toThrow(SchemaValidationError);
    });
  });

  // ─── Rule 3 (C3): bidirectional consistency ──────────────────────────────

  describe("Rule 3 — bidirectional consistency", () => {
    it("does not throw for a valid one-to-many / many-to-one pair", () => {
      expect(() => validateRelationships(makeBidirectionalModels())).not.toThrow();
    });

    it("does not throw for a valid one-to-one / one-to-one pair", () => {
      expect(() => validateRelationships(makeOneToOneModels())).not.toThrow();
    });

    it("throws SchemaValidationError when relation has no counterpart (unidirectional)", () => {
      // User.posts one-to-many Post, but Post has no back-relation to User
      const user = parseModelSchema("User", {
        id: { type: "string", primary: true },
        name: { type: "string" },
        posts: {
          type: "relation",
          model: "Post",
          foreignKey: "authorId",
          relation: "one-to-many",
        },
      });
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        title: { type: "string" },
        authorId: { type: "string" },
        // No back-relation to User
      });
      const models = new Map([
        ["User", user],
        ["Post", post],
      ]);

      expect(() => validateRelationships(models)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError when back-relation has mismatched foreignKey", () => {
      const user = parseModelSchema("User", {
        id: { type: "string", primary: true },
        name: { type: "string" },
        posts: {
          type: "relation",
          model: "Post",
          foreignKey: "authorId",
          relation: "one-to-many",
        },
      });
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        creatorId: { type: "string" },
        // Same model, but different FK
        author: {
          type: "relation",
          model: "User",
          foreignKey: "creatorId", // mismatch: 'creatorId' vs 'authorId'
          relation: "many-to-one",
        },
      });
      const models = new Map([
        ["User", user],
        ["Post", post],
      ]);

      expect(() => validateRelationships(models)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError when back-relation has incompatible relation type", () => {
      const user = parseModelSchema("User", {
        id: { type: "string", primary: true },
        name: { type: "string" },
        posts: {
          type: "relation",
          model: "Post",
          foreignKey: "authorId",
          relation: "one-to-many",
        },
      });
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        // Wrong: should be many-to-one, not one-to-one
        author: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "one-to-one",
        },
      });
      const models = new Map([
        ["User", user],
        ["Post", post],
      ]);

      expect(() => validateRelationships(models)).toThrow(SchemaValidationError);
    });

    it("error from incompatible relation type includes expected relation type in meta", () => {
      const user = parseModelSchema("User", {
        id: { type: "string", primary: true },
        name: { type: "string" },
        posts: {
          type: "relation",
          model: "Post",
          foreignKey: "authorId",
          relation: "one-to-many",
        },
      });
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        author: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "one-to-one", // wrong
        },
      });
      const models = new Map([
        ["User", user],
        ["Post", post],
      ]);

      try {
        validateRelationships(models);
        expect.fail("Expected SchemaValidationError to be thrown");
      } catch (error) {
        expect(error).toBeInstanceOf(SchemaValidationError);
        const e = error as SchemaValidationError;
        const meta = e.meta as Record<string, unknown>;
        expect(meta["expectedRelationB"]).toBe("many-to-one");
      }
    });

    it("error from missing counterpart includes the declaring and referenced model in meta", () => {
      const user = parseModelSchema("User", {
        id: { type: "string", primary: true },
        name: { type: "string" },
        posts: {
          type: "relation",
          model: "Post",
          foreignKey: "authorId",
          relation: "one-to-many",
        },
      });
      const post = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        title: { type: "string" },
        authorId: { type: "string" },
        // No back-relation
      });
      const models = new Map([
        ["User", user],
        ["Post", post],
      ]);

      try {
        validateRelationships(models);
        expect.fail("Expected SchemaValidationError to be thrown");
      } catch (error) {
        expect(error).toBeInstanceOf(SchemaValidationError);
        const e = error as SchemaValidationError;
        const meta = e.meta as Record<string, unknown>;
        expect(meta["declaringModel"]).toBe("User");
        expect(meta["referencedModel"]).toBe("Post");
      }
    });
  });
});

// ---------------------------------------------------------------------------
// runStartupSchemaValidation()
// ---------------------------------------------------------------------------

describe("runStartupSchemaValidation()", () => {
  let disk: PersistedSchema;

  beforeEach(() => {
    disk = serializeSchema(
      new Map([
        ["User", makeScalarModel("User")],
        ["Post", makeScalarModel("Post")],
      ]),
    );
  });

  it("does not throw for identical schemas with no relations", () => {
    const models = new Map([
      ["User", makeScalarModel("User")],
      ["Post", makeScalarModel("Post")],
    ]);

    expect(() => runStartupSchemaValidation(models, disk, "block")).not.toThrow();
  });

  it("does not throw when diskSchema is null (first-time init)", () => {
    const models = new Map([["User", makeScalarModel("User")]]);

    expect(() => runStartupSchemaValidation(models, null, "block")).not.toThrow();
  });

  it("does not throw for valid bidirectional relations with block strategy and matching disk", () => {
    const biModels = makeBidirectionalModels();
    const biDisk = serializeSchema(biModels);

    expect(() => runStartupSchemaValidation(biModels, biDisk, "block")).not.toThrow();
  });

  it("throws SchemaValidationError (not SchemaMismatchError) when relations are invalid", () => {
    // Self-referential relation — throws from validateRelationships before validateSchema
    const category = parseModelSchema("Category", {
      id: { type: "string", primary: true },
      parentId: { type: "string" },
      parent: {
        type: "relation",
        model: "Category",
        foreignKey: "parentId",
        relation: "many-to-one",
      },
    });
    const models = new Map([["Category", category]]);

    expect(() => runStartupSchemaValidation(models, null, "block")).toThrow(SchemaValidationError);
  });

  it("throws SchemaMismatchError for schema changes with block strategy after relations pass", () => {
    // Valid relations, but code has a new model not on disk
    const codeModels = new Map([
      ["User", makeScalarModel("User")],
      ["Post", makeScalarModel("Post")],
      ["Comment", makeScalarModel("Comment")], // new model not on disk
    ]);

    expect(() => runStartupSchemaValidation(codeModels, disk, "block")).toThrow(SchemaMismatchError);
  });

  it("does not throw with warn-and-continue when only additive changes are present", () => {
    const codeModels = new Map([
      ["User", makeScalarModel("User")],
      ["Post", makeScalarModel("Post")],
      ["Comment", makeScalarModel("Comment")],
    ]);
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    try {
      expect(() => runStartupSchemaValidation(codeModels, disk, "warn-and-continue")).not.toThrow();
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("relation errors take priority over mismatch errors", () => {
    // Invalid relation on a code model — this should throw SchemaValidationError, not SchemaMismatchError
    // even though there are also schema differences
    const post = parseModelSchema("Post", {
      id: { type: "string", primary: true },
      authorId: { type: "string" },
      author: {
        type: "relation",
        model: "NonExistent", // missing referenced model
        foreignKey: "authorId",
        relation: "many-to-one",
      },
    });
    const models = new Map([
      ["User", makeScalarModel("User")],
      ["Post", post],
    ]);

    try {
      runStartupSchemaValidation(models, disk, "block");
      expect.fail("Expected an error to be thrown");
    } catch (error) {
      // validateRelationships runs first and throws SchemaValidationError
      expect(error).toBeInstanceOf(SchemaValidationError);
    }
  });
});
