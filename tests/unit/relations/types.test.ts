// tests/unit/relations/types.test.ts

import { describe, it, expect } from "vitest";
import {
  INCLUDE_ALL,
  isIncludeAll,
  isIncludeObject,
  isNestedCreate,
  isNestedConnect,
  buildRelationDescriptor,
} from "../../../src/relations/index.js";
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
  relationFields: [string, ParsedRelationField][] = [],
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
    relationFields: new Map(relationFields),
  };
}

// ---------------------------------------------------------------------------
// isIncludeAll
// ---------------------------------------------------------------------------

describe("isIncludeAll()", () => {
  it("returns true for the INCLUDE_ALL constant", () => {
    expect(isIncludeAll(INCLUDE_ALL)).toBe(true);
  });

  it("returns true for literal true", () => {
    expect(isIncludeAll(true)).toBe(true);
  });

  it("returns false for an object include value with select", () => {
    expect(isIncludeAll({ select: { name: true } })).toBe(false);
  });

  it("returns false for an empty object", () => {
    expect(isIncludeAll({})).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// isIncludeObject
// ---------------------------------------------------------------------------

describe("isIncludeObject()", () => {
  it("returns true for an object include value", () => {
    expect(isIncludeObject({ select: { id: true } })).toBe(true);
  });

  it("returns true for an empty object", () => {
    expect(isIncludeObject({})).toBe(true);
  });

  it("returns false for INCLUDE_ALL / true", () => {
    expect(isIncludeObject(true)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// isNestedCreate
// ---------------------------------------------------------------------------

describe("isNestedCreate()", () => {
  it("returns true for an object with a create key", () => {
    expect(isNestedCreate({ create: { title: "Post" } })).toBe(true);
  });

  it("returns true for a combined create + connect object", () => {
    expect(isNestedCreate({ create: {}, connect: {} })).toBe(true);
  });

  it("returns false for an object without a create key", () => {
    expect(isNestedCreate({ connect: { id: "1" } })).toBe(false);
  });

  it("returns false for null", () => {
    expect(isNestedCreate(null)).toBe(false);
  });

  it("returns false for a string", () => {
    expect(isNestedCreate("hello")).toBe(false);
  });

  it("returns false for a number", () => {
    expect(isNestedCreate(42)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// isNestedConnect
// ---------------------------------------------------------------------------

describe("isNestedConnect()", () => {
  it("returns true for an object with a connect key", () => {
    expect(isNestedConnect({ connect: { id: "1" } })).toBe(true);
  });

  it("returns true for a combined create + connect object", () => {
    expect(isNestedConnect({ create: {}, connect: {} })).toBe(true);
  });

  it("returns false for an object without a connect key", () => {
    expect(isNestedConnect({ create: { title: "Post" } })).toBe(false);
  });

  it("returns false for null", () => {
    expect(isNestedConnect(null)).toBe(false);
  });

  it("returns false for a number", () => {
    expect(isNestedConnect(42)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// buildRelationDescriptor
// ---------------------------------------------------------------------------

describe("buildRelationDescriptor()", () => {
  describe("many-to-one", () => {
    it("sets ownerSide to 'declaring'", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>([["User", makeSchema("User", ["id", "name"])]]);
      const rf = makeRelField("author", "User", "authorId", "many-to-one");

      const descriptor = buildRelationDescriptor("author", rf, "Post", allSchemas);

      expect(descriptor.ownerSide).toBe("declaring");
    });

    it("returns correct field metadata for many-to-one", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>([["User", makeSchema("User", ["id", "name"])]]);
      const rf = makeRelField("author", "User", "authorId", "many-to-one");

      const descriptor = buildRelationDescriptor("author", rf, "Post", allSchemas);

      expect(descriptor.field).toBe("author");
      expect(descriptor.relatedModel).toBe("User");
      expect(descriptor.foreignKey).toBe("authorId");
      expect(descriptor.relationType).toBe("many-to-one");
    });
  });

  describe("one-to-many", () => {
    it("sets ownerSide to 'related'", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>([
        ["Post", makeSchema("Post", ["id", "authorId", "title"])],
      ]);
      const rf = makeRelField("posts", "Post", "authorId", "one-to-many");

      const descriptor = buildRelationDescriptor("posts", rf, "User", allSchemas);

      expect(descriptor.ownerSide).toBe("related");
    });

    it("returns correct field metadata for one-to-many", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>([
        ["Post", makeSchema("Post", ["id", "authorId", "title"])],
      ]);
      const rf = makeRelField("posts", "Post", "authorId", "one-to-many");

      const descriptor = buildRelationDescriptor("posts", rf, "User", allSchemas);

      expect(descriptor.relatedModel).toBe("Post");
      expect(descriptor.foreignKey).toBe("authorId");
      expect(descriptor.relationType).toBe("one-to-many");
    });
  });

  describe("one-to-one — FK on declaring model", () => {
    it("sets ownerSide to 'declaring' when FK is a scalar field on the declaring model", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>([
        ["Profile", makeSchema("Profile", ["id", "bio"])],
        // User has 'profileId' as a scalar field
        ["User", makeSchema("User", ["id", "name", "profileId"])],
      ]);
      const rf = makeRelField("profile", "Profile", "profileId", "one-to-one");

      const descriptor = buildRelationDescriptor("profile", rf, "User", allSchemas);

      expect(descriptor.ownerSide).toBe("declaring");
    });
  });

  describe("one-to-one — FK on related model", () => {
    it("sets ownerSide to 'related' when FK is not a scalar field on the declaring model", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>([
        // Profile holds 'userId' (FK pointing to User)
        ["Profile", makeSchema("Profile", ["id", "bio", "userId"])],
        // User does NOT have 'userId' scalar field
        ["User", makeSchema("User", ["id", "name"])],
      ]);
      const rf = makeRelField("profile", "Profile", "userId", "one-to-one");

      const descriptor = buildRelationDescriptor("profile", rf, "User", allSchemas);

      expect(descriptor.ownerSide).toBe("related");
    });

    it("sets ownerSide to 'related' when declaring model is absent from allSchemas (conservative fallback)", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>([
        ["Profile", makeSchema("Profile", ["id", "bio", "userId"])],
        // 'User' is NOT in allSchemas — FK existence check returns false
      ]);
      const rf = makeRelField("profile", "Profile", "userId", "one-to-one");

      const descriptor = buildRelationDescriptor("profile", rf, "User", allSchemas);

      expect(descriptor.ownerSide).toBe("related");
    });
  });

  describe("error cases", () => {
    it("throws RelationError when the related model schema is not found", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>(); // empty
      const rf = makeRelField("posts", "Post", "authorId", "one-to-many");

      expect(() => buildRelationDescriptor("posts", rf, "User", allSchemas)).toThrow(RelationError);
    });

    it("error message identifies the missing related model", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>();
      const rf = makeRelField("posts", "NonExistentModel", "authorId", "one-to-many");

      expect(() => buildRelationDescriptor("posts", rf, "User", allSchemas)).toThrow(/NonExistentModel/);
    });

    it("error message identifies the declaring model and field", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>();
      const rf = makeRelField("items", "Item", "refId", "one-to-many");

      expect(() => buildRelationDescriptor("items", rf, "Order", allSchemas)).toThrow(/items/);
    });

    it("throws RelationError (not a generic Error) for missing related schema", () => {
      const allSchemas = new Map<string, ParsedModelDefinition>();
      const rf = makeRelField("author", "MissingModel", "authorId", "many-to-one");

      let caught: unknown;
      try {
        buildRelationDescriptor("author", rf, "Post", allSchemas);
      } catch (e) {
        caught = e;
      }

      expect(caught).toBeInstanceOf(RelationError);
    });
  });
});
