import { SchemaValidationError } from "../../../src/errors/index.js";
import { parseModelSchema, isValidFieldValue } from "../../../src/schema/index.js";
import type { SchemaInput } from "../../../src/schema/index.js";

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

const minimalInput: SchemaInput = {
  id: { type: "string", primary: true },
  name: { type: "string" },
};

const fullInput: SchemaInput = {
  id: { type: "string", primary: true, default: () => "generated" },
  name: { type: "string", required: true },
  age: { type: "number", unique: true },
  active: { type: "boolean", default: true },
  born: { type: "date" },
  meta: { type: "json", default: {} },
  status: { type: "enum", values: ["active", "inactive"], default: "active" },
  authorId: { type: "string" },
  author: {
    type: "relation",
    model: "User",
    foreignKey: "authorId",
    relation: "many-to-one",
  },
};

// ---------------------------------------------------------------------------
// parseModelSchema()
// ---------------------------------------------------------------------------

describe("parseModelSchema()", () => {
  // -------------------------------------------------------------------------
  describe("model-level validation", () => {
    it("throws SchemaValidationError for empty model name", () => {
      expect(() => parseModelSchema("", minimalInput)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for model name starting with digit", () => {
      expect(() => parseModelSchema("1User", minimalInput)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for model name with spaces", () => {
      expect(() => parseModelSchema("My Model", minimalInput)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for model name with hyphens", () => {
      expect(() => parseModelSchema("my-model", minimalInput)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for model name with special characters", () => {
      expect(() => parseModelSchema("User!", minimalInput)).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for schema with zero fields", () => {
      expect(() => parseModelSchema("User", {})).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for reserved field name _deleted", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          _deleted: { type: "boolean" },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for reserved field name _createdAt", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          _createdAt: { type: "string" },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for reserved field name _updatedAt", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          _updatedAt: { type: "string" },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for zero primary key fields", () => {
      expect(() =>
        parseModelSchema("User", {
          name: { type: "string" },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for more than one primary key field", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          uuid: { type: "string", primary: true },
          name: { type: "string" },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for primary key on boolean field", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "boolean", primary: true } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for primary key on date field", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "date", primary: true } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for primary key on json field", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "json", primary: true } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for primary key on enum field", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "enum", values: ["a", "b"], primary: true } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for unique on boolean field", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          flag: { type: "boolean", unique: true } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for unique on json field", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          data: { type: "json", unique: true } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for unique on enum field", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          status: { type: "enum", values: ["a", "b"], unique: true } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });
  });

  // -------------------------------------------------------------------------
  describe("field-level validation — number", () => {
    it("throws SchemaValidationError for NaN as static default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          score: { type: "number", default: NaN },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for Infinity as static default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          score: { type: "number", default: Infinity },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for -Infinity as static default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          score: { type: "number", default: -Infinity },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("accepts valid integer default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          score: { type: "number", default: 42 },
        }),
      ).not.toThrow();
    });

    it("accepts valid float default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          score: { type: "number", default: 3.14 },
        }),
      ).not.toThrow();
    });
  });

  // -------------------------------------------------------------------------
  describe("field-level validation — date", () => {
    it("throws SchemaValidationError for invalid string date default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          born: { type: "date", default: "not-a-date" },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for non-Date non-string default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          born: { type: "date", default: 12345 as unknown as Date },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("accepts Date instance as default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          born: { type: "date", default: new Date("2024-01-01") },
        }),
      ).not.toThrow();
    });

    it("accepts valid ISO 8601 string as default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          born: { type: "date", default: "2024-01-01T00:00:00.000Z" },
        }),
      ).not.toThrow();
    });
  });

  // -------------------------------------------------------------------------
  describe("field-level validation — json", () => {
    it("throws SchemaValidationError for array as static default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          data: { type: "json", default: [] as unknown as Record<string, unknown> },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for null as static default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          data: { type: "json", default: null as unknown as Record<string, unknown> },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for primitive as static default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          data: { type: "json", default: 42 as unknown as Record<string, unknown> },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("accepts plain object as static default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          data: { type: "json", default: { key: "value" } },
        }),
      ).not.toThrow();
    });
  });

  // -------------------------------------------------------------------------
  describe("field-level validation — enum", () => {
    it("throws SchemaValidationError for missing values array", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          status: { type: "enum", values: undefined as unknown as string[] },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for empty values array", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          status: { type: "enum", values: [] },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for values containing empty string", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          status: { type: "enum", values: ["active", ""] },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for duplicate entries in values", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          status: { type: "enum", values: ["active", "active"] },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for static default not in values", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          status: { type: "enum", values: ["active", "inactive"], default: "banned" },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("accepts valid values array with valid default", () => {
      expect(() =>
        parseModelSchema("User", {
          id: { type: "string", primary: true },
          status: { type: "enum", values: ["active", "inactive"], default: "active" },
        }),
      ).not.toThrow();
    });
  });

  // -------------------------------------------------------------------------
  describe("field-level validation — relation", () => {
    it("throws SchemaValidationError for relation with primary: true", () => {
      expect(() =>
        parseModelSchema("Post", {
          id: { type: "string", primary: true },
          authorId: { type: "string" },
          author: {
            type: "relation",
            model: "User",
            foreignKey: "authorId",
            relation: "many-to-one",
            primary: true,
          } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for relation with unique: true", () => {
      expect(() =>
        parseModelSchema("Post", {
          id: { type: "string", primary: true },
          authorId: { type: "string" },
          author: {
            type: "relation",
            model: "User",
            foreignKey: "authorId",
            relation: "many-to-one",
            unique: true,
          } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for relation with required: true", () => {
      expect(() =>
        parseModelSchema("Post", {
          id: { type: "string", primary: true },
          authorId: { type: "string" },
          author: {
            type: "relation",
            model: "User",
            foreignKey: "authorId",
            relation: "many-to-one",
            required: true,
          } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for relation with default declared", () => {
      expect(() =>
        parseModelSchema("Post", {
          id: { type: "string", primary: true },
          authorId: { type: "string" },
          author: {
            type: "relation",
            model: "User",
            foreignKey: "authorId",
            relation: "many-to-one",
            default: "something",
          } as SchemaInput[string],
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for empty model string", () => {
      expect(() =>
        parseModelSchema("Post", {
          id: { type: "string", primary: true },
          authorId: { type: "string" },
          author: {
            type: "relation",
            model: "",
            foreignKey: "authorId",
            relation: "many-to-one",
          },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for empty foreignKey string", () => {
      expect(() =>
        parseModelSchema("Post", {
          id: { type: "string", primary: true },
          authorId: { type: "string" },
          author: {
            type: "relation",
            model: "User",
            foreignKey: "",
            relation: "many-to-one",
          },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("throws SchemaValidationError for invalid relation type string", () => {
      expect(() =>
        parseModelSchema("Post", {
          id: { type: "string", primary: true },
          authorId: { type: "string" },
          author: {
            type: "relation",
            model: "User",
            foreignKey: "authorId",
            relation: "many-to-many" as "many-to-one",
          },
        }),
      ).toThrow(SchemaValidationError);
    });

    it("accepts valid relation field declaration", () => {
      expect(() =>
        parseModelSchema("Post", {
          id: { type: "string", primary: true },
          authorId: { type: "string" },
          author: {
            type: "relation",
            model: "User",
            foreignKey: "authorId",
            relation: "many-to-one",
          },
        }),
      ).not.toThrow();
    });
  });

  // -------------------------------------------------------------------------
  describe("successful parse — ParsedModelDefinition shape", () => {
    it("name matches the model name argument", () => {
      const result = parseModelSchema("User", minimalInput);

      expect(result.name).toBe("User");
    });

    it("primaryKeyField is correctly identified", () => {
      const result = parseModelSchema("User", minimalInput);

      expect(result.primaryKeyField).toBe("id");
    });

    it("uniqueFields contains only fields with unique: true", () => {
      const result = parseModelSchema("User", {
        id: { type: "string", primary: true },
        email: { type: "string", unique: true },
        name: { type: "string" },
      });

      expect(result.uniqueFields).toEqual(new Set(["email"]));
    });

    it("indexedFields contains the primary key field", () => {
      const result = parseModelSchema("User", minimalInput);

      expect(result.indexedFields.has("id")).toBe(true);
    });

    it("indexedFields contains unique fields", () => {
      const result = parseModelSchema("User", {
        id: { type: "string", primary: true },
        email: { type: "string", unique: true },
        name: { type: "string" },
      });

      expect(result.indexedFields.has("email")).toBe(true);
    });

    it("indexedFields equals uniqueFields plus the primary key", () => {
      const result = parseModelSchema("User", {
        id: { type: "string", primary: true },
        email: { type: "string", unique: true },
        name: { type: "string" },
      });

      expect(result.indexedFields).toEqual(new Set(["email", "id"]));
    });

    it("fields Map contains all scalar and relation fields", () => {
      const result = parseModelSchema("Post", fullInput);

      expect(result.fields.has("id")).toBe(true);
      expect(result.fields.has("name")).toBe(true);
      expect(result.fields.has("author")).toBe(true);
    });

    it("relationFields Map contains only relation fields", () => {
      const result = parseModelSchema("Post", fullInput);

      expect(result.relationFields.has("author")).toBe(true);
      expect(result.relationFields.has("id")).toBe(false);
      expect(result.relationFields.has("name")).toBe(false);
    });

    it("ParsedScalarField.primary defaults to false when not declared", () => {
      const result = parseModelSchema("User", minimalInput);
      const nameField = result.fields.get("name");

      expect(nameField?.type).not.toBe("relation");
      if (nameField?.type !== "relation") {
        expect(nameField?.primary).toBe(false);
      }
    });

    it("ParsedScalarField.unique defaults to false when not declared", () => {
      const result = parseModelSchema("User", minimalInput);
      const nameField = result.fields.get("name");

      if (nameField?.type !== "relation") {
        expect(nameField?.unique).toBe(false);
      }
    });

    it("ParsedScalarField.required defaults to false when not declared", () => {
      const result = parseModelSchema("User", minimalInput);
      const nameField = result.fields.get("name");

      if (nameField?.type !== "relation") {
        expect(nameField?.required).toBe(false);
      }
    });

    it("ParsedScalarField.hasDefault is true when default declared", () => {
      const result = parseModelSchema("User", {
        id: { type: "string", primary: true, default: () => "gen" },
        score: { type: "number", default: 0 },
      });

      const idField = result.fields.get("id");
      const scoreField = result.fields.get("score");

      if (idField?.type !== "relation") expect(idField?.hasDefault).toBe(true);
      if (scoreField?.type !== "relation") expect(scoreField?.hasDefault).toBe(true);
    });

    it("ParsedScalarField.hasDefault is false when no default", () => {
      const result = parseModelSchema("User", minimalInput);
      const nameField = result.fields.get("name");

      if (nameField?.type !== "relation") {
        expect(nameField?.hasDefault).toBe(false);
      }
    });

    it("ParsedScalarField.enumValues populated only for enum fields", () => {
      const result = parseModelSchema("User", {
        id: { type: "string", primary: true },
        status: { type: "enum", values: ["active", "inactive"] },
      });

      const statusField = result.fields.get("status");
      const idField = result.fields.get("id");

      if (statusField?.type !== "relation") expect(statusField?.enumValues).toEqual(["active", "inactive"]);
      if (idField?.type !== "relation") expect(idField?.enumValues).toBeUndefined();
    });

    it("dynamic default stored as function reference in defaultValue", () => {
      const fn = (): string => "gen";
      const result = parseModelSchema("User", {
        id: { type: "string", primary: true, default: fn },
      });

      const idField = result.fields.get("id");

      if (idField?.type !== "relation") {
        expect(idField?.defaultValue).toBe(fn);
      }
    });

    it("static default stored as value in defaultValue", () => {
      const result = parseModelSchema("User", {
        id: { type: "string", primary: true },
        score: { type: "number", default: 100 },
      });

      const scoreField = result.fields.get("score");

      if (scoreField?.type !== "relation") {
        expect(scoreField?.defaultValue).toBe(100);
      }
    });
  });
});

// ---------------------------------------------------------------------------
// isValidFieldValue()
// ---------------------------------------------------------------------------

describe("isValidFieldValue()", () => {
  it("returns true for string", () => {
    expect(isValidFieldValue("hello")).toBe(true);
  });

  it("returns true for number", () => {
    expect(isValidFieldValue(42)).toBe(true);
  });

  it("returns true for boolean", () => {
    expect(isValidFieldValue(true)).toBe(true);
  });

  it("returns true for null", () => {
    expect(isValidFieldValue(null)).toBe(true);
  });

  it("returns false for undefined", () => {
    expect(isValidFieldValue(undefined)).toBe(false);
  });

  it("returns false for plain object", () => {
    expect(isValidFieldValue({ key: "value" })).toBe(false);
  });

  it("returns false for array", () => {
    expect(isValidFieldValue([1, 2, 3])).toBe(false);
  });

  it("returns false for function", () => {
    expect(isValidFieldValue(() => "test")).toBe(false);
  });
});
