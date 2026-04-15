import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CompactionError, SchemaMismatchError } from "../../../src/errors/index.js";
import {
  CURRENT_SCHEMA_VERSION,
  serializeSchema,
  deserializeSchema,
  writeSchemaFile,
  readSchemaFile,
  parseModelSchema,
} from "../../../src/schema/index.js";
import type { ParsedModelDefinition, PersistedSchema } from "../../../src/schema/index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeModel(name: string): ParsedModelDefinition {
  return parseModelSchema(name, {
    id: { type: "string", primary: true },
    name: { type: "string" },
  });
}

function makeModels(...names: string[]): Map<string, ParsedModelDefinition> {
  const map = new Map<string, ParsedModelDefinition>();
  for (const n of names) map.set(n, makeModel(n));
  return map;
}

// ---------------------------------------------------------------------------
// Test setup / teardown
// ---------------------------------------------------------------------------

let testDir: string;

beforeEach(() => {
  testDir = mkdtempSync(join(tmpdir(), "oriondb-serializer-test-"));
});

afterEach(() => {
  rmSync(testDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// serializeSchema()
// ---------------------------------------------------------------------------

describe("serializeSchema()", () => {
  it("returns an object with the current schema version", () => {
    const result = serializeSchema(makeModels("User"));

    expect(result.version).toBe(CURRENT_SCHEMA_VERSION);
  });

  it("includes a generatedAt ISO timestamp string", () => {
    const result = serializeSchema(makeModels("User"));

    expect(typeof result.generatedAt).toBe("string");
    expect(() => new Date(result.generatedAt)).not.toThrow();
    expect(new Date(result.generatedAt).toISOString()).toBe(result.generatedAt);
  });

  it("includes one entry per model", () => {
    const result = serializeSchema(makeModels("User", "Post"));

    expect(Object.keys(result.models)).toHaveLength(2);
    expect(result.models).toHaveProperty("User");
    expect(result.models).toHaveProperty("Post");
  });

  it("includes all scalar fields for a model", () => {
    const definition = parseModelSchema("User", {
      id: { type: "string", primary: true },
      email: { type: "string", unique: true },
      name: { type: "string" },
    });
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const userModel = result.models["User"];
    expect(userModel).toBeDefined();
    expect(userModel?.fields).toHaveProperty("id");
    expect(userModel?.fields).toHaveProperty("email");
    expect(userModel?.fields).toHaveProperty("name");
  });

  it("omits default values from serialized fields", () => {
    const definition = parseModelSchema("User", {
      id: { type: "string", primary: true, default: () => "gen" },
      score: { type: "number", default: 42 },
    });
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const idField = result.models["User"]?.fields["id"];
    const scoreField = result.models["User"]?.fields["score"];

    expect(idField).not.toHaveProperty("default");
    expect(scoreField).not.toHaveProperty("default");
  });

  it("omits primary flag when false", () => {
    const definition = makeModel("User");
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const nameField = result.models["User"]?.fields["name"];

    expect(nameField).not.toHaveProperty("primary");
  });

  it("includes primary: true when field is primary", () => {
    const definition = makeModel("User");
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const idField = result.models["User"]?.fields["id"];

    expect((idField as { primary?: boolean }).primary).toBe(true);
  });

  it("omits unique flag when false", () => {
    const definition = makeModel("User");
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const nameField = result.models["User"]?.fields["name"];

    expect(nameField).not.toHaveProperty("unique");
  });

  it("includes unique: true when field has unique constraint", () => {
    const definition = parseModelSchema("User", {
      id: { type: "string", primary: true },
      email: { type: "string", unique: true },
    });
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const emailField = result.models["User"]?.fields["email"];

    expect((emailField as { unique?: boolean }).unique).toBe(true);
  });

  it("omits required flag when false", () => {
    const definition = makeModel("User");
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const nameField = result.models["User"]?.fields["name"];

    expect(nameField).not.toHaveProperty("required");
  });

  it("includes required: true when field is required", () => {
    const definition = parseModelSchema("User", {
      id: { type: "string", primary: true },
      name: { type: "string", required: true },
    });
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const nameField = result.models["User"]?.fields["name"];

    expect((nameField as { required?: boolean }).required).toBe(true);
  });

  it("includes enumValues for enum fields", () => {
    const definition = parseModelSchema("User", {
      id: { type: "string", primary: true },
      status: { type: "enum", values: ["active", "inactive"] },
    });
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const statusField = result.models["User"]?.fields["status"];

    expect((statusField as { enumValues?: unknown }).enumValues).toEqual(["active", "inactive"]);
  });

  it("does not include enumValues for non-enum scalar fields", () => {
    const definition = makeModel("User");
    const models = new Map([["User", definition]]);
    const result = serializeSchema(models);

    const idField = result.models["User"]?.fields["id"];

    expect(idField).not.toHaveProperty("enumValues");
  });

  it("includes relation fields separately under relations", () => {
    const definition = parseModelSchema("Post", {
      id: { type: "string", primary: true },
      authorId: { type: "string" },
      author: {
        type: "relation",
        model: "User",
        foreignKey: "authorId",
        relation: "many-to-one",
      },
    });
    const models = new Map([["Post", definition]]);
    const result = serializeSchema(models);

    const postModel = result.models["Post"];
    expect(postModel?.relations).toHaveProperty("author");
    expect(postModel?.fields).not.toHaveProperty("author");
  });

  it("serializes relation fields with model, foreignKey, and relation type", () => {
    const definition = parseModelSchema("Post", {
      id: { type: "string", primary: true },
      authorId: { type: "string" },
      author: {
        type: "relation",
        model: "User",
        foreignKey: "authorId",
        relation: "many-to-one",
      },
    });
    const models = new Map([["Post", definition]]);
    const result = serializeSchema(models);

    const authorRelation = result.models["Post"]?.relations?.["author"];

    expect(authorRelation?.model).toBe("User");
    expect(authorRelation?.foreignKey).toBe("authorId");
    expect(authorRelation?.relation).toBe("many-to-one");
  });

  it("returns empty models object for empty input", () => {
    const result = serializeSchema(new Map());

    expect(result.models).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// deserializeSchema()
// ---------------------------------------------------------------------------

describe("deserializeSchema()", () => {
  it("rejects a non-object top level value", () => {
    expect(() => deserializeSchema("not-an-object")).toThrow(SchemaMismatchError);
  });

  it("rejects null", () => {
    expect(() => deserializeSchema(null)).toThrow(SchemaMismatchError);
  });

  it("rejects when version field is absent", () => {
    expect(() => deserializeSchema({ generatedAt: new Date().toISOString(), models: {} })).toThrow(SchemaMismatchError);
  });

  it("rejects when version is not a number", () => {
    expect(() => deserializeSchema({ version: "one", generatedAt: new Date().toISOString(), models: {} })).toThrow(
      SchemaMismatchError,
    );
  });

  it("rejects when version is a non-integer number (float)", () => {
    expect(() => deserializeSchema({ version: 1.5, generatedAt: new Date().toISOString(), models: {} })).toThrow(
      SchemaMismatchError,
    );
  });

  it("rejects when version does not match CURRENT_SCHEMA_VERSION", () => {
    expect(() => deserializeSchema({ version: 2, generatedAt: new Date().toISOString(), models: {} })).toThrow(
      SchemaMismatchError,
    );
  });

  it("error for wrong version includes expected and found in meta", () => {
    try {
      deserializeSchema({ version: 99, generatedAt: new Date().toISOString(), models: {} });
      expect.fail("Expected SchemaMismatchError to be thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaMismatchError);
      const e = error as SchemaMismatchError;
      expect((e.meta as Record<string, unknown>)["found"]).toBe(99);
      expect((e.meta as Record<string, unknown>)["expected"]).toBe(CURRENT_SCHEMA_VERSION);
    }
  });

  it("rejects when generatedAt is absent", () => {
    expect(() => deserializeSchema({ version: CURRENT_SCHEMA_VERSION, models: {} })).toThrow(SchemaMismatchError);
  });

  it("rejects when generatedAt is not a string", () => {
    expect(() => deserializeSchema({ version: CURRENT_SCHEMA_VERSION, generatedAt: 12345, models: {} })).toThrow(
      SchemaMismatchError,
    );
  });

  it("rejects when models is absent", () => {
    expect(() => deserializeSchema({ version: CURRENT_SCHEMA_VERSION, generatedAt: new Date().toISOString() })).toThrow(
      SchemaMismatchError,
    );
  });

  it("rejects when models is not a plain object", () => {
    expect(() =>
      deserializeSchema({
        version: CURRENT_SCHEMA_VERSION,
        generatedAt: new Date().toISOString(),
        models: "not-an-object",
      }),
    ).toThrow(SchemaMismatchError);
  });

  it("rejects when a model field type is invalid", () => {
    expect(() =>
      deserializeSchema({
        version: CURRENT_SCHEMA_VERSION,
        generatedAt: new Date().toISOString(),
        models: {
          User: {
            fields: { id: { type: "invalid-type" } },
          },
        },
      }),
    ).toThrow(SchemaMismatchError);
  });

  it("rejects when a relation type is invalid", () => {
    expect(() =>
      deserializeSchema({
        version: CURRENT_SCHEMA_VERSION,
        generatedAt: new Date().toISOString(),
        models: {
          Post: {
            fields: { id: { type: "string", primary: true } },
            relations: { author: { model: "User", foreignKey: "authorId", relation: "bad-type" } },
          },
        },
      }),
    ).toThrow(SchemaMismatchError);
  });

  it("successfully round-trips a serialized schema", () => {
    const original = makeModels("User", "Post");
    const serialized = serializeSchema(original);
    const deserialized = deserializeSchema(serialized);

    expect(deserialized).toBeDefined();
    expect(deserialized?.version).toBe(CURRENT_SCHEMA_VERSION);
    expect(typeof deserialized?.models).toBe("object");
    expect(Object.keys(deserialized?.models ?? {})).toHaveLength(2);
  });

  it("returns a valid PersistedSchema object for minimal valid input", () => {
    const input: PersistedSchema = {
      version: CURRENT_SCHEMA_VERSION,
      generatedAt: new Date().toISOString(),
      models: {},
    };
    const result = deserializeSchema(input);

    expect(result?.version).toBe(CURRENT_SCHEMA_VERSION);
    expect(result?.models).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// writeSchemaFile() + readSchemaFile()
// ---------------------------------------------------------------------------

describe("writeSchemaFile()", () => {
  it("writes a valid JSON file to the given path", async () => {
    const filePath = join(testDir, "_schema.json");
    const models = makeModels("User");

    await writeSchemaFile(filePath, models);

    const { readFileSync } = await import("node:fs");
    const content = readFileSync(filePath, "utf8");
    const parsed = JSON.parse(content) as unknown;

    expect(parsed).toMatchObject({ version: CURRENT_SCHEMA_VERSION });
  });

  it("throws CompactionError when directory does not exist", async () => {
    const filePath = join(testDir, "nonexistent", "_schema.json");
    const models = makeModels("User");

    await expect(writeSchemaFile(filePath, models)).rejects.toBeInstanceOf(CompactionError);
  });

  it("overwrites existing file with updated schema", async () => {
    const filePath = join(testDir, "_schema.json");
    const models1 = makeModels("User");
    const models2 = makeModels("User", "Post");

    await writeSchemaFile(filePath, models1);
    await writeSchemaFile(filePath, models2);

    const { readFileSync } = await import("node:fs");
    const parsed = JSON.parse(readFileSync(filePath, "utf8")) as PersistedSchema;
    expect(Object.keys(parsed.models)).toHaveLength(2);
  });
});

describe("readSchemaFile()", () => {
  it("returns null when file does not exist (ENOENT)", async () => {
    const filePath = join(testDir, "nonexistent_schema.json");

    const result = await readSchemaFile(filePath);

    expect(result).toBeNull();
  });

  it("returns the parsed PersistedSchema when file exists", async () => {
    const filePath = join(testDir, "_schema.json");
    const models = makeModels("User");
    await writeSchemaFile(filePath, models);

    const result = await readSchemaFile(filePath);

    expect(result).not.toBeNull();
    expect(result?.version).toBe(CURRENT_SCHEMA_VERSION);
    expect(result?.models).toHaveProperty("User");
  });

  it("throws SchemaMismatchError when file contains invalid JSON", async () => {
    const filePath = join(testDir, "_schema.json");
    writeFileSync(filePath, "{ this is not: valid json }");

    await expect(readSchemaFile(filePath)).rejects.toBeInstanceOf(SchemaMismatchError);
  });

  it("throws SchemaMismatchError when file contains wrong schema version", async () => {
    const filePath = join(testDir, "_schema.json");
    writeFileSync(filePath, JSON.stringify({ version: 99, generatedAt: new Date().toISOString(), models: {} }));

    await expect(readSchemaFile(filePath)).rejects.toBeInstanceOf(SchemaMismatchError);
  });

  it("throws SchemaMismatchError when file is readable but not a directory", async () => {
    // Write a JSON file that is valid JSON but structurally invalid schema
    const filePath = join(testDir, "_schema.json");
    writeFileSync(filePath, JSON.stringify({ not: "a schema at all" }));

    await expect(readSchemaFile(filePath)).rejects.toBeInstanceOf(SchemaMismatchError);
  });

  it("round-trips a schema correctly", async () => {
    const filePath = join(testDir, "_schema.json");
    const original = makeModels("Alpha", "Beta");
    await writeSchemaFile(filePath, original);

    const restored = await readSchemaFile(filePath);

    expect(Object.keys(restored?.models ?? {})).toHaveLength(2);
    expect(restored?.models).toHaveProperty("Alpha");
    expect(restored?.models).toHaveProperty("Beta");
  });
});
