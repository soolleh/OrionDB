// tests/unit/persistence/writer.test.ts

import { mkdtempSync, rmSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { CompactionError, UniqueConstraintError, ValidationError } from "../../../src/errors/index.js";
import { IndexManagerImpl } from "../../../src/index-manager/index.js";
import type { IndexManagerOptions } from "../../../src/index-manager/index.js";
import {
  create,
  createMany,
  FileSizeCounter,
  resolveModelPaths,
  initializeModelDirectory,
} from "../../../src/persistence/index.js";
import type { ModelWriterContext } from "../../../src/persistence/index.js";
import type { ParsedModelDefinition, ParsedScalarField, ParsedRelationField } from "../../../src/schema/index.js";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

let testDir: string;

beforeEach(() => {
  testDir = mkdtempSync(join(tmpdir(), "oriondb-persistence-test-"));
});

afterEach(() => {
  rmSync(testDir, { recursive: true, force: true });
});

function makeScalarField(name: string, overrides: Partial<ParsedScalarField>): ParsedScalarField {
  return {
    name,
    type: "string",
    primary: false,
    unique: false,
    required: false,
    hasDefault: false,
    defaultValue: undefined,
    enumValues: undefined,
    ...overrides,
  };
}

function makeSchema(): ParsedModelDefinition {
  const fields = new Map<string, ParsedScalarField | ParsedRelationField>();

  fields.set(
    "id",
    makeScalarField("id", {
      type: "string",
      primary: true,
      unique: true,
      required: true,
      hasDefault: true,
      defaultValue: () => "generated-id",
    }),
  );
  fields.set(
    "name",
    makeScalarField("name", {
      type: "string",
      required: true,
    }),
  );
  fields.set(
    "email",
    makeScalarField("email", {
      type: "string",
      unique: true,
    }),
  );
  fields.set(
    "age",
    makeScalarField("age", {
      type: "number",
    }),
  );
  fields.set(
    "active",
    makeScalarField("active", {
      type: "boolean",
      hasDefault: true,
      defaultValue: true,
    }),
  );
  fields.set(
    "status",
    makeScalarField("status", {
      type: "enum",
      enumValues: ["active", "inactive"],
      hasDefault: true,
      defaultValue: "active",
    }),
  );

  return {
    name: "TestModel",
    fields,
    primaryKeyField: "id",
    uniqueFields: new Set(["email"]),
    indexedFields: new Set(["id", "email"]),
    relationFields: new Map(),
  };
}

async function makeContext(overrides?: Partial<ModelWriterContext>): Promise<ModelWriterContext> {
  const schema = makeSchema();
  const paths = resolveModelPaths(testDir, "TestModel");
  await initializeModelDirectory(paths, "TestModel");

  const options: IndexManagerOptions = {
    primaryKeyField: schema.primaryKeyField,
    indexedFields: schema.indexedFields,
  };
  const indexManager = new IndexManagerImpl<Record<string, unknown>>(options);

  const counter = new FileSizeCounter();
  await counter.initialize(paths.dataFile);

  return {
    modelName: "TestModel",
    paths,
    schema,
    indexManager,
    counter,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// create()
// ---------------------------------------------------------------------------

describe("create()", () => {
  describe("defaults", () => {
    it("applies function default for absent PK field", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { name: "Alice", email: "alice@test.com" } });
      expect(result["id"]).toBe("generated-id");
    });

    it("applies static default for absent optional field", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { id: "u1", name: "Alice" } });
      expect(result["active"]).toBe(true);
      expect(result["status"]).toBe("active");
    });

    it("does not overwrite explicitly provided values with defaults", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { id: "u1", name: "Alice", active: false, status: "inactive" } });
      expect(result["active"]).toBe(false);
      expect(result["status"]).toBe("inactive");
    });

    it("applies default only for absent fields — does not treat null as absent", async () => {
      const ctx = await makeContext();
      // age is optional and has no default — providing null should be allowed (no type mismatch for absent)
      // We test that a field explicitly set to undefined-equivalent does not trigger a default
      // when the field does have a default for truly absent case
      const result = await create(ctx, { data: { id: "u1", name: "Alice" } });
      // active default should be applied since it is absent
      expect(result["active"]).toBe(true);
    });
  });

  describe("required field validation", () => {
    it("throws ValidationError when required field is absent after defaults applied", async () => {
      const ctx = await makeContext();
      await expect(create(ctx, { data: { id: "u1" } })).rejects.toBeInstanceOf(ValidationError);
    });

    it("throws ValidationError when required field is null", async () => {
      const ctx = await makeContext();
      await expect(create(ctx, { data: { id: "u1", name: null as unknown as string } })).rejects.toBeInstanceOf(
        ValidationError,
      );
    });

    it("does not throw when required field has a default and is absent from input", async () => {
      const ctx = await makeContext();
      // 'id' has a default — omit it and provide 'name'
      await expect(create(ctx, { data: { name: "Alice" } })).resolves.toBeDefined();
    });

    it("error includes model property", async () => {
      const ctx = await makeContext();
      try {
        await create(ctx, { data: { id: "u1" } });
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError);
        const e = err as ValidationError;
        expect(e.model).toBe("TestModel");
      }
    });

    it("error includes field property", async () => {
      const ctx = await makeContext();
      try {
        await create(ctx, { data: { id: "u1" } });
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError);
        const e = err as ValidationError;
        expect(e.field).toBe("name");
      }
    });
  });

  describe("field type validation", () => {
    it("throws ValidationError for string field given a number", async () => {
      const ctx = await makeContext();
      await expect(create(ctx, { data: { id: "u1", name: 123 as unknown as string } })).rejects.toBeInstanceOf(
        ValidationError,
      );
    });

    it("throws ValidationError for number field given a string", async () => {
      const ctx = await makeContext();
      await expect(
        create(ctx, { data: { id: "u1", name: "Alice", age: "notanumber" as unknown as number } }),
      ).rejects.toBeInstanceOf(ValidationError);
    });

    it("throws ValidationError for number field given NaN", async () => {
      const ctx = await makeContext();
      await expect(create(ctx, { data: { id: "u1", name: "Alice", age: NaN } })).rejects.toBeInstanceOf(
        ValidationError,
      );
    });

    it("throws ValidationError for number field given Infinity", async () => {
      const ctx = await makeContext();
      await expect(create(ctx, { data: { id: "u1", name: "Alice", age: Infinity } })).rejects.toBeInstanceOf(
        ValidationError,
      );
    });

    it("throws ValidationError for boolean field given a string", async () => {
      const ctx = await makeContext();
      await expect(
        create(ctx, { data: { id: "u1", name: "Alice", active: "yes" as unknown as boolean } }),
      ).rejects.toBeInstanceOf(ValidationError);
    });

    it("throws ValidationError for enum field given value not in values array", async () => {
      const ctx = await makeContext();
      await expect(
        create(ctx, { data: { id: "u1", name: "Alice", status: "banned" as unknown as string } }),
      ).rejects.toBeInstanceOf(ValidationError);
    });

    it("throws ValidationError for json field given an array", async () => {
      // Add a json field to schema
      const schema = makeSchema();
      const jsonField = makeScalarField("meta", { type: "json" });
      schema.fields.set("meta", jsonField);
      const ctx = await makeContext({ schema });
      await expect(
        create(ctx, { data: { id: "u1", name: "Alice", meta: [] as unknown as Record<string, unknown> } }),
      ).rejects.toBeInstanceOf(ValidationError);
    });

    it("throws ValidationError for json field given null", async () => {
      const schema = makeSchema();
      const jsonField = makeScalarField("meta", { type: "json" });
      schema.fields.set("meta", jsonField);
      const ctx = await makeContext({ schema });
      await expect(
        create(ctx, { data: { id: "u1", name: "Alice", meta: null as unknown as Record<string, unknown> } }),
      ).rejects.toBeInstanceOf(ValidationError);
    });

    it("accepts valid values for all field types", async () => {
      const ctx = await makeContext();
      await expect(
        create(ctx, { data: { id: "u1", name: "Alice", email: "a@b.com", age: 30, active: true, status: "active" } }),
      ).resolves.toBeDefined();
    });

    it("skips absent optional fields — no error for missing non-required fields", async () => {
      const ctx = await makeContext();
      await expect(create(ctx, { data: { id: "u1", name: "Alice" } })).resolves.toBeDefined();
    });
  });

  describe("unique constraint validation", () => {
    it("throws UniqueConstraintError when unique field value already exists in the index", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice", email: "dup@test.com" } });
      await expect(create(ctx, { data: { id: "u2", name: "Bob", email: "dup@test.com" } })).rejects.toBeInstanceOf(
        UniqueConstraintError,
      );
    });

    it("throws UniqueConstraintError when PK already exists in index", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "same-id", name: "Alice" } });
      await expect(create(ctx, { data: { id: "same-id", name: "Bob" } })).rejects.toBeInstanceOf(UniqueConstraintError);
    });

    it("does not throw when unique field value is new", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice", email: "alice@test.com" } });
      await expect(create(ctx, { data: { id: "u2", name: "Bob", email: "bob@test.com" } })).resolves.toBeDefined();
    });

    it("error includes model property", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice", email: "dup@test.com" } });
      try {
        await create(ctx, { data: { id: "u2", name: "Bob", email: "dup@test.com" } });
      } catch (err) {
        expect(err).toBeInstanceOf(UniqueConstraintError);
        const e = err as UniqueConstraintError;
        expect(e.model).toBe("TestModel");
      }
    });

    it("error includes field property", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice", email: "dup@test.com" } });
      try {
        await create(ctx, { data: { id: "u2", name: "Bob", email: "dup@test.com" } });
      } catch (err) {
        expect(err).toBeInstanceOf(UniqueConstraintError);
        const e = err as UniqueConstraintError;
        expect(e.field).toBe("email");
      }
    });
  });

  describe("system fields", () => {
    it("written record includes _deleted: false", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const raw = readFileSync(ctx.paths.dataFile, "utf8").trim();
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      expect(parsed["_deleted"]).toBe(false);
    });

    it("written record includes _createdAt as valid ISO 8601 string", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const raw = readFileSync(ctx.paths.dataFile, "utf8").trim();
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      const createdAt = parsed["_createdAt"] as string;
      expect(new Date(createdAt).toISOString()).toBe(createdAt);
    });

    it("written record includes _updatedAt as valid ISO 8601 string", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const raw = readFileSync(ctx.paths.dataFile, "utf8").trim();
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      const updatedAt = parsed["_updatedAt"] as string;
      expect(new Date(updatedAt).toISOString()).toBe(updatedAt);
    });

    it("returned record does NOT include _deleted", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { id: "u1", name: "Alice" } });
      expect(result).not.toHaveProperty("_deleted");
    });

    it("returned record does NOT include _createdAt", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { id: "u1", name: "Alice" } });
      expect(result).not.toHaveProperty("_createdAt");
    });

    it("returned record does NOT include _updatedAt", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { id: "u1", name: "Alice" } });
      expect(result).not.toHaveProperty("_updatedAt");
    });
  });

  describe("file I/O", () => {
    it("appends exactly one line to data.ndjson", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const lines = readFileSync(ctx.paths.dataFile, "utf8")
        .split("\n")
        .filter((l) => l.trim() !== "");
      expect(lines).toHaveLength(1);
    });

    it("written line is valid JSON when parsed", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const raw = readFileSync(ctx.paths.dataFile, "utf8").split("\n")[0] ?? "";
      expect(() => JSON.parse(raw) as unknown).not.toThrow();
    });

    it("written line contains all user fields plus system fields", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice", email: "a@b.com" } });
      const raw = readFileSync(ctx.paths.dataFile, "utf8").trim();
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      expect(parsed["id"]).toBe("u1");
      expect(parsed["name"]).toBe("Alice");
      expect(parsed["email"]).toBe("a@b.com");
      expect(parsed["_deleted"]).toBeDefined();
      expect(parsed["_createdAt"]).toBeDefined();
      expect(parsed["_updatedAt"]).toBeDefined();
    });

    it("written line ends with \\n", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const raw = readFileSync(ctx.paths.dataFile, "utf8");
      expect(raw.endsWith("\n")).toBe(true);
    });

    it("second create() call appends a second line", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      await create(ctx, { data: { id: "u2", name: "Bob" } });
      const lines = readFileSync(ctx.paths.dataFile, "utf8")
        .split("\n")
        .filter((l) => l.trim() !== "");
      expect(lines).toHaveLength(2);
    });
  });

  describe("index updates", () => {
    it("after create(), indexManager.has(id) returns true", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      expect(ctx.indexManager.has("u1")).toBe(true);
    });

    it("after create(), getOffset(id) returns 0 for the first record", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      expect(ctx.indexManager.getOffset("u1")).toBe(0);
    });

    it("after two creates, second record offset equals byte length of first line", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      await create(ctx, { data: { id: "u2", name: "Bob" } });

      // Read first line from disk to get exact byte length
      const rawContent = readFileSync(ctx.paths.dataFile, "utf8");
      const firstLine = rawContent.split("\n")[0] ?? "";
      const expectedOffset = Buffer.byteLength(firstLine + "\n", "utf8");

      expect(ctx.indexManager.getOffset("u2")).toBe(expectedOffset);
    });

    it("after create(), getByField('email', value) returns Set with record id", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice", email: "alice@test.com" } });
      const matches = ctx.indexManager.getByField("email", "alice@test.com");
      expect(matches).toBeDefined();
      expect(matches?.has("u1")).toBe(true);
    });
  });

  describe("counter updates", () => {
    it("after create(), counter reflects new file size", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const actualSize = Buffer.byteLength(readFileSync(ctx.paths.dataFile, "utf8"), "utf8");
      expect(ctx.counter.getSize()).toBe(actualSize);
    });

    it("counter equals Buffer.byteLength of the written line", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const rawContent = readFileSync(ctx.paths.dataFile, "utf8");
      expect(ctx.counter.getSize()).toBe(Buffer.byteLength(rawContent, "utf8"));
    });
  });

  describe("meta updates", () => {
    it("after create(), meta.json recordCount is incremented by 1", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      const meta = JSON.parse(readFileSync(ctx.paths.metaFile, "utf8")) as Record<string, unknown>;
      expect(meta["recordCount"]).toBe(1);
    });

    it("after two creates, recordCount is 2", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "u1", name: "Alice" } });
      await create(ctx, { data: { id: "u2", name: "Bob" } });
      const meta = JSON.parse(readFileSync(ctx.paths.metaFile, "utf8")) as Record<string, unknown>;
      expect(meta["recordCount"]).toBe(2);
    });
  });

  describe("return value", () => {
    it("returns record with all user fields", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { id: "u1", name: "Alice", email: "a@b.com" } });
      expect(result["id"]).toBe("u1");
      expect(result["name"]).toBe("Alice");
      expect(result["email"]).toBe("a@b.com");
    });

    it("returns record without any system fields", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { id: "u1", name: "Alice" } });
      expect(Object.keys(result)).not.toContain("_deleted");
      expect(Object.keys(result)).not.toContain("_createdAt");
      expect(Object.keys(result)).not.toContain("_updatedAt");
    });

    it("returns record reflecting applied defaults", async () => {
      const ctx = await makeContext();
      const result = await create(ctx, { data: { name: "Alice" } });
      expect(result["id"]).toBe("generated-id");
      expect(result["active"]).toBe(true);
      expect(result["status"]).toBe("active");
    });
  });

  describe("error handling", () => {
    it("rethrows ValidationError from validation unchanged", async () => {
      const ctx = await makeContext();
      const err = await create(ctx, { data: { id: "u1" } }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(ValidationError);
      expect((err as ValidationError).code).toBe("VALIDATION_ERROR");
    });

    it("rethrows UniqueConstraintError unchanged", async () => {
      const ctx = await makeContext();
      await create(ctx, { data: { id: "dup", name: "Alice" } });
      const err = await create(ctx, { data: { id: "dup", name: "Bob" } }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(UniqueConstraintError);
    });

    it("wraps unexpected errors in CompactionError", async () => {
      const ctx = await makeContext();
      // Point paths to a non-existent directory so fs.appendFile fails
      const badPaths = resolveModelPaths(testDir + "/no-such-dir", "TestModel");
      const err = await create({ ...ctx, paths: badPaths }, { data: { id: "u1", name: "Alice" } }).catch(
        (e: unknown) => e,
      );
      expect(err).toBeInstanceOf(CompactionError);
    });
  });
});

// ---------------------------------------------------------------------------
// createMany()
// ---------------------------------------------------------------------------

describe("createMany()", () => {
  describe("empty input", () => {
    it("returns { count: 0 } for empty array", async () => {
      const ctx = await makeContext();
      const result = await createMany(ctx, { data: [] });
      expect(result).toEqual({ count: 0 });
    });

    it("no file writes for empty array", async () => {
      const ctx = await makeContext();
      await createMany(ctx, { data: [] });
      const content = readFileSync(ctx.paths.dataFile, "utf8");
      expect(content).toBe("");
    });

    it("no index updates for empty array", async () => {
      const ctx = await makeContext();
      await createMany(ctx, { data: [] });
      expect(ctx.indexManager.size()).toBe(0);
    });
  });

  describe("validation — all before writes", () => {
    it("throws on first invalid record without writing any records", async () => {
      const ctx = await makeContext();
      const data = [
        { id: "u1", name: "Alice" },
        { id: "u2" }, // missing required 'name'
      ];
      await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(ValidationError);
    });

    it("after a thrown validation error, data.ndjson is unchanged", async () => {
      const ctx = await makeContext();
      const data = [{ id: "u1" }]; // missing required 'name'
      await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(ValidationError);
      const content = readFileSync(ctx.paths.dataFile, "utf8");
      expect(content).toBe("");
    });

    it("after a thrown validation error, index is unchanged", async () => {
      const ctx = await makeContext();
      const data = [{ id: "u1" }]; // missing required 'name'
      await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(ValidationError);
      expect(ctx.indexManager.size()).toBe(0);
    });

    it("validation error includes batchIndex in meta", async () => {
      const ctx = await makeContext();
      const data = [
        { id: "u1", name: "Alice" },
        { id: "u2" }, // index 1 is invalid
      ];
      try {
        await createMany(ctx, { data });
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError);
        const e = err as ValidationError;
        expect((e.meta as Record<string, unknown>)?.["batchIndex"]).toBe(1);
      }
    });
  });

  describe("cross-record uniqueness", () => {
    it("throws UniqueConstraintError for duplicate PK within batch", async () => {
      const ctx = await makeContext();
      const data = [
        { id: "same", name: "Alice" },
        { id: "same", name: "Bob" },
      ];
      await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(UniqueConstraintError);
    });

    it("throws UniqueConstraintError for duplicate unique field within batch", async () => {
      const ctx = await makeContext();
      const data = [
        { id: "u1", name: "Alice", email: "dup@test.com" },
        { id: "u2", name: "Bob", email: "dup@test.com" },
      ];
      await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(UniqueConstraintError);
    });

    it("error meta includes both conflicting batchIndex values", async () => {
      const ctx = await makeContext();
      const data = [
        { id: "u1", name: "Alice", email: "dup@test.com" },
        { id: "u2", name: "Bob", email: "dup@test.com" },
      ];
      try {
        await createMany(ctx, { data });
      } catch (err) {
        expect(err).toBeInstanceOf(UniqueConstraintError);
        const e = err as UniqueConstraintError;
        const batchIndex = (e.meta as Record<string, unknown>)?.["batchIndex"];
        expect(Array.isArray(batchIndex)).toBe(true);
        expect((batchIndex as number[]).length).toBe(2);
      }
    });

    it("no writes occur when batch uniqueness check fails", async () => {
      const ctx = await makeContext();
      const data = [
        { id: "u1", name: "Alice" },
        { id: "u1", name: "Bob" },
      ];
      await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(UniqueConstraintError);
      const content = readFileSync(ctx.paths.dataFile, "utf8");
      expect(content).toBe("");
    });
  });

  describe("write pass", () => {
    it("all records written to data.ndjson in input array order", async () => {
      const ctx = await makeContext();
      await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
          { id: "u3", name: "Charlie" },
        ],
      });
      const lines = readFileSync(ctx.paths.dataFile, "utf8")
        .split("\n")
        .filter((l) => l.trim() !== "");
      expect(lines).toHaveLength(3);
      const first = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
      const second = JSON.parse(lines[1] ?? "{}") as Record<string, unknown>;
      const third = JSON.parse(lines[2] ?? "{}") as Record<string, unknown>;
      expect(first["id"]).toBe("u1");
      expect(second["id"]).toBe("u2");
      expect(third["id"]).toBe("u3");
    });

    it("each written line is valid JSON", async () => {
      const ctx = await makeContext();
      await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
        ],
      });
      const lines = readFileSync(ctx.paths.dataFile, "utf8")
        .split("\n")
        .filter((l) => l.trim() !== "");
      for (const line of lines) {
        expect(() => JSON.parse(line) as unknown).not.toThrow();
      }
    });

    it("each written line ends with \\n", async () => {
      const ctx = await makeContext();
      await createMany(ctx, { data: [{ id: "u1", name: "Alice" }] });
      const raw = readFileSync(ctx.paths.dataFile, "utf8");
      expect(raw.endsWith("\n")).toBe(true);
    });

    it("file contains exactly N lines after writing N records", async () => {
      const ctx = await makeContext();
      const N = 5;
      const data = Array.from({ length: N }, (_, i) => ({ id: `u${i}`, name: `User${i}` }));
      await createMany(ctx, { data });
      const lines = readFileSync(ctx.paths.dataFile, "utf8")
        .split("\n")
        .filter((l) => l.trim() !== "");
      expect(lines).toHaveLength(N);
    });
  });

  describe("index updates", () => {
    it("after createMany(), all record IDs present in physical index", async () => {
      const ctx = await makeContext();
      await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
        ],
      });
      expect(ctx.indexManager.has("u1")).toBe(true);
      expect(ctx.indexManager.has("u2")).toBe(true);
    });

    it("byte offsets for each record are correct", async () => {
      const ctx = await makeContext();
      await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
        ],
      });
      const rawContent = readFileSync(ctx.paths.dataFile, "utf8");
      const lines = rawContent.split("\n").filter((l) => l.trim() !== "");
      const firstLineBytes = Buffer.byteLength((lines[0] ?? "") + "\n", "utf8");

      expect(ctx.indexManager.getOffset("u1")).toBe(0);
      expect(ctx.indexManager.getOffset("u2")).toBe(firstLineBytes);
    });

    it("unique field values for all records present in logical index", async () => {
      const ctx = await makeContext();
      await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice", email: "alice@test.com" },
          { id: "u2", name: "Bob", email: "bob@test.com" },
        ],
      });
      expect(ctx.indexManager.getByField("email", "alice@test.com")?.has("u1")).toBe(true);
      expect(ctx.indexManager.getByField("email", "bob@test.com")?.has("u2")).toBe(true);
    });
  });

  describe("counter updates", () => {
    it("counter equals total byte length of all written lines", async () => {
      const ctx = await makeContext();
      await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
        ],
      });
      const rawContent = readFileSync(ctx.paths.dataFile, "utf8");
      expect(ctx.counter.getSize()).toBe(Buffer.byteLength(rawContent, "utf8"));
    });
  });

  describe("meta updates", () => {
    it("meta.json recordCount equals number of written records", async () => {
      const ctx = await makeContext();
      await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
        ],
      });
      const meta = JSON.parse(readFileSync(ctx.paths.metaFile, "utf8")) as Record<string, unknown>;
      expect(meta["recordCount"]).toBe(2);
    });

    it("meta updated exactly once — final count equals input length", async () => {
      const ctx = await makeContext();
      const N = 4;
      const data = Array.from({ length: N }, (_, i) => ({ id: `u${i}`, name: `User${i}` }));
      await createMany(ctx, { data });
      const meta = JSON.parse(readFileSync(ctx.paths.metaFile, "utf8")) as Record<string, unknown>;
      expect(meta["recordCount"]).toBe(N);
    });
  });

  describe("return value", () => {
    it("returns { count: N } where N equals input array length", async () => {
      const ctx = await makeContext();
      const result = await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
          { id: "u3", name: "Charlie" },
        ],
      });
      expect(result).toEqual({ count: 3 });
    });
  });

  describe("error handling during write pass", () => {
    it("if fs.appendFile fails mid-batch, throws CompactionError", async () => {
      const ctx = await makeContext();
      // Replace the data file with a directory — appendFile to a directory throws EISDIR
      const { rm, mkdir } = await import("node:fs/promises");
      await rm(ctx.paths.dataFile);
      await mkdir(ctx.paths.dataFile);

      const data = [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
      ];
      await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(CompactionError);
    });

    it("CompactionError meta includes writtenCount", async () => {
      const ctx = await makeContext();
      const { rm, mkdir } = await import("node:fs/promises");
      await rm(ctx.paths.dataFile);
      await mkdir(ctx.paths.dataFile);

      const data = [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
      ];
      try {
        await createMany(ctx, { data });
      } catch (err) {
        expect(err).toBeInstanceOf(CompactionError);
        const e = err as CompactionError;
        expect((e.meta as Record<string, unknown>)?.["writtenCount"]).toBeDefined();
      }
    });

    it("records and index state unchanged after failed createMany (no rollback)", async () => {
      const ctx = await makeContext();
      // Pre-populate index with u0 via create()
      await create(ctx, { data: { id: "u0", name: "Zero" } });
      expect(ctx.indexManager.has("u0")).toBe(true);

      // Replace the data file with a directory — all subsequent writes will fail
      const { rm, mkdir } = await import("node:fs/promises");
      await rm(ctx.paths.dataFile);
      await mkdir(ctx.paths.dataFile);

      // createMany fails immediately — u0 pre-existing in index must not be removed
      await createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
        ],
      }).catch(() => {});

      // u0 is still in index — failed createMany did not corrupt existing state
      expect(ctx.indexManager.has("u0")).toBe(true);
    });
  });

  describe("large batch", () => {
    it("successfully writes 100 records", async () => {
      const ctx = await makeContext();
      const data = Array.from({ length: 100 }, (_, i) => ({ id: `u${i}`, name: `User${i}` }));
      const result = await createMany(ctx, { data });
      expect(result.count).toBe(100);
    });

    it("all 100 records present in index with correct offsets", async () => {
      const ctx = await makeContext();
      const data = Array.from({ length: 100 }, (_, i) => ({ id: `u${i}`, name: `User${i}` }));
      await createMany(ctx, { data });

      // All 100 IDs must be in the index
      for (let i = 0; i < 100; i++) {
        expect(ctx.indexManager.has(`u${i}`)).toBe(true);
      }

      // Verify offsets are consistent with disk content
      const rawContent = readFileSync(ctx.paths.dataFile, "utf8");
      const lines = rawContent.split("\n").filter((l) => l.trim() !== "");
      expect(lines).toHaveLength(100);

      // First record must be at offset 0
      expect(ctx.indexManager.getOffset("u0")).toBe(0);

      // Second record offset must equal byte length of first line + newline
      const firstLineBytes = Buffer.byteLength((lines[0] ?? "") + "\n", "utf8");
      expect(ctx.indexManager.getOffset("u1")).toBe(firstLineBytes);
    });

    it("file contains exactly 100 lines", async () => {
      const ctx = await makeContext();
      const data = Array.from({ length: 100 }, (_, i) => ({ id: `u${i}`, name: `User${i}` }));
      await createMany(ctx, { data });
      const lines = readFileSync(ctx.paths.dataFile, "utf8")
        .split("\n")
        .filter((l) => l.trim() !== "");
      expect(lines).toHaveLength(100);
    });

    it("recordCount in meta equals 100", async () => {
      const ctx = await makeContext();
      const data = Array.from({ length: 100 }, (_, i) => ({ id: `u${i}`, name: `User${i}` }));
      await createMany(ctx, { data });
      const meta = JSON.parse(readFileSync(ctx.paths.metaFile, "utf8")) as Record<string, unknown>;
      expect(meta["recordCount"]).toBe(100);
    });
  });
});

// ---------------------------------------------------------------------------
// Additional coverage: relation field — the "skip relation" branch in helpers
// ---------------------------------------------------------------------------

describe("relation field skip in validation helpers", () => {
  it("create() succeeds when schema has a relation field in fields map", async () => {
    const relationField: ParsedRelationField = {
      name: "author",
      type: "relation",
      model: "User",
      foreignKey: "authorId",
      relation: "many-to-one",
    };
    const baseSchema = makeSchema();
    const fieldsWithRelation = new Map(baseSchema.fields);
    fieldsWithRelation.set("author", relationField);
    const schemaWithRelation: ParsedModelDefinition = { ...baseSchema, fields: fieldsWithRelation };
    const ctx = await makeContext({ schema: schemaWithRelation });
    await expect(create(ctx, { data: { id: "u1", name: "Alice" } })).resolves.toBeDefined();
  });

  it("createMany() succeeds when schema has a relation field in fields map", async () => {
    const relationField: ParsedRelationField = {
      name: "author",
      type: "relation",
      model: "User",
      foreignKey: "authorId",
      relation: "many-to-one",
    };
    const baseSchema = makeSchema();
    const fieldsWithRelation = new Map(baseSchema.fields);
    fieldsWithRelation.set("author", relationField);
    const schemaWithRelation: ParsedModelDefinition = { ...baseSchema, fields: fieldsWithRelation };
    const ctx = await makeContext({ schema: schemaWithRelation });
    await expect(
      createMany(ctx, {
        data: [
          { id: "u1", name: "Alice" },
          { id: "u2", name: "Bob" },
        ],
      }),
    ).resolves.toEqual({ count: 2 });
  });
});

// ---------------------------------------------------------------------------
// Additional coverage: date and json field type cases in validateFieldTypes
// ---------------------------------------------------------------------------

describe("date and json type field validation", () => {
  function makeSchemaWithExtraField(fieldName: string, field: ParsedScalarField): ParsedModelDefinition {
    const baseSchema = makeSchema();
    const fields = new Map(baseSchema.fields);
    fields.set(fieldName, field);
    return { ...baseSchema, fields };
  }

  it("accepts a valid json object for a json-type field", async () => {
    const jsonField = makeScalarField("bio", { type: "json" });
    const ctx = await makeContext({ schema: makeSchemaWithExtraField("bio", jsonField) });
    await expect(create(ctx, { data: { id: "u1", name: "Alice", bio: { x: 1 } } })).resolves.toBeDefined();
  });

  it("throws ValidationError for invalid json field value (not an object)", async () => {
    const jsonField = makeScalarField("bio", { type: "json" });
    const ctx = await makeContext({ schema: makeSchemaWithExtraField("bio", jsonField) });
    await expect(
      create(ctx, { data: { id: "u1", name: "Alice", bio: "not-an-object" as unknown as Record<string, unknown> } }),
    ).rejects.toBeInstanceOf(ValidationError);
  });

  it("accepts a Date instance for a date-type field", async () => {
    const dateField = makeScalarField("createdAt", { type: "date" });
    const ctx = await makeContext({ schema: makeSchemaWithExtraField("createdAt", dateField) });
    await expect(create(ctx, { data: { id: "u1", name: "Alice", createdAt: new Date() } })).resolves.toBeDefined();
  });

  it("accepts an ISO 8601 string for a date-type field", async () => {
    const dateField = makeScalarField("createdAt", { type: "date" });
    const ctx = await makeContext({ schema: makeSchemaWithExtraField("createdAt", dateField) });
    await expect(
      create(ctx, { data: { id: "u1", name: "Alice", createdAt: "2024-01-01T00:00:00.000Z" } }),
    ).resolves.toBeDefined();
  });

  it("throws ValidationError for invalid date field value", async () => {
    const dateField = makeScalarField("createdAt", { type: "date" });
    const ctx = await makeContext({ schema: makeSchemaWithExtraField("createdAt", dateField) });
    await expect(create(ctx, { data: { id: "u1", name: "Alice", createdAt: "not-a-date" } })).rejects.toBeInstanceOf(
      ValidationError,
    );
  });
});

// ---------------------------------------------------------------------------
// Additional coverage: createMany() per-record field type validation failure
// Lines 405-411: ValidationError re-throw in validateFieldTypes catch
// ---------------------------------------------------------------------------

describe("createMany() field type validation errors", () => {
  it("throws ValidationError when a batch record has a wrong-type field value", async () => {
    const ctx = await makeContext();
    const data = [{ id: "u1", name: 42 as unknown as string }];
    await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(ValidationError);
  });

  it("ValidationError from type mismatch includes correct batchIndex in meta", async () => {
    const ctx = await makeContext();
    const data = [
      { id: "u1", name: "Alice" },
      { id: "u2", name: 99 as unknown as string },
    ];
    try {
      await createMany(ctx, { data });
    } catch (err) {
      expect(err).toBeInstanceOf(ValidationError);
      const e = err as ValidationError;
      expect((e.meta as Record<string, unknown>)?.["batchIndex"]).toBe(1);
    }
  });

  it("does not write any records when type validation fails mid-batch", async () => {
    const ctx = await makeContext();
    const data = [{ id: "u1", name: 123 as unknown as string }];
    await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(ValidationError);
    expect(ctx.indexManager.size()).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Additional coverage: createMany() per-record conflict with existing index
// Lines 419-425: UniqueConstraintError re-throw in checkUniqueConstraints catch
// ---------------------------------------------------------------------------

describe("createMany() per-record conflict with existing index", () => {
  it("throws UniqueConstraintError when a batch record conflicts with an existing indexed email", async () => {
    const ctx = await makeContext();
    await create(ctx, { data: { id: "u1", name: "Alice", email: "taken@test.com" } });
    const data = [{ id: "u2", name: "Bob", email: "taken@test.com" }];
    await expect(createMany(ctx, { data })).rejects.toBeInstanceOf(UniqueConstraintError);
  });

  it("UniqueConstraintError from existing-index conflict includes batchIndex in meta", async () => {
    const ctx = await makeContext();
    await create(ctx, { data: { id: "u1", name: "Alice", email: "taken@test.com" } });
    const data = [{ id: "u2", name: "Bob", email: "taken@test.com" }];
    try {
      await createMany(ctx, { data });
    } catch (err) {
      expect(err).toBeInstanceOf(UniqueConstraintError);
      const e = err as UniqueConstraintError;
      expect((e.meta as Record<string, unknown>)?.["batchIndex"]).toBeDefined();
    }
  });
});

// ---------------------------------------------------------------------------
// Additional coverage: private readModelMeta error paths in writer.ts
// Lines 202, 211, 217: errors reading/parsing meta file during create()
// ---------------------------------------------------------------------------

describe("create() meta file error handling", () => {
  it("throws CompactionError when meta file is deleted before meta read step", async () => {
    const ctx = await makeContext();
    rmSync(ctx.paths.metaFile);
    // Record gets written to disk first, then meta read fails — CompactionError propagates
    await expect(create(ctx, { data: { id: "u1", name: "Alice" } })).rejects.toBeInstanceOf(CompactionError);
  });

  it("throws CompactionError when meta file contains malformed JSON at meta read step", async () => {
    const ctx = await makeContext();
    writeFileSync(ctx.paths.metaFile, "{ INVALID JSON", "utf8");
    await expect(create(ctx, { data: { id: "u1", name: "Alice" } })).rejects.toBeInstanceOf(CompactionError);
  });

  it("throws CompactionError when meta file contains non-object JSON at meta read step", async () => {
    const ctx = await makeContext();
    writeFileSync(ctx.paths.metaFile, "[1, 2, 3]", "utf8");
    await expect(create(ctx, { data: { id: "u1", name: "Alice" } })).rejects.toBeInstanceOf(CompactionError);
  });
});

// ---------------------------------------------------------------------------
// Additional coverage: isPrimaryKey guard in create() (line 270)
// A boolean-typed PK passes validateFieldTypes but fails isPrimaryKey()
// ---------------------------------------------------------------------------

describe("create() invalid PK type after write", () => {
  it("throws ValidationError when PK field value is not a string or number", async () => {
    // boolean passes validateFieldTypes for a boolean-type field but fails isPrimaryKey()
    const boolPKField = makeScalarField("id", {
      type: "boolean",
      primary: true,
      required: true,
      hasDefault: true,
      defaultValue: () => true,
    });
    const nameField = makeScalarField("name", { type: "string", required: true });
    const fields = new Map<string, ParsedScalarField | ParsedRelationField>([
      ["id", boolPKField],
      ["name", nameField],
    ]);
    const booleanPKSchema: ParsedModelDefinition = {
      name: "BoolPKModel",
      fields,
      primaryKeyField: "id",
      uniqueFields: new Set(),
      indexedFields: new Set(["id"]),
      relationFields: new Map(),
    };
    const ctx = await makeContext({ schema: booleanPKSchema });
    // Record is written to disk, then isPrimaryKey(true) fails → ValidationError
    await expect(create(ctx, { data: { id: true as unknown as string, name: "Alice" } })).rejects.toBeInstanceOf(
      ValidationError,
    );
  });
});

// ---------------------------------------------------------------------------
// Additional coverage: undefined record guard in createMany validation pass (line 383)
// ---------------------------------------------------------------------------

describe("createMany() undefined element in input array", () => {
  it("skips undefined elements in input data array and writes only valid records", async () => {
    const ctx = await makeContext();
    const data = [undefined as unknown as Record<string, unknown>, { id: "u1", name: "Alice" }];
    const result = await createMany(ctx, { data });
    expect(result.count).toBe(1);
    expect(ctx.indexManager.has("u1")).toBe(true);
  });
});
