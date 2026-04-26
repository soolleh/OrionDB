// tests/unit/persistence/writer-update-delete.test.ts
// Covers: update(), updateMany(), deleteRecord(), deleteMany()

import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { RecordNotFoundError, UniqueConstraintError, ValidationError } from "../../../src/errors/index.js";
import { IndexManagerImpl } from "../../../src/index-manager/index.js";
import type { IndexManagerOptions } from "../../../src/index-manager/index.js";
import {
  create,
  deleteMany,
  deleteRecord,
  FileSizeCounter,
  findMany,
  findUnique,
  initializeModelDirectory,
  resolveModelPaths,
  update,
  updateMany,
} from "../../../src/persistence/index.js";
import type { ModelMeta, ModelWriterContext, ModelReaderContext } from "../../../src/persistence/index.js";
import type { ParsedModelDefinition, ParsedScalarField, ParsedRelationField } from "../../../src/schema/index.js";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

let testDir: string;

beforeEach(() => {
  testDir = mkdtempSync(join(tmpdir(), "oriondb-upd-del-test-"));
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
      defaultValue: () => "test-id",
    }),
  );
  fields.set("name", makeScalarField("name", { type: "string", required: true }));
  fields.set("email", makeScalarField("email", { type: "string", unique: true }));
  fields.set("age", makeScalarField("age", { type: "number" }));
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

async function makeWriterContext(overrides?: Partial<ModelWriterContext>): Promise<ModelWriterContext> {
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

function makeReaderContext(writerCtx: ModelWriterContext): ModelReaderContext {
  return {
    modelName: writerCtx.modelName,
    paths: writerCtx.paths,
    schema: writerCtx.schema,
    indexManager: writerCtx.indexManager,
  };
}

function readMeta(writerCtx: ModelWriterContext): ModelMeta {
  return JSON.parse(readFileSync(writerCtx.paths.metaFile, "utf8")) as ModelMeta;
}

async function seedRecord(ctx: ModelWriterContext, data: Record<string, unknown>): Promise<Record<string, unknown>> {
  return create(ctx, { data });
}

async function seedMany(
  ctx: ModelWriterContext,
  records: Record<string, unknown>[],
): Promise<Record<string, unknown>[]> {
  const results: Record<string, unknown>[] = [];
  for (const data of records) {
    results.push(await seedRecord(ctx, data));
  }
  return results;
}

// ---------------------------------------------------------------------------
// update()
// ---------------------------------------------------------------------------

describe("update()", () => {
  describe("basic update", () => {
    it("returns the updated record", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      const result = await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      expect(result["id"]).toBe("u1");
      expect(result["name"]).toBe("Alicia");
    });

    it("merges data with existing record fields", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com", age: 25 });
      const result = await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      expect(result["email"]).toBe("alice@test.com");
      expect(result["age"]).toBe(25);
      expect(result["name"]).toBe("Alicia");
    });

    it("updated record reflects via findUnique", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      const found = await findUnique(makeReaderContext(ctx), { where: { id: "u1" } });
      expect(found?.["name"]).toBe("Alicia");
    });

    it("update can be found by unchanged unique field after update", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      const found = await findUnique(makeReaderContext(ctx), { where: { email: "alice@test.com" } });
      expect(found?.["id"]).toBe("u1");
      expect(found?.["name"]).toBe("Alicia");
    });

    it("strip system fields from returned record", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      const result = await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      expect(result).not.toHaveProperty("_deleted");
      expect(result).not.toHaveProperty("_createdAt");
      expect(result).not.toHaveProperty("_updatedAt");
    });
  });

  describe("system field preservation", () => {
    it("preserves original _createdAt after update", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });

      // Read raw from file to capture original _createdAt
      const rawContent = readFileSync(ctx.paths.dataFile, "utf8");
      const originalRaw = JSON.parse(rawContent.trim()) as Record<string, unknown>;
      const originalCreatedAt = originalRaw["_createdAt"] as string;

      await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });

      // The new line (second line) should have the same _createdAt
      const lines = readFileSync(ctx.paths.dataFile, "utf8").trim().split("\n");
      const lastLine = lines[lines.length - 1] as string;
      const newRaw = JSON.parse(lastLine) as Record<string, unknown>;
      expect(newRaw["_createdAt"]).toBe(originalCreatedAt);
    });

    it("_updatedAt is updated after update", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });

      const line1 = readFileSync(ctx.paths.dataFile, "utf8").trim();
      const originalUpdatedAt = (JSON.parse(line1) as Record<string, unknown>)["_updatedAt"];

      // Ensure time advances
      await new Promise((r) => setTimeout(r, 5));
      await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });

      const lines = readFileSync(ctx.paths.dataFile, "utf8").trim().split("\n");
      const lastLine = lines[lines.length - 1] as string;
      const newUpdatedAt = (JSON.parse(lastLine) as Record<string, unknown>)["_updatedAt"];
      expect(newUpdatedAt).not.toBe(originalUpdatedAt);
    });
  });

  describe("physical index update", () => {
    it("new offset stored in physical index after update", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      const offsetBefore = ctx.indexManager.getOffset("u1");
      await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      const offsetAfter = ctx.indexManager.getOffset("u1");
      expect(offsetAfter).toBeGreaterThan(offsetBefore as number);
    });

    it("old email no longer resolves after updating email", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "old@test.com" });
      await update(ctx, { where: { id: "u1" }, data: { email: "new@test.com" } });

      const byOld = await findUnique(makeReaderContext(ctx), { where: { email: "old@test.com" } });
      expect(byOld).toBeNull();
      const byNew = await findUnique(makeReaderContext(ctx), { where: { email: "new@test.com" } });
      expect(byNew?.["id"]).toBe("u1");
    });

    it("only one record for id appears in findMany results after update", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      const results = await findMany(makeReaderContext(ctx), {});
      const u1records = results.filter((r) => r["id"] === "u1");
      expect(u1records).toHaveLength(1);
      expect(u1records[0]?.["name"]).toBe("Alicia");
    });
  });

  describe("meta updates", () => {
    it("tombstoneCount incremented by 1 after update", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      const metaBefore = readMeta(ctx);
      await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      const metaAfter = readMeta(ctx);
      expect(metaAfter.tombstoneCount).toBe(metaBefore.tombstoneCount + 1);
    });

    it("recordCount unchanged after update", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      const metaBefore = readMeta(ctx);
      await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
      const metaAfter = readMeta(ctx);
      expect(metaAfter.recordCount).toBe(metaBefore.recordCount);
    });
  });

  describe("PK mutation guard", () => {
    it("throws ValidationError when trying to change primary key", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      await expect(update(ctx, { where: { id: "u1" }, data: { id: "u2" } })).rejects.toBeInstanceOf(ValidationError);
    });

    it("does not throw when data.id equals the existing PK value", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      await expect(update(ctx, { where: { id: "u1" }, data: { id: "u1", name: "Alicia" } })).resolves.toBeDefined();
    });
  });

  describe("error cases", () => {
    it("throws RecordNotFoundError when record does not exist", async () => {
      const ctx = await makeWriterContext();
      await expect(update(ctx, { where: { id: "nonexistent" }, data: { name: "X" } })).rejects.toBeInstanceOf(
        RecordNotFoundError,
      );
    });

    it("RecordNotFoundError contains where in meta", async () => {
      const ctx = await makeWriterContext();
      try {
        await update(ctx, { where: { id: "nonexistent" }, data: { name: "X" } });
      } catch (err) {
        expect(err).toBeInstanceOf(RecordNotFoundError);
        const e = err as RecordNotFoundError;
        expect((e.meta as Record<string, unknown>)?.["where"]).toEqual({ id: "nonexistent" });
      }
    });

    it("throws ValidationError for invalid field type in data", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      await expect(update(ctx, { where: { id: "u1" }, data: { age: "not-a-number" } })).rejects.toBeInstanceOf(
        ValidationError,
      );
    });

    it("throws UniqueConstraintError when updated email collides with another record", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await seedRecord(ctx, { id: "u2", name: "Bob", email: "bob@test.com" });
      await expect(update(ctx, { where: { id: "u2" }, data: { email: "alice@test.com" } })).rejects.toBeInstanceOf(
        UniqueConstraintError,
      );
    });

    it("self-collision allowed — updating email to same value does not throw", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await expect(update(ctx, { where: { id: "u1" }, data: { email: "alice@test.com" } })).resolves.toBeDefined();
    });
  });
});

// ---------------------------------------------------------------------------
// updateMany()
// ---------------------------------------------------------------------------

describe("updateMany()", () => {
  it("returns { count: 0 } when no matching records", async () => {
    const ctx = await makeWriterContext();
    const result = await updateMany(ctx, { data: { name: "X" } });
    expect(result).toEqual({ count: 0 });
  });

  it("returns { count: N } for N updated records", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
      { id: "u3", name: "Carol" },
    ]);
    const result = await updateMany(ctx, { data: { status: "inactive" } });
    expect(result).toEqual({ count: 3 });
  });

  it("all updated records reflect new data via findMany", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    await updateMany(ctx, { data: { status: "inactive" } });
    const results = await findMany(makeReaderContext(ctx), {});
    for (const r of results) {
      expect(r["status"]).toBe("inactive");
    }
  });

  it("each updated record appears exactly once in findMany results (no duplicates)", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    await updateMany(ctx, { data: { status: "inactive" } });
    const results = await findMany(makeReaderContext(ctx), {});
    expect(results).toHaveLength(2);
    const ids = results.map((r) => r["id"]);
    expect(new Set(ids).size).toBe(2);
  });

  it("throws ValidationError before any writes if any record fails type validation", async () => {
    const ctx = await makeWriterContext();
    await seedRecord(ctx, { id: "u1", name: "Alice" });
    // 'age' must be a number — passing a string should throw
    await expect(updateMany(ctx, { data: { age: "not-a-number" } })).rejects.toBeInstanceOf(ValidationError);
  });

  it("throws ValidationError when PK mutation attempted via updateMany", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    await expect(updateMany(ctx, { data: { id: "new-pk" } })).rejects.toBeInstanceOf(ValidationError);
  });

  it("tombstoneCount incremented by N after updateMany N records", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "A" },
      { id: "u2", name: "B" },
      { id: "u3", name: "C" },
    ]);
    const metaBefore = readMeta(ctx);
    await updateMany(ctx, { data: { status: "inactive" } });
    const metaAfter = readMeta(ctx);
    expect(metaAfter.tombstoneCount).toBe(metaBefore.tombstoneCount + 3);
  });

  it("compiledFilter narrows which records are updated", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice", age: 20 },
      { id: "u2", name: "Bob", age: 40 },
    ]);
    const filter = (r: Record<string, unknown>): boolean => (r["age"] as number) >= 30;
    const result = await updateMany(ctx, { data: { status: "inactive" } }, filter);
    expect(result).toEqual({ count: 1 });
    const u1 = await findUnique(makeReaderContext(ctx), { where: { id: "u1" } });
    const u2 = await findUnique(makeReaderContext(ctx), { where: { id: "u2" } });
    expect(u1?.["status"]).not.toBe("inactive");
    expect(u2?.["status"]).toBe("inactive");
  });
});

// ---------------------------------------------------------------------------
// deleteRecord()
// ---------------------------------------------------------------------------

describe("deleteRecord()", () => {
  describe("basic delete", () => {
    it("returns the pre-deletion record", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      const result = await deleteRecord(ctx, { where: { id: "u1" } });
      expect(result["id"]).toBe("u1");
      expect(result["name"]).toBe("Alice");
    });

    it("returned record has system fields stripped", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      const result = await deleteRecord(ctx, { where: { id: "u1" } });
      expect(result).not.toHaveProperty("_deleted");
      expect(result).not.toHaveProperty("_createdAt");
      expect(result).not.toHaveProperty("_updatedAt");
    });

    it("findUnique returns null after delete", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await deleteRecord(ctx, { where: { id: "u1" } });
      const found = await findUnique(makeReaderContext(ctx), { where: { id: "u1" } });
      expect(found).toBeNull();
    });

    it("record no longer appears in findMany after delete", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await seedRecord(ctx, { id: "u2", name: "Bob" });
      await deleteRecord(ctx, { where: { id: "u1" } });
      const results = await findMany(makeReaderContext(ctx), {});
      expect(results).toHaveLength(1);
      expect(results[0]?.["id"]).toBe("u2");
    });
  });

  describe("tombstone structure", () => {
    it("tombstone appended to data file with _deleted: true", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      await deleteRecord(ctx, { where: { id: "u1" } });

      const lines = readFileSync(ctx.paths.dataFile, "utf8").trim().split("\n");
      const lastLine = lines[lines.length - 1] as string;
      const tombstone = JSON.parse(lastLine) as Record<string, unknown>;
      expect(tombstone["_deleted"]).toBe(true);
      expect(tombstone["id"]).toBe("u1");
    });

    it("tombstone preserves _createdAt from original record", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });

      const firstLine = readFileSync(ctx.paths.dataFile, "utf8").trim().split("\n")[0] as string;
      const originalCreatedAt = (JSON.parse(firstLine) as Record<string, unknown>)["_createdAt"];

      await deleteRecord(ctx, { where: { id: "u1" } });

      const lines = readFileSync(ctx.paths.dataFile, "utf8").trim().split("\n");
      const tombstoneLine = lines[lines.length - 1] as string;
      const tombstone = JSON.parse(tombstoneLine) as Record<string, unknown>;
      expect(tombstone["_createdAt"]).toBe(originalCreatedAt);
    });
  });

  describe("index removal", () => {
    it("ID removed from physical index after delete", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      expect(ctx.indexManager.has("u1")).toBe(true);
      await deleteRecord(ctx, { where: { id: "u1" } });
      expect(ctx.indexManager.has("u1")).toBe(false);
    });

    it("email removed from logical index after delete", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await deleteRecord(ctx, { where: { id: "u1" } });
      const matches = ctx.indexManager.getByField("email", "alice@test.com");
      expect(matches === undefined || matches.size === 0).toBe(true);
    });

    it("unique email can be reused after delete", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await deleteRecord(ctx, { where: { id: "u1" } });
      await expect(seedRecord(ctx, { id: "u2", name: "Alice2", email: "alice@test.com" })).resolves.toBeDefined();
    });
  });

  describe("meta updates", () => {
    it("recordCount decremented by 1 after delete", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      const metaBefore = readMeta(ctx);
      await deleteRecord(ctx, { where: { id: "u1" } });
      const metaAfter = readMeta(ctx);
      expect(metaAfter.recordCount).toBe(metaBefore.recordCount - 1);
    });

    it("tombstoneCount incremented by 1 after delete", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice" });
      const metaBefore = readMeta(ctx);
      await deleteRecord(ctx, { where: { id: "u1" } });
      const metaAfter = readMeta(ctx);
      expect(metaAfter.tombstoneCount).toBe(metaBefore.tombstoneCount + 1);
    });
  });

  describe("delete by unique field", () => {
    it("can delete via unique email field", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      const result = await deleteRecord(ctx, { where: { email: "alice@test.com" } });
      expect(result["id"]).toBe("u1");
      const found = await findUnique(makeReaderContext(ctx), { where: { id: "u1" } });
      expect(found).toBeNull();
    });
  });

  describe("error cases", () => {
    it("throws RecordNotFoundError for non-existent ID", async () => {
      const ctx = await makeWriterContext();
      await expect(deleteRecord(ctx, { where: { id: "nonexistent" } })).rejects.toBeInstanceOf(RecordNotFoundError);
    });

    it("throws RecordNotFoundError for already-deleted record", async () => {
      const ctx = await makeWriterContext();
      await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await deleteRecord(ctx, { where: { id: "u1" } });
      await expect(deleteRecord(ctx, { where: { id: "u1" } })).rejects.toBeInstanceOf(RecordNotFoundError);
    });
  });
});

// ---------------------------------------------------------------------------
// deleteMany()
// ---------------------------------------------------------------------------

describe("deleteMany()", () => {
  it("returns { count: 0 } when no records exist", async () => {
    const ctx = await makeWriterContext();
    const result = await deleteMany(ctx, {});
    expect(result).toEqual({ count: 0 });
  });

  it("returns { count: N } for N deleted records", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
      { id: "u3", name: "Carol" },
    ]);
    const result = await deleteMany(ctx, {});
    expect(result).toEqual({ count: 3 });
  });

  it("all records return null via findUnique after deleteMany", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    await deleteMany(ctx, {});
    const u1 = await findUnique(makeReaderContext(ctx), { where: { id: "u1" } });
    const u2 = await findUnique(makeReaderContext(ctx), { where: { id: "u2" } });
    expect(u1).toBeNull();
    expect(u2).toBeNull();
  });

  it("findMany returns empty array after deleteMany deletes all records", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    await deleteMany(ctx, {});
    const results = await findMany(makeReaderContext(ctx), {});
    expect(results).toHaveLength(0);
  });

  it("tombstones appended for each deleted record", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    await deleteMany(ctx, {});
    const lines = readFileSync(ctx.paths.dataFile, "utf8").trim().split("\n");
    // 2 original records + 2 tombstones = 4 lines
    expect(lines).toHaveLength(4);
    const tombstones = lines.slice(2).map((l) => JSON.parse(l) as Record<string, unknown>);
    for (const t of tombstones) {
      expect(t["_deleted"]).toBe(true);
    }
  });

  it("indexes cleared for all deleted records", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice", email: "a@test.com" },
      { id: "u2", name: "Bob", email: "b@test.com" },
    ]);
    await deleteMany(ctx, {});
    expect(ctx.indexManager.has("u1")).toBe(false);
    expect(ctx.indexManager.has("u2")).toBe(false);
    expect(ctx.indexManager.getByField("email", "a@test.com")?.size ?? 0).toBe(0);
  });

  it("meta updated once: recordCount decremented by N, tombstoneCount incremented by N", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "A" },
      { id: "u2", name: "B" },
      { id: "u3", name: "C" },
    ]);
    const metaBefore = readMeta(ctx);
    await deleteMany(ctx, {});
    const metaAfter = readMeta(ctx);
    expect(metaAfter.recordCount).toBe(metaBefore.recordCount - 3);
    expect(metaAfter.tombstoneCount).toBe(metaBefore.tombstoneCount + 3);
  });

  it("compiledFilter deletes only matching records, leaving others intact", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice", age: 20 },
      { id: "u2", name: "Bob", age: 40 },
      { id: "u3", name: "Carol", age: 35 },
    ]);
    const filter = (r: Record<string, unknown>): boolean => (r["age"] as number) >= 30;
    const result = await deleteMany(ctx, {}, filter);
    expect(result).toEqual({ count: 2 });
    const remaining = await findMany(makeReaderContext(ctx), {});
    expect(remaining).toHaveLength(1);
    expect(remaining[0]?.["id"]).toBe("u1");
  });
});

// ---------------------------------------------------------------------------
// Integration lifecycle tests
// ---------------------------------------------------------------------------

describe("Integration lifecycle", () => {
  it("create → update → findUnique reflects updated value", async () => {
    const ctx = await makeWriterContext();
    await seedRecord(ctx, { id: "u1", name: "Alice", email: "a@test.com" });
    await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
    const found = await findUnique(makeReaderContext(ctx), { where: { id: "u1" } });
    expect(found?.["name"]).toBe("Alicia");
    expect(found?.["email"]).toBe("a@test.com");
  });

  it("create → delete → findUnique returns null", async () => {
    const ctx = await makeWriterContext();
    await seedRecord(ctx, { id: "u1", name: "Alice", email: "a@test.com" });
    await deleteRecord(ctx, { where: { id: "u1" } });
    const found = await findUnique(makeReaderContext(ctx), { where: { id: "u1" } });
    expect(found).toBeNull();
  });

  it("create multiple → deleteMany → findMany returns remaining", async () => {
    const ctx = await makeWriterContext();
    await seedMany(ctx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
      { id: "u3", name: "Carol" },
    ]);
    const filter = (r: Record<string, unknown>): boolean => r["name"] === "Bob";
    await deleteMany(ctx, {}, filter);
    const remaining = await findMany(makeReaderContext(ctx), {});
    expect(remaining).toHaveLength(2);
    const names = remaining.map((r) => r["name"]);
    expect(names).toContain("Alice");
    expect(names).toContain("Carol");
    expect(names).not.toContain("Bob");
  });

  it("create → update multiple times → only latest version in findMany", async () => {
    const ctx = await makeWriterContext();
    await seedRecord(ctx, { id: "u1", name: "Alice" });
    await update(ctx, { where: { id: "u1" }, data: { name: "Alicia" } });
    await update(ctx, { where: { id: "u1" }, data: { name: "Alison" } });
    const results = await findMany(makeReaderContext(ctx), {});
    expect(results).toHaveLength(1);
    expect(results[0]?.["name"]).toBe("Alison");
  });

  it("create → delete → create same id again → findUnique returns new record", async () => {
    const ctx = await makeWriterContext();
    await seedRecord(ctx, { id: "u1", name: "Alice", email: "alice@test.com" });
    await deleteRecord(ctx, { where: { id: "u1" } });
    await seedRecord(ctx, { id: "u1", name: "AliceNew", email: "alice@test.com" });
    const found = await findUnique(makeReaderContext(ctx), { where: { id: "u1" } });
    expect(found?.["name"]).toBe("AliceNew");
  });
});
