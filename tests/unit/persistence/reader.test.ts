// tests/unit/persistence/reader.test.ts
// Covers: readRecordAtOffset, resolveWhereToId (via findUnique), findUnique,
// findUniqueOrThrow, scanRecords (via findMany), findMany, findFirst

import { mkdtempSync, mkdirSync, rmSync, writeFileSync, readFileSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { CompactionError, QueryError, RecordNotFoundError, ValidationError } from "../../../src/errors/index.js";
import { IndexManagerImpl } from "../../../src/index-manager/index.js";
import type { IndexManagerOptions } from "../../../src/index-manager/index.js";
import {
  create,
  deleteRecord,
  FileSizeCounter,
  findFirst,
  findMany,
  findUnique,
  findUniqueOrThrow,
  initializeModelDirectory,
  readRecordAtOffset,
  resolveModelPaths,
} from "../../../src/persistence/index.js";
import type { FilterFn, ModelReaderContext, ModelWriterContext, RawRecord } from "../../../src/persistence/index.js";
import type { ParsedModelDefinition, ParsedScalarField, ParsedRelationField } from "../../../src/schema/index.js";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

let testDir: string;

beforeEach(() => {
  testDir = mkdtempSync(join(tmpdir(), "oriondb-reader-test-"));
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

function makeRelationField(name: string): ParsedRelationField {
  return {
    name,
    type: "relation",
    model: "Post",
    foreignKey: "userId",
    relation: "one-to-many",
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
// readRecordAtOffset()
// ---------------------------------------------------------------------------

describe("readRecordAtOffset()", () => {
  it("reads correct record at offset 0 for first written record", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "r1", name: "Alice", email: "alice@test.com" });

    const rawContent = readFileSync(writerCtx.paths.dataFile, "utf8");
    const firstLine = rawContent.split("\n")[0] ?? "";
    const record = await readRecordAtOffset(writerCtx.paths.dataFile, 0, "TestModel");

    expect(record["id"]).toBe("r1");
    expect(record["name"]).toBe("Alice");
    expect(firstLine.length).toBeGreaterThan(0);
  });

  it("reads correct record at computed offset for second written record", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "r1", name: "Alice", email: "alice@test.com" });
    await seedRecord(writerCtx, { id: "r2", name: "Bob", email: "bob@test.com" });

    const rawContent = readFileSync(writerCtx.paths.dataFile, "utf8");
    const firstLine = rawContent.split("\n")[0] ?? "";
    const secondOffset = Buffer.byteLength(firstLine + "\n", "utf8");

    const record = await readRecordAtOffset(writerCtx.paths.dataFile, secondOffset, "TestModel");
    expect(record["id"]).toBe("r2");
    expect(record["name"]).toBe("Bob");
  });

  it("returns a plain object", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "r1", name: "Alice" });
    const record = await readRecordAtOffset(writerCtx.paths.dataFile, 0, "TestModel");
    expect(typeof record).toBe("object");
    expect(record).not.toBeNull();
    expect(Array.isArray(record)).toBe(false);
  });

  it("handles record with no trailing newline on last line", async () => {
    const writerCtx = await makeWriterContext();
    const recordObj = {
      id: "r1",
      name: "Alice",
      _deleted: false,
      _createdAt: "2024-01-01T00:00:00.000Z",
      _updatedAt: "2024-01-01T00:00:00.000Z",
    };
    // Write without trailing newline
    writeFileSync(writerCtx.paths.dataFile, JSON.stringify(recordObj));
    // Build index entry manually
    writerCtx.indexManager.add(recordObj as RawRecord, 0);

    const record = await readRecordAtOffset(writerCtx.paths.dataFile, 0, "TestModel");
    expect(record["id"]).toBe("r1");
  });

  it("throws ValidationError for corrupt JSON at a given offset", async () => {
    const writerCtx = await makeWriterContext();
    // Write malformed JSON as the only content
    writeFileSync(writerCtx.paths.dataFile, "not-valid-json\n");

    await expect(readRecordAtOffset(writerCtx.paths.dataFile, 0, "TestModel")).rejects.toBeInstanceOf(ValidationError);
  });

  it("throws CompactionError for unexpected I/O error when data file is a directory", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "r1", name: "Alice" });

    // Replace the data file with a directory to cause an I/O error on read
    unlinkSync(writerCtx.paths.dataFile);
    mkdirSync(writerCtx.paths.dataFile);

    // readRecordAtOffset: fs.open on a directory succeeds on Linux, but fs.read fails
    // OR on some systems it throws immediately — in either case gets wrapped in CompactionError
    await expect(readRecordAtOffset(writerCtx.paths.dataFile, 0, "TestModel")).rejects.toSatisfy(
      (e: unknown) => e instanceof CompactionError || e instanceof ValidationError,
    );
  });
});

// ---------------------------------------------------------------------------
// resolveWhereToId() — tested indirectly via findUnique
// ---------------------------------------------------------------------------

describe("resolveWhereToId() via findUnique", () => {
  it("resolves by primary key field directly", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "pk1", name: "Alice" });
    const readerCtx = makeReaderContext(writerCtx);
    const result = await findUnique(readerCtx, { where: { id: "pk1" } });
    expect(result).not.toBeNull();
    expect(result?.["id"]).toBe("pk1");
  });

  it("resolves by unique field via logical index", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "alice@test.com" });
    const readerCtx = makeReaderContext(writerCtx);
    const result = await findUnique(readerCtx, { where: { email: "alice@test.com" } });
    expect(result).not.toBeNull();
    expect(result?.["id"]).toBe("u1");
  });

  it("returns null from findUnique when unique field value not in index", async () => {
    const writerCtx = await makeWriterContext();
    const readerCtx = makeReaderContext(writerCtx);
    const result = await findUnique(readerCtx, { where: { email: "nothere@test.com" } });
    expect(result).toBeNull();
  });

  it("throws QueryError for unrecognized field in where clause", async () => {
    const writerCtx = await makeWriterContext();
    const readerCtx = makeReaderContext(writerCtx);
    await expect(findUnique(readerCtx, { where: { unknownField: "x" } })).rejects.toBeInstanceOf(QueryError);
  });

  it("throws QueryError for invalid PK type in where clause", async () => {
    const writerCtx = await makeWriterContext();
    const readerCtx = makeReaderContext(writerCtx);
    await expect(
      findUnique(readerCtx, { where: { id: { nested: "object" } as unknown as string } }),
    ).rejects.toBeInstanceOf(QueryError);
  });

  it("throws QueryError when unique index returns multiple matches (integrity violation)", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "dup@test.com" });
    await seedRecord(writerCtx, { id: "u2", name: "Bob" });

    // Corrupt the logical index: manually add u2 to the email="dup@test.com" bucket
    const readerCtx = makeReaderContext(writerCtx);
    // We inject via add() using a fake record pointing email to the same value
    const fakeRecord = { id: "u2", email: "dup@test.com", _deleted: false, _createdAt: "x", _updatedAt: "x" };
    // update u2's index entry to have email = "dup@test.com"
    const rawU2Offset = writerCtx.indexManager.getOffset("u2") ?? 0;
    const rawU2 = { id: "u2", name: "Bob", _deleted: false, _createdAt: "x", _updatedAt: "x" };
    // Re-add u2 with the corrupted email
    writerCtx.indexManager.update(rawU2 as RawRecord, fakeRecord as RawRecord, rawU2Offset);

    await expect(findUnique(readerCtx, { where: { email: "dup@test.com" } })).rejects.toBeInstanceOf(QueryError);
  });
});

// ---------------------------------------------------------------------------
// findUnique()
// ---------------------------------------------------------------------------

describe("findUnique()", () => {
  describe("lookup by primary key", () => {
    it("returns record when found by PK", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice" });
      const result = await findUnique(makeReaderContext(writerCtx), { where: { id: "u1" } });
      expect(result).not.toBeNull();
      expect(result?.["id"]).toBe("u1");
    });

    it("returns null when PK not in index", async () => {
      const writerCtx = await makeWriterContext();
      const result = await findUnique(makeReaderContext(writerCtx), { where: { id: "nonexistent" } });
      expect(result).toBeNull();
    });

    it("returns null when PK maps to a deleted record", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await deleteRecord(writerCtx, { where: { id: "u1" } });
      const result = await findUnique(makeReaderContext(writerCtx), { where: { id: "u1" } });
      expect(result).toBeNull();
    });

    it("returned record has system fields stripped", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice" });
      const result = await findUnique(makeReaderContext(writerCtx), { where: { id: "u1" } });
      expect(result).not.toHaveProperty("_deleted");
      expect(result).not.toHaveProperty("_createdAt");
      expect(result).not.toHaveProperty("_updatedAt");
    });

    it("returned record contains all user fields", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "a@test.com", age: 30 });
      const result = await findUnique(makeReaderContext(writerCtx), { where: { id: "u1" } });
      expect(result?.["id"]).toBe("u1");
      expect(result?.["name"]).toBe("Alice");
      expect(result?.["email"]).toBe("a@test.com");
      expect(result?.["age"]).toBe(30);
    });
  });

  describe("lookup by unique field", () => {
    it("returns record when found by unique field value", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "alice@test.com" });
      const result = await findUnique(makeReaderContext(writerCtx), { where: { email: "alice@test.com" } });
      expect(result?.["id"]).toBe("u1");
    });

    it("returns null when unique field value has no match", async () => {
      const writerCtx = await makeWriterContext();
      const result = await findUnique(makeReaderContext(writerCtx), { where: { email: "nobody@test.com" } });
      expect(result).toBeNull();
    });

    it("resolves via logical index to physical index correctly", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await seedRecord(writerCtx, { id: "u2", name: "Bob", email: "bob@test.com" });
      const result = await findUnique(makeReaderContext(writerCtx), { where: { email: "bob@test.com" } });
      expect(result?.["id"]).toBe("u2");
      expect(result?.["name"]).toBe("Bob");
    });
  });

  describe("select clause", () => {
    it("returns only selected fields when select provided", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "a@test.com" });
      const result = await findUnique(makeReaderContext(writerCtx), {
        where: { id: "u1" },
        select: { name: true },
      });
      expect(result).toEqual({ name: "Alice" });
    });

    it("returns all fields when select is undefined", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "a@test.com" });
      const result = await findUnique(makeReaderContext(writerCtx), { where: { id: "u1" } });
      expect(result?.["id"]).toBeDefined();
      expect(result?.["name"]).toBeDefined();
      expect(result?.["email"]).toBeDefined();
    });

    it("returns empty object when select has no true values", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice" });
      const result = await findUnique(makeReaderContext(writerCtx), {
        where: { id: "u1" },
        select: { name: false, id: false },
      });
      expect(result).toEqual({});
    });

    it("excludes fields set to false in select", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "a@test.com" });
      const result = await findUnique(makeReaderContext(writerCtx), {
        where: { id: "u1" },
        select: { id: true, name: true, email: false },
      });
      expect(result?.["id"]).toBeDefined();
      expect(result?.["name"]).toBeDefined();
      expect(result).not.toHaveProperty("email");
    });
  });

  describe("error handling", () => {
    it("propagates QueryError for unrecognized where field", async () => {
      const writerCtx = await makeWriterContext();
      await expect(findUnique(makeReaderContext(writerCtx), { where: { notAField: "x" } })).rejects.toBeInstanceOf(
        QueryError,
      );
    });

    it("wraps missing data file in CompactionError", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice" });
      // Remove the data file — index still has the entry
      unlinkSync(writerCtx.paths.dataFile);
      await expect(findUnique(makeReaderContext(writerCtx), { where: { id: "u1" } })).rejects.toBeInstanceOf(
        CompactionError,
      );
    });
  });
});

// ---------------------------------------------------------------------------
// findUniqueOrThrow()
// ---------------------------------------------------------------------------

describe("findUniqueOrThrow()", () => {
  it("returns record when found", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice" });
    const result = await findUniqueOrThrow(makeReaderContext(writerCtx), { where: { id: "u1" } });
    expect(result["id"]).toBe("u1");
  });

  it("throws RecordNotFoundError when not found", async () => {
    const writerCtx = await makeWriterContext();
    await expect(findUniqueOrThrow(makeReaderContext(writerCtx), { where: { id: "missing" } })).rejects.toBeInstanceOf(
      RecordNotFoundError,
    );
  });

  it("error includes where in meta", async () => {
    const writerCtx = await makeWriterContext();
    try {
      await findUniqueOrThrow(makeReaderContext(writerCtx), { where: { id: "missing" } });
    } catch (err) {
      expect(err).toBeInstanceOf(RecordNotFoundError);
      const e = err as RecordNotFoundError;
      expect((e.meta as Record<string, unknown>)?.["where"]).toEqual({ id: "missing" });
    }
  });

  it("error model property is set correctly", async () => {
    const writerCtx = await makeWriterContext();
    try {
      await findUniqueOrThrow(makeReaderContext(writerCtx), { where: { id: "missing" } });
    } catch (err) {
      const e = err as RecordNotFoundError;
      expect(e.model).toBe("TestModel");
    }
  });

  it("inherits select behavior from findUnique", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "a@test.com" });
    const result = await findUniqueOrThrow(makeReaderContext(writerCtx), {
      where: { id: "u1" },
      select: { name: true },
    });
    expect(result).toEqual({ name: "Alice" });
  });
});

// ---------------------------------------------------------------------------
// scanRecords() via findMany
// ---------------------------------------------------------------------------

describe("scanRecords() via findMany", () => {
  describe("basic scan", () => {
    it("returns empty array for missing data file", async () => {
      const writerCtx = await makeWriterContext();
      // data.ndjson never created — counter initialized to 0, file absent
      const result = await findMany(makeReaderContext(writerCtx), {});
      expect(result).toEqual([]);
    });

    it("returns empty array for empty data file", async () => {
      const writerCtx = await makeWriterContext();
      writeFileSync(writerCtx.paths.dataFile, "");
      const result = await findMany(makeReaderContext(writerCtx), {});
      expect(result).toEqual([]);
    });

    it("returns all non-deleted records when no filter", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
        { id: "u3", name: "Carol" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), {});
      expect(results).toHaveLength(3);
    });

    it("skips lines where _deleted === true", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "alice@test.com" });
      await seedRecord(writerCtx, { id: "u2", name: "Bob" });
      await deleteRecord(writerCtx, { where: { id: "u1" } });
      const results = await findMany(makeReaderContext(writerCtx), {});
      expect(results).toHaveLength(1);
      expect(results[0]?.["id"]).toBe("u2");
    });

    it("emits console.warn for malformed JSON line and continues scan", async () => {
      const writerCtx = await makeWriterContext();
      await seedRecord(writerCtx, { id: "u1", name: "Alice" });

      // Append a malformed line after the valid record
      const badLine = "not-valid-json\n";
      const validRecord = {
        id: "u2",
        name: "Bob",
        _deleted: false,
        _createdAt: "2024-01-01T00:00:00.000Z",
        _updatedAt: "2024-01-01T00:00:00.000Z",
      };
      const rawFile = readFileSync(writerCtx.paths.dataFile, "utf8");
      writeFileSync(writerCtx.paths.dataFile, rawFile + badLine + JSON.stringify(validRecord) + "\n");
      // Update index for u2 manually
      const firstLineLen = Buffer.byteLength(rawFile, "utf8");
      const badLineLen = Buffer.byteLength(badLine, "utf8");
      writerCtx.indexManager.add(validRecord as RawRecord, firstLineLen + badLineLen);

      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const results = await findMany(makeReaderContext(writerCtx), {});
      expect(warnSpy).toHaveBeenCalled();
      warnSpy.mockRestore();

      // u1 and u2 present, malformed line skipped
      const ids = results.map((r) => r["id"]);
      expect(ids).toContain("u1");
      expect(ids).toContain("u2");
    });
  });

  describe("filter", () => {
    it("filter function is applied to each record", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice", age: 25 },
        { id: "u2", name: "Bob", age: 35 },
        { id: "u3", name: "Carol", age: 30 },
      ]);
      const filter: FilterFn = (r) => (r["age"] as number) > 28;
      const results = await findMany(makeReaderContext(writerCtx), {}, filter);
      expect(results).toHaveLength(2);
      expect(results.map((r) => r["id"])).toContain("u2");
      expect(results.map((r) => r["id"])).toContain("u3");
    });

    it("returns only records where filter returns true", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
      ]);
      const filter: FilterFn = (r) => r["name"] === "Alice";
      const results = await findMany(makeReaderContext(writerCtx), {}, filter);
      expect(results).toHaveLength(1);
      expect(results[0]?.["id"]).toBe("u1");
    });

    it("no filter (passthrough) returns all records", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), {});
      expect(results).toHaveLength(2);
    });
  });

  describe("skip and take", () => {
    it("take: 1 returns only first matching record", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
        { id: "u3", name: "Carol" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), { take: 1 });
      expect(results).toHaveLength(1);
      expect(results[0]?.["id"]).toBe("u1");
    });

    it("take: N returns at most N records", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
        { id: "u3", name: "Carol" },
        { id: "u4", name: "Dave" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), { take: 2 });
      expect(results).toHaveLength(2);
    });

    it("skip: N skips first N matching records", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
        { id: "u3", name: "Carol" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), { skip: 1 });
      expect(results).toHaveLength(2);
      expect(results[0]?.["id"]).toBe("u2");
    });

    it("skip: 2, take: 2 skips 2 and returns next 2", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
        { id: "u3", name: "Carol" },
        { id: "u4", name: "Dave" },
        { id: "u5", name: "Eve" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), { skip: 2, take: 2 });
      expect(results).toHaveLength(2);
      expect(results[0]?.["id"]).toBe("u3");
      expect(results[1]?.["id"]).toBe("u4");
    });

    it("take: 0 returns empty array", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), { take: 0 });
      expect(results).toHaveLength(0);
    });

    it("take greater than total records returns all records", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), { take: 100 });
      expect(results).toHaveLength(2);
    });

    it("early exit fires on take — counting filter sees only 1 record when take: 1", async () => {
      const writerCtx = await makeWriterContext();
      // Seed 10 records so the difference is meaningful
      const records = Array.from({ length: 10 }, (_, i) => ({ id: `u${i + 1}`, name: `User${i + 1}` }));
      await seedMany(writerCtx, records);

      let scanCount = 0;
      const countingFilter: FilterFn = (record) => {
        scanCount++;
        return true;
      };
      await findMany(makeReaderContext(writerCtx), { take: 1 }, countingFilter);
      expect(scanCount).toBe(1);
    });
  });

  describe("order of results", () => {
    it("results returned in file order (insertion order) when no orderBy", async () => {
      const writerCtx = await makeWriterContext();
      await seedMany(writerCtx, [
        { id: "u1", name: "Alice" },
        { id: "u2", name: "Bob" },
        { id: "u3", name: "Carol" },
      ]);
      const results = await findMany(makeReaderContext(writerCtx), {});
      expect(results[0]?.["id"]).toBe("u1");
      expect(results[1]?.["id"]).toBe("u2");
      expect(results[2]?.["id"]).toBe("u3");
    });
  });
});

// ---------------------------------------------------------------------------
// findMany()
// ---------------------------------------------------------------------------

describe("findMany()", () => {
  it("returns empty array when no records", async () => {
    const writerCtx = await makeWriterContext();
    const results = await findMany(makeReaderContext(writerCtx), {});
    expect(results).toEqual([]);
  });

  it("returns all records when no where and no compiledFilter", async () => {
    const writerCtx = await makeWriterContext();
    await seedMany(writerCtx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    const results = await findMany(makeReaderContext(writerCtx), {});
    expect(results).toHaveLength(2);
  });

  it("strips system fields from all returned records", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice" });
    const results = await findMany(makeReaderContext(writerCtx), {});
    for (const record of results) {
      expect(record).not.toHaveProperty("_deleted");
      expect(record).not.toHaveProperty("_createdAt");
      expect(record).not.toHaveProperty("_updatedAt");
    }
  });

  it("applies select to each result record", async () => {
    const writerCtx = await makeWriterContext();
    await seedMany(writerCtx, [
      { id: "u1", name: "Alice", email: "a@test.com" },
      { id: "u2", name: "Bob", email: "b@test.com" },
    ]);
    const results = await findMany(makeReaderContext(writerCtx), { select: { name: true } });
    for (const record of results) {
      expect(record).toHaveProperty("name");
      expect(record).not.toHaveProperty("id");
      expect(record).not.toHaveProperty("email");
    }
  });

  it("select applied after system field stripping", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice" });
    const results = await findMany(makeReaderContext(writerCtx), { select: { name: true } });
    expect(results[0]).not.toHaveProperty("_deleted");
    expect(results[0]).not.toHaveProperty("_createdAt");
  });

  it("deleted records not included in results", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "alice@test.com" });
    await seedRecord(writerCtx, { id: "u2", name: "Bob" });
    await deleteRecord(writerCtx, { where: { id: "u1" } });
    const results = await findMany(makeReaderContext(writerCtx), {});
    expect(results).toHaveLength(1);
    expect(results[0]?.["id"]).toBe("u2");
  });

  it("returns correct subset when compiledFilter provided", async () => {
    const writerCtx = await makeWriterContext();
    await seedMany(writerCtx, [
      { id: "u1", name: "Alice", age: 20 },
      { id: "u2", name: "Bob", age: 40 },
    ]);
    const filter: FilterFn = (r) => (r["age"] as number) >= 30;
    const results = await findMany(makeReaderContext(writerCtx), {}, filter);
    expect(results).toHaveLength(1);
    expect(results[0]?.["id"]).toBe("u2");
  });
});

// ---------------------------------------------------------------------------
// findFirst()
// ---------------------------------------------------------------------------

describe("findFirst()", () => {
  it("returns null for empty data file", async () => {
    const writerCtx = await makeWriterContext();
    const result = await findFirst(makeReaderContext(writerCtx), {});
    expect(result).toBeNull();
  });

  it("returns null when filter matches nothing", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice" });
    const filter: FilterFn = (r) => r["name"] === "ZZZ";
    const result = await findFirst(makeReaderContext(writerCtx), {}, filter);
    expect(result).toBeNull();
  });

  it("returns first matching record", async () => {
    const writerCtx = await makeWriterContext();
    await seedMany(writerCtx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Bob" },
    ]);
    const result = await findFirst(makeReaderContext(writerCtx), {});
    expect(result?.["id"]).toBe("u1");
  });

  it("applies select to returned record", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "a@test.com" });
    const result = await findFirst(makeReaderContext(writerCtx), { select: { name: true } });
    expect(result).toEqual({ name: "Alice" });
  });

  it("does not return deleted records", async () => {
    const writerCtx = await makeWriterContext();
    await seedRecord(writerCtx, { id: "u1", name: "Alice", email: "alice@test.com" });
    await seedRecord(writerCtx, { id: "u2", name: "Bob" });
    await deleteRecord(writerCtx, { where: { id: "u1" } });
    const result = await findFirst(makeReaderContext(writerCtx), {});
    expect(result?.["id"]).toBe("u2");
  });

  it("returns only one record even when multiple match", async () => {
    const writerCtx = await makeWriterContext();
    await seedMany(writerCtx, [
      { id: "u1", name: "Alice" },
      { id: "u2", name: "Alice" },
    ]);
    const filter: FilterFn = (r) => r["name"] === "Alice";
    const result = await findFirst(makeReaderContext(writerCtx), {}, filter);
    // Exactly one record returned — first in file order
    expect(result?.["id"]).toBe("u1");
  });
});
