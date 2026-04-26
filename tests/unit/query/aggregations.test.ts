// tests/unit/query/aggregations.test.ts

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { QueryError } from "../../../src/errors/index.js";
import { IndexManagerImpl } from "../../../src/index-manager/index.js";
import type { IndexManagerOptions } from "../../../src/index-manager/index.js";
import {
  create,
  deleteRecord,
  FileSizeCounter,
  resolveModelPaths,
  initializeModelDirectory,
} from "../../../src/persistence/index.js";
import type { ModelWriterContext, ModelReaderContext } from "../../../src/persistence/index.js";
import type { ParsedModelDefinition, ParsedScalarField, ParsedRelationField } from "../../../src/schema/index.js";
import { count, aggregate, groupBy } from "../../../src/query/index.js";
import { compileFilter } from "../../../src/query/index.js";

// ---------------------------------------------------------------------------
// File system setup
// ---------------------------------------------------------------------------

let testDir: string;

beforeEach(() => {
  testDir = mkdtempSync(join(tmpdir(), "oriondb-query-test-"));
});

afterEach(() => {
  rmSync(testDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

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

let idCounter = 0;

function makeSchema(): ParsedModelDefinition {
  idCounter = 0;
  const fields = new Map<string, ParsedScalarField | ParsedRelationField>();

  fields.set(
    "id",
    makeScalarField("id", {
      primary: true,
      unique: true,
      required: true,
      hasDefault: true,
      defaultValue: () => `id-${++idCounter}`,
    }),
  );
  fields.set("name", makeScalarField("name", { required: true }));
  fields.set("score", makeScalarField("score", { type: "number" }));
  fields.set("active", makeScalarField("active", { type: "boolean", hasDefault: true, defaultValue: true }));
  fields.set(
    "status",
    makeScalarField("status", {
      type: "enum",
      enumValues: ["active", "inactive", "banned"],
      hasDefault: true,
      defaultValue: "active",
    }),
  );
  fields.set("createdAt", makeScalarField("createdAt", { type: "string" }));

  return {
    name: "TestModel",
    fields,
    primaryKeyField: "id",
    uniqueFields: new Set<string>(),
    indexedFields: new Set(["id"]),
    relationFields: new Map(),
  };
}

async function makeWriterContext(): Promise<ModelWriterContext> {
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

  return { modelName: "TestModel", paths, schema, indexManager, counter };
}

function makeReaderContext(writerCtx: ModelWriterContext): ModelReaderContext {
  return {
    modelName: writerCtx.modelName,
    paths: writerCtx.paths,
    schema: writerCtx.schema,
    indexManager: writerCtx.indexManager,
  };
}

async function seedRecords(ctx: ModelWriterContext, records: Record<string, unknown>[]): Promise<void> {
  for (const record of records) {
    await create(ctx, { data: record });
  }
}

// ---------------------------------------------------------------------------
// describe('count()')
// ---------------------------------------------------------------------------

describe("count()", () => {
  it("returns 0 for empty model", async () => {
    const wCtx = await makeWriterContext();
    const rCtx = makeReaderContext(wCtx);
    expect(await count(rCtx, {})).toBe(0);
  });

  it("returns total count when no where", async () => {
    const wCtx = await makeWriterContext();
    await seedRecords(wCtx, [
      { name: "Alice", status: "active" },
      { name: "Bob", status: "inactive" },
      { name: "Charlie", status: "active" },
    ]);
    const rCtx = makeReaderContext(wCtx);
    expect(await count(rCtx, {})).toBe(3);
  });

  it("returns filtered count with compiledFilter", async () => {
    const wCtx = await makeWriterContext();
    await seedRecords(wCtx, [
      { name: "Alice", status: "active" },
      { name: "Bob", status: "inactive" },
      { name: "Charlie", status: "active" },
    ]);
    const rCtx = makeReaderContext(wCtx);
    const filter = compileFilter({ status: "active" });
    expect(await count(rCtx, {}, filter)).toBe(2);
  });

  it("returns filtered count matching specific field value", async () => {
    const wCtx = await makeWriterContext();
    await seedRecords(wCtx, [
      { name: "Alice", status: "active" },
      { name: "Bob", status: "banned" },
    ]);
    const rCtx = makeReaderContext(wCtx);
    const filter = compileFilter({ status: "banned" });
    expect(await count(rCtx, {}, filter)).toBe(1);
  });

  it("does not count deleted records", async () => {
    const wCtx = await makeWriterContext();
    await seedRecords(wCtx, [
      { name: "Alice", status: "active" },
      { name: "Bob", status: "active" },
    ]);
    const bobId = `id-2`;
    await deleteRecord(wCtx, { where: { id: bobId } });
    const rCtx = makeReaderContext(wCtx);
    expect(await count(rCtx, {})).toBe(1);
  });

  it("returns correct count after deleteRecord", async () => {
    const wCtx = await makeWriterContext();
    await seedRecords(wCtx, [{ name: "Alice" }, { name: "Bob" }, { name: "Charlie" }]);
    await deleteRecord(wCtx, { where: { id: "id-1" } });
    const rCtx = makeReaderContext(wCtx);
    expect(await count(rCtx, {})).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// describe('aggregate()')
// ---------------------------------------------------------------------------

describe("aggregate()", () => {
  describe("empty input", () => {
    it("returns {} when no aggregation keys provided", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice", score: 10 }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, {});
      expect(result).toEqual({});
    });

    it("returns result with computed values for empty match set", async () => {
      const wCtx = await makeWriterContext();
      const rCtx = makeReaderContext(wCtx);
      const filter = compileFilter({ status: "active" });
      const result = await aggregate(rCtx, { _count: true }, filter);
      expect(result._count).toBe(0);
    });
  });

  describe("_count", () => {
    it("_count: true returns total matched records as number", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: 20 },
        { name: "Charlie", score: 30 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _count: true });
      expect(result._count).toBe(3);
    });

    it("_count: { score: true } returns count of records where score is not null/undefined", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: 20 },
        { name: "Charlie" }, // no score
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _count: { score: true } });
      expect(typeof result._count).toBe("object");
      expect((result._count as Record<string, number>)["score"]).toBe(2);
    });

    it("_count: { score: true } excludes records without the score field", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice", score: 10 }, { name: "Bob" }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _count: { score: true } });
      expect((result._count as Record<string, number>)["score"]).toBe(1);
    });

    it("multiple fields in _count object counted independently", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10, createdAt: "2024-01-01" },
        { name: "Bob", createdAt: "2024-01-02" },
        { name: "Charlie" }, // no score, no createdAt
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _count: { score: true, createdAt: true } });
      const countObj = result._count as Record<string, number>;
      expect(countObj["score"]).toBe(1);
      expect(countObj["createdAt"]).toBe(2);
    });
  });

  describe("_sum", () => {
    it("returns correct sum of numeric field", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: 20 },
        { name: "Charlie", score: 30 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _sum: { score: true } });
      expect(result._sum?.["score"]).toBe(60);
    });

    it("returns null when no numeric values exist for field", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice" }, { name: "Bob" }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _sum: { score: true } });
      expect(result._sum?.["score"]).toBeNull();
    });

    it("skips absent score field in sum — only present numeric values included", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice", score: 10 }, { name: "Bob" }, { name: "Charlie", score: 30 }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _sum: { score: true } });
      expect(result._sum?.["score"]).toBe(40);
    });

    it("correctly handles negative numbers in sum", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: -10 },
        { name: "Charlie", score: 30 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _sum: { score: true } });
      expect(result._sum?.["score"]).toBe(30);
    });

    it("returns null for field not present on any record", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice" }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _sum: { score: true } });
      expect(result._sum?.["score"]).toBeNull();
    });
  });

  describe("_avg", () => {
    it("returns correct average for numeric field", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: 20 },
        { name: "Charlie", score: 30 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _avg: { score: true } });
      expect(result._avg?.["score"]).toBe(20);
    });

    it("returns null for empty numeric set", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice" }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _avg: { score: true } });
      expect(result._avg?.["score"]).toBeNull();
    });

    it("correctly handles decimal averages", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: 11 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _avg: { score: true } });
      expect(result._avg?.["score"]).toBe(10.5);
    });

    it("_avg and _sum consistent: _avg === _sum / _count", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: 20 },
        { name: "Charlie", score: 30 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _avg: { score: true }, _sum: { score: true }, _count: { score: true } });
      const cnt = (result._count as Record<string, number>)["score"] ?? 0;
      const sum = result._sum?.["score"] ?? 0;
      const avg = result._avg?.["score"] ?? 0;
      expect(avg).toBeCloseTo(sum / cnt);
    });
  });

  describe("_min", () => {
    it("returns minimum numeric value", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 30 },
        { name: "Bob", score: 10 },
        { name: "Charlie", score: 20 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _min: { score: true } });
      expect(result._min?.["score"]).toBe(10);
    });

    it("returns minimum string value (lexicographic)", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Charlie" }, { name: "Alice" }, { name: "Bob" }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _min: { name: true } });
      expect(result._min?.["name"]).toBe("Alice");
    });

    it("returns null when no values", async () => {
      const wCtx = await makeWriterContext();
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _min: { score: true } });
      expect(result._min?.["score"]).toBeNull();
    });

    it("numeric minimum takes precedence over string fallback", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 5 },
        { name: "Bob", score: 100 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _min: { score: true } });
      expect(result._min?.["score"]).toBe(5);
    });
  });

  describe("_max", () => {
    it("returns maximum numeric value", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: 50 },
        { name: "Charlie", score: 30 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _max: { score: true } });
      expect(result._max?.["score"]).toBe(50);
    });

    it("returns maximum string value (lexicographic)", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice" }, { name: "Zara" }, { name: "Bob" }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _max: { name: true } });
      expect(result._max?.["name"]).toBe("Zara");
    });

    it("returns null when no values", async () => {
      const wCtx = await makeWriterContext();
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, { _max: { score: true } });
      expect(result._max?.["score"]).toBeNull();
    });
  });

  describe("combined aggregations", () => {
    it("_count, _avg, _sum, _min, _max all computed correctly in single call", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 10 },
        { name: "Bob", score: 20 },
        { name: "Charlie", score: 30 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await aggregate(rCtx, {
        _count: true,
        _avg: { score: true },
        _sum: { score: true },
        _min: { score: true },
        _max: { score: true },
      });
      expect(result._count).toBe(3);
      expect(result._sum?.["score"]).toBe(60);
      expect(result._avg?.["score"]).toBe(20);
      expect(result._min?.["score"]).toBe(10);
      expect(result._max?.["score"]).toBe(30);
    });

    it("results consistent with individually computed values", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", score: 15 },
        { name: "Bob", score: 25 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const combined = await aggregate(rCtx, {
        _sum: { score: true },
        _avg: { score: true },
      });
      expect(combined._sum?.["score"]).toBe(40);
      expect(combined._avg?.["score"]).toBe(20);
    });
  });

  describe("with filter", () => {
    it("aggregations applied to filtered subset only", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active", score: 10 },
        { name: "Bob", status: "inactive", score: 999 },
        { name: "Charlie", status: "active", score: 20 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const filter = compileFilter({ status: "active" });
      const result = await aggregate(rCtx, { _sum: { score: true } }, filter);
      expect(result._sum?.["score"]).toBe(30);
    });

    it("count in aggregate matches standalone count for same filter", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active" },
        { name: "Bob", status: "inactive" },
        { name: "Charlie", status: "active" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const filter = compileFilter({ status: "active" });
      const standaloneCount = await count(rCtx, {}, filter);
      const aggResult = await aggregate(rCtx, { _count: true }, filter);
      expect(aggResult._count).toBe(standaloneCount);
    });
  });
});

// ---------------------------------------------------------------------------
// describe('groupBy()')
// ---------------------------------------------------------------------------

describe("groupBy()", () => {
  describe("validation", () => {
    it("throws QueryError for empty by array", async () => {
      const wCtx = await makeWriterContext();
      const rCtx = makeReaderContext(wCtx);
      await expect(groupBy(rCtx, { by: [] })).rejects.toThrow(QueryError);
    });

    it("throws QueryError for non-array by", async () => {
      const wCtx = await makeWriterContext();
      const rCtx = makeReaderContext(wCtx);
      await expect(groupBy(rCtx, { by: "status" as unknown as string[] })).rejects.toThrow(QueryError);
    });
  });

  describe("single-field grouping", () => {
    it("groups records by single string field correctly", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active" },
        { name: "Bob", status: "inactive" },
        { name: "Charlie", status: "active" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], _count: true });
      expect(result).toHaveLength(2);
    });

    it("each group contains only records with matching field value", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active" },
        { name: "Bob", status: "inactive" },
        { name: "Charlie", status: "active" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], _count: true });
      const activeGroup = result.find((g) => g["status"] === "active");
      const inactiveGroup = result.find((g) => g["status"] === "inactive");
      expect(activeGroup?._count).toBe(2);
      expect(inactiveGroup?._count).toBe(1);
    });

    it("number of groups equals number of distinct field values", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active" },
        { name: "Bob", status: "inactive" },
        { name: "Charlie", status: "banned" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"] });
      expect(result).toHaveLength(3);
    });

    it("group result contains the grouping field value", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice", status: "active" }]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"] });
      expect(result[0]?.["status"]).toBe("active");
    });
  });

  describe("multi-field grouping", () => {
    it("groups records by composite key of multiple fields", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active", active: true },
        { name: "Bob", status: "active", active: false },
        { name: "Charlie", status: "active", active: true },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status", "active"], _count: true });
      expect(result).toHaveLength(2);
    });

    it("records with same values for all by fields are in same group", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active", active: true },
        { name: "Charlie", status: "active", active: true },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status", "active"], _count: true });
      expect(result).toHaveLength(1);
      expect(result[0]?._count).toBe(2);
    });

    it("records differing in any by field are in different groups", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active", active: true },
        { name: "Bob", status: "active", active: false },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status", "active"], _count: true });
      expect(result).toHaveLength(2);
    });
  });

  describe("null group key", () => {
    it("records with absent by field form their own null group", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice" }, // no createdAt → null group
        { name: "Bob", createdAt: "2024-01-01" },
        { name: "Charlie" }, // no createdAt → null group
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["createdAt"], _count: true });
      const nullGroup = result.find((g) => g["createdAt"] === null || g["createdAt"] === undefined);
      expect(nullGroup?._count).toBe(2);
    });

    it("records without the by field are grouped in the same null group", async () => {
      const wCtx = await makeWriterContext();
      // Records without the field have it as undefined → maps to null in buildGroupKey
      await seedRecords(wCtx, [
        { name: "Alice" }, // no createdAt → null group
        { name: "Bob" }, // no createdAt → null group
        { name: "Charlie", createdAt: "2024-01-01" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["createdAt"], _count: true });
      // absent fields map to null in buildGroupKey
      const nullGroup = result.find((g) => g["createdAt"] === null || g["createdAt"] === undefined);
      expect(nullGroup?._count).toBe(2);
    });
  });

  describe("aggregations per group", () => {
    it("_count: true counted per group independently", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active" },
        { name: "Bob", status: "active" },
        { name: "Charlie", status: "inactive" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], _count: true });
      const activeGroup = result.find((g) => g["status"] === "active");
      const inactiveGroup = result.find((g) => g["status"] === "inactive");
      expect(activeGroup?._count).toBe(2);
      expect(inactiveGroup?._count).toBe(1);
    });

    it("_sum computed per group", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active", score: 10 },
        { name: "Bob", status: "active", score: 20 },
        { name: "Charlie", status: "inactive", score: 100 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], _sum: { score: true } });
      const activeGroup = result.find((g) => g["status"] === "active");
      expect(activeGroup?._sum?.["score"]).toBe(30);
    });

    it("_avg computed per group", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active", score: 10 },
        { name: "Bob", status: "active", score: 30 },
        { name: "Charlie", status: "inactive", score: 5 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], _avg: { score: true } });
      const activeGroup = result.find((g) => g["status"] === "active");
      expect(activeGroup?._avg?.["score"]).toBe(20);
    });

    it("_min and _max computed per group", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active", score: 10 },
        { name: "Bob", status: "active", score: 30 },
        { name: "Charlie", status: "inactive", score: 5 },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], _min: { score: true }, _max: { score: true } });
      const activeGroup = result.find((g) => g["status"] === "active");
      expect(activeGroup?._min?.["score"]).toBe(10);
      expect(activeGroup?._max?.["score"]).toBe(30);
    });
  });

  describe("ordering of groups", () => {
    it("orderBy applied to group results — groups sorted by specified field", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "inactive" },
        { name: "Bob", status: "active" },
        { name: "Charlie", status: "banned" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], orderBy: { status: "asc" } });
      expect(result.map((g) => g["status"])).toEqual(["active", "banned", "inactive"]);
    });

    it("without orderBy: groups in insertion order", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "inactive" },
        { name: "Bob", status: "active" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"] });
      expect(result[0]?.["status"]).toBe("inactive");
      expect(result[1]?.["status"]).toBe("active");
    });
  });

  describe("pagination of groups", () => {
    it("take: 2 returns only first 2 groups", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active" },
        { name: "Bob", status: "inactive" },
        { name: "Charlie", status: "banned" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], orderBy: { status: "asc" }, take: 2 });
      expect(result).toHaveLength(2);
    });

    it("skip: 1 skips first group", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active" },
        { name: "Bob", status: "inactive" },
        { name: "Charlie", status: "banned" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], orderBy: { status: "asc" }, skip: 1 });
      expect(result[0]?.["status"]).toBe("banned");
    });

    it("skip + take window applied to group array", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "Alice", status: "active" },
        { name: "Bob", status: "inactive" },
        { name: "Charlie", status: "banned" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, {
        by: ["status"],
        orderBy: { status: "asc" },
        skip: 1,
        take: 1,
      });
      expect(result).toHaveLength(1);
      expect(result[0]?.["status"]).toBe("banned");
    });
  });

  describe("empty result", () => {
    it("returns [] when no records match where", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [{ name: "Alice", status: "active" }]);
      const rCtx = makeReaderContext(wCtx);
      const filter = compileFilter({ status: "banned" });
      const result = await groupBy(rCtx, { by: ["status"] }, filter);
      expect(result).toHaveLength(0);
    });
  });

  describe("full pipeline", () => {
    it("seed 10 records across 3 status values — groupBy returns 3 groups with correct counts summing to total", async () => {
      const wCtx = await makeWriterContext();
      await seedRecords(wCtx, [
        { name: "R1", status: "active" },
        { name: "R2", status: "active" },
        { name: "R3", status: "active" },
        { name: "R4", status: "active" },
        { name: "R5", status: "inactive" },
        { name: "R6", status: "inactive" },
        { name: "R7", status: "inactive" },
        { name: "R8", status: "banned" },
        { name: "R9", status: "banned" },
        { name: "R10", status: "banned" },
      ]);
      const rCtx = makeReaderContext(wCtx);
      const result = await groupBy(rCtx, { by: ["status"], _count: true });

      expect(result).toHaveLength(3);

      const activeGroup = result.find((g) => g["status"] === "active");
      const inactiveGroup = result.find((g) => g["status"] === "inactive");
      const bannedGroup = result.find((g) => g["status"] === "banned");

      expect(activeGroup?._count).toBe(4);
      expect(inactiveGroup?._count).toBe(3);
      expect(bannedGroup?._count).toBe(3);

      const counts = [activeGroup?._count, inactiveGroup?._count, bannedGroup?._count].map((v) =>
        typeof v === "number" ? v : 0,
      );
      const total = counts.reduce((s, v) => s + v, 0);
      expect(total).toBe(10);
    });
  });
});
