// tests/unit/query/sort.test.ts

import { describe, it, expect } from "vitest";
import { QueryError } from "../../../src/errors/index.js";
import { compileSort, applySort } from "../../../src/query/index.js";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function rec(overrides: Record<string, unknown>): Record<string, unknown> {
  return overrides;
}

// ---------------------------------------------------------------------------
// describe('compileSort()')
// ---------------------------------------------------------------------------

describe("compileSort()", () => {
  // -------------------------------------------------------------------------
  // Passthrough cases
  // -------------------------------------------------------------------------

  describe("passthrough cases", () => {
    it("undefined returns undefined", () => {
      expect(compileSort(undefined)).toBeUndefined();
    });

    it("empty array [] returns undefined", () => {
      expect(compileSort([])).toBeUndefined();
    });
  });

  // -------------------------------------------------------------------------
  // Single field — ascending
  // -------------------------------------------------------------------------

  describe("single field — ascending", () => {
    it("sorts strings alphabetically ascending", () => {
      const sort = compileSort({ name: "asc" })!;
      const records = [rec({ name: "Charlie" }), rec({ name: "Alice" }), rec({ name: "Bob" })];
      const sorted = [...records].sort(sort);
      expect(sorted.map((r) => r["name"])).toEqual(["Alice", "Bob", "Charlie"]);
    });

    it("sorts numbers ascending", () => {
      const sort = compileSort({ score: "asc" })!;
      const records = [rec({ score: 30 }), rec({ score: 10 }), rec({ score: 20 })];
      const sorted = [...records].sort(sort);
      expect(sorted.map((r) => r["score"])).toEqual([10, 20, 30]);
    });

    it("sorts booleans: false before true", () => {
      const sort = compileSort({ active: "asc" })!;
      const records = [rec({ active: true }), rec({ active: false }), rec({ active: true })];
      const sorted = [...records].sort(sort);
      expect(sorted[0]?.["active"]).toBe(false);
    });

    it("sorts ISO date strings ascending by date value", () => {
      const sort = compileSort({ createdAt: "asc" })!;
      const records = [
        rec({ createdAt: "2024-03-01T00:00:00.000Z" }),
        rec({ createdAt: "2024-01-01T00:00:00.000Z" }),
        rec({ createdAt: "2024-02-01T00:00:00.000Z" }),
      ];
      const sorted = [...records].sort(sort);
      expect(sorted.map((r) => r["createdAt"])).toEqual([
        "2024-01-01T00:00:00.000Z",
        "2024-02-01T00:00:00.000Z",
        "2024-03-01T00:00:00.000Z",
      ]);
    });
  });

  // -------------------------------------------------------------------------
  // Single field — descending
  // -------------------------------------------------------------------------

  describe("single field — descending", () => {
    it("sorts strings reverse alphabetically", () => {
      const sort = compileSort({ name: "desc" })!;
      const records = [rec({ name: "Alice" }), rec({ name: "Charlie" }), rec({ name: "Bob" })];
      const sorted = [...records].sort(sort);
      expect(sorted.map((r) => r["name"])).toEqual(["Charlie", "Bob", "Alice"]);
    });

    it("sorts numbers descending", () => {
      const sort = compileSort({ score: "desc" })!;
      const records = [rec({ score: 10 }), rec({ score: 30 }), rec({ score: 20 })];
      const sorted = [...records].sort(sort);
      expect(sorted.map((r) => r["score"])).toEqual([30, 20, 10]);
    });

    it("sorts booleans: true before false", () => {
      const sort = compileSort({ active: "desc" })!;
      const records = [rec({ active: false }), rec({ active: true }), rec({ active: false })];
      const sorted = [...records].sort(sort);
      expect(sorted[0]?.["active"]).toBe(true);
    });

    it("sorts ISO date strings descending", () => {
      const sort = compileSort({ createdAt: "desc" })!;
      const records = [
        rec({ createdAt: "2024-01-01T00:00:00.000Z" }),
        rec({ createdAt: "2024-03-01T00:00:00.000Z" }),
        rec({ createdAt: "2024-02-01T00:00:00.000Z" }),
      ];
      const sorted = [...records].sort(sort);
      expect(sorted.map((r) => r["createdAt"])).toEqual([
        "2024-03-01T00:00:00.000Z",
        "2024-02-01T00:00:00.000Z",
        "2024-01-01T00:00:00.000Z",
      ]);
    });
  });

  // -------------------------------------------------------------------------
  // Multi-field sort
  // -------------------------------------------------------------------------

  describe("multi-field sort", () => {
    it("primary sort field applied first", () => {
      const sort = compileSort([{ status: "asc" }, { name: "asc" }])!;
      const records = [
        rec({ status: "inactive", name: "Alice" }),
        rec({ status: "active", name: "Bob" }),
        rec({ status: "active", name: "Alice" }),
      ];
      const sorted = [...records].sort(sort);
      expect(sorted[0]?.["status"]).toBe("active");
      expect(sorted[1]?.["status"]).toBe("active");
      expect(sorted[2]?.["status"]).toBe("inactive");
    });

    it("secondary (tiebreaker) field applied when primary ties", () => {
      const sort = compileSort([{ status: "asc" }, { name: "asc" }])!;
      const records = [
        rec({ status: "active", name: "Bob" }),
        rec({ status: "active", name: "Alice" }),
        rec({ status: "inactive", name: "Charlie" }),
      ];
      const sorted = [...records].sort(sort);
      expect(sorted[0]?.["name"]).toBe("Alice");
      expect(sorted[1]?.["name"]).toBe("Bob");
      expect(sorted[2]?.["name"]).toBe("Charlie");
    });

    it("tertiary field applied when primary and secondary both tie", () => {
      const sort = compileSort([{ status: "asc" }, { name: "asc" }, { score: "asc" }])!;
      const records = [
        rec({ status: "active", name: "Alice", score: 20 }),
        rec({ status: "active", name: "Alice", score: 10 }),
        rec({ status: "active", name: "Bob", score: 5 }),
      ];
      const sorted = [...records].sort(sort);
      expect(sorted[0]?.["score"]).toBe(10);
      expect(sorted[1]?.["score"]).toBe(20);
      expect(sorted[2]?.["score"]).toBe(5);
    });

    it("records that tie on all sort fields retain stable order", () => {
      const sort = compileSort({ name: "asc" })!;
      const records = [rec({ name: "Alice", id: 1 }), rec({ name: "Alice", id: 2 }), rec({ name: "Alice", id: 3 })];
      const sorted = [...records].sort(sort);
      // All names tie — order among them should remain [1,2,3]
      expect(sorted.map((r) => r["id"])).toEqual([1, 2, 3]);
    });
  });

  // -------------------------------------------------------------------------
  // Null and undefined handling
  // -------------------------------------------------------------------------

  describe("null and undefined handling", () => {
    it("null values sort last in ascending order", () => {
      const sort = compileSort({ score: "asc" })!;
      const records = [rec({ score: null }), rec({ score: 10 }), rec({ score: 5 })];
      const sorted = [...records].sort(sort);
      expect(sorted[2]?.["score"]).toBeNull();
    });

    it("null values sort last in descending order", () => {
      const sort = compileSort({ score: "desc" })!;
      const records = [rec({ score: 10 }), rec({ score: null }), rec({ score: 5 })];
      const sorted = [...records].sort(sort);
      expect(sorted[2]?.["score"]).toBeNull();
    });

    it("undefined values sort last in ascending order", () => {
      const sort = compileSort({ score: "asc" })!;
      const records = [rec({ score: undefined }), rec({ score: 10 }), rec({ score: 5 })];
      const sorted = [...records].sort(sort);
      expect(sorted[2]?.["score"]).toBeUndefined();
    });

    it("undefined values sort last in descending order", () => {
      const sort = compileSort({ score: "desc" })!;
      const records = [rec({ score: 10 }), rec({ score: undefined }), rec({ score: 5 })];
      const sorted = [...records].sort(sort);
      expect(sorted[2]?.["score"]).toBeUndefined();
    });

    it("two null values compare as equal (return 0)", () => {
      const sort = compileSort({ score: "asc" })!;
      expect(sort(rec({ score: null }), rec({ score: null }))).toBe(0);
    });

    it("null and undefined both sort after any real value", () => {
      const sort = compileSort({ score: "asc" })!;
      expect(sort(rec({ score: null }), rec({ score: 5 }))).toBeGreaterThan(0);
      expect(sort(rec({ score: undefined }), rec({ score: 5 }))).toBeGreaterThan(0);
    });
  });

  // -------------------------------------------------------------------------
  // Date sorting
  // -------------------------------------------------------------------------

  describe("date sorting", () => {
    it("Date instances compared correctly as milliseconds", () => {
      const sort = compileSort({ ts: "asc" })!;
      const d1 = new Date("2024-01-01T00:00:00.000Z");
      const d2 = new Date("2024-06-01T00:00:00.000Z");
      const records = [rec({ ts: d2 }), rec({ ts: d1 })];
      const sorted = [...records].sort(sort);
      expect((sorted[0]?.["ts"] as Date).toISOString()).toBe("2024-01-01T00:00:00.000Z");
    });

    it("ISO string dates compared correctly", () => {
      const sort = compileSort({ ts: "asc" })!;
      const records = [rec({ ts: "2024-06-01T00:00:00.000Z" }), rec({ ts: "2024-01-01T00:00:00.000Z" })];
      const sorted = [...records].sort(sort);
      expect(sorted[0]?.["ts"]).toBe("2024-01-01T00:00:00.000Z");
    });

    it("mixed Date and ISO string compared correctly", () => {
      const sort = compileSort({ ts: "asc" })!;
      const records = [rec({ ts: new Date("2024-06-01T00:00:00.000Z") }), rec({ ts: "2024-01-01T00:00:00.000Z" })];
      const sorted = [...records].sort(sort);
      expect(sorted[0]?.["ts"]).toBe("2024-01-01T00:00:00.000Z");
    });
  });

  // -------------------------------------------------------------------------
  // Invalid direction
  // -------------------------------------------------------------------------

  describe("invalid direction", () => {
    it('throws QueryError at compile time for "ascending"', () => {
      expect(() => compileSort({ name: "ascending" as "asc" })).toThrow(QueryError);
    });

    it('throws QueryError at compile time for "DESC"', () => {
      expect(() => compileSort({ name: "DESC" as "asc" })).toThrow(QueryError);
    });

    it("throws QueryError at compile time for numeric direction", () => {
      expect(() => compileSort({ name: 1 as unknown as "asc" })).toThrow(QueryError);
    });
  });

  // -------------------------------------------------------------------------
  // Mixed types
  // -------------------------------------------------------------------------

  describe("mixed types", () => {
    it("mixed string and number values in same field return 0 — no throw", () => {
      const sort = compileSort({ value: "asc" })!;
      expect(() => sort(rec({ value: "hello" }), rec({ value: 42 }))).not.toThrow();
      expect(sort(rec({ value: "hello" }), rec({ value: 42 }))).toBe(0);
    });
  });
});

// ---------------------------------------------------------------------------
// describe('applySort()')
// ---------------------------------------------------------------------------

describe("applySort()", () => {
  it("returns original array reference when compiledSort is undefined", () => {
    const records = [rec({ name: "Alice" }), rec({ name: "Bob" })];
    const result = applySort(records, undefined);
    expect(result).toBe(records);
  });

  it("returns a new array (not same reference) when sort applied", () => {
    const sort = compileSort({ name: "asc" })!;
    const records = [rec({ name: "Bob" }), rec({ name: "Alice" })];
    const result = applySort(records, sort);
    expect(result).not.toBe(records);
  });

  it("does not mutate input array — verify original order unchanged", () => {
    const sort = compileSort({ name: "asc" })!;
    const records = [rec({ name: "Bob" }), rec({ name: "Alice" })];
    applySort(records, sort);
    expect(records[0]?.["name"]).toBe("Bob");
    expect(records[1]?.["name"]).toBe("Alice");
  });

  it("sorted result matches expected order for strings", () => {
    const sort = compileSort({ name: "asc" })!;
    const records = [rec({ name: "Charlie" }), rec({ name: "Alice" }), rec({ name: "Bob" })];
    const result = applySort(records, sort);
    expect(result.map((r) => r["name"])).toEqual(["Alice", "Bob", "Charlie"]);
  });

  it("sorted result matches expected order for numbers", () => {
    const sort = compileSort({ score: "desc" })!;
    const records = [rec({ score: 10 }), rec({ score: 30 }), rec({ score: 20 })];
    const result = applySort(records, sort);
    expect(result.map((r) => r["score"])).toEqual([30, 20, 10]);
  });

  it("empty array returns empty array", () => {
    const sort = compileSort({ name: "asc" })!;
    expect(applySort([], sort)).toEqual([]);
  });
});
