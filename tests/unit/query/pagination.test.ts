// tests/unit/query/pagination.test.ts

import { describe, it, expect } from "vitest";
import { QueryError } from "../../../src/errors/index.js";
import {
  applyPagination,
  buildPaginationStrategy,
  getPageInfo,
  compileFilter,
  compileSort,
  applySort,
} from "../../../src/query/index.js";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makeRecords(count: number): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, i) => ({ id: `rec-${i + 1}`, index: i + 1 }));
}

// ---------------------------------------------------------------------------
// describe('applyPagination()')
// ---------------------------------------------------------------------------

describe("applyPagination()", () => {
  // -------------------------------------------------------------------------
  // No-op cases
  // -------------------------------------------------------------------------

  describe("no-op cases", () => {
    it("both undefined returns original array reference", () => {
      const records = makeRecords(5);
      const result = applyPagination(records, undefined, undefined);
      expect(result).toBe(records);
    });

    it("explicitly verify same reference via expect(result).toBe(input)", () => {
      const records = makeRecords(3);
      const result = applyPagination(records, undefined, undefined);
      expect(result).toBe(records);
    });
  });

  // -------------------------------------------------------------------------
  // skip only
  // -------------------------------------------------------------------------

  describe("skip only", () => {
    it("skip: 0 returns all records", () => {
      const records = makeRecords(5);
      const result = applyPagination(records, 0, undefined);
      expect(result).toHaveLength(5);
    });

    it("skip: 1 removes first record", () => {
      const records = makeRecords(5);
      const result = applyPagination(records, 1, undefined);
      expect(result).toHaveLength(4);
      expect(result[0]?.["index"]).toBe(2);
    });

    it("skip: 2 removes first two records", () => {
      const records = makeRecords(5);
      const result = applyPagination(records, 2, undefined);
      expect(result).toHaveLength(3);
      expect(result[0]?.["index"]).toBe(3);
    });

    it("skip equal to array length returns empty array", () => {
      const records = makeRecords(5);
      expect(applyPagination(records, 5, undefined)).toHaveLength(0);
    });

    it("skip greater than array length returns empty array", () => {
      const records = makeRecords(5);
      expect(applyPagination(records, 10, undefined)).toHaveLength(0);
    });

    it("does not mutate input array", () => {
      const records = makeRecords(5);
      applyPagination(records, 2, undefined);
      expect(records).toHaveLength(5);
    });
  });

  // -------------------------------------------------------------------------
  // take only
  // -------------------------------------------------------------------------

  describe("take only", () => {
    it("take: 0 returns empty array", () => {
      const records = makeRecords(5);
      expect(applyPagination(records, undefined, 0)).toHaveLength(0);
    });

    it("take: 1 returns first record only", () => {
      const records = makeRecords(5);
      const result = applyPagination(records, undefined, 1);
      expect(result).toHaveLength(1);
      expect(result[0]?.["index"]).toBe(1);
    });

    it("take: N returns first N records", () => {
      const records = makeRecords(5);
      const result = applyPagination(records, undefined, 3);
      expect(result).toHaveLength(3);
      expect(result.map((r) => r["index"])).toEqual([1, 2, 3]);
    });

    it("take greater than array length returns all records", () => {
      const records = makeRecords(3);
      const result = applyPagination(records, undefined, 10);
      expect(result).toHaveLength(3);
    });

    it("does not mutate input array", () => {
      const records = makeRecords(5);
      applyPagination(records, undefined, 2);
      expect(records).toHaveLength(5);
    });
  });

  // -------------------------------------------------------------------------
  // skip + take
  // -------------------------------------------------------------------------

  describe("skip + take", () => {
    it("skip: 1, take: 2 returns correct window", () => {
      const records = makeRecords(5);
      const result = applyPagination(records, 1, 2);
      expect(result.map((r) => r["index"])).toEqual([2, 3]);
    });

    it("skip: 2, take: 10 with 5 records returns 3 records", () => {
      const records = makeRecords(5);
      const result = applyPagination(records, 2, 10);
      expect(result).toHaveLength(3);
    });

    it("skip: 0, take: 0 returns empty array", () => {
      const records = makeRecords(5);
      expect(applyPagination(records, 0, 0)).toHaveLength(0);
    });

    it("window slides correctly across array", () => {
      const records = makeRecords(10);
      const page1 = applyPagination(records, 0, 3);
      const page2 = applyPagination(records, 3, 3);
      const page3 = applyPagination(records, 6, 3);
      expect(page1.map((r) => r["index"])).toEqual([1, 2, 3]);
      expect(page2.map((r) => r["index"])).toEqual([4, 5, 6]);
      expect(page3.map((r) => r["index"])).toEqual([7, 8, 9]);
    });
  });

  // -------------------------------------------------------------------------
  // Validation
  // -------------------------------------------------------------------------

  describe("validation", () => {
    it("negative skip throws QueryError", () => {
      expect(() => applyPagination([], -1, undefined)).toThrow(QueryError);
    });

    it("negative take throws QueryError", () => {
      expect(() => applyPagination([], undefined, -1)).toThrow(QueryError);
    });

    it("float skip (e.g. 1.5) throws QueryError", () => {
      expect(() => applyPagination([], 1.5, undefined)).toThrow(QueryError);
    });

    it("float take (e.g. 2.7) throws QueryError", () => {
      expect(() => applyPagination([], undefined, 2.7)).toThrow(QueryError);
    });

    it("NaN skip throws QueryError", () => {
      expect(() => applyPagination([], NaN, undefined)).toThrow(QueryError);
    });

    it("NaN take throws QueryError", () => {
      expect(() => applyPagination([], undefined, NaN)).toThrow(QueryError);
    });

    it("Infinity skip throws QueryError", () => {
      expect(() => applyPagination([], Infinity, undefined)).toThrow(QueryError);
    });

    it("Infinity take throws QueryError", () => {
      expect(() => applyPagination([], undefined, Infinity)).toThrow(QueryError);
    });

    it("0 is valid for skip — no error", () => {
      expect(() => applyPagination([], 0, undefined)).not.toThrow();
    });

    it("0 is valid for take — no error", () => {
      expect(() => applyPagination([], undefined, 0)).not.toThrow();
    });

    it("undefined is valid for both — no error", () => {
      expect(() => applyPagination([], undefined, undefined)).not.toThrow();
    });
  });
});

// ---------------------------------------------------------------------------
// describe('buildPaginationStrategy()')
// ---------------------------------------------------------------------------

describe("buildPaginationStrategy()", () => {
  it("no orderBy: skip/take assigned to scan, post undefined", () => {
    const result = buildPaginationStrategy(undefined, 5, 10);
    expect(result.scanSkip).toBe(5);
    expect(result.scanTake).toBe(10);
    expect(result.postSkip).toBeUndefined();
    expect(result.postTake).toBeUndefined();
  });

  it("no orderBy, empty array: same as no orderBy", () => {
    const result = buildPaginationStrategy([], 5, 10);
    expect(result.scanSkip).toBe(5);
    expect(result.scanTake).toBe(10);
    expect(result.postSkip).toBeUndefined();
    expect(result.postTake).toBeUndefined();
  });

  it("with orderBy: skip/take assigned to post, scan undefined", () => {
    const result = buildPaginationStrategy({ name: "asc" }, 5, 10);
    expect(result.scanSkip).toBeUndefined();
    expect(result.scanTake).toBeUndefined();
    expect(result.postSkip).toBe(5);
    expect(result.postTake).toBe(10);
  });

  it("with single-field orderBy: deferred to post", () => {
    const result = buildPaginationStrategy({ score: "desc" }, 0, 20);
    expect(result.postSkip).toBe(0);
    expect(result.postTake).toBe(20);
  });

  it("with multi-field orderBy array: deferred to post", () => {
    const result = buildPaginationStrategy([{ name: "asc" }, { score: "desc" }], 1, 5);
    expect(result.scanSkip).toBeUndefined();
    expect(result.postSkip).toBe(1);
    expect(result.postTake).toBe(5);
  });

  it("all four output fields present regardless of input", () => {
    const result = buildPaginationStrategy(undefined, undefined, undefined);
    expect("scanSkip" in result).toBe(true);
    expect("scanTake" in result).toBe(true);
    expect("postSkip" in result).toBe(true);
    expect("postTake" in result).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// describe('getPageInfo()')
// ---------------------------------------------------------------------------

describe("getPageInfo()", () => {
  it("totalCount equals input totalMatchedCount", () => {
    const info = getPageInfo(25, 0, 10);
    expect(info.totalCount).toBe(25);
  });

  it("returnedCount equals take when within bounds", () => {
    const info = getPageInfo(25, 0, 10);
    expect(info.returnedCount).toBe(10);
  });

  it("returnedCount equals remaining records when near end", () => {
    const info = getPageInfo(25, 20, 10);
    expect(info.returnedCount).toBe(5);
  });

  it("returnedCount is 0 when skip >= totalMatchedCount", () => {
    const info = getPageInfo(25, 30, 10);
    expect(info.returnedCount).toBe(0);
  });

  it("hasNextPage is false when take is undefined", () => {
    const info = getPageInfo(25, 0, undefined);
    expect(info.hasNextPage).toBe(false);
  });

  it("hasNextPage is false when on last page", () => {
    const info = getPageInfo(25, 20, 10);
    expect(info.hasNextPage).toBe(false);
  });

  it("hasNextPage is true when more records exist", () => {
    const info = getPageInfo(25, 0, 10);
    expect(info.hasNextPage).toBe(true);
  });

  it("hasPreviousPage is false when skip is 0", () => {
    const info = getPageInfo(25, 0, 10);
    expect(info.hasPreviousPage).toBe(false);
  });

  it("hasPreviousPage is false when skip is undefined", () => {
    const info = getPageInfo(25, undefined, 10);
    expect(info.hasPreviousPage).toBe(false);
  });

  it("hasPreviousPage is true when skip > 0", () => {
    const info = getPageInfo(25, 5, 10);
    expect(info.hasPreviousPage).toBe(true);
  });

  it("all fields correct for first page: skip: 0, take: 10 with 25 total records", () => {
    const info = getPageInfo(25, 0, 10);
    expect(info.totalCount).toBe(25);
    expect(info.returnedCount).toBe(10);
    expect(info.hasNextPage).toBe(true);
    expect(info.hasPreviousPage).toBe(false);
  });

  it("all fields correct for middle page: skip: 10, take: 10 with 25 total records", () => {
    const info = getPageInfo(25, 10, 10);
    expect(info.totalCount).toBe(25);
    expect(info.returnedCount).toBe(10);
    expect(info.hasNextPage).toBe(true);
    expect(info.hasPreviousPage).toBe(true);
  });

  it("all fields correct for last page: skip: 20, take: 10 with 25 total records", () => {
    const info = getPageInfo(25, 20, 10);
    expect(info.totalCount).toBe(25);
    expect(info.returnedCount).toBe(5);
    expect(info.hasNextPage).toBe(false);
    expect(info.hasPreviousPage).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Integration — filter + sort + pagination
// ---------------------------------------------------------------------------

describe("integration — filter + sort + pagination", () => {
  const baseRecords: Record<string, unknown>[] = [
    { id: "1", name: "Charlie", active: true, score: 30 },
    { id: "2", name: "Alice", active: true, score: 10 },
    { id: "3", name: "Bob", active: false, score: 20 },
    { id: "4", name: "Dave", active: true, score: 40 },
    { id: "5", name: "Eve", active: true, score: 50 },
  ];

  it("filter + sort + paginate returns correct window of sorted active records", () => {
    const filter = compileFilter({ active: true });
    const filtered = baseRecords.filter(filter);
    const sort = compileSort({ name: "asc" })!;
    const sorted = applySort(filtered, sort);
    const result = applyPagination(sorted, 1, 2);
    // Active records sorted by name: Alice, Charlie, Dave, Eve → skip 1 take 2 = Charlie, Dave
    expect(result.map((r) => r["name"])).toEqual(["Charlie", "Dave"]);
  });

  it("empty filter result with sort and pagination returns []", () => {
    const filter = compileFilter({ active: false, name: "Alice" });
    const filtered = baseRecords.filter(filter);
    const sort = compileSort({ name: "asc" })!;
    const sorted = applySort(filtered, sort);
    const result = applyPagination(sorted, 0, 10);
    expect(result).toHaveLength(0);
  });

  it("all records matching filter with take larger than result returns all matched records", () => {
    const filter = compileFilter({ active: true });
    const filtered = baseRecords.filter(filter);
    const sort = compileSort({ name: "asc" })!;
    const sorted = applySort(filtered, sort);
    const result = applyPagination(sorted, 0, 100);
    expect(result).toHaveLength(4); // Alice, Charlie, Dave, Eve
  });

  it("buildPaginationStrategy + applySort + applyPagination consistent without orderBy", () => {
    const strategy = buildPaginationStrategy(undefined, 1, 2);
    // No orderBy: scan handles it, post is undefined → applyPagination is a no-op
    const preSorted = baseRecords.slice(
      strategy.scanSkip ?? 0,
      (strategy.scanSkip ?? 0) + (strategy.scanTake ?? baseRecords.length),
    );
    const sorted = applySort(preSorted, undefined);
    const result = applyPagination(sorted, strategy.postSkip, strategy.postTake);
    // applyPagination is no-op since postSkip/postTake are undefined
    expect(result).toBe(sorted);
    expect(result).toHaveLength(2);
  });

  it("buildPaginationStrategy + applySort + applyPagination consistent with orderBy", () => {
    const orderBy = { score: "asc" } as const;
    const strategy = buildPaginationStrategy(orderBy, 1, 2);
    // With orderBy: scan returns all, sort applied, then paginate
    const sort = compileSort(strategy.scanSkip === undefined ? orderBy : undefined);
    const sorted = applySort(baseRecords, sort);
    const result = applyPagination(sorted, strategy.postSkip, strategy.postTake);
    // sorted by score asc: 10,20,30,40,50 → skip 1 take 2 → 20,30
    expect(result.map((r) => r["score"])).toEqual([20, 30]);
  });
});
