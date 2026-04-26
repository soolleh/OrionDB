// tests/unit/query/filter.test.ts

import { describe, it, expect } from "vitest";
import { QueryError } from "../../../src/errors/index.js";
import { compileFilter } from "../../../src/query/index.js";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makeRecord(overrides?: Record<string, unknown>): Record<string, unknown> {
  return {
    id: "rec-1",
    name: "Alice",
    email: "alice@example.com",
    age: 30,
    active: true,
    status: "active",
    score: 100,
    createdAt: "2024-01-15T10:00:00.000Z",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// describe('compileFilter()')
// ---------------------------------------------------------------------------

describe("compileFilter()", () => {
  // -------------------------------------------------------------------------
  // Passthrough cases
  // -------------------------------------------------------------------------

  describe("passthrough cases", () => {
    it("undefined where returns true for any record", () => {
      const filter = compileFilter(undefined);
      expect(filter(makeRecord())).toBe(true);
    });

    it("empty object {} returns true for any record", () => {
      const filter = compileFilter({});
      expect(filter(makeRecord())).toBe(true);
    });
  });

  // -------------------------------------------------------------------------
  // Scalar shorthand
  // -------------------------------------------------------------------------

  describe("scalar shorthand", () => {
    it('{ name: "Alice" } matches record where name === "Alice"', () => {
      const filter = compileFilter({ name: "Alice" });
      expect(filter(makeRecord())).toBe(true);
    });

    it('{ name: "Alice" } does not match name === "Bob"', () => {
      const filter = compileFilter({ name: "Alice" });
      expect(filter(makeRecord({ name: "Bob" }))).toBe(false);
    });

    it("{ age: null } matches record where age === null", () => {
      const filter = compileFilter({ age: null });
      expect(filter(makeRecord({ age: null }))).toBe(true);
    });

    it("{ age: null } does not match record where age === 30", () => {
      const filter = compileFilter({ age: null });
      expect(filter(makeRecord({ age: 30 }))).toBe(false);
    });

    it("{ active: true } matches boolean field correctly", () => {
      const filter = compileFilter({ active: true });
      expect(filter(makeRecord({ active: true }))).toBe(true);
      expect(filter(makeRecord({ active: false }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // Multiple top-level keys — implicit AND
  // -------------------------------------------------------------------------

  describe("multiple top-level keys — implicit AND", () => {
    it("matches only when both conditions are true", () => {
      const filter = compileFilter({ name: "Alice", active: true });
      expect(filter(makeRecord({ name: "Alice", active: true }))).toBe(true);
    });

    it("does not match when only one condition is true", () => {
      const filter = compileFilter({ name: "Alice", active: true });
      expect(filter(makeRecord({ name: "Alice", active: false }))).toBe(false);
      expect(filter(makeRecord({ name: "Bob", active: true }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // equals operator
  // -------------------------------------------------------------------------

  describe("equals operator", () => {
    it("matches exact string", () => {
      const filter = compileFilter({ name: { equals: "Alice" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(true);
      expect(filter(makeRecord({ name: "Bob" }))).toBe(false);
    });

    it("matches exact number", () => {
      const filter = compileFilter({ age: { equals: 30 } });
      expect(filter(makeRecord({ age: 30 }))).toBe(true);
      expect(filter(makeRecord({ age: 25 }))).toBe(false);
    });

    it("matches null", () => {
      const filter = compileFilter({ age: { equals: null } });
      expect(filter(makeRecord({ age: null }))).toBe(true);
      expect(filter(makeRecord({ age: 30 }))).toBe(false);
    });

    it('does not match undefined field with equals: "value"', () => {
      const filter = compileFilter({ missingField: { equals: "value" } });
      expect(filter(makeRecord())).toBe(false);
    });

    it("matches Date stored as ISO string when equals is a Date instance", () => {
      const date = new Date("2024-01-15T10:00:00.000Z");
      const filter = compileFilter({ createdAt: { equals: date } });
      expect(filter(makeRecord({ createdAt: "2024-01-15T10:00:00.000Z" }))).toBe(true);
      expect(filter(makeRecord({ createdAt: "2024-01-16T10:00:00.000Z" }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // not operator
  // -------------------------------------------------------------------------

  describe("not operator", () => {
    it('{ name: { not: "Alice" } } matches name !== "Alice"', () => {
      const filter = compileFilter({ name: { not: "Alice" } });
      expect(filter(makeRecord({ name: "Bob" }))).toBe(true);
    });

    it('{ name: { not: "Alice" } } does not match name === "Alice"', () => {
      const filter = compileFilter({ name: { not: "Alice" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(false);
    });

    it("nested filter object: { age: { not: { gt: 18 } } } matches record where age <= 18", () => {
      const filter = compileFilter({ age: { not: { gt: 18 } } });
      expect(filter(makeRecord({ age: 10 }))).toBe(true);
      expect(filter(makeRecord({ age: 18 }))).toBe(true);
      expect(filter(makeRecord({ age: 19 }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // in operator
  // -------------------------------------------------------------------------

  describe("in operator", () => {
    it("matches when field value is in array", () => {
      const filter = compileFilter({ status: { in: ["active", "inactive"] } });
      expect(filter(makeRecord({ status: "active" }))).toBe(true);
    });

    it("does not match when field value is not in array", () => {
      const filter = compileFilter({ status: { in: ["inactive"] } });
      expect(filter(makeRecord({ status: "active" }))).toBe(false);
    });

    it("empty in array never matches", () => {
      const filter = compileFilter({ status: { in: [] } });
      expect(filter(makeRecord())).toBe(false);
    });

    it("throws QueryError at compile time for non-array value", () => {
      expect(() => compileFilter({ status: { in: "active" as unknown as string[] } })).toThrow(QueryError);
    });
  });

  // -------------------------------------------------------------------------
  // notIn operator
  // -------------------------------------------------------------------------

  describe("notIn operator", () => {
    it("does not match when field value is in array", () => {
      const filter = compileFilter({ status: { notIn: ["active"] } });
      expect(filter(makeRecord({ status: "active" }))).toBe(false);
    });

    it("matches when field value is not in array", () => {
      const filter = compileFilter({ status: { notIn: ["inactive"] } });
      expect(filter(makeRecord({ status: "active" }))).toBe(true);
    });

    it("empty notIn array always matches", () => {
      const filter = compileFilter({ status: { notIn: [] } });
      expect(filter(makeRecord())).toBe(true);
    });
  });

  // -------------------------------------------------------------------------
  // contains operator
  // -------------------------------------------------------------------------

  describe("contains operator", () => {
    it("matches substring in string field", () => {
      const filter = compileFilter({ name: { contains: "lic" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(true);
    });

    it("does not match when substring absent", () => {
      const filter = compileFilter({ name: { contains: "xyz" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(false);
    });

    it("returns false (not throw) for non-string field value", () => {
      const filter = compileFilter({ age: { contains: "3" } });
      expect(() => filter(makeRecord({ age: 30 }))).not.toThrow();
      expect(filter(makeRecord({ age: 30 }))).toBe(false);
    });

    it("is case-sensitive", () => {
      const filter = compileFilter({ name: { contains: "alice" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // startsWith operator
  // -------------------------------------------------------------------------

  describe("startsWith operator", () => {
    it("matches prefix", () => {
      const filter = compileFilter({ name: { startsWith: "Ali" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(true);
    });

    it("does not match non-prefix", () => {
      const filter = compileFilter({ name: { startsWith: "ice" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(false);
    });

    it("returns false for non-string field", () => {
      const filter = compileFilter({ age: { startsWith: "3" } });
      expect(filter(makeRecord({ age: 30 }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // endsWith operator
  // -------------------------------------------------------------------------

  describe("endsWith operator", () => {
    it("matches suffix", () => {
      const filter = compileFilter({ name: { endsWith: "ice" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(true);
    });

    it("does not match non-suffix", () => {
      const filter = compileFilter({ name: { endsWith: "Ali" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(false);
    });

    it("returns false for non-string field", () => {
      const filter = compileFilter({ age: { endsWith: "0" } });
      expect(filter(makeRecord({ age: 30 }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // lt, lte, gt, gte — numbers
  // -------------------------------------------------------------------------

  describe("lt / lte / gt / gte — numbers", () => {
    it("{ age: { gt: 18 } } matches age === 30", () => {
      const filter = compileFilter({ age: { gt: 18 } });
      expect(filter(makeRecord({ age: 30 }))).toBe(true);
    });

    it("{ age: { gt: 18 } } does not match age === 18", () => {
      const filter = compileFilter({ age: { gt: 18 } });
      expect(filter(makeRecord({ age: 18 }))).toBe(false);
    });

    it("{ age: { gte: 18 } } matches age === 18", () => {
      const filter = compileFilter({ age: { gte: 18 } });
      expect(filter(makeRecord({ age: 18 }))).toBe(true);
    });

    it("{ age: { lt: 100 } } matches age === 30", () => {
      const filter = compileFilter({ age: { lt: 100 } });
      expect(filter(makeRecord({ age: 30 }))).toBe(true);
    });

    it("{ age: { lte: 30 } } matches age === 30", () => {
      const filter = compileFilter({ age: { lte: 30 } });
      expect(filter(makeRecord({ age: 30 }))).toBe(true);
    });

    it("{ age: { gt: 18, lte: 65 } } matches age === 30", () => {
      const filter = compileFilter({ age: { gt: 18, lte: 65 } });
      expect(filter(makeRecord({ age: 30 }))).toBe(true);
    });

    it("{ age: { gt: 18, lte: 65 } } does not match age === 10", () => {
      const filter = compileFilter({ age: { gt: 18, lte: 65 } });
      expect(filter(makeRecord({ age: 10 }))).toBe(false);
    });

    it("returns false (not throw) for non-numeric field value", () => {
      const filter = compileFilter({ age: { gt: 18 } });
      expect(() => filter(makeRecord({ age: "thirty" }))).not.toThrow();
      expect(filter(makeRecord({ age: "thirty" }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // lt, lte, gt, gte — dates
  // -------------------------------------------------------------------------

  describe("lt / lte / gt / gte — dates", () => {
    it("matches ISO string field against Date instance value", () => {
      const date = new Date("2024-01-01T00:00:00.000Z");
      const filter = compileFilter({ createdAt: { gt: date } });
      expect(filter(makeRecord({ createdAt: "2024-01-15T10:00:00.000Z" }))).toBe(true);
      expect(filter(makeRecord({ createdAt: "2023-12-01T00:00:00.000Z" }))).toBe(false);
    });

    it("matches ISO string field against ISO string value", () => {
      const filter = compileFilter({ createdAt: { gte: "2024-01-15T10:00:00.000Z" } });
      expect(filter(makeRecord({ createdAt: "2024-01-15T10:00:00.000Z" }))).toBe(true);
      expect(filter(makeRecord({ createdAt: "2024-01-14T00:00:00.000Z" }))).toBe(false);
    });

    it("gt date comparison works across different dates", () => {
      const filter = compileFilter({ createdAt: { gt: "2024-01-10T00:00:00.000Z" } });
      expect(filter(makeRecord({ createdAt: "2024-01-15T10:00:00.000Z" }))).toBe(true);
      expect(filter(makeRecord({ createdAt: "2024-01-05T00:00:00.000Z" }))).toBe(false);
    });

    it("returns false for non-date field value", () => {
      const filter = compileFilter({ createdAt: { gt: "2024-01-01T00:00:00.000Z" } });
      expect(filter(makeRecord({ createdAt: 12345 }))).toBe(false);
    });

    it("throws QueryError at compile time for non-date, non-number value", () => {
      expect(() => compileFilter({ age: { gt: "not-a-date-and-not-a-number" } })).toThrow(QueryError);
    });
  });

  // -------------------------------------------------------------------------
  // AND operator
  // -------------------------------------------------------------------------

  describe("AND operator", () => {
    it("array form: matches when all sub-clauses match", () => {
      const filter = compileFilter({ AND: [{ name: "Alice" }, { active: true }] });
      expect(filter(makeRecord({ name: "Alice", active: true }))).toBe(true);
    });

    it("array form: does not match when any sub-clause fails", () => {
      const filter = compileFilter({ AND: [{ name: "Alice" }, { active: true }] });
      expect(filter(makeRecord({ name: "Alice", active: false }))).toBe(false);
    });

    it("single object form: treated as single filter", () => {
      const filter = compileFilter({ AND: { name: "Alice" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(true);
      expect(filter(makeRecord({ name: "Bob" }))).toBe(false);
    });

    it("empty array matches all records", () => {
      const filter = compileFilter({ AND: [] });
      expect(filter(makeRecord())).toBe(true);
    });

    it("throws QueryError at compile time for non-object, non-array value", () => {
      expect(() => compileFilter({ AND: "invalid" as unknown as [] })).toThrow(QueryError);
    });
  });

  // -------------------------------------------------------------------------
  // OR operator
  // -------------------------------------------------------------------------

  describe("OR operator", () => {
    it("matches when at least one sub-clause matches", () => {
      const filter = compileFilter({ OR: [{ name: "Alice" }, { name: "Bob" }] });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(true);
      expect(filter(makeRecord({ name: "Bob" }))).toBe(true);
    });

    it("does not match when no sub-clause matches", () => {
      const filter = compileFilter({ OR: [{ name: "Alice" }, { name: "Bob" }] });
      expect(filter(makeRecord({ name: "Charlie" }))).toBe(false);
    });

    it("empty array never matches any record", () => {
      const filter = compileFilter({ OR: [] });
      expect(filter(makeRecord())).toBe(false);
    });

    it("throws QueryError at compile time for non-array value", () => {
      expect(() => compileFilter({ OR: "invalid" as unknown as [] })).toThrow(QueryError);
    });
  });

  // -------------------------------------------------------------------------
  // NOT operator
  // -------------------------------------------------------------------------

  describe("NOT operator", () => {
    it("single object form: negates the compiled filter", () => {
      const filter = compileFilter({ NOT: { name: "Alice" } });
      expect(filter(makeRecord({ name: "Alice" }))).toBe(false);
      expect(filter(makeRecord({ name: "Bob" }))).toBe(true);
    });

    it("array form: negates the AND combination of all sub-clauses", () => {
      const filter = compileFilter({ NOT: [{ name: "Alice" }, { active: true }] });
      // NOT(Alice AND active) = true when NOT both conditions hold
      expect(filter(makeRecord({ name: "Alice", active: true }))).toBe(false);
      expect(filter(makeRecord({ name: "Alice", active: false }))).toBe(true);
    });

    it("{ NOT: { active: true } } matches records where active !== true", () => {
      const filter = compileFilter({ NOT: { active: true } });
      expect(filter(makeRecord({ active: false }))).toBe(true);
      expect(filter(makeRecord({ active: true }))).toBe(false);
    });

    it("throws QueryError at compile time for invalid value", () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
      expect(() => compileFilter({ NOT: "invalid" as any })).toThrow(QueryError);
    });
  });

  // -------------------------------------------------------------------------
  // Absent and null field handling
  // -------------------------------------------------------------------------

  describe("absent and null field handling", () => {
    it('absent field does not satisfy equals: "value"', () => {
      const filter = compileFilter({ missingField: { equals: "value" } });
      expect(filter(makeRecord())).toBe(false);
    });

    it('absent field satisfies not: "value" when value is not undefined', () => {
      const filter = compileFilter({ missingField: { not: "something" } });
      expect(filter(makeRecord())).toBe(true);
    });

    it("null field satisfies equals: null", () => {
      const filter = compileFilter({ age: { equals: null } });
      expect(filter(makeRecord({ age: null }))).toBe(true);
    });

    it('null field does not satisfy contains: "x"', () => {
      const filter = compileFilter({ name: { contains: "x" } });
      expect(filter(makeRecord({ name: null }))).toBe(false);
    });

    it("null field does not satisfy gt: 0", () => {
      const filter = compileFilter({ age: { gt: 0 } });
      expect(filter(makeRecord({ age: null }))).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  // Unknown operator
  // -------------------------------------------------------------------------

  describe("unknown operator", () => {
    it("throws QueryError at compile time for unknown operator key", () => {
      expect(() => compileFilter({ name: { unknownOp: "value" } as Record<string, unknown> })).toThrow(QueryError);
    });
  });

  // -------------------------------------------------------------------------
  // Nested logical operators
  // -------------------------------------------------------------------------

  describe("nested logical operators", () => {
    it("AND containing OR sub-clauses evaluates correctly", () => {
      const filter = compileFilter({
        AND: [{ OR: [{ name: "Alice" }, { name: "Bob" }] }, { active: true }],
      });
      expect(filter(makeRecord({ name: "Alice", active: true }))).toBe(true);
      expect(filter(makeRecord({ name: "Bob", active: true }))).toBe(true);
      expect(filter(makeRecord({ name: "Charlie", active: true }))).toBe(false);
      expect(filter(makeRecord({ name: "Alice", active: false }))).toBe(false);
    });

    it("OR containing AND sub-clauses evaluates correctly", () => {
      const filter = compileFilter({
        OR: [{ AND: [{ name: "Alice" }, { active: true }] }, { name: "Bob" }],
      });
      expect(filter(makeRecord({ name: "Alice", active: true }))).toBe(true);
      expect(filter(makeRecord({ name: "Bob", active: false }))).toBe(true);
      expect(filter(makeRecord({ name: "Alice", active: false }))).toBe(false);
    });

    it("NOT containing AND evaluates correctly", () => {
      const filter = compileFilter({ NOT: { AND: [{ name: "Alice" }, { active: true }] } });
      expect(filter(makeRecord({ name: "Alice", active: true }))).toBe(false);
      expect(filter(makeRecord({ name: "Alice", active: false }))).toBe(true);
    });

    it("triple nesting: AND containing OR and NOT evaluates correctly", () => {
      const filter = compileFilter({
        AND: [{ OR: [{ name: "Alice" }, { name: "Bob" }] }, { NOT: { active: false } }],
      });
      expect(filter(makeRecord({ name: "Alice", active: true }))).toBe(true);
      expect(filter(makeRecord({ name: "Bob", active: true }))).toBe(true);
      expect(filter(makeRecord({ name: "Alice", active: false }))).toBe(false);
      expect(filter(makeRecord({ name: "Charlie", active: true }))).toBe(false);
    });
  });
});
