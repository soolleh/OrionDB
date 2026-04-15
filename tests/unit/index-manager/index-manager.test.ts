import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { IndexManagerImpl } from "../../../src/index-manager/index.js";
import type { IndexManagerOptions } from "../../../src/index-manager/index.js";

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

type TestRecord = Record<string, unknown>;

const OPTIONS: IndexManagerOptions = {
  primaryKeyField: "id",
  indexedFields: new Set(["email", "status"]),
};

// 'name' is intentionally NOT included in indexedFields
const RECORD_1: TestRecord = { id: "id-1", email: "alice@example.com", status: "active", name: "Alice" };
const RECORD_2: TestRecord = { id: "id-2", email: "bob@example.com", status: "inactive", name: "Bob" };
const RECORD_3: TestRecord = { id: "id-3", email: "carol@example.com", status: "active", name: "Carol" };

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("IndexManager", () => {
  let manager: IndexManagerImpl<TestRecord>;

  beforeEach(() => {
    manager = new IndexManagerImpl<TestRecord>(OPTIONS);
  });

  // -------------------------------------------------------------------------
  describe("add()", () => {
    it("adds primary key to physical index with correct offset", () => {
      manager.add(RECORD_1, 42);

      expect(manager.getOffset("id-1")).toBe(42);
    });

    it("adds primary key to logical index for each field in indexedFields", () => {
      manager.add(RECORD_1, 0);

      expect(manager.getByField("email", "alice@example.com")).toEqual(new Set(["id-1"]));
      expect(manager.getByField("status", "active")).toEqual(new Set(["id-1"]));
    });

    it("does NOT add the primary key field itself to the logical index", () => {
      manager.add(RECORD_1, 0);

      expect(manager.getByField("id", "id-1")).toBeUndefined();
    });

    it("does NOT add fields outside indexedFields to the logical index", () => {
      manager.add(RECORD_1, 0);

      expect(manager.getByField("name", "Alice")).toBeUndefined();
    });

    it("upsert: calling add() twice with same id overwrites the physical offset", () => {
      manager.add(RECORD_1, 0);
      manager.add(RECORD_1, 100);

      expect(manager.getOffset("id-1")).toBe(100);
    });

    it("upsert: calling add() twice with same id adds new logical index values", () => {
      manager.add(RECORD_1, 0);
      const updated: TestRecord = { ...RECORD_1, email: "alice2@example.com" };
      manager.add(updated, 100);

      // New value must be present
      expect(manager.getByField("email", "alice2@example.com")).toEqual(new Set(["id-1"]));
      // Physical index reflects the latest offset
      expect(manager.getOffset("id-1")).toBe(100);
    });

    it("skips field values that are plain objects — does not throw, does not insert", () => {
      const record: TestRecord = { id: "id-obj", email: { nested: "bad" }, status: "active" };

      expect(() => manager.add(record, 0)).not.toThrow();
      // Only 'status' should be indexed; 'email' is skipped
      expect(manager.getByField("status", "active")).toEqual(new Set(["id-obj"]));
      expect(manager.has("id-obj")).toBe(true);
    });

    it("skips field values that are arrays — does not throw", () => {
      const record: TestRecord = { id: "id-arr", email: ["bad"], status: "active" };

      expect(() => manager.add(record, 0)).not.toThrow();
      expect(manager.has("id-arr")).toBe(true);
    });

    it("skips field values that are undefined — does not throw", () => {
      // 'email' key is absent — value is undefined
      const record: TestRecord = { id: "id-undef", status: "active" };

      expect(() => manager.add(record, 0)).not.toThrow();
      expect(manager.has("id-undef")).toBe(true);
    });

    it("handles null as a valid FieldValue in the logical index", () => {
      const record: TestRecord = { id: "id-null", email: null, status: "active" };
      manager.add(record, 0);

      expect(manager.getByField("email", null)).toEqual(new Set(["id-null"]));
    });

    it("handles boolean as a valid FieldValue in the logical index", () => {
      const record: TestRecord = { id: "id-bool", email: "x@x.com", status: true };
      manager.add(record, 0);

      expect(manager.getByField("status", true)).toEqual(new Set(["id-bool"]));
    });

    it("handles number as a valid FieldValue in the logical index", () => {
      const record: TestRecord = { id: "id-num", email: 42, status: "active" };
      manager.add(record, 0);

      expect(manager.getByField("email", 42)).toEqual(new Set(["id-num"]));
    });

    it("works correctly with a numeric primary key", () => {
      const numericManager = new IndexManagerImpl<TestRecord>(OPTIONS);
      const record: TestRecord = { id: 99, email: "num@example.com", status: "active" };
      numericManager.add(record, 0);

      expect(numericManager.has(99)).toBe(true);
      expect(numericManager.getOffset(99)).toBe(0);
      expect(numericManager.getByField("email", "num@example.com")).toEqual(new Set([99]));
    });

    it("skips records whose primary key is not a string or number — does not throw", () => {
      const record: TestRecord = { id: { nested: "object" }, email: "x@x.com", status: "active" };

      expect(() => manager.add(record, 0)).not.toThrow();
      expect(manager.size()).toBe(0);
    });

    it("skips the primaryKeyField itself when it appears inside indexedFields", () => {
      const optionsWithPkInIndexed: IndexManagerOptions = {
        primaryKeyField: "id",
        indexedFields: new Set(["id", "email"]),
      };
      const pkManager = new IndexManagerImpl<TestRecord>(optionsWithPkInIndexed);
      pkManager.add(RECORD_1, 0);

      // 'id' field must NOT appear in the logical index — physical index covers PK lookups
      expect(pkManager.getByField("id", "id-1")).toBeUndefined();
      // 'email' field must still be indexed
      expect(pkManager.getByField("email", "alice@example.com")).toEqual(new Set(["id-1"]));
    });
  });

  // -------------------------------------------------------------------------
  describe("has()", () => {
    it("returns true for a primary key that was added", () => {
      manager.add(RECORD_1, 0);

      expect(manager.has("id-1")).toBe(true);
    });

    it("returns false for a primary key that was never added", () => {
      expect(manager.has("nonexistent")).toBe(false);
    });

    it("returns false after the record has been deleted", () => {
      manager.add(RECORD_1, 0);
      manager.delete("id-1");

      expect(manager.has("id-1")).toBe(false);
    });
  });

  // -------------------------------------------------------------------------
  describe("size()", () => {
    it("returns 0 on a fresh instance", () => {
      expect(manager.size()).toBe(0);
    });

    it("returns correct count after adding records", () => {
      manager.add(RECORD_1, 0);
      manager.add(RECORD_2, 50);

      expect(manager.size()).toBe(2);
    });

    it("decrements correctly after delete", () => {
      manager.add(RECORD_1, 0);
      manager.add(RECORD_2, 50);
      manager.delete("id-1");

      expect(manager.size()).toBe(1);
    });

    it("does not change after a no-op delete on a missing id", () => {
      manager.add(RECORD_1, 0);
      manager.delete("nonexistent");

      expect(manager.size()).toBe(1);
    });
  });

  // -------------------------------------------------------------------------
  describe("getOffset()", () => {
    it("returns the correct byte offset for a known id", () => {
      manager.add(RECORD_1, 77);

      expect(manager.getOffset("id-1")).toBe(77);
    });

    it("returns undefined for an unknown id", () => {
      expect(manager.getOffset("nonexistent")).toBeUndefined();
    });

    it("returns the updated offset after update() changes the offset", () => {
      manager.add(RECORD_1, 0);
      const updated: TestRecord = { ...RECORD_1, email: "updated@example.com" };
      manager.update(RECORD_1, updated, 999);

      expect(manager.getOffset("id-1")).toBe(999);
    });
  });

  // -------------------------------------------------------------------------
  describe("getByField()", () => {
    it("returns a Set of primary keys for a known field and value", () => {
      manager.add(RECORD_1, 0); // status: 'active'
      manager.add(RECORD_3, 50); // status: 'active'

      expect(manager.getByField("status", "active")).toEqual(new Set(["id-1", "id-3"]));
    });

    it("returns undefined for a field not in indexedFields", () => {
      manager.add(RECORD_1, 0);

      expect(manager.getByField("name", "Alice")).toBeUndefined();
    });

    it("returns undefined for an indexed field with no matching records for that value", () => {
      manager.add(RECORD_1, 0);

      expect(manager.getByField("email", "nobody@example.com")).toBeUndefined();
    });

    it("returns undefined instead of an empty Set after all matching records are deleted", () => {
      manager.add(RECORD_1, 0);
      manager.delete("id-1");

      expect(manager.getByField("email", "alice@example.com")).toBeUndefined();
    });

    it("returns a defensive copy — mutating the returned Set does not affect internal index state", () => {
      manager.add(RECORD_1, 0);
      const result = manager.getByField("status", "active");
      result?.add("injected-id");

      // Internal state must be unaffected
      expect(manager.getByField("status", "active")).toEqual(new Set(["id-1"]));
    });

    it("reflects updated values after update() — old value absent, new value present", () => {
      manager.add(RECORD_1, 0);
      const updated: TestRecord = { ...RECORD_1, email: "new@example.com" };
      manager.update(RECORD_1, updated, 50);

      expect(manager.getByField("email", "alice@example.com")).toBeUndefined();
      expect(manager.getByField("email", "new@example.com")).toEqual(new Set(["id-1"]));
    });

    it("does not return a deleted id after delete()", () => {
      manager.add(RECORD_1, 0); // status: 'active'
      manager.add(RECORD_3, 50); // status: 'active'
      manager.delete("id-1");

      const result = manager.getByField("status", "active");
      expect(result?.has("id-1")).toBeFalsy();
      expect(result?.has("id-3")).toBe(true);
    });
  });

  // -------------------------------------------------------------------------
  describe("update()", () => {
    it("removes primary key from the old field value Set in the logical index", () => {
      manager.add(RECORD_1, 0);
      const updated: TestRecord = { ...RECORD_1, email: "new@example.com" };
      manager.update(RECORD_1, updated, 50);

      expect(manager.getByField("email", "alice@example.com")).toBeUndefined();
    });

    it("adds primary key to the new field value Set in the logical index", () => {
      manager.add(RECORD_1, 0);
      const updated: TestRecord = { ...RECORD_1, email: "new@example.com" };
      manager.update(RECORD_1, updated, 50);

      expect(manager.getByField("email", "new@example.com")).toEqual(new Set(["id-1"]));
    });

    it("updates the physical index to the new offset", () => {
      manager.add(RECORD_1, 0);
      const updated: TestRecord = { ...RECORD_1, email: "new@example.com" };
      manager.update(RECORD_1, updated, 500);

      expect(manager.getOffset("id-1")).toBe(500);
    });

    it("cleans up empty Sets from the logical index after a value change", () => {
      manager.add(RECORD_1, 0); // only record with email='alice@example.com'
      const updated: TestRecord = { ...RECORD_1, email: "new@example.com" };
      manager.update(RECORD_1, updated, 50);

      // The 'alice@example.com' entry must be fully removed, not left as an empty Set
      expect(manager.getByField("email", "alice@example.com")).toBeUndefined();
    });

    it("does not affect unrelated records in the same logical index entry", () => {
      manager.add(RECORD_1, 0); // status: 'active'
      manager.add(RECORD_3, 50); // status: 'active'
      const updated: TestRecord = { ...RECORD_1, status: "inactive" };
      manager.update(RECORD_1, updated, 100);

      // record3 must still be indexed under 'active'
      expect(manager.getByField("status", "active")).toEqual(new Set(["id-3"]));
    });

    it("updates reverse map so a subsequent delete() removes the correct logical entries", () => {
      manager.add(RECORD_1, 0);
      const updated: TestRecord = { ...RECORD_1, email: "updated@example.com", status: "inactive" };
      manager.update(RECORD_1, updated, 50);
      manager.delete("id-1");

      // The NEW values should be cleaned up — not the stale old ones
      expect(manager.getByField("email", "updated@example.com")).toBeUndefined();
      expect(manager.getByField("status", "inactive")).toBeUndefined();
    });

    it("keeps id in logical index when old value and new value are the same field value", () => {
      manager.add(RECORD_1, 0);
      const unchanged: TestRecord = { ...RECORD_1 };
      manager.update(RECORD_1, unchanged, 50);

      expect(manager.getByField("email", "alice@example.com")).toEqual(new Set(["id-1"]));
    });

    it("skips records whose primary key is not a string or number — does not throw", () => {
      const invalid: TestRecord = { id: { nested: "bad" }, email: "x@x.com", status: "active" };
      const valid: TestRecord = { ...RECORD_2 };

      expect(() => manager.update(invalid, valid, 0)).not.toThrow();
    });

    it("handles an old record whose indexed field value is not a valid FieldValue type", () => {
      // Add with invalid email type — email won't be in reverse map or logical index
      const withInvalidEmail: TestRecord = { id: "id-1", email: { bad: true }, status: "active" };
      manager.add(withInvalidEmail, 0);
      // Now update with a valid new email — the old invalid email branch is silently skipped
      const newRecord: TestRecord = { id: "id-1", email: "valid@example.com", status: "active" };
      expect(() => manager.update(withInvalidEmail, newRecord, 50)).not.toThrow();
      expect(manager.getByField("email", "valid@example.com")).toEqual(new Set(["id-1"]));
    });

    it("creates a new field Map when the field has no prior logical index entry", () => {
      // Call update() on a fresh manager — logicalIndex has no entry for 'email'
      const record: TestRecord = { ...RECORD_1 };
      const updated: TestRecord = { ...RECORD_1, email: "fresh@example.com" };
      manager.update(record, updated, 0);

      expect(manager.getByField("email", "fresh@example.com")).toEqual(new Set(["id-1"]));
    });

    it("silently skips old value removal when that specific value is not in the field Map", () => {
      manager.add(RECORD_1, 0); // email: alice@example.com is in the logical index
      // Use a wrong old record (different email than what's actually indexed)
      const wrongOld: TestRecord = { ...RECORD_1, email: "not-actually-indexed@example.com" };
      const newRecord: TestRecord = { ...RECORD_1, email: "new@example.com" };

      expect(() => manager.update(wrongOld, newRecord, 50)).not.toThrow();
      expect(manager.getByField("email", "new@example.com")).toEqual(new Set(["id-1"]));
    });

    it("merges primary key into an existing Set when the new value already has other records", () => {
      manager.add(RECORD_1, 0); // email: alice@example.com
      manager.add(RECORD_2, 50); // email: bob@example.com
      const updated: TestRecord = { ...RECORD_1, email: "bob@example.com" };
      manager.update(RECORD_1, updated, 100);

      // id-1 and id-2 should both be in the 'bob@example.com' Set
      expect(manager.getByField("email", "bob@example.com")).toEqual(new Set(["id-1", "id-2"]));
    });
  });

  // -------------------------------------------------------------------------
  describe("delete()", () => {
    it("removes primary key from the physical index", () => {
      manager.add(RECORD_1, 0);
      manager.delete("id-1");

      expect(manager.getOffset("id-1")).toBeUndefined();
    });

    it("removes primary key from all relevant Sets in the logical index", () => {
      manager.add(RECORD_1, 0);
      manager.delete("id-1");

      expect(manager.getByField("email", "alice@example.com")).toBeUndefined();
      expect(manager.getByField("status", "active")).toBeUndefined();
    });

    it("cleans up empty Sets from the logical index after deletion", () => {
      manager.add(RECORD_1, 0); // only record with email='alice@example.com'
      manager.delete("id-1");

      // Set must be removed entirely, not left empty
      expect(manager.getByField("email", "alice@example.com")).toBeUndefined();
    });

    it("is a no-op when the id does not exist — does not throw", () => {
      expect(() => manager.delete("nonexistent")).not.toThrow();
    });

    it("does not affect other records in the same logical index entry", () => {
      manager.add(RECORD_1, 0); // status: 'active'
      manager.add(RECORD_3, 50); // status: 'active'
      manager.delete("id-1");

      expect(manager.getByField("status", "active")).toEqual(new Set(["id-3"]));
    });

    it("has() returns false for that id after delete", () => {
      manager.add(RECORD_1, 0);
      manager.delete("id-1");

      expect(manager.has("id-1")).toBe(false);
    });

    it("getOffset() returns undefined for that id after delete", () => {
      manager.add(RECORD_1, 0);
      manager.delete("id-1");

      expect(manager.getOffset("id-1")).toBeUndefined();
    });

    it("getByField() does not return that id after delete", () => {
      manager.add(RECORD_1, 0);
      manager.delete("id-1");

      expect(manager.getByField("email", "alice@example.com")).toBeUndefined();
    });
  });

  // -------------------------------------------------------------------------
  describe("clear()", () => {
    it("resets the physical index to empty", () => {
      manager.add(RECORD_1, 0);
      manager.clear();

      expect(manager.getOffset("id-1")).toBeUndefined();
    });

    it("resets the logical index to empty", () => {
      manager.add(RECORD_1, 0);
      manager.clear();

      expect(manager.getByField("email", "alice@example.com")).toBeUndefined();
    });

    it("resets the reverse map so subsequent deletes are no-ops", () => {
      manager.add(RECORD_1, 0);
      manager.clear();

      // delete on a cleared index should be a no-op, not throw
      expect(() => manager.delete("id-1")).not.toThrow();
    });

    it("size() returns 0 after clear", () => {
      manager.add(RECORD_1, 0);
      manager.add(RECORD_2, 50);
      manager.clear();

      expect(manager.size()).toBe(0);
    });

    it("has() returns false for previously added ids after clear", () => {
      manager.add(RECORD_1, 0);
      manager.clear();

      expect(manager.has("id-1")).toBe(false);
    });

    it("can add records again after clear without errors", () => {
      manager.add(RECORD_1, 0);
      manager.clear();

      expect(() => manager.add(RECORD_1, 0)).not.toThrow();
      expect(manager.has("id-1")).toBe(true);
    });
  });

  // -------------------------------------------------------------------------
  describe("rebuild()", () => {
    let testDir: string;
    let filePath: string;

    beforeEach(() => {
      testDir = mkdtempSync(join(tmpdir(), "oriondb-test-"));
      filePath = join(testDir, "data.ndjson");
    });

    afterEach(() => {
      rmSync(testDir, { recursive: true, force: true });
    });

    it("resolves immediately without error if the file does not exist", async () => {
      const nonExistentPath = join(testDir, "nonexistent.ndjson");

      await expect(manager.rebuild(nonExistentPath)).resolves.toBeUndefined();
    });

    it("clears existing index state before rebuilding", async () => {
      // Pre-load state that is NOT in the file
      manager.add(RECORD_1, 0);
      const record = { id: "id-2", email: "bob@example.com", status: "inactive", _deleted: false };
      writeFileSync(filePath, JSON.stringify(record) + "\n");

      await manager.rebuild(filePath);

      expect(manager.has("id-1")).toBe(false);
      expect(manager.has("id-2")).toBe(true);
    });

    it("correctly indexes a single non-deleted record", async () => {
      const record = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      writeFileSync(filePath, JSON.stringify(record) + "\n");

      await manager.rebuild(filePath);

      expect(manager.has("id-1")).toBe(true);
      expect(manager.getByField("email", "alice@example.com")).toEqual(new Set(["id-1"]));
      expect(manager.getByField("status", "active")).toEqual(new Set(["id-1"]));
    });

    it("correctly indexes multiple non-deleted records", async () => {
      const r1 = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      const r2 = { id: "id-2", email: "bob@example.com", status: "inactive", _deleted: false };
      writeFileSync(filePath, JSON.stringify(r1) + "\n" + JSON.stringify(r2) + "\n");

      await manager.rebuild(filePath);

      expect(manager.has("id-1")).toBe(true);
      expect(manager.has("id-2")).toBe(true);
      expect(manager.size()).toBe(2);
    });

    it("does not index tombstoned records (_deleted: true)", async () => {
      const tombstone = { id: "id-1", email: "alice@example.com", status: "active", _deleted: true };
      writeFileSync(filePath, JSON.stringify(tombstone) + "\n");

      await manager.rebuild(filePath);

      expect(manager.has("id-1")).toBe(false);
      expect(manager.size()).toBe(0);
    });

    it("removes a record from the index if a later tombstone appears for the same id", async () => {
      const record = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      const tombstone = { id: "id-1", _deleted: true };
      writeFileSync(filePath, JSON.stringify(record) + "\n" + JSON.stringify(tombstone) + "\n");

      await manager.rebuild(filePath);

      expect(manager.has("id-1")).toBe(false);
      expect(manager.size()).toBe(0);
    });

    it("last-occurrence-wins: second occurrence of same id overwrites first in both logical and physical indexes", async () => {
      const first = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      const second = { id: "id-1", email: "newalice@example.com", status: "inactive", _deleted: false };
      const line1 = JSON.stringify(first) + "\n";
      const line2 = JSON.stringify(second) + "\n";
      writeFileSync(filePath, line1 + line2);

      await manager.rebuild(filePath);

      // Physical index: second occurrence offset
      const expectedOffset = Buffer.byteLength(line1, "utf8");
      expect(manager.getOffset("id-1")).toBe(expectedOffset);

      // Logical index: new values from second occurrence are indexed
      expect(manager.getByField("email", "newalice@example.com")).toEqual(new Set(["id-1"]));
      expect(manager.getByField("status", "inactive")).toEqual(new Set(["id-1"]));

      // Only one active record in the physical index
      expect(manager.size()).toBe(1);
    });

    it("tracks byte offsets correctly — getOffset() returns the start byte of each record's line", async () => {
      const r1 = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      const r2 = { id: "id-2", email: "bob@example.com", status: "inactive", _deleted: false };
      const line1 = JSON.stringify(r1) + "\n";
      const line2 = JSON.stringify(r2) + "\n";
      writeFileSync(filePath, line1 + line2);

      await manager.rebuild(filePath);

      expect(manager.getOffset("id-1")).toBe(0);
      expect(manager.getOffset("id-2")).toBe(Buffer.byteLength(line1, "utf8"));
    });

    it("emits console.warn and resolves normally for a malformed final line", async () => {
      const record = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      writeFileSync(filePath, JSON.stringify(record) + "\n" + "NOT VALID JSON");
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

      await expect(manager.rebuild(filePath)).resolves.toBeUndefined();
      expect(warnSpy).toHaveBeenCalledOnce();

      warnSpy.mockRestore();
    });

    it("does not add a malformed final line to any index", async () => {
      writeFileSync(filePath, "NOT VALID JSON");
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

      await manager.rebuild(filePath);
      warnSpy.mockRestore();

      expect(manager.size()).toBe(0);
    });

    it("throws ValidationError for a malformed non-final line", async () => {
      const validRecord = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      writeFileSync(filePath, "NOT VALID JSON\n" + JSON.stringify(validRecord) + "\n");

      await expect(manager.rebuild(filePath)).rejects.toMatchObject({
        code: "VALIDATION_ERROR",
      });
    });

    it("handles an empty file (zero bytes) without error", async () => {
      writeFileSync(filePath, "");

      await expect(manager.rebuild(filePath)).resolves.toBeUndefined();
      expect(manager.size()).toBe(0);
    });

    it("handles a file with only tombstones — indexes remain empty after rebuild", async () => {
      const t1 = { id: "id-1", _deleted: true };
      const t2 = { id: "id-2", _deleted: true };
      writeFileSync(filePath, JSON.stringify(t1) + "\n" + JSON.stringify(t2) + "\n");

      await manager.rebuild(filePath);

      expect(manager.size()).toBe(0);
    });

    it("skips records whose primary key is not a string or number during rebuild", async () => {
      const invalidPk = { id: { nested: "bad" }, email: "x@x.com", status: "active", _deleted: false };
      const validRecord = { id: "id-2", email: "bob@example.com", status: "active", _deleted: false };
      writeFileSync(filePath, JSON.stringify(invalidPk) + "\n" + JSON.stringify(validRecord) + "\n");

      await manager.rebuild(filePath);

      expect(manager.size()).toBe(1);
      expect(manager.has("id-2")).toBe(true);
    });

    it("emits console.warn and resolves for a non-object value (e.g. array) as the final line", async () => {
      const record = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      // Write a valid record then a JSON array as the last line (valid JSON but not an object)
      writeFileSync(filePath, JSON.stringify(record) + "\n" + JSON.stringify([1, 2, 3]));
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

      await expect(manager.rebuild(filePath)).resolves.toBeUndefined();
      expect(warnSpy).toHaveBeenCalledOnce();
      warnSpy.mockRestore();

      // The array line must not be added to the index
      expect(manager.size()).toBe(1);
    });

    it("throws ValidationError for a non-object value (e.g. array) on a non-final line", async () => {
      const validRecord = { id: "id-1", email: "alice@example.com", status: "active", _deleted: false };
      // Array is valid JSON but not a plain object — non-final line must throw ValidationError
      writeFileSync(filePath, JSON.stringify([1, 2, 3]) + "\n" + JSON.stringify(validRecord) + "\n");

      await expect(manager.rebuild(filePath)).rejects.toMatchObject({
        code: "VALIDATION_ERROR",
      });
    });

    it("rejects when the underlying stream emits an I/O error", async () => {
      // Pointing rebuild at a directory: access() succeeds (directory exists via F_OK)
      // but createReadStream on a directory emits an EISDIR error on the stream,
      // which readline propagates through the rl.on("error") handler.
      await expect(manager.rebuild(testDir)).rejects.toBeInstanceOf(Error);
    });
  });
});
