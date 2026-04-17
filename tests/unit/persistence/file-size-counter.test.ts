// tests/unit/persistence/file-size-counter.test.ts

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { CompactionError, ValidationError } from "../../../src/errors/index.js";
import { FileSizeCounter, FileSizeCounterManager } from "../../../src/persistence/index.js";

let testDir: string;

beforeEach(() => {
  testDir = mkdtempSync(join(tmpdir(), "oriondb-persistence-test-"));
});

afterEach(() => {
  rmSync(testDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTempFile(content = ""): string {
  const filePath = join(testDir, `test-${Math.random().toString(36).slice(2)}.ndjson`);
  writeFileSync(filePath, content, "utf8");
  return filePath;
}

// ---------------------------------------------------------------------------
// FileSizeCounter
// ---------------------------------------------------------------------------

describe("FileSizeCounter", () => {
  describe("initialize()", () => {
    it("sets size to 0 for an empty file", async () => {
      const counter = new FileSizeCounter();
      const filePath = makeTempFile("");
      await counter.initialize(filePath);
      expect(counter.getSize()).toBe(0);
    });

    it("sets size to actual byte count for a non-empty file", async () => {
      const content = '{"id":"abc"}\n';
      const filePath = makeTempFile(content);
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(counter.getSize()).toBe(Buffer.byteLength(content, "utf8"));
    });

    it("can be called multiple times — re-reads from disk each time", async () => {
      const filePath = makeTempFile("");
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(counter.getSize()).toBe(0);
      // Write content then re-initialize
      writeFileSync(filePath, "hello\n", "utf8");
      await counter.initialize(filePath);
      expect(counter.getSize()).toBe(Buffer.byteLength("hello\n", "utf8"));
    });

    it("returns void", async () => {
      const filePath = makeTempFile();
      const counter = new FileSizeCounter();
      const result = await counter.initialize(filePath);
      expect(result).toBeUndefined();
    });

    it("sets isInitialized() to true", async () => {
      const filePath = makeTempFile();
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(counter.isInitialized()).toBe(true);
    });

    it("throws CompactionError on unexpected I/O failure", async () => {
      // Use a path that is a directory, not a file — stat() on a dir succeeds,
      // but we can simulate failure via a path with a null byte
      const counter = new FileSizeCounter();
      // A path with a null byte is always invalid on Linux
      const badPath = join(testDir, "bad\0file.ndjson");
      await expect(counter.initialize(badPath)).rejects.toBeInstanceOf(CompactionError);
    });

    it("does not throw for missing file — treats as 0 bytes", async () => {
      const counter = new FileSizeCounter();
      const missingPath = join(testDir, "missing.ndjson");
      await expect(counter.initialize(missingPath)).resolves.toBeUndefined();
      expect(counter.getSize()).toBe(0);
    });
  });

  describe("getSize()", () => {
    it("throws ValidationError before initialize() is called", () => {
      const counter = new FileSizeCounter();
      expect(() => counter.getSize()).toThrow(ValidationError);
    });

    it("returns 0 after initializing with empty file", async () => {
      const filePath = makeTempFile("");
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(counter.getSize()).toBe(0);
    });

    it("returns correct byte count after initializing with content", async () => {
      const content = '{"id":"x","name":"Alice"}\n';
      const filePath = makeTempFile(content);
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(counter.getSize()).toBe(Buffer.byteLength(content, "utf8"));
    });
  });

  describe("increment()", () => {
    it("throws ValidationError before initialize() is called", () => {
      const counter = new FileSizeCounter();
      expect(() => counter.increment(10)).toThrow(ValidationError);
    });

    it("throws ValidationError for negative byteCount", async () => {
      const filePath = makeTempFile();
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(() => counter.increment(-1)).toThrow(ValidationError);
    });

    it("throws ValidationError for NaN", async () => {
      const filePath = makeTempFile();
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(() => counter.increment(NaN)).toThrow(ValidationError);
    });

    it("throws ValidationError for Infinity", async () => {
      const filePath = makeTempFile();
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(() => counter.increment(Infinity)).toThrow(ValidationError);
    });

    it("correctly increments internal size", async () => {
      const filePath = makeTempFile("");
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      counter.increment(10);
      expect(counter.getSize()).toBe(10);
    });

    it("multiple increments accumulate correctly", async () => {
      const filePath = makeTempFile("");
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      counter.increment(10);
      counter.increment(20);
      counter.increment(5);
      expect(counter.getSize()).toBe(35);
    });
  });

  describe("reset()", () => {
    it("re-reads file size from disk", async () => {
      const filePath = makeTempFile("");
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(counter.getSize()).toBe(0);
      writeFileSync(filePath, "new content\n", "utf8");
      await counter.reset(filePath);
      expect(counter.getSize()).toBe(Buffer.byteLength("new content\n", "utf8"));
    });

    it("after reset, getSize() reflects the current file size", async () => {
      const content = '{"id":"1"}\n';
      const filePath = makeTempFile(content);
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      counter.increment(50); // simulate drift
      await counter.reset(filePath);
      expect(counter.getSize()).toBe(Buffer.byteLength(content, "utf8"));
    });

    it("works correctly after file content has changed between initialization and reset", async () => {
      const filePath = makeTempFile("initial\n");
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      const newContent = "initial\nalso-this\n";
      writeFileSync(filePath, newContent, "utf8");
      await counter.reset(filePath);
      expect(counter.getSize()).toBe(Buffer.byteLength(newContent, "utf8"));
    });
  });

  describe("isInitialized()", () => {
    it("returns false before initialize() is called", () => {
      const counter = new FileSizeCounter();
      expect(counter.isInitialized()).toBe(false);
    });

    it("returns true after initialize() is called", async () => {
      const filePath = makeTempFile();
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(counter.isInitialized()).toBe(true);
    });

    it("always safe to call regardless of state", () => {
      const counter = new FileSizeCounter();
      expect(() => counter.isInitialized()).not.toThrow();
    });
  });

  describe("integration — offset tracking", () => {
    it("getSize() equals Buffer.byteLength of written content", async () => {
      const content = '{"id":"abc","name":"test"}\n';
      const filePath = makeTempFile(content);
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      expect(counter.getSize()).toBe(Buffer.byteLength(content, "utf8"));
    });

    it("simulate write: increment advances to correct next-record offset", async () => {
      const filePath = makeTempFile("");
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      const line = '{"id":"1"}\n';
      const byteLen = Buffer.byteLength(line, "utf8");
      counter.increment(byteLen);
      expect(counter.getSize()).toBe(byteLen);
    });

    it("simulate two sequential writes: offsets advance correctly", async () => {
      const filePath = makeTempFile("");
      const counter = new FileSizeCounter();
      await counter.initialize(filePath);
      const line1 = '{"id":"1","name":"Alice"}\n';
      const line2 = '{"id":"2","name":"Bob"}\n';
      const byte1 = Buffer.byteLength(line1, "utf8");
      const byte2 = Buffer.byteLength(line2, "utf8");
      // Offset for record 1 is 0 (captured before increment)
      const offset1 = counter.getSize();
      counter.increment(byte1);
      // Offset for record 2 is byte1
      const offset2 = counter.getSize();
      counter.increment(byte2);
      expect(offset1).toBe(0);
      expect(offset2).toBe(byte1);
      expect(counter.getSize()).toBe(byte1 + byte2);
    });
  });
});

// ---------------------------------------------------------------------------
// FileSizeCounterManager
// ---------------------------------------------------------------------------

describe("FileSizeCounterManager", () => {
  describe("initializeModel()", () => {
    it("creates and initializes counter for the given model", async () => {
      const manager = new FileSizeCounterManager();
      const filePath = makeTempFile("content\n");
      await manager.initializeModel("User", filePath);
      expect(manager.getSize("User")).toBe(Buffer.byteLength("content\n", "utf8"));
    });

    it("re-initializes existing counter without creating a new one", async () => {
      const manager = new FileSizeCounterManager();
      const filePath = makeTempFile("");
      await manager.initializeModel("User", filePath);
      writeFileSync(filePath, "new\n", "utf8");
      await manager.initializeModel("User", filePath);
      expect(manager.getSize("User")).toBe(Buffer.byteLength("new\n", "utf8"));
    });

    it("after initialization, hasModel() returns true", async () => {
      const manager = new FileSizeCounterManager();
      const filePath = makeTempFile();
      await manager.initializeModel("User", filePath);
      expect(manager.hasModel("User")).toBe(true);
    });
  });

  describe("getCounter()", () => {
    it("returns the initialized counter for a known model", async () => {
      const manager = new FileSizeCounterManager();
      const filePath = makeTempFile();
      await manager.initializeModel("User", filePath);
      const counter = manager.getCounter("User");
      expect(counter).toBeInstanceOf(FileSizeCounter);
    });

    it("throws ValidationError for unknown model name", () => {
      const manager = new FileSizeCounterManager();
      expect(() => manager.getCounter("Unknown")).toThrow(ValidationError);
    });

    it("error includes model name in meta", () => {
      const manager = new FileSizeCounterManager();
      try {
        manager.getCounter("MissingModel");
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError);
        const e = err as ValidationError;
        expect(JSON.stringify(e.meta)).toContain("MissingModel");
      }
    });
  });

  describe("getSize()", () => {
    it("returns correct size for initialized model", async () => {
      const manager = new FileSizeCounterManager();
      const content = "hello\n";
      const filePath = makeTempFile(content);
      await manager.initializeModel("User", filePath);
      expect(manager.getSize("User")).toBe(Buffer.byteLength(content, "utf8"));
    });

    it("throws ValidationError for unknown model", () => {
      const manager = new FileSizeCounterManager();
      expect(() => manager.getSize("Unknown")).toThrow(ValidationError);
    });
  });

  describe("increment()", () => {
    it("increments the correct model's counter", async () => {
      const manager = new FileSizeCounterManager();
      const filePath = makeTempFile("");
      await manager.initializeModel("User", filePath);
      manager.increment("User", 20);
      expect(manager.getSize("User")).toBe(20);
    });

    it("does not affect other models' counters", async () => {
      const manager = new FileSizeCounterManager();
      const file1 = makeTempFile("");
      const file2 = makeTempFile("abc\n");
      await manager.initializeModel("User", file1);
      await manager.initializeModel("Post", file2);
      manager.increment("User", 15);
      expect(manager.getSize("Post")).toBe(Buffer.byteLength("abc\n", "utf8"));
    });

    it("throws ValidationError for unknown model", () => {
      const manager = new FileSizeCounterManager();
      expect(() => manager.increment("Unknown", 10)).toThrow(ValidationError);
    });
  });

  describe("resetModel()", () => {
    it("re-reads file size for the given model", async () => {
      const manager = new FileSizeCounterManager();
      const filePath = makeTempFile("");
      await manager.initializeModel("User", filePath);
      manager.increment("User", 100); // manual drift
      writeFileSync(filePath, "new content\n", "utf8");
      await manager.resetModel("User", filePath);
      expect(manager.getSize("User")).toBe(Buffer.byteLength("new content\n", "utf8"));
    });

    it("throws ValidationError for unknown model", async () => {
      const manager = new FileSizeCounterManager();
      const filePath = makeTempFile();
      await expect(manager.resetModel("Unknown", filePath)).rejects.toThrow(ValidationError);
    });
  });

  describe("hasModel()", () => {
    it("returns false before model is initialized", () => {
      const manager = new FileSizeCounterManager();
      expect(manager.hasModel("User")).toBe(false);
    });

    it("returns true after model is initialized", async () => {
      const manager = new FileSizeCounterManager();
      const filePath = makeTempFile();
      await manager.initializeModel("User", filePath);
      expect(manager.hasModel("User")).toBe(true);
    });
  });
});
