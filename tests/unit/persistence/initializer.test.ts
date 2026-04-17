// tests/unit/persistence/initializer.test.ts

import { mkdtempSync, rmSync, writeFileSync, readFileSync, mkdirSync, chmodSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolve } from "node:path";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { CompactionError } from "../../../src/errors/index.js";
import {
  resolveDatabasePaths,
  resolveModelPaths,
  initializeDatabaseDirectory,
  initializeModelDirectory,
  initializeAllModelDirectories,
  updateModelMeta,
} from "../../../src/persistence/index.js";
import type { ModelPaths } from "../../../src/persistence/index.js";

let testDir: string;

beforeEach(() => {
  testDir = mkdtempSync(join(tmpdir(), "oriondb-persistence-test-"));
});

afterEach(() => {
  rmSync(testDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// resolveDatabasePaths()
// ---------------------------------------------------------------------------

describe("resolveDatabasePaths()", () => {
  it("returns an absolute path for dbLocation", () => {
    const result = resolveDatabasePaths(testDir);
    expect(result.dbLocation).toBe(resolve(testDir));
  });

  it("returns correct schemaFile path using _schema.json", () => {
    const result = resolveDatabasePaths(testDir);
    expect(result.schemaFile).toBe(join(resolve(testDir), "_schema.json"));
  });

  it("returns correct metaFile path using _meta.json", () => {
    const result = resolveDatabasePaths(testDir);
    expect(result.metaFile).toBe(join(resolve(testDir), "_meta.json"));
  });

  it("resolves relative paths to absolute", () => {
    const result = resolveDatabasePaths("./somerelative");
    expect(result.dbLocation).toBe(resolve("./somerelative"));
  });

  it("is a pure function — no I/O performed (paths for non-existent dir are returned)", () => {
    const fakePath = join(testDir, "does-not-exist");
    const result = resolveDatabasePaths(fakePath);
    expect(result.dbLocation).toBe(resolve(fakePath));
  });
});

// ---------------------------------------------------------------------------
// resolveModelPaths()
// ---------------------------------------------------------------------------

describe("resolveModelPaths()", () => {
  it("returns correct modelDir path with case-preserved model name", () => {
    const result = resolveModelPaths(testDir, "User");
    expect(result.modelDir).toBe(join(testDir, "User"));
  });

  it("returns dataFile as modelDir/data.ndjson", () => {
    const result = resolveModelPaths(testDir, "User");
    expect(result.dataFile).toBe(join(testDir, "User", "data.ndjson"));
  });

  it("returns metaFile as modelDir/meta.json", () => {
    const result = resolveModelPaths(testDir, "User");
    expect(result.metaFile).toBe(join(testDir, "User", "meta.json"));
  });

  it("preserves model name casing exactly in the path", () => {
    const result = resolveModelPaths(testDir, "BlogPost");
    expect(result.modelDir).toContain("BlogPost");
  });

  it("is a pure function — no I/O performed", () => {
    const fakePath = join(testDir, "does-not-exist");
    const result = resolveModelPaths(fakePath, "User");
    expect(result.modelDir).toBe(join(fakePath, "User"));
  });
});

// ---------------------------------------------------------------------------
// initializeDatabaseDirectory()
// ---------------------------------------------------------------------------

describe("initializeDatabaseDirectory()", () => {
  it("creates dbLocation if it does not exist", async () => {
    const dbPath = join(testDir, "newdb");
    const paths = resolveDatabasePaths(dbPath);
    await initializeDatabaseDirectory(paths);
    const { existsSync } = await import("node:fs");
    expect(existsSync(dbPath)).toBe(true);
  });

  it("does not throw if dbLocation already exists", async () => {
    const paths = resolveDatabasePaths(testDir);
    await expect(initializeDatabaseDirectory(paths)).resolves.toBeUndefined();
  });

  it("creates _meta.json with correct structure when absent", async () => {
    const paths = resolveDatabasePaths(testDir);
    await initializeDatabaseDirectory(paths);
    const meta = JSON.parse(readFileSync(paths.metaFile, "utf8")) as Record<string, unknown>;
    expect(meta).toHaveProperty("version");
    expect(meta).toHaveProperty("oriondbVersion");
    expect(meta).toHaveProperty("createdAt");
    expect(meta).toHaveProperty("location");
  });

  it("_meta.json location equals the absolute dbLocation", async () => {
    const paths = resolveDatabasePaths(testDir);
    await initializeDatabaseDirectory(paths);
    const meta = JSON.parse(readFileSync(paths.metaFile, "utf8")) as Record<string, unknown>;
    expect(meta.location).toBe(paths.dbLocation);
  });

  it("does not overwrite existing _meta.json", async () => {
    const paths = resolveDatabasePaths(testDir);
    // Create existing meta with a sentinel value
    const existing = {
      version: 99,
      oriondbVersion: "sentinel",
      createdAt: "2000-01-01T00:00:00.000Z",
      location: testDir,
    };
    writeFileSync(paths.metaFile, JSON.stringify(existing), "utf8");
    await initializeDatabaseDirectory(paths);
    const meta = JSON.parse(readFileSync(paths.metaFile, "utf8")) as Record<string, unknown>;
    expect(meta.version).toBe(99);
    expect(meta.oriondbVersion).toBe("sentinel");
  });

  it("does not create _schema.json", async () => {
    const paths = resolveDatabasePaths(testDir);
    await initializeDatabaseDirectory(paths);
    const { existsSync } = await import("node:fs");
    expect(existsSync(paths.schemaFile)).toBe(false);
  });

  it("throws CompactionError on I/O failure (path with null byte)", async () => {
    // A null byte in the path is always invalid on Linux
    const invalidPath = join(testDir, "bad\0path");
    const paths = resolveDatabasePaths(invalidPath);
    await expect(initializeDatabaseDirectory(paths)).rejects.toBeInstanceOf(CompactionError);
  });
});

// ---------------------------------------------------------------------------
// initializeModelDirectory()
// ---------------------------------------------------------------------------

describe("initializeModelDirectory()", () => {
  it("creates modelDir if it does not exist", async () => {
    const paths = resolveModelPaths(testDir, "User");
    await initializeModelDirectory(paths, "User");
    const { existsSync } = await import("node:fs");
    expect(existsSync(paths.modelDir)).toBe(true);
  });

  it("creates empty data.ndjson if absent", async () => {
    const paths = resolveModelPaths(testDir, "User");
    await initializeModelDirectory(paths, "User");
    const content = readFileSync(paths.dataFile, "utf8");
    expect(content).toBe("");
  });

  it("creates meta.json with recordCount: 0", async () => {
    const paths = resolveModelPaths(testDir, "User");
    const meta = await initializeModelDirectory(paths, "User");
    expect(meta.recordCount).toBe(0);
  });

  it("creates meta.json with tombstoneCount: 0", async () => {
    const paths = resolveModelPaths(testDir, "User");
    const meta = await initializeModelDirectory(paths, "User");
    expect(meta.tombstoneCount).toBe(0);
  });

  it("creates meta.json with totalLines: 0", async () => {
    const paths = resolveModelPaths(testDir, "User");
    const meta = await initializeModelDirectory(paths, "User");
    expect(meta.totalLines).toBe(0);
  });

  it("creates meta.json with lastCompactedAt: null", async () => {
    const paths = resolveModelPaths(testDir, "User");
    const meta = await initializeModelDirectory(paths, "User");
    expect(meta.lastCompactedAt).toBeNull();
  });

  it("meta.json createdAt is a valid ISO 8601 string", async () => {
    const paths = resolveModelPaths(testDir, "User");
    const meta = await initializeModelDirectory(paths, "User");
    expect(new Date(meta.createdAt).toISOString()).toBe(meta.createdAt);
  });

  it("does not overwrite existing meta.json", async () => {
    const paths = resolveModelPaths(testDir, "User");
    await initializeModelDirectory(paths, "User");
    // Update the meta to a sentinel value
    writeFileSync(
      paths.metaFile,
      JSON.stringify({
        modelName: "User",
        recordCount: 42,
        tombstoneCount: 0,
        totalLines: 42,
        lastCompactedAt: null,
        createdAt: "2000-01-01T00:00:00.000Z",
      }),
      "utf8",
    );
    await initializeModelDirectory(paths, "User");
    const meta = JSON.parse(readFileSync(paths.metaFile, "utf8")) as Record<string, unknown>;
    expect(meta.recordCount).toBe(42);
  });

  it("does not overwrite existing data.ndjson", async () => {
    const paths = resolveModelPaths(testDir, "User");
    await initializeModelDirectory(paths, "User");
    writeFileSync(paths.dataFile, '{"id":"existing"}\n', "utf8");
    await initializeModelDirectory(paths, "User");
    const content = readFileSync(paths.dataFile, "utf8");
    expect(content).toBe('{"id":"existing"}\n');
  });

  it("returns existing ModelMeta when meta.json already exists", async () => {
    const paths = resolveModelPaths(testDir, "User");
    await initializeModelDirectory(paths, "User");
    await updateModelMeta(paths, { recordCount: 5 });
    const meta = await initializeModelDirectory(paths, "User");
    expect(meta.recordCount).toBe(5);
  });

  it("returns freshly created ModelMeta when meta.json is absent", async () => {
    const paths = resolveModelPaths(testDir, "User");
    const meta = await initializeModelDirectory(paths, "User");
    expect(meta.modelName).toBe("User");
    expect(meta.recordCount).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// initializeAllModelDirectories()
// ---------------------------------------------------------------------------

describe("initializeAllModelDirectories()", () => {
  it("returns a Map with one entry per model name", async () => {
    const result = await initializeAllModelDirectories(testDir, ["User", "Post"]);
    expect(result.size).toBe(2);
  });

  it("each entry is a valid ModelMeta", async () => {
    const result = await initializeAllModelDirectories(testDir, ["User"]);
    const meta = result.get("User");
    expect(meta).toBeDefined();
    expect(meta?.recordCount).toBe(0);
  });

  it("initializes all named models correctly", async () => {
    const result = await initializeAllModelDirectories(testDir, ["User", "Post", "Comment"]);
    expect(result.has("User")).toBe(true);
    expect(result.has("Post")).toBe(true);
    expect(result.has("Comment")).toBe(true);
  });

  it("returns empty Map for empty model names array", async () => {
    const result = await initializeAllModelDirectories(testDir, []);
    expect(result.size).toBe(0);
  });

  it("propagates error if any single model initialization fails", async () => {
    // Pass a path with a null byte for one model to force an error
    const badLocation = testDir + "\0";
    await expect(initializeAllModelDirectories(badLocation, ["User"])).rejects.toBeInstanceOf(CompactionError);
  });
});

// ---------------------------------------------------------------------------
// updateModelMeta()
// ---------------------------------------------------------------------------

describe("updateModelMeta()", () => {
  let modelPaths: ModelPaths;

  beforeEach(async () => {
    modelPaths = resolveModelPaths(testDir, "User");
    await initializeModelDirectory(modelPaths, "User");
  });

  it("merges provided updates into existing meta", async () => {
    const result = await updateModelMeta(modelPaths, { recordCount: 10 });
    expect(result.recordCount).toBe(10);
  });

  it("totalLines is always recomputed as recordCount + tombstoneCount", async () => {
    const result = await updateModelMeta(modelPaths, { recordCount: 7, tombstoneCount: 3 });
    expect(result.totalLines).toBe(10);
  });

  it("does not modify modelName", async () => {
    const result = await updateModelMeta(modelPaths, { recordCount: 1 });
    expect(result.modelName).toBe("User");
  });

  it("does not modify createdAt", async () => {
    const before = JSON.parse(readFileSync(modelPaths.metaFile, "utf8")) as Record<string, unknown>;
    const result = await updateModelMeta(modelPaths, { recordCount: 1 });
    expect(result.createdAt).toBe(before.createdAt);
  });

  it("returns the final merged ModelMeta", async () => {
    const result = await updateModelMeta(modelPaths, { tombstoneCount: 2 });
    expect(result).toMatchObject({ tombstoneCount: 2, recordCount: 0, totalLines: 2 });
  });

  it("throws CompactionError if meta.json does not exist", async () => {
    const badPaths = resolveModelPaths(testDir, "NonExistent");
    await expect(updateModelMeta(badPaths, { recordCount: 1 })).rejects.toBeInstanceOf(CompactionError);
  });
});

// ---------------------------------------------------------------------------
// I/O failure paths — write errors via read-only directory (chmod)
// ---------------------------------------------------------------------------

describe("initializeDatabaseDirectory() write failure", () => {
  it("throws CompactionError when database meta file cannot be written", async () => {
    // Create a fresh subdirectory and make it read-only so writeFile fails
    const dbPath = join(testDir, "readonly-db");
    mkdirSync(dbPath);
    chmodSync(dbPath, 0o444);
    const paths = resolveDatabasePaths(dbPath);
    try {
      await expect(initializeDatabaseDirectory(paths)).rejects.toBeInstanceOf(CompactionError);
    } finally {
      chmodSync(dbPath, 0o755);
    }
  });
});

describe("initializeModelDirectory() write failures", () => {
  it("throws CompactionError when data file cannot be created (directory read-only)", async () => {
    // Create the model directory manually then make it read-only before calling initializeModelDirectory
    const paths = resolveModelPaths(testDir, "ReadOnlyModel");
    mkdirSync(paths.modelDir, { recursive: true });
    chmodSync(paths.modelDir, 0o444);
    try {
      await expect(initializeModelDirectory(paths, "ReadOnlyModel")).rejects.toBeInstanceOf(CompactionError);
    } finally {
      chmodSync(paths.modelDir, 0o755);
    }
  });

  it("throws CompactionError when model meta file cannot be written (data file exists, dir read-only)", async () => {
    // 0o555 keeps execute (traverse) so fileExists works, but no write means writeFile(metaFile) fails
    const paths = resolveModelPaths(testDir, "ReadOnlyMeta");
    mkdirSync(paths.modelDir, { recursive: true });
    writeFileSync(paths.dataFile, "", "utf8");
    chmodSync(paths.modelDir, 0o555);
    try {
      await expect(initializeModelDirectory(paths, "ReadOnlyMeta")).rejects.toBeInstanceOf(CompactionError);
    } finally {
      chmodSync(paths.modelDir, 0o755);
    }
  });
});

describe("updateModelMeta() write and parse failures", () => {
  let modelPaths: ModelPaths;

  beforeEach(async () => {
    modelPaths = resolveModelPaths(testDir, "User");
    await initializeModelDirectory(modelPaths, "User");
  });

  it("throws CompactionError when meta file cannot be overwritten (read-only file)", async () => {
    // Make the meta file itself read-only so writeFile(metaFile) fails
    // (POSIX: writing an existing file needs write perm on the FILE, not the directory)
    chmodSync(modelPaths.metaFile, 0o444);
    try {
      await expect(updateModelMeta(modelPaths, { recordCount: 1 })).rejects.toBeInstanceOf(CompactionError);
    } finally {
      chmodSync(modelPaths.metaFile, 0o644);
    }
  });

  it("throws CompactionError when meta file contains malformed JSON", async () => {
    writeFileSync(modelPaths.metaFile, "{ INVALID JSON", "utf8");
    await expect(updateModelMeta(modelPaths, { recordCount: 1 })).rejects.toBeInstanceOf(CompactionError);
  });

  it("throws CompactionError when meta file contains non-object JSON (array)", async () => {
    writeFileSync(modelPaths.metaFile, "[1, 2, 3]", "utf8");
    await expect(updateModelMeta(modelPaths, { recordCount: 1 })).rejects.toBeInstanceOf(CompactionError);
  });

  it("throws CompactionError when meta file contains non-object JSON (null)", async () => {
    writeFileSync(modelPaths.metaFile, "null", "utf8");
    await expect(updateModelMeta(modelPaths, { recordCount: 1 })).rejects.toBeInstanceOf(CompactionError);
  });
});
