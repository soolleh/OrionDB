/**
 * Smoke test — public API only.
 * All imports from 'src/index.js' — never internal paths.
 * This file tests the library as a consumer would use it.
 */

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, beforeEach, afterEach } from "vitest";

// ── ALL imports from public barrel only ───────────────────────
import {
  createOrionDB,
  OrionDBError,
  ValidationError,
  UniqueConstraintError,
  RecordNotFoundError,
  QueryError,
} from "../../src/index.js";
import type {
  OrionDBConfig,
  OrionDB,
  CreateInput,
  FindManyClientInput,
  WhereInput,
  IncludeClause,
  SchemaDefinition,
} from "../../src/index.js";

// ── Schema typed using public types ──────────────────────────
// Schema uses the flat format: model name → field map.
// Scalar fields and relation fields are keyed together in one object.
const schema: SchemaDefinition = {
  User: {
    id: { type: "string", primary: true, default: () => crypto.randomUUID() },
    name: { type: "string", required: true },
    email: { type: "string", unique: true },
    age: { type: "number" },
    posts: { type: "relation", model: "Post", foreignKey: "authorId", relation: "one-to-many" },
  },
  Post: {
    id: { type: "string", primary: true, default: () => crypto.randomUUID() },
    title: { type: "string", required: true },
    authorId: { type: "string", required: true },
    author: { type: "relation", model: "User", foreignKey: "authorId", relation: "many-to-one" },
  },
};

let testDir: string;
let db: OrionDB;

beforeEach(async () => {
  testDir = mkdtempSync(join(tmpdir(), "oriondb-smoke-"));
  const config: OrionDBConfig = {
    dbLocation: testDir,
    logLevel: "error",
    schema,
  };
  db = createOrionDB(config);
  await db.$connect();
});

afterEach(async () => {
  try {
    await db.$disconnect();
  } catch {
    /* ignore */
  }
  rmSync(testDir, { recursive: true, force: true });
});

describe("public API smoke test", () => {
  it("createOrionDB returns a connected instance", () => {
    expect(db.$isConnected).toBe(true);
  });

  it("create and findUnique round-trip", async () => {
    const input: CreateInput = {
      data: { name: "Alice", email: "alice@example.com", age: 30 },
    };
    const alice = await db.user.create(input);

    expect(typeof alice.id).toBe("string");
    expect(alice.name).toBe("Alice");
    expect(alice.email).toBe("alice@example.com");

    const found = await db.user.findUnique({ where: { id: alice.id } });
    expect(found).not.toBeNull();
    expect(found?.name).toBe("Alice");
  });

  it("findMany with typed WhereInput", async () => {
    await db.user.create({ data: { name: "Alice", email: "alice@example.com", age: 30 } });
    await db.user.create({ data: { name: "Bob", email: "bob@example.com", age: 20 } });

    const where: WhereInput = { age: { gt: 25 } };
    const input: FindManyClientInput = { where };
    const results = await db.user.findMany(input);

    expect(results).toHaveLength(1);
    expect(results[0]?.name).toBe("Alice");
  });

  it("UniqueConstraintError thrown on duplicate", async () => {
    await db.user.create({ data: { name: "Alice", email: "alice@example.com" } });

    await expect(db.user.create({ data: { name: "Alice2", email: "alice@example.com" } })).rejects.toBeInstanceOf(
      UniqueConstraintError,
    );
  });

  it("ValidationError thrown for missing required field", async () => {
    await expect(db.user.create({ data: { email: "no-name@example.com" } })).rejects.toBeInstanceOf(ValidationError);
  });

  it("RecordNotFoundError thrown from findUniqueOrThrow", async () => {
    await expect(db.user.findUniqueOrThrow({ where: { id: "does-not-exist" } })).rejects.toBeInstanceOf(
      RecordNotFoundError,
    );
  });

  it("QueryError thrown for invalid where clause", async () => {
    await expect(
      db.user.findMany({
        where: { age: { in: "not-an-array" as unknown as unknown[] } },
      }),
    ).rejects.toBeInstanceOf(QueryError);
  });

  it("update returns updated record", async () => {
    const alice = await db.user.create({
      data: { name: "Alice", email: "alice@example.com", age: 30 },
    });

    const updated = await db.user.update({
      where: { id: alice.id },
      data: { age: 31 },
    });

    expect(updated.age).toBe(31);
    expect(updated.name).toBe("Alice");
  });

  it("delete removes record", async () => {
    const alice = await db.user.create({ data: { name: "Alice", email: "alice@example.com" } });

    await db.user.delete({ where: { id: alice.id } });

    const found = await db.user.findUnique({ where: { id: alice.id } });
    expect(found).toBeNull();
  });

  it("include resolves related records", async () => {
    const alice = await db.user.create({ data: { name: "Alice", email: "alice@example.com" } });

    await db.post.create({ data: { title: "Post 1", authorId: alice.id as string } });
    await db.post.create({ data: { title: "Post 2", authorId: alice.id as string } });

    const include: IncludeClause = { posts: true };
    const withPosts = await db.user.findUnique({
      where: { id: alice.id },
      include,
    });

    expect(Array.isArray(withPosts?.posts)).toBe(true);
    expect((withPosts?.posts as unknown[]).length).toBe(2);
  });

  it("count returns correct number", async () => {
    await db.user.create({ data: { name: "A", email: "a@example.com" } });
    await db.user.create({ data: { name: "B", email: "b@example.com" } });

    const total = await db.user.count();
    expect(total).toBe(2);
  });

  it("$disconnect and reconnect preserves data", async () => {
    const alice = await db.user.create({ data: { name: "Alice", email: "alice@example.com" } });

    await db.$disconnect();

    const db2 = createOrionDB({
      dbLocation: testDir,
      logLevel: "error",
      schema,
    });
    await db2.$connect();

    const found = await db2.user.findUnique({ where: { id: alice.id } });
    expect(found?.name).toBe("Alice");

    await db2.$disconnect();
    db = db2; // prevent afterEach double-disconnect
  });

  it("OrionDBError is base class of all errors", async () => {
    try {
      await db.user.create({ data: { email: "no-name@example.com" } });
    } catch (err) {
      expect(err).toBeInstanceOf(OrionDBError);
      expect(err).toBeInstanceOf(ValidationError);
    }
  });

  it("$compact runs without error", async () => {
    await db.user.create({ data: { name: "Alice", email: "alice@example.com" } });
    await db.user.update({
      where: { email: "alice@example.com" },
      data: { age: 31 },
    });

    const results = await db.$compact(undefined, { force: true });
    expect(Array.isArray(results)).toBe(true);
  });
});
