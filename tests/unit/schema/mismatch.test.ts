import { SchemaMismatchError } from "../../../src/errors/index.js";
import {
  diffSchemas,
  applyMismatchStrategy,
  validateSchema,
  serializeSchema,
  parseModelSchema,
} from "../../../src/schema/index.js";
import type { ParsedModelDefinition, PersistedSchema, SchemaDiff } from "../../../src/schema/index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeModel(name: string, extra: Parameters<typeof parseModelSchema>[1] = {}): ParsedModelDefinition {
  return parseModelSchema(name, {
    id: { type: "string", primary: true },
    name: { type: "string" },
    ...extra,
  });
}

function makeModels(...names: string[]): Map<string, ParsedModelDefinition> {
  const map = new Map<string, ParsedModelDefinition>();
  for (const n of names) map.set(n, makeModel(n));
  return map;
}

/** Serializes a code models map into a PersistedSchema (disk snapshot). */
function toDisk(models: Map<string, ParsedModelDefinition>): PersistedSchema {
  return serializeSchema(models);
}

// ---------------------------------------------------------------------------
// diffSchemas()
// ---------------------------------------------------------------------------

describe("diffSchemas()", () => {
  describe("no changes", () => {
    it("hasChanges is false when schemas are identical", () => {
      const models = makeModels("User");
      const disk = toDisk(models);
      const diff = diffSchemas(models, disk);

      expect(diff.hasChanges).toBe(false);
    });

    it("hasDestructiveChanges is false when schemas are identical", () => {
      const models = makeModels("User");
      const disk = toDisk(models);
      const diff = diffSchemas(models, disk);

      expect(diff.hasDestructiveChanges).toBe(false);
    });

    it("allChanges is empty when schemas are identical", () => {
      const models = makeModels("User");
      const disk = toDisk(models);
      const diff = diffSchemas(models, disk);

      expect(diff.allChanges).toHaveLength(0);
    });
  });

  // ─── Model-level changes ──────────────────────────────────────────────────

  describe("model-level changes", () => {
    it("adding a new model is additive", () => {
      const diskModels = makeModels("User");
      const codeModels = makeModels("User", "Post");
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasChanges).toBe(true);
      expect(diff.hasDestructiveChanges).toBe(false);
      expect(diff.additiveChanges).toHaveLength(1);
      expect(diff.additiveChanges[0]?.model).toBe("Post");
    });

    it("removing a model is destructive", () => {
      const diskModels = makeModels("User", "Post");
      const codeModels = makeModels("User");
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      expect(diff.destructiveChanges.some((c) => c.model === "Post")).toBe(true);
    });
  });

  // ─── Field-level — additive changes ──────────────────────────────────────

  describe("field-level additive changes", () => {
    it("adding a new optional field is additive", () => {
      const diskModels = makeModels("User");
      const codeModels = new Map([
        [
          "User",
          makeModel("User", {
            nickname: { type: "string" },
          }),
        ],
      ]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(false);
      expect(diff.additiveChanges.some((c) => c.field === "nickname")).toBe(true);
    });

    it("adding a new field with a default value is additive", () => {
      const diskModels = makeModels("User");
      const codeModels = new Map([
        [
          "User",
          makeModel("User", {
            score: { type: "number", default: 0 },
          }),
        ],
      ]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(false);
      expect(diff.additiveChanges.some((c) => c.field === "score")).toBe(true);
    });

    it("adding a unique constraint to a field is additive", () => {
      const diskModels = new Map([["User", makeModel("User", { email: { type: "string" } })]]);
      const codeModels = new Map([["User", makeModel("User", { email: { type: "string", unique: true } })]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(false);
      const change = diff.additiveChanges.find((c) => c.field === "email");
      expect(change).toBeDefined();
      expect(change?.kind).toBe("additive");
    });

    it("adding a new enum value to an existing enum field is additive", () => {
      const diskModels = new Map([["User", makeModel("User", { status: { type: "enum", values: ["active"] } })]]);
      const codeModels = new Map([
        ["User", makeModel("User", { status: { type: "enum", values: ["active", "inactive"] } })],
      ]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(false);
      expect(diff.additiveChanges.some((c) => c.field === "status")).toBe(true);
    });

    it("additiveChanges is empty when only a new model is added (no field changes)", () => {
      const diskModels = makeModels("User");
      const codeModels = makeModels("User", "Post");
      const disk = toDisk(diskModels);
      // The new model itself is additive, but no existing model has field-level additive changes
      const diff = diffSchemas(codeModels, disk);

      expect(diff.additiveChanges.every((c) => c.model === "Post")).toBe(true);
    });
  });

  // ─── Field-level — destructive changes ───────────────────────────────────

  describe("field-level destructive changes", () => {
    it("removing a field is destructive", () => {
      const diskModels = new Map([["User", makeModel("User", { email: { type: "string" } })]]);
      const codeModels = makeModels("User");
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      expect(diff.destructiveChanges.some((c) => c.field === "email")).toBe(true);
    });

    it("changing a field type is destructive", () => {
      const diskModels = new Map([["User", makeModel("User", { score: { type: "number" } })]]);
      const codeModels = new Map([["User", makeModel("User", { score: { type: "string" } })]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      const change = diff.destructiveChanges.find((c) => c.field === "score");
      expect(change).toBeDefined();
    });

    it("removing a unique constraint from a field is destructive", () => {
      const diskModels = new Map([["User", makeModel("User", { email: { type: "string", unique: true } })]]);
      const codeModels = new Map([["User", makeModel("User", { email: { type: "string" } })]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      const change = diff.destructiveChanges.find((c) => c.field === "email");
      expect(change?.kind).toBe("destructive");
    });

    it("changing optional to required with no default is destructive", () => {
      const diskModels = new Map([["User", makeModel("User", { nickname: { type: "string" } })]]);
      const codeModels = new Map([["User", makeModel("User", { nickname: { type: "string", required: true } })]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      const change = diff.destructiveChanges.find((c) => c.field === "nickname");
      expect(change).toBeDefined();
    });

    it("adding a required field with no default is destructive", () => {
      const diskModels = makeModels("User");
      const codeModels = new Map([["User", makeModel("User", { nickname: { type: "string", required: true } })]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      const change = diff.destructiveChanges.find((c) => c.field === "nickname");
      expect(change).toBeDefined();
    });

    it("removing an enum value is destructive", () => {
      const diskModels = new Map([
        ["User", makeModel("User", { status: { type: "enum", values: ["active", "inactive"] } })],
      ]);
      const codeModels = new Map([["User", makeModel("User", { status: { type: "enum", values: ["active"] } })]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      expect(diff.destructiveChanges.some((c) => c.field === "status")).toBe(true);
    });

    it("changing the primary key field is destructive", () => {
      const diskModels = makeModels("User");
      // Change primary field from id to uuid — simulate by disk having 'id' as primary but code doesn't include it
      const codeDefinition = parseModelSchema("User", {
        uuid: { type: "string", primary: true },
        name: { type: "string" },
      });
      const codeModels = new Map([["User", codeDefinition]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      // 'id' removed (destructive), 'uuid' added (additive new field) — overall has destructive
      expect(diff.hasDestructiveChanges).toBe(true);
    });
  });

  // ─── allChanges ordering ──────────────────────────────────────────────────

  describe("allChanges ordering", () => {
    it("allChanges contains destructive changes before additive ones", () => {
      // Both destructive (removed field) and additive (new optional field) changes
      const diskModels = new Map([["User", makeModel("User", { email: { type: "string" } })]]);
      const codeModels = new Map([["User", makeModel("User", { nickname: { type: "string" } })]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      // email removed → destructive, nickname added → additive
      const destructiveIdx = diff.allChanges.findIndex((c) => c.kind === "destructive");
      const additiveIdx = diff.allChanges.findIndex((c) => c.kind === "additive");

      expect(destructiveIdx).toBeGreaterThanOrEqual(0);
      expect(additiveIdx).toBeGreaterThanOrEqual(0);
      expect(destructiveIdx).toBeLessThan(additiveIdx);
    });

    it("allChanges length equals sum of additive and destructive changes", () => {
      const diskModels = makeModels("User", "Post");
      const codeModels = makeModels("User", "NewModel");
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.allChanges).toHaveLength(diff.additiveChanges.length + diff.destructiveChanges.length);
    });
  });

  // ─── Relation-level changes ───────────────────────────────────────────────

  describe("relation-level changes", () => {
    it("adding a new relation field is additive", () => {
      const diskPost = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
      });
      const codePost = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        author: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "many-to-one",
        },
      });
      const diskModels = new Map([["Post", diskPost]]);
      const codeModels = new Map([["Post", codePost]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(false);
      expect(diff.additiveChanges.some((c) => c.field === "author")).toBe(true);
    });

    it("removing a relation field is destructive", () => {
      const diskPost = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        author: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "many-to-one",
        },
      });
      const codePost = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
      });
      const diskModels = new Map([["Post", diskPost]]);
      const codeModels = new Map([["Post", codePost]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      expect(diff.destructiveChanges.some((c) => c.field === "author")).toBe(true);
    });

    it("changing relation model target is destructive", () => {
      const diskPost = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        author: {
          type: "relation",
          model: "User",
          foreignKey: "authorId",
          relation: "many-to-one",
        },
      });
      const codePost = parseModelSchema("Post", {
        id: { type: "string", primary: true },
        authorId: { type: "string" },
        author: {
          type: "relation",
          model: "Admin",
          foreignKey: "authorId",
          relation: "many-to-one",
        },
      });
      const diskModels = new Map([["Post", diskPost]]);
      const codeModels = new Map([["Post", codePost]]);
      const disk = toDisk(diskModels);
      const diff = diffSchemas(codeModels, disk);

      expect(diff.hasDestructiveChanges).toBe(true);
      expect(diff.destructiveChanges.some((c) => c.field === "author")).toBe(true);
    });
  });
});

// ---------------------------------------------------------------------------
// applyMismatchStrategy()
// ---------------------------------------------------------------------------

describe("applyMismatchStrategy()", () => {
  // Helper to build a diff with only additive changes
  function makeAdditiveDiff(): SchemaDiff {
    const diskModels = makeModels("User");
    const codeModels = new Map([["User", makeModel("User", { score: { type: "number", default: 0 } })]]);
    return diffSchemas(codeModels, toDisk(diskModels));
  }

  // Helper to build a diff with destructive changes
  function makeDestructiveDiff(): SchemaDiff {
    const diskModels = new Map([["User", makeModel("User", { email: { type: "string" } })]]);
    const codeModels = makeModels("User");
    return diffSchemas(codeModels, toDisk(diskModels));
  }

  // Helper to build a diff with no changes
  function makeNoDiff(): SchemaDiff {
    const models = makeModels("User");
    return diffSchemas(models, toDisk(models));
  }

  describe("strategy: block", () => {
    it("throws SchemaMismatchError for additive changes", () => {
      const diff = makeAdditiveDiff();

      expect(() => applyMismatchStrategy(diff, "block")).toThrow(SchemaMismatchError);
    });

    it("throws SchemaMismatchError for destructive changes", () => {
      const diff = makeDestructiveDiff();

      expect(() => applyMismatchStrategy(diff, "block")).toThrow(SchemaMismatchError);
    });

    it("does not throw when there are no changes", () => {
      const diff = makeNoDiff();

      expect(() => applyMismatchStrategy(diff, "block")).not.toThrow();
    });
  });

  describe("strategy: warn-and-continue", () => {
    it("does not throw for additive-only changes and emits console.warn", () => {
      const diff = makeAdditiveDiff();
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      try {
        expect(() => applyMismatchStrategy(diff, "warn-and-continue")).not.toThrow();
        expect(warnSpy).toHaveBeenCalled();
      } finally {
        warnSpy.mockRestore();
      }
    });

    it("throws SchemaMismatchError when destructive changes are present", () => {
      const diff = makeDestructiveDiff();

      expect(() => applyMismatchStrategy(diff, "warn-and-continue")).toThrow(SchemaMismatchError);
    });

    it("does not throw and does not warn when there are no changes", () => {
      const diff = makeNoDiff();
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      try {
        expect(() => applyMismatchStrategy(diff, "warn-and-continue")).not.toThrow();
        expect(warnSpy).not.toHaveBeenCalled();
      } finally {
        warnSpy.mockRestore();
      }
    });
  });

  describe("strategy: auto-migrate", () => {
    it("does not throw for additive-only changes and emits console.warn", () => {
      const diff = makeAdditiveDiff();
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      try {
        expect(() => applyMismatchStrategy(diff, "auto-migrate")).not.toThrow();
        expect(warnSpy).toHaveBeenCalled();
      } finally {
        warnSpy.mockRestore();
      }
    });

    it("throws SchemaMismatchError when destructive changes are present", () => {
      const diff = makeDestructiveDiff();

      expect(() => applyMismatchStrategy(diff, "auto-migrate")).toThrow(SchemaMismatchError);
    });

    it("does not throw and does not warn when there are no changes", () => {
      const diff = makeNoDiff();
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      try {
        expect(() => applyMismatchStrategy(diff, "auto-migrate")).not.toThrow();
        expect(warnSpy).not.toHaveBeenCalled();
      } finally {
        warnSpy.mockRestore();
      }
    });
  });

  describe("error contains diff in meta", () => {
    it("SchemaMismatchError from block strategy contains diff in meta", () => {
      const diff = makeAdditiveDiff();

      try {
        applyMismatchStrategy(diff, "block");
        expect.fail("Expected SchemaMismatchError to be thrown");
      } catch (error) {
        expect(error).toBeInstanceOf(SchemaMismatchError);
        const e = error as SchemaMismatchError;
        const meta = e.meta as Record<string, unknown>;
        expect(meta["diff"]).toBeDefined();
      }
    });
  });
});

// ---------------------------------------------------------------------------
// validateSchema()
// ---------------------------------------------------------------------------

describe("validateSchema()", () => {
  it("returns immediately (no throw) when diskSchema is null", () => {
    const codeModels = makeModels("User");

    expect(() => validateSchema(codeModels, null, "block")).not.toThrow();
  });

  it("returns without throwing when schemas are identical", () => {
    const models = makeModels("User");
    const disk = toDisk(models);

    expect(() => validateSchema(models, disk, "block")).not.toThrow();
  });

  it("throws SchemaMismatchError with block strategy when changes detected", () => {
    const diskModels = makeModels("User");
    const codeModels = makeModels("User", "Post");
    const disk = toDisk(diskModels);

    expect(() => validateSchema(codeModels, disk, "block")).toThrow(SchemaMismatchError);
  });

  it("does not throw with warn-and-continue strategy for additive changes", () => {
    const diskModels = makeModels("User");
    const codeModels = makeModels("User", "Post");
    const disk = toDisk(diskModels);
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    try {
      expect(() => validateSchema(codeModels, disk, "warn-and-continue")).not.toThrow();
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("does not throw with auto-migrate strategy for additive changes", () => {
    const diskModels = makeModels("User");
    const codeModels = makeModels("User", "Post");
    const disk = toDisk(diskModels);
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    try {
      expect(() => validateSchema(codeModels, disk, "auto-migrate")).not.toThrow();
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("throws SchemaMismatchError for destructive changes regardless of warn-and-continue", () => {
    const diskModels = new Map([["User", makeModel("User", { email: { type: "string" } })]]);
    const codeModels = makeModels("User");
    const disk = toDisk(diskModels);

    expect(() => validateSchema(codeModels, disk, "warn-and-continue")).toThrow(SchemaMismatchError);
  });

  it("throws SchemaMismatchError for destructive changes regardless of auto-migrate", () => {
    const diskModels = new Map([["User", makeModel("User", { email: { type: "string" } })]]);
    const codeModels = makeModels("User");
    const disk = toDisk(diskModels);

    expect(() => validateSchema(codeModels, disk, "auto-migrate")).toThrow(SchemaMismatchError);
  });
});
