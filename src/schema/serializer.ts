// src/schema/serializer.ts

import { promises as fs } from "node:fs";
import { CompactionError, SchemaMismatchError } from "../errors/index.js";
import { CURRENT_SCHEMA_VERSION } from "./types.js";
import type {
  ParsedModelDefinition,
  PersistedSchema,
  ScalarFieldType,
  RelationType,
  SerializableFieldDefinition,
  SerializableRelationDefinition,
  SerializableModelDefinition,
} from "./types.js";

// ---------------------------------------------------------------------------
// Private helper
// ---------------------------------------------------------------------------

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// ---------------------------------------------------------------------------
// serializeSchema
// ---------------------------------------------------------------------------

/**
 * Converts the in-memory parsed model map into a PersistedSchema ready for
 * JSON serialization. Dynamic defaults (functions) are never included.
 */
export function serializeSchema(models: Map<string, ParsedModelDefinition>): PersistedSchema {
  const serializedModels: Record<string, SerializableModelDefinition> = {};

  for (const [modelName, modelDef] of models) {
    const fields: Record<string, SerializableFieldDefinition> = {};
    const relations: Record<string, SerializableRelationDefinition> = {};

    for (const [fieldName, fieldDef] of modelDef.fields) {
      if (fieldDef.type === "relation") {
        relations[fieldName] = {
          model: fieldDef.model,
          foreignKey: fieldDef.foreignKey,
          relation: fieldDef.relation,
        };
        continue;
      }

      // Scalar field — omit boolean flags when false, omit default always
      const serialized: SerializableFieldDefinition = { type: fieldDef.type };
      if (fieldDef.primary) serialized.primary = true;
      if (fieldDef.unique) serialized.unique = true;
      if (fieldDef.required) serialized.required = true;
      if (fieldDef.type === "enum" && fieldDef.enumValues !== undefined) {
        serialized.enumValues = fieldDef.enumValues;
      }
      fields[fieldName] = serialized;
    }

    serializedModels[modelName] = { fields, relations };
  }

  return {
    version: CURRENT_SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    models: serializedModels,
  };
}

// ---------------------------------------------------------------------------
// deserializeSchema
// ---------------------------------------------------------------------------

const VALID_SCALAR_TYPES: ReadonlySet<string> = new Set(["string", "number", "boolean", "date", "json", "enum"]);

const VALID_RELATION_TYPES: ReadonlySet<string> = new Set(["one-to-one", "one-to-many", "many-to-one"]);

/**
 * Validates a raw parsed JSON value and returns a fully typed PersistedSchema.
 * Throws SchemaMismatchError on any structural violation.
 */
export function deserializeSchema(raw: unknown): PersistedSchema {
  if (!isPlainObject(raw)) {
    throw new SchemaMismatchError("_schema.json root must be a plain object.", {
      meta: { path: "(root)", found: raw },
    });
  }

  // version
  if (typeof raw["version"] !== "number") {
    throw new SchemaMismatchError(`_schema.json "version" must be a number, got ${typeof raw["version"]}.`, {
      meta: { path: "version", found: raw["version"] },
    });
  }
  if (!Number.isInteger(raw["version"])) {
    throw new SchemaMismatchError(`_schema.json "version" must be an integer, got ${raw["version"]}.`, {
      meta: { path: "version", found: raw["version"] },
    });
  }
  if (raw["version"] !== CURRENT_SCHEMA_VERSION) {
    throw new SchemaMismatchError(
      `_schema.json version mismatch: found ${raw["version"]}, expected ${CURRENT_SCHEMA_VERSION}.`,
      { meta: { path: "version", found: raw["version"], expected: CURRENT_SCHEMA_VERSION } },
    );
  }

  // generatedAt
  if (typeof raw["generatedAt"] !== "string") {
    throw new SchemaMismatchError(`_schema.json "generatedAt" must be a string.`, {
      meta: { path: "generatedAt", found: raw["generatedAt"] },
    });
  }

  // models
  if (!isPlainObject(raw["models"])) {
    throw new SchemaMismatchError(`_schema.json "models" must be a plain object.`, {
      meta: { path: "models", found: raw["models"] },
    });
  }

  const models: Record<string, SerializableModelDefinition> = {};

  for (const [modelName, rawModel] of Object.entries(raw["models"])) {
    const modelPath = `models.${modelName}`;

    if (!isPlainObject(rawModel)) {
      throw new SchemaMismatchError(`${modelPath} must be a plain object.`, {
        meta: { path: modelPath, found: rawModel },
      });
    }

    if (!isPlainObject(rawModel["fields"])) {
      throw new SchemaMismatchError(`${modelPath}.fields must be a plain object.`, {
        meta: { path: `${modelPath}.fields`, found: rawModel["fields"] },
      });
    }

    if (!isPlainObject(rawModel["relations"])) {
      throw new SchemaMismatchError(`${modelPath}.relations must be a plain object.`, {
        meta: { path: `${modelPath}.relations`, found: rawModel["relations"] },
      });
    }

    const fields: Record<string, SerializableFieldDefinition> = {};
    for (const [fieldName, rawField] of Object.entries(rawModel["fields"])) {
      const fieldPath = `${modelPath}.fields.${fieldName}`;

      if (!isPlainObject(rawField)) {
        throw new SchemaMismatchError(`${fieldPath} must be a plain object.`, {
          meta: { path: fieldPath, found: rawField },
        });
      }

      if (typeof rawField["type"] !== "string" || !VALID_SCALAR_TYPES.has(rawField["type"])) {
        throw new SchemaMismatchError(
          `${fieldPath}.type must be one of the valid scalar types, got ${JSON.stringify(rawField["type"])}.`,
          { meta: { path: `${fieldPath}.type`, found: rawField["type"] } },
        );
      }
      const fieldType = rawField["type"] as ScalarFieldType;

      const serializedField: SerializableFieldDefinition = { type: fieldType };

      if ("primary" in rawField) {
        if (typeof rawField["primary"] !== "boolean") {
          throw new SchemaMismatchError(`${fieldPath}.primary must be boolean.`, {
            meta: { path: `${fieldPath}.primary`, found: rawField["primary"] },
          });
        }
        serializedField.primary = rawField["primary"];
      }

      if ("unique" in rawField) {
        if (typeof rawField["unique"] !== "boolean") {
          throw new SchemaMismatchError(`${fieldPath}.unique must be boolean.`, {
            meta: { path: `${fieldPath}.unique`, found: rawField["unique"] },
          });
        }
        serializedField.unique = rawField["unique"];
      }

      if ("required" in rawField) {
        if (typeof rawField["required"] !== "boolean") {
          throw new SchemaMismatchError(`${fieldPath}.required must be boolean.`, {
            meta: { path: `${fieldPath}.required`, found: rawField["required"] },
          });
        }
        serializedField.required = rawField["required"];
      }

      if ("enumValues" in rawField) {
        const ev = rawField["enumValues"];
        if (!Array.isArray(ev) || ev.some((e) => typeof e !== "string" || e.length === 0)) {
          throw new SchemaMismatchError(`${fieldPath}.enumValues must be an array of non-empty strings.`, {
            meta: { path: `${fieldPath}.enumValues`, found: ev },
          });
        }
        serializedField.enumValues = ev as string[];
      }

      fields[fieldName] = serializedField;
    }

    const relations: Record<string, SerializableRelationDefinition> = {};
    for (const [relName, rawRel] of Object.entries(rawModel["relations"])) {
      const relPath = `${modelPath}.relations.${relName}`;

      if (!isPlainObject(rawRel)) {
        throw new SchemaMismatchError(`${relPath} must be a plain object.`, { meta: { path: relPath, found: rawRel } });
      }

      if (typeof rawRel["model"] !== "string" || rawRel["model"].trim().length === 0) {
        throw new SchemaMismatchError(`${relPath}.model must be a non-empty string.`, {
          meta: { path: `${relPath}.model`, found: rawRel["model"] },
        });
      }

      if (typeof rawRel["foreignKey"] !== "string" || rawRel["foreignKey"].trim().length === 0) {
        throw new SchemaMismatchError(`${relPath}.foreignKey must be a non-empty string.`, {
          meta: { path: `${relPath}.foreignKey`, found: rawRel["foreignKey"] },
        });
      }

      if (typeof rawRel["relation"] !== "string" || !VALID_RELATION_TYPES.has(rawRel["relation"])) {
        throw new SchemaMismatchError(`${relPath}.relation must be one of: one-to-one, one-to-many, many-to-one.`, {
          meta: { path: `${relPath}.relation`, found: rawRel["relation"] },
        });
      }

      relations[relName] = {
        model: rawRel["model"],
        foreignKey: rawRel["foreignKey"],
        relation: rawRel["relation"] as RelationType,
      };
    }

    models[modelName] = { fields, relations };
  }

  return {
    version: raw["version"],
    generatedAt: raw["generatedAt"],
    models,
  };
}

// ---------------------------------------------------------------------------
// writeSchemaFile
// ---------------------------------------------------------------------------

/**
 * Serializes the models map and writes pretty-printed JSON to filePath.
 * Throws SchemaMismatchError on any write failure.
 */
export async function writeSchemaFile(filePath: string, models: Map<string, ParsedModelDefinition>): Promise<void> {
  const schema = serializeSchema(models);
  const json = JSON.stringify(schema, null, 2);
  try {
    await fs.writeFile(filePath, json, "utf8");
  } catch (err: unknown) {
    // CompactionError is used here as the closest structural I/O error.
    // A dedicated StorageError class should be introduced in a future pass.
    throw new CompactionError(`Failed to write schema file at "${filePath}".`, { meta: { cause: err } });
  }
}

// ---------------------------------------------------------------------------
// readSchemaFile
// ---------------------------------------------------------------------------

/**
 * Reads _schema.json from disk and returns a validated PersistedSchema,
 * or null if the file does not exist.
 */
export async function readSchemaFile(filePath: string): Promise<PersistedSchema | null> {
  let raw: string;
  try {
    raw = await fs.readFile(filePath, "utf8");
  } catch (err: unknown) {
    const isNotFound =
      err !== null && typeof err === "object" && "code" in err && (err as NodeJS.ErrnoException).code === "ENOENT";
    if (isNotFound) return null;
    throw new SchemaMismatchError(`Failed to read schema file at "${filePath}".`, { meta: { cause: err } });
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err: unknown) {
    throw new SchemaMismatchError(`Schema file at "${filePath}" contains invalid JSON and cannot be parsed.`, {
      meta: { cause: err },
    });
  }

  return deserializeSchema(parsed);
}
