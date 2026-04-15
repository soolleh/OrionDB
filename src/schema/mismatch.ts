// src/schema/mismatch.ts

import { SchemaMismatchError } from "../errors/index.js";
import type { ParsedModelDefinition, PersistedSchema, SchemaMismatchStrategy } from "./types.js";

// ---------------------------------------------------------------------------
// Mismatch detection — types
// ---------------------------------------------------------------------------

export type ChangeKind = "additive" | "destructive";

export interface SchemaChange {
  kind: ChangeKind;
  description: string;
  model: string;
  field?: string;
}

export interface SchemaDiff {
  hasChanges: boolean;
  hasDestructiveChanges: boolean;
  additiveChanges: SchemaChange[];
  destructiveChanges: SchemaChange[];
  allChanges: SchemaChange[];
}

// ---------------------------------------------------------------------------
// diffSchemas — pure, synchronous, never throws
// ---------------------------------------------------------------------------

/**
 * Compares the in-memory code schema against the persisted disk schema and
 * returns a structured SchemaDiff. Pure function — no side effects, no I/O.
 */
export function diffSchemas(codeModels: Map<string, ParsedModelDefinition>, diskSchema: PersistedSchema): SchemaDiff {
  const additive: SchemaChange[] = [];
  const destructive: SchemaChange[] = [];

  const add = (change: SchemaChange): void => {
    if (change.kind === "additive") {
      additive.push(change);
    } else {
      destructive.push(change);
    }
  };

  // Step 1 — removed models (exist on disk, absent in code)
  for (const modelName of Object.keys(diskSchema.models)) {
    if (!codeModels.has(modelName)) {
      add({
        kind: "destructive",
        description: `Model '${modelName}' exists on disk but is absent in code`,
        model: modelName,
      });
    }
  }

  // Step 2 & 3 — iterate code models
  for (const [modelName, codeDef] of codeModels) {
    const diskModel = diskSchema.models[modelName];

    // Step 2 — added model
    if (diskModel === undefined) {
      add({
        kind: "additive",
        description: `Model '${modelName}' is new in code schema`,
        model: modelName,
      });
      continue; // no field-level comparison for a brand-new model
    }

    // Step 3a — removed scalar fields
    for (const diskFieldName of Object.keys(diskModel.fields)) {
      const codeField = codeDef.fields.get(diskFieldName);
      if (codeField === undefined || codeField.type === "relation") {
        add({
          kind: "destructive",
          description: `Field '${modelName}.${diskFieldName}' exists on disk but is absent in code`,
          model: modelName,
          field: diskFieldName,
        });
      }
    }

    // Step 3b — removed relations
    for (const diskRelName of Object.keys(diskModel.relations)) {
      if (!codeDef.relationFields.has(diskRelName)) {
        add({
          kind: "destructive",
          description: `Relation '${modelName}.${diskRelName}' exists on disk but is absent in code`,
          model: modelName,
          field: diskRelName,
        });
      }
    }

    // Step 3c — added scalar fields
    for (const [fieldName, codeField] of codeDef.fields) {
      if (codeField.type === "relation") continue;
      if (diskModel.fields[fieldName] !== undefined) continue; // existing — handled below

      // New scalar field
      const isAdditive = codeField.hasDefault || !codeField.required;
      if (isAdditive) {
        const reason = codeField.hasDefault ? "has a default value" : "is optional";
        add({
          kind: "additive",
          description: `Field '${modelName}.${fieldName}' is new and ${reason}`,
          model: modelName,
          field: fieldName,
        });
      } else {
        add({
          kind: "destructive",
          description: `Field '${modelName}.${fieldName}' is required with no default — existing records cannot satisfy this`,
          model: modelName,
          field: fieldName,
        });
      }
    }

    // Step 3d — added relations
    for (const [relName] of codeDef.relationFields) {
      if (diskModel.relations[relName] === undefined) {
        add({
          kind: "additive",
          description: `Relation '${modelName}.${relName}' is new in code schema`,
          model: modelName,
          field: relName,
        });
      }
    }

    // Step 3e — compare existing scalar fields
    for (const [fieldName, codeField] of codeDef.fields) {
      if (codeField.type === "relation") continue;
      const diskField = diskModel.fields[fieldName];
      if (diskField === undefined) continue; // already handled in 3c

      if (codeField.type !== diskField.type) {
        add({
          kind: "destructive",
          description: `Field '${modelName}.${fieldName}' type changed from '${diskField.type}' to '${codeField.type}'`,
          model: modelName,
          field: fieldName,
        });
        continue; // further checks on a type-changed field are meaningless
      }

      if (Boolean(codeField.primary) !== Boolean(diskField.primary)) {
        add({
          kind: "destructive",
          description: `Field '${modelName}.${fieldName}' primary designation changed`,
          model: modelName,
          field: fieldName,
        });
      }

      const diskUnique = diskField.unique ?? false;
      const codeUnique = codeField.unique ?? false;

      if (diskUnique && !codeUnique) {
        add({
          kind: "destructive",
          description: `Field '${modelName}.${fieldName}' unique constraint removed`,
          model: modelName,
          field: fieldName,
        });
      } else if (!diskUnique && codeUnique) {
        add({
          kind: "additive",
          description: `Field '${modelName}.${fieldName}' unique constraint added`,
          model: modelName,
          field: fieldName,
        });
      }

      // required false→true with no default
      const diskRequired = diskField.required ?? false;
      if (!diskRequired && codeField.required && !codeField.hasDefault) {
        add({
          kind: "destructive",
          description: `Field '${modelName}.${fieldName}' changed from optional to required with no default`,
          model: modelName,
          field: fieldName,
        });
      }

      // enum value changes
      if (codeField.type === "enum" && codeField.enumValues !== undefined) {
        const diskValues = new Set<string>(diskField.enumValues ?? []);
        const codeValues = new Set<string>(codeField.enumValues);

        for (const v of diskValues) {
          if (!codeValues.has(v)) {
            add({
              kind: "destructive",
              description: `Enum value '${v}' removed from '${modelName}.${fieldName}'`,
              model: modelName,
              field: fieldName,
            });
          }
        }
        for (const v of codeValues) {
          if (!diskValues.has(v)) {
            add({
              kind: "additive",
              description: `Enum value '${v}' added to '${modelName}.${fieldName}'`,
              model: modelName,
              field: fieldName,
            });
          }
        }
      }
    }

    // Step 3f — compare existing relations
    for (const [relName, codeRel] of codeDef.relationFields) {
      const diskRel = diskModel.relations[relName];
      if (diskRel === undefined) continue; // already handled in 3d

      if (codeRel.model !== diskRel.model) {
        add({
          kind: "destructive",
          description: `Relation '${modelName}.${relName}' model target changed from '${diskRel.model}' to '${codeRel.model}'`,
          model: modelName,
          field: relName,
        });
      }
      if (codeRel.foreignKey !== diskRel.foreignKey) {
        add({
          kind: "destructive",
          description: `Relation '${modelName}.${relName}' foreignKey changed from '${diskRel.foreignKey}' to '${codeRel.foreignKey}'`,
          model: modelName,
          field: relName,
        });
      }
      if (codeRel.relation !== diskRel.relation) {
        add({
          kind: "destructive",
          description: `Relation '${modelName}.${relName}' relation type changed from '${diskRel.relation}' to '${codeRel.relation}'`,
          model: modelName,
          field: relName,
        });
      }
    }
  }

  return {
    hasChanges: additive.length > 0 || destructive.length > 0,
    hasDestructiveChanges: destructive.length > 0,
    additiveChanges: additive,
    destructiveChanges: destructive,
    allChanges: [...destructive, ...additive],
  };
}

// ---------------------------------------------------------------------------
// applyMismatchStrategy
// ---------------------------------------------------------------------------

/**
 * Applies the configured SchemaMismatchStrategy to the computed diff.
 * Throws SchemaMismatchError when the strategy requires it.
 */
export function applyMismatchStrategy(diff: SchemaDiff, strategy: SchemaMismatchStrategy): void {
  // Destructive changes are always fatal regardless of strategy
  if (diff.hasDestructiveChanges) {
    const lines = diff.destructiveChanges.map((c) => `  - ${c.description}`).join("\n");
    throw new SchemaMismatchError(
      `Schema mismatch (strategy: '${strategy}'): destructive changes detected and cannot be applied automatically:\n${lines}`,
      { meta: { diff, strategy } },
    );
  }

  if (!diff.hasChanges) return;

  // Only additive changes remain from here
  switch (strategy) {
    case "block": {
      const lines = diff.allChanges.map((c) => `  - ${c.description}`).join("\n");
      throw new SchemaMismatchError(
        `Schema mismatch (strategy: 'block'): changes detected. Resolve the mismatch or change the strategy:\n${lines}`,
        { meta: { diff, strategy } },
      );
    }
    case "warn-and-continue": {
      const lines = diff.additiveChanges.map((c) => `  - ${c.description}`).join("\n");
      console.warn(
        `[OrionDB] Schema has additive changes (strategy: 'warn-and-continue'). Continuing with code schema:\n${lines}`,
      );
      break;
    }
    case "auto-migrate": {
      const lines = diff.additiveChanges.map((c) => `  - ${c.description}`).join("\n");
      console.warn(
        `[OrionDB] Schema auto-migrated (strategy: 'auto-migrate'). Changes are informational — code schema is authoritative:\n${lines}`,
      );
      break;
    }
  }
}

// ---------------------------------------------------------------------------
// validateSchema — startup entry point
// ---------------------------------------------------------------------------

/**
 * Orchestrates the full schema mismatch detection flow.
 * Called once during client startup. Returns void or throws.
 */
export function validateSchema(
  codeModels: Map<string, ParsedModelDefinition>,
  diskSchema: PersistedSchema | null,
  strategy: SchemaMismatchStrategy,
): void {
  if (diskSchema === null) return; // first-time initialization

  const diff = diffSchemas(codeModels, diskSchema);
  if (!diff.hasChanges) return;

  applyMismatchStrategy(diff, strategy);
}
