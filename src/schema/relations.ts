// src/schema/relations.ts

import { SchemaValidationError } from "../errors/index.js";
import { validateSchema } from "./mismatch.js";
import type {
  ParsedModelDefinition,
  ParsedRelationField,
  RelationType,
  PersistedSchema,
  SchemaMismatchStrategy,
} from "./types.js";

// ---------------------------------------------------------------------------
// Private: symmetry table for bidirectional relation type validation
// ---------------------------------------------------------------------------

const EXPECTED_COUNTERPART: Record<RelationType, RelationType> = {
  "one-to-many": "many-to-one",
  "many-to-one": "one-to-many",
  "one-to-one": "one-to-one",
};

// ---------------------------------------------------------------------------
// Private: foreign key field existence validator
// ---------------------------------------------------------------------------

function validateForeignKeyExists(
  modelName: string,
  fieldName: string,
  rel: ParsedRelationField,
  declaringModel: ParsedModelDefinition,
  referencedModel: ParsedModelDefinition,
): void {
  const fkName = rel.foreignKey;

  const fkOnReferenced = ((): boolean => {
    const f = referencedModel.fields.get(fkName);
    return f !== undefined && f.type !== "relation";
  })();

  const fkOnDeclaring = ((): boolean => {
    const f = declaringModel.fields.get(fkName);
    return f !== undefined && f.type !== "relation";
  })();

  switch (rel.relation) {
    case "one-to-many": {
      // FK must live on the referenced (child) model
      if (!fkOnReferenced) {
        throw new SchemaValidationError(
          `Relation '${modelName}.${fieldName}' (one-to-many) declares foreignKey '${fkName}', but that field does not exist as a scalar field on '${referencedModel.name}'.`,
          {
            model: modelName,
            field: fieldName,
            meta: {
              declaringModel: modelName,
              referencedModel: referencedModel.name,
              foreignKey: fkName,
              expectedOn: referencedModel.name,
            },
          },
        );
      }
      break;
    }
    case "many-to-one": {
      // FK must live on the declaring model (self)
      if (!fkOnDeclaring) {
        throw new SchemaValidationError(
          `Relation '${modelName}.${fieldName}' (many-to-one) declares foreignKey '${fkName}', but that field does not exist as a scalar field on '${modelName}'.`,
          {
            model: modelName,
            field: fieldName,
            meta: {
              declaringModel: modelName,
              referencedModel: referencedModel.name,
              foreignKey: fkName,
              expectedOn: modelName,
            },
          },
        );
      }
      break;
    }
    case "one-to-one": {
      // FK may live on either side — check referenced first, then declaring
      if (!fkOnReferenced && !fkOnDeclaring) {
        throw new SchemaValidationError(
          `Relation '${modelName}.${fieldName}' (one-to-one) declares foreignKey '${fkName}', but that field does not exist as a scalar field on '${referencedModel.name}' or '${modelName}'.`,
          {
            model: modelName,
            field: fieldName,
            meta: { declaringModel: modelName, referencedModel: referencedModel.name, foreignKey: fkName },
          },
        );
      }
      break;
    }
  }
}

// ---------------------------------------------------------------------------
// validateRelationships — cross-model consistency validator
// ---------------------------------------------------------------------------

/**
 * Validates all relation fields across all models for cross-model consistency.
 * Runs as a second pass after all models are individually parsed.
 * Throws SchemaValidationError on the first violation found.
 */
export function validateRelationships(models: Map<string, ParsedModelDefinition>): void {
  // Pass 1 — Rules 1, 2, 4 per relation field
  for (const [modelName, modelDef] of models) {
    for (const [fieldName, rel] of modelDef.relationFields) {
      // Rule 4 — no self-referential relations in Phase 1
      if (rel.model === modelName) {
        throw new SchemaValidationError(
          `Relation '${modelName}.${fieldName}' references its own model. Self-referential relations are not supported in Phase 1.`,
          { model: modelName, field: fieldName, meta: { declaringModel: modelName, referencedModel: rel.model } },
        );
      }

      // Rule 1 — referenced model must exist
      const referencedModel = models.get(rel.model);
      if (referencedModel === undefined) {
        throw new SchemaValidationError(
          `Relation '${modelName}.${fieldName}' references model '${rel.model}' which does not exist in the schema.`,
          { model: modelName, field: fieldName, meta: { declaringModel: modelName, missingModel: rel.model } },
        );
      }

      // Rule 2 — foreign key field must exist on the correct model
      validateForeignKeyExists(modelName, fieldName, rel, modelDef, referencedModel);
    }
  }

  // Pass 2 — Rule 3: bidirectional consistency
  for (const [modelAName, modelADef] of models) {
    for (const [fieldAName, relA] of modelADef.relationFields) {
      const modelBName = relA.model;
      const modelBDef = models.get(modelBName);
      if (modelBDef === undefined) continue; // already caught in pass 1

      // Find any relation field in B that points back to A with same FK
      let foundCounterpart = false;
      for (const [fieldBName, relB] of modelBDef.relationFields) {
        if (relB.model !== modelAName) continue;
        foundCounterpart = true;

        // Both sides reference each other — check consistency
        if (relA.foreignKey !== relB.foreignKey) {
          throw new SchemaValidationError(
            `Bidirectional relation between '${modelAName}.${fieldAName}' and '${modelBName}.${fieldBName}' declares different foreignKey values ('${relA.foreignKey}' vs '${relB.foreignKey}'). Both sides must agree.`,
            {
              model: modelAName,
              field: fieldAName,
              meta: {
                modelA: modelAName,
                fieldA: fieldAName,
                foreignKeyA: relA.foreignKey,
                modelB: modelBName,
                fieldB: fieldBName,
                foreignKeyB: relB.foreignKey,
              },
            },
          );
        }

        const expectedB = EXPECTED_COUNTERPART[relA.relation];
        if (relB.relation !== expectedB) {
          throw new SchemaValidationError(
            `Bidirectional relation between '${modelAName}.${fieldAName}' (${relA.relation}) and '${modelBName}.${fieldBName}' (${relB.relation}) has inconsistent relation types. '${relA.relation}' must pair with '${expectedB}'.`,
            {
              model: modelAName,
              field: fieldAName,
              meta: {
                modelA: modelAName,
                fieldA: fieldAName,
                relationA: relA.relation,
                modelB: modelBName,
                fieldB: fieldBName,
                relationB: relB.relation,
                expectedRelationB: expectedB,
              },
            },
          );
        }
      }
      if (!foundCounterpart) {
        throw new SchemaValidationError(
          `Relation '${modelAName}.${fieldAName}' points to model '${modelBName}' but '${modelBName}' declares no corresponding relation back to '${modelAName}'. Both sides must be declared.`,
          {
            model: modelAName,
            field: fieldAName,
            meta: { declaringModel: modelAName, referencedModel: modelBName },
          },
        );
      }
    }
  }
}

// ---------------------------------------------------------------------------
// runStartupSchemaValidation — full startup entry point
// ---------------------------------------------------------------------------

/**
 * Full startup schema validation sequence.
 * Called by the client on every startup. Runs relationship validation first,
 * then applies the mismatch strategy against the disk schema.
 */
export function runStartupSchemaValidation(
  codeModels: Map<string, ParsedModelDefinition>,
  diskSchema: PersistedSchema | null,
  strategy: SchemaMismatchStrategy,
): void {
  validateRelationships(codeModels);
  validateSchema(codeModels, diskSchema, strategy);
}
