// src/schema/parser.ts

import { SchemaValidationError } from "../errors/index.js";
import { SYSTEM_FIELDS } from "./types.js";
import type {
  ScalarFieldType,
  RelationType,
  SchemaInput,
  ParsedScalarField,
  ParsedRelationField,
  ParsedField,
  ParsedModelDefinition,
  FieldDefinition,
  EnumFieldDefinition,
  RelationFieldDefinition,
  FieldValue,
} from "./types.js";

// ---------------------------------------------------------------------------
// Internal constants
// ---------------------------------------------------------------------------

// Model names map to directory names. Leading underscores are reserved for system files (_schema.json, _meta.json).
const MODEL_NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/;
const FIELD_NAME_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const SCALAR_PRIMARY_TYPES: ReadonlySet<ScalarFieldType> = new Set(["string", "number"]);
const UNIQUE_ALLOWED_TYPES: ReadonlySet<ScalarFieldType> = new Set(["string", "number", "date"]);

// ---------------------------------------------------------------------------
// isValidFieldValue
// ---------------------------------------------------------------------------

/**
 * Returns true if value is a valid FieldValue (string, number, boolean, or null).
 */
export function isValidFieldValue(value: unknown): value is FieldValue {
  return value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}

// ---------------------------------------------------------------------------
// parseModelSchema
// ---------------------------------------------------------------------------

/**
 * Parses and validates a raw SchemaInput object for a single model.
 * Returns a fully normalised ParsedModelDefinition on success.
 * Throws SchemaValidationError for any validation failure.
 */
export function parseModelSchema(modelName: string, input: SchemaInput): ParsedModelDefinition {
  // ── Model name validation ──────────────────────────────────────────────
  if (typeof modelName !== "string" || modelName.trim().length === 0) {
    throw new SchemaValidationError("Model name must be a non-empty string.", {
      model: modelName,
      meta: { modelName },
    });
  }
  if (!MODEL_NAME_PATTERN.test(modelName)) {
    throw new SchemaValidationError(
      `Model name "${modelName}" is invalid. Must start with a letter and contain only letters, digits, and underscores.`,
      { model: modelName, meta: { modelName } },
    );
  }

  // ── Field count ────────────────────────────────────────────────────────
  const fieldEntries = Object.entries(input);
  if (fieldEntries.length === 0) {
    throw new SchemaValidationError(`Model "${modelName}" must define at least one field.`, {
      model: modelName,
      meta: {},
    });
  }

  // ── Per-field validation — first pass ─────────────────────────────────
  const fields = new Map<string, ParsedField>();
  const uniqueFields = new Set<string>();
  const relationFields = new Map<string, ParsedRelationField>();
  let primaryKeyField: string | undefined;

  for (const [fieldName, definition] of fieldEntries) {
    // ── Reserved field names ─────────────────────────────────────────────
    if (SYSTEM_FIELDS.includes(fieldName)) {
      throw new SchemaValidationError(
        `Field name "${fieldName}" is reserved by the OrionDB system and cannot be used.`,
        { model: modelName, field: fieldName, meta: { fieldName } },
      );
    }

    // ── Field name format ────────────────────────────────────────────────
    if (!FIELD_NAME_PATTERN.test(fieldName)) {
      throw new SchemaValidationError(
        `Field name "${fieldName}" is invalid. Must start with a letter or underscore and contain only letters, digits, and underscores.`,
        { model: modelName, field: fieldName, meta: { fieldName } },
      );
    }

    // ── Dispatch by type ─────────────────────────────────────────────────
    if (definition.type === "relation") {
      const parsed = validateRelationField(modelName, fieldName, definition);
      fields.set(fieldName, parsed);
      relationFields.set(fieldName, parsed);
      continue;
    }

    const parsed = validateScalarField(modelName, fieldName, definition);

    if (parsed.primary) {
      if (primaryKeyField !== undefined) {
        throw new SchemaValidationError(
          `Model "${modelName}" has more than one primary key field ("${primaryKeyField}" and "${fieldName}"). Only one is allowed.`,
          { model: modelName, field: fieldName, meta: { firstPk: primaryKeyField, secondPk: fieldName } },
        );
      }
      primaryKeyField = fieldName;
    }

    if (parsed.unique) {
      uniqueFields.add(fieldName);
    }

    fields.set(fieldName, parsed);
  }

  // ── Primary key — must exist ───────────────────────────────────────────
  if (primaryKeyField === undefined) {
    throw new SchemaValidationError(
      `Model "${modelName}" has no primary key field. Declare exactly one field with primary: true.`,
      { model: modelName, meta: {} },
    );
  }

  const indexedFields = new Set(uniqueFields);
  indexedFields.add(primaryKeyField);

  return {
    name: modelName,
    fields,
    primaryKeyField,
    uniqueFields,
    indexedFields,
    relationFields,
  };
}

// ---------------------------------------------------------------------------
// Internal: validate a relation field
// ---------------------------------------------------------------------------

function validateRelationField(
  modelName: string,
  fieldName: string,
  def: RelationFieldDefinition,
): ParsedRelationField {
  if (typeof def.model !== "string" || def.model.trim().length === 0) {
    throw new SchemaValidationError(
      `Relation field "${fieldName}" on model "${modelName}" must have a non-empty "model" string.`,
      { model: modelName, field: fieldName, meta: { provided: def.model } },
    );
  }
  if (typeof def.foreignKey !== "string" || def.foreignKey.trim().length === 0) {
    throw new SchemaValidationError(
      `Relation field "${fieldName}" on model "${modelName}" must have a non-empty "foreignKey" string.`,
      { model: modelName, field: fieldName, meta: { provided: def.foreignKey } },
    );
  }
  const validRelationTypes: readonly RelationType[] = ["one-to-one", "one-to-many", "many-to-one"];
  if (!validRelationTypes.includes(def.relation)) {
    throw new SchemaValidationError(
      `Relation field "${fieldName}" on model "${modelName}" has invalid relation type "${String(def.relation)}". Must be one of: ${validRelationTypes.join(", ")}.`,
      { model: modelName, field: fieldName, meta: { provided: def.relation } },
    );
  }

  // primary, unique, required, default must not be present on relation fields
  const forbidden = ["primary", "unique", "required", "default"] as const;
  for (const key of forbidden) {
    if (key in def) {
      throw new SchemaValidationError(
        `Relation field "${fieldName}" on model "${modelName}" must not declare "${key}". Relation fields do not support scalar field options.`,
        { model: modelName, field: fieldName, meta: { forbiddenKey: key } },
      );
    }
  }

  return {
    name: fieldName,
    type: "relation",
    model: def.model,
    foreignKey: def.foreignKey,
    relation: def.relation,
  };
}

// ---------------------------------------------------------------------------
// Internal: validate a scalar field
// ---------------------------------------------------------------------------

function validateScalarField(
  modelName: string,
  fieldName: string,
  def: Exclude<FieldDefinition, RelationFieldDefinition>,
): ParsedScalarField {
  // ── required must be boolean ─────────────────────────────────────────
  if ("required" in def && def.required !== undefined && typeof def.required !== "boolean") {
    throw new SchemaValidationError(
      `Field "${fieldName}" on model "${modelName}" has an invalid "required" option. Must be boolean.`,
      { model: modelName, field: fieldName, meta: { provided: def.required } },
    );
  }

  // ── primary / unique constraints on incompatible types ───────────────
  const hasPrimary = "primary" in def && (def as { primary?: boolean }).primary === true;
  const hasUnique = "unique" in def && (def as { unique?: boolean }).unique === true;

  if (hasPrimary && !SCALAR_PRIMARY_TYPES.has(def.type)) {
    throw new SchemaValidationError(
      `Field "${fieldName}" on model "${modelName}" of type "${def.type}" cannot be a primary key. Only "string" and "number" fields may be primary.`,
      { model: modelName, field: fieldName, meta: { type: def.type } },
    );
  }

  if (hasUnique && !UNIQUE_ALLOWED_TYPES.has(def.type)) {
    throw new SchemaValidationError(
      `Field "${fieldName}" on model "${modelName}" of type "${def.type}" cannot have unique: true. Only "string", "number", and "date" fields support unique constraints.`,
      { model: modelName, field: fieldName, meta: { type: def.type } },
    );
  }

  // ── Type-specific default validation ─────────────────────────────────
  const hasDefault = "default" in def;
  const rawDefault = hasDefault ? (def as { default?: unknown }).default : undefined;

  if (hasDefault && rawDefault !== undefined && typeof rawDefault !== "function") {
    validateStaticDefault(modelName, fieldName, def, rawDefault);
  }

  // ── enum-specific: values array ───────────────────────────────────────
  let enumValues: string[] | undefined;
  if (def.type === "enum") {
    enumValues = validateEnumValues(modelName, fieldName, def, rawDefault);
  }

  return {
    name: fieldName,
    type: def.type,
    primary: hasPrimary,
    unique: hasUnique,
    required: (def as { required?: boolean }).required ?? false,
    hasDefault,
    defaultValue: rawDefault,
    enumValues,
  };
}

// ---------------------------------------------------------------------------
// Internal: validate a static default value for a scalar field
// ---------------------------------------------------------------------------

function validateStaticDefault(
  modelName: string,
  fieldName: string,
  def: Exclude<FieldDefinition, RelationFieldDefinition>,
  value: unknown,
): void {
  switch (def.type) {
    case "string": {
      if (typeof value !== "string") {
        throw new SchemaValidationError(`Field "${fieldName}" on model "${modelName}" default must be a string.`, {
          model: modelName,
          field: fieldName,
          meta: { provided: value },
        });
      }
      break;
    }
    case "number": {
      if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
        throw new SchemaValidationError(
          `Field "${fieldName}" on model "${modelName}" default must be a finite number (not NaN or Infinity).`,
          { model: modelName, field: fieldName, meta: { provided: value } },
        );
      }
      break;
    }
    case "boolean": {
      if (typeof value !== "boolean") {
        throw new SchemaValidationError(`Field "${fieldName}" on model "${modelName}" default must be a boolean.`, {
          model: modelName,
          field: fieldName,
          meta: { provided: value },
        });
      }
      break;
    }
    case "date": {
      const isValidDate =
        value instanceof Date
          ? !Number.isNaN(value.getTime())
          : typeof value === "string" && !Number.isNaN(new Date(value).getTime());
      if (!isValidDate) {
        throw new SchemaValidationError(
          `Field "${fieldName}" on model "${modelName}" default must be a Date instance or a valid ISO 8601 string.`,
          { model: modelName, field: fieldName, meta: { provided: value } },
        );
      }
      break;
    }
    case "json": {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new SchemaValidationError(
          `Field "${fieldName}" on model "${modelName}" default must be a plain object (not null, not an array, not a primitive).`,
          { model: modelName, field: fieldName, meta: { provided: value } },
        );
      }
      break;
    }
    case "enum": {
      // enum default validation is handled in validateEnumValues after the values array is checked
      break;
    }
  }
}

// ---------------------------------------------------------------------------
// Internal: validate enum field values array and default
// ---------------------------------------------------------------------------

function validateEnumValues(
  modelName: string,
  fieldName: string,
  def: EnumFieldDefinition,
  rawDefault: unknown,
): string[] {
  if (!Array.isArray(def.values) || def.values.length === 0) {
    throw new SchemaValidationError(
      `Enum field "${fieldName}" on model "${modelName}" must declare a non-empty "values" array.`,
      { model: modelName, field: fieldName, meta: { provided: def.values } },
    );
  }

  for (const entry of def.values) {
    if (typeof entry !== "string" || entry.length === 0) {
      throw new SchemaValidationError(
        `Enum field "${fieldName}" on model "${modelName}" has an invalid value: all entries must be non-empty strings.`,
        { model: modelName, field: fieldName, meta: { invalidEntry: entry } },
      );
    }
  }

  const seen = new Set<string>();
  for (const entry of def.values) {
    if (seen.has(entry)) {
      throw new SchemaValidationError(
        `Enum field "${fieldName}" on model "${modelName}" has duplicate value "${entry}".`,
        { model: modelName, field: fieldName, meta: { duplicate: entry } },
      );
    }
    seen.add(entry);
  }

  // Validate static default is one of the declared values
  if (rawDefault !== undefined && typeof rawDefault !== "function") {
    if (typeof rawDefault !== "string" || !def.values.includes(rawDefault)) {
      throw new SchemaValidationError(
        `Enum field "${fieldName}" on model "${modelName}" default value is not in the declared values list.`,
        { model: modelName, field: fieldName, meta: { provided: rawDefault, values: def.values } },
      );
    }
  }

  return def.values;
}
