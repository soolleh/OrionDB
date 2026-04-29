// src/schema/types.ts

// ---------------------------------------------------------------------------
// Primitive type aliases
// ---------------------------------------------------------------------------

export type ScalarFieldType = "string" | "number" | "boolean" | "date" | "json" | "enum";

export type RelationType = "one-to-one" | "one-to-many" | "many-to-one";

// ---------------------------------------------------------------------------
// Field definition discriminated union
// ---------------------------------------------------------------------------

export interface StringFieldDefinition {
  type: "string";
  primary?: boolean;
  unique?: boolean;
  required?: boolean;
  default?: string | (() => string);
}

export interface NumberFieldDefinition {
  type: "number";
  primary?: boolean;
  unique?: boolean;
  required?: boolean;
  default?: number | (() => number);
}

export interface BooleanFieldDefinition {
  type: "boolean";
  required?: boolean;
  default?: boolean | (() => boolean);
}

export interface DateFieldDefinition {
  type: "date";
  unique?: boolean;
  required?: boolean;
  default?: Date | string | (() => Date);
}

export interface JsonFieldDefinition {
  type: "json";
  required?: boolean;
  default?: Record<string, unknown> | (() => Record<string, unknown>);
}

export interface EnumFieldDefinition {
  type: "enum";
  values: string[];
  required?: boolean;
  default?: string | (() => string);
}

export interface RelationFieldDefinition {
  type: "relation";
  model: string;
  foreignKey: string;
  relation: RelationType;
}

export type FieldDefinition =
  | StringFieldDefinition
  | NumberFieldDefinition
  | BooleanFieldDefinition
  | DateFieldDefinition
  | JsonFieldDefinition
  | EnumFieldDefinition
  | RelationFieldDefinition;

// ---------------------------------------------------------------------------
// Schema input (user-facing)
// ---------------------------------------------------------------------------

export type SchemaInput = Record<string, FieldDefinition>;

/**
 * Public alias for a single model's field map.
 * Used to define the fields (and inline relations) for one model.
 */
export type ModelDefinition = SchemaInput;

/**
 * Public alias for a relation field definition.
 * The `type` field holds the relation cardinality (`'one-to-many'`, etc.).
 */
export type RelationDefinition = RelationFieldDefinition;

/**
 * The top-level schema passed to `createOrionDB`.
 * Maps model names to their field definitions.
 */
export type SchemaDefinition = Record<string, ModelDefinition>;

// ---------------------------------------------------------------------------
// Internal parsed representations
// ---------------------------------------------------------------------------

export interface ParsedScalarField {
  name: string;
  type: ScalarFieldType;
  primary: boolean;
  unique: boolean;
  required: boolean;
  hasDefault: boolean;
  defaultValue: unknown;
  enumValues: string[] | undefined;
}

export interface ParsedRelationField {
  name: string;
  type: "relation";
  model: string;
  foreignKey: string;
  relation: RelationType;
}

export type ParsedField = ParsedScalarField | ParsedRelationField;

export interface ParsedModelDefinition {
  name: string;
  fields: Map<string, ParsedField>;
  primaryKeyField: string;
  uniqueFields: Set<string>;
  indexedFields: Set<string>;
  relationFields: Map<string, ParsedRelationField>;
}

// ---------------------------------------------------------------------------
// Serializable schema — for _schema.json
// ---------------------------------------------------------------------------

export interface SerializableFieldDefinition {
  type: ScalarFieldType;
  primary?: boolean;
  unique?: boolean;
  required?: boolean;
  enumValues?: string[];
}

export interface SerializableRelationDefinition {
  model: string;
  foreignKey: string;
  relation: RelationType;
}

export interface SerializableModelDefinition {
  fields: Record<string, SerializableFieldDefinition>;
  relations: Record<string, SerializableRelationDefinition>;
}

export interface PersistedSchema {
  version: number;
  generatedAt: string;
  models: Record<string, SerializableModelDefinition>;
}

// ---------------------------------------------------------------------------
// Schema mismatch strategy
// ---------------------------------------------------------------------------

export type SchemaMismatchStrategy = "block" | "warn-and-continue" | "auto-migrate";

// ---------------------------------------------------------------------------
// FieldValue type alias (mirrors Index Manager — centralised here)
// ---------------------------------------------------------------------------

export type FieldValue = string | number | boolean | null;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const CURRENT_SCHEMA_VERSION: number = 1;

export const SYSTEM_FIELDS: readonly string[] = ["_deleted", "_createdAt", "_updatedAt"];

export const RESERVED_FILENAMES: readonly string[] = ["_schema.json", "_meta.json"];
