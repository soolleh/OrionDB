// src/schema/index.ts — barrel re-exports only

export type {
  ScalarFieldType,
  RelationType,
  StringFieldDefinition,
  NumberFieldDefinition,
  BooleanFieldDefinition,
  DateFieldDefinition,
  JsonFieldDefinition,
  EnumFieldDefinition,
  RelationFieldDefinition,
  FieldDefinition,
  SchemaInput,
  ParsedScalarField,
  ParsedRelationField,
  ParsedField,
  ParsedModelDefinition,
  SerializableFieldDefinition,
  SerializableRelationDefinition,
  SerializableModelDefinition,
  PersistedSchema,
  SchemaMismatchStrategy,
  FieldValue,
} from "./types.js";

export { CURRENT_SCHEMA_VERSION, SYSTEM_FIELDS, RESERVED_FILENAMES } from "./types.js";

export { isValidFieldValue, parseModelSchema } from "./parser.js";

export { serializeSchema, deserializeSchema, writeSchemaFile, readSchemaFile } from "./serializer.js";

export type { ChangeKind, SchemaChange, SchemaDiff } from "./mismatch.js";
export { diffSchemas, applyMismatchStrategy, validateSchema } from "./mismatch.js";

export { validateRelationships, runStartupSchemaValidation } from "./relations.js";
