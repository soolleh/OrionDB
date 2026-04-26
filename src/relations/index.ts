// src/relations/index.ts — barrel: re-exports only, zero logic

export type {
  RelationType,
  ResolvedRelation,
  IncludeClause,
  IncludeValue,
  RelationDescriptor,
  RelationResolverContext,
  FindManyForResolver,
  IncludeResult,
  NestedCreateInput,
  NestedWriteOperation,
  IncludeAll,
} from "./types.js";

export { INCLUDE_ALL, isIncludeAll, isIncludeObject, buildRelationDescriptor } from "./types.js";

export { resolveIncludes, attachIncludes } from "./resolver.js";

export { extractNestedWrites, executeNestedWrites } from "./nested-writes.js";
