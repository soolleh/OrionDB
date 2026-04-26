// src/query/index.ts — barrel re-exports only, zero logic

export type {
  StringFilter,
  NumberFilter,
  BooleanFilter,
  DateFilter,
  EnumFilter,
  WhereInput,
  OrderByDirection,
  OrderByInput,
  SelectInput,
  PaginationInput,
  FindManyInput,
  FindFirstInput,
  FindUniqueInput,
  AggregateInput,
  AggregateResult,
  GroupByInput,
  GroupByResult,
  CompiledFilter,
  CompiledSort,
  LogicalOperator,
  ScalarOperator,
} from "./types.js";

export { LOGICAL_OPERATORS, SCALAR_OPERATORS, isLogicalOperator, isScalarOperator } from "./types.js";

export { compileFilter } from "./filter.js";
export { compileSort, applySort } from "./sort.js";
export { applyPagination, buildPaginationStrategy, getPageInfo } from "./pagination.js";
