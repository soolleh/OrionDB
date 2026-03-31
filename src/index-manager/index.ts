// src/index-manager/index.ts

export type PrimaryKey = string | number;

export type FieldValue = string | number | boolean | null;

export interface IndexManagerOptions {
  primaryKeyField: string;
  indexedFields: Set<string>;
}

export type LogicalIndex = Map<string, Map<FieldValue, Set<PrimaryKey>>>;

export type PhysicalIndex = Map<PrimaryKey, number>;

export interface IndexManager<TRecord extends Record<string, unknown>> {
  add(record: TRecord, offset: number): void;

  update(oldRecord: TRecord, newRecord: TRecord, newOffset: number): void;

  delete(id: PrimaryKey): void;

  getOffset(id: PrimaryKey): number | undefined;

  getByField(field: string, value: FieldValue): Set<PrimaryKey> | undefined;

  clear(): void;

  rebuild(filePath: string): Promise<void>;

  has(id: PrimaryKey): boolean;

  size(): number;
}
