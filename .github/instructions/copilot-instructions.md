# OrionDB — Developer Instructions & Contributor Guide

> **Version:** 0.2.0-draft | **Phase:** 1 MVP | **Last Updated:** 2026-04-14
> This document is the single source of truth for how OrionDB is built, structured, tested, and extended. Every contributor and AI coding agent working on this codebase must read and follow this document completely.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Overview](#2-architecture-overview)
3. [File System & Project Structure](#3-file-system--project-structure)
4. [Technology Stack](#4-technology-stack)
5. [TypeScript Standards](#5-typescript-standards)
6. [Code Style & Formatting](#6-code-style--formatting)
7. [Naming Conventions](#7-naming-conventions)
8. [Module & Import Rules](#8-module--import-rules)
9. [Error Handling Standards](#9-error-handling-standards)
10. [Testing Standards](#10-testing-standards)
11. [Feature Implementation Sequence](#11-feature-implementation-sequence)
12. [Index Manager — Deep Spec](#12-index-manager--deep-spec)
13. [Schema System — Deep Spec](#13-schema-system--deep-spec)
14. [Persistence Layer — Deep Spec](#14-persistence-layer--deep-spec)
15. [Query Engine — Deep Spec](#15-query-engine--deep-spec)
16. [Relationship Resolver — Deep Spec](#16-relationship-resolver--deep-spec)
17. [Compaction — Deep Spec](#17-compaction--deep-spec)
18. [Public API & Barrel Exports](#18-public-api--barrel-exports)
19. [Performance Constraints](#19-performance-constraints)
20. [Crash Safety & Canonical State Rules](#20-crash-safety--canonical-state-rules)
21. [Phase 1 Non-Goals](#21-phase-1-non-goals)
22. [Git & Branching Strategy](#22-git--branching-strategy)
23. [Definition of Done](#23-definition-of-done)

---

## 1. Project Overview

### 1.1 What OrionDB Is

OrionDB is a **TypeScript-first, zero-config, file-based relational database** with a Prisma-like API. It is an **embedded, single-process** database — no server, no native bindings, no setup step.

```
npm install oriondb
```

That is the entire setup. Data is stored in human-readable NDJSON files on the local file system.

### 1.2 Positioning

| Claim | Meaning |
|---|---|
| Zero-config | No connection strings, no server, no CLI setup |
| File-based | Data lives in inspectable, editable, git-committable files |
| Prisma-like API | `findMany`, `create`, `include`, `where` — same mental model |
| No native bindings | Installs cleanly on any platform, any CI, any serverless environment |
| TypeScript-first | Full type inference from schema definition — no codegen step |

### 1.3 Target Users

- TypeScript developers building **local-first applications**
- CLI tools that need structured relational persistence
- Electron / Tauri desktop applications
- Offline-first web applications
- Developer tools and internal tooling
- Rapid prototyping with a real query API
- Single-user or low-concurrency applications

### 1.4 Core Promises (Never Break These)

1. `npm install oriondb` — zero additional tooling, zero native compilation
2. Data is always stored in human-readable NDJSON
3. API mirrors Prisma — any Prisma-familiar developer is immediately productive
4. Relational data modelling with schema validation out of the box
5. No code generation step — all types inferred at definition time

---

## 2. Architecture Overview

### 2.1 Layer Model

```
┌─────────────────────────────────────┐
│           Public API Layer          │  ← db.user.findMany(), db.post.create()
├─────────────────────────────────────┤
│           Query Engine              │  ← filter, sort, paginate, aggregate
├─────────────────────────────────────┤
│         Relationship Resolver       │  ← include, nested writes, batched resolution
├─────────────────────────────────────┤
│         Schema & Validator          │  ← type checking, constraint enforcement
├─────────────────────────────────────┤
│         Index Manager               │  ← logical index + physical index (per model)
├─────────────────────────────────────┤
│         Persistence Layer           │  ← NDJSON read/write, offset tracking, compaction
└─────────────────────────────────────┘
              File System
```

### 2.2 Architectural Principles

| Principle | Implementation |
|---|---|
| Append-only writes | Records are never overwritten in-place — always appended |
| Tombstone deletes | Deleted records get a new line with `_deleted: true` appended |
| In-memory indexes | Always rebuilt from disk on startup — never persisted |
| Last-occurrence-wins | During index rebuild, the last line for any ID is canonical |
| Async-first | All public API methods return Promises |
| Single-process | No file locking, no WAL in Phase 1 |
| Typed errors | Always throw typed error class instances — never result objects, never raw `Error` |

### 2.3 Key Architectural Decisions (All Locked for Phase 1)

| Decision | Choice | Rationale |
|---|---|---|
| Storage format | NDJSON (one record per line) | Streamable, appendable, human-readable |
| Write strategy | Append-only with tombstone deletes | O(1) writes, crash-safe, no full rewrites |
| Index strategy | In-memory, rebuilt on startup | No stale index risk, no index file corruption |
| Index scope | Logical (field→value→ids) + Physical (id→byteOffset) | O(1) lookups end-to-end for indexed fields |
| ID generation | CUID2 by default | Sortable, collision-resistant, performant index locality |
| API style | Async (Promise-based) | Non-blocking, Prisma-consistent |
| Relationship storage | Foreign keys in child records | Standard relational model, no join tables needed |
| Index scoping | Per-model instances | Clean API, filesystem boundary alignment, memory isolation |

---

## 3. File System & Project Structure

### 3.1 Database File Layout (Runtime)

```
<dbLocation>/
├── _schema.json          ← persisted schema snapshot (auto-managed, never hand-edited)
├── _meta.json            ← db version, created timestamp, oriondb version
└── <ModelName>/          ← one folder per model — exact model name, case-preserved
    ├── data.ndjson       ← append-only record store
    └── meta.json         ← record count, tombstone count, last compacted timestamp
```

**Rules:**
- Model folder names are **case-preserved** exactly as defined in code
- `data.ndjson` is always the primary data file — never renamed by users
- `_schema.json` and `_meta.json` are reserved filenames

### 3.2 Source Code Structure

```
oriondb/
├── src/
│   ├── client/
│   │   └── index.ts           ← createClient, db instance, model proxy
│   ├── schema/
│   │   └── index.ts           ← schema definition, validation, mismatch detection
│   ├── query/
│   │   └── index.ts           ← filter engine, orderBy, pagination, aggregations
│   ├── persistence/
│   │   └── index.ts           ← NDJSON read/write, file size counter, startup scan
│   ├── index-manager/
│   │   └── index.ts           ← IndexManager class, logical + physical indexes
│   ├── relations/
│   │   └── index.ts           ← relationship resolver, batched include, nested writes
│   ├── errors/
│   │   └── index.ts           ← full error hierarchy
│   └── index.ts               ← public barrel export
├── tests/
│   └── unit/
│       ├── errors/
│       │   └── errors.test.ts
│       ├── index-manager/
│       │   └── index-manager.test.ts
│       ├── schema/
│       │   └── schema.test.ts
│       ├── persistence/
│       │   └── persistence.test.ts
│       ├── query/
│       │   └── query.test.ts
│       ├── relations/
│       │   └── relations.test.ts
│       └── placeholder.test.ts
├── dist/                      ← build output (never commit)
├── instructions.md            ← this file
├── README.md
├── package.json
├── tsconfig.json
├── tsdown.config.ts           ← build config
├── vitest.config.ts
├── eslint.config.ts
└── .prettierrc
```

### 3.3 NDJSON Record Format

Every line in `data.ndjson` is a complete, self-contained JSON object:

```json
{"id":"clx1a2b3c","name":"Alice","email":"alice@example.com","_deleted":false,"_createdAt":"2024-01-01T00:00:00.000Z","_updatedAt":"2024-01-01T00:00:00.000Z"}
```

**Reserved system fields (always present, not exposed to users):**

| Field | Type | Description |
|---|---|---|
| `_deleted` | `boolean` | Tombstone flag. `true` = record is soft-deleted |
| `_createdAt` | ISO 8601 string | Timestamp of original insert |
| `_updatedAt` | ISO 8601 string | Timestamp of last mutation |

System fields are **stripped from all query results** unless the caller explicitly requests raw internal access (internal use only).

---

## 4. Technology Stack

### 4.1 Runtime

| Tool | Version | Role |
|---|---|---|
| Node.js | 18+ | Runtime target |
| TypeScript | 5.4 | Language |
| `@paralleldrive/cuid2` | latest | ID generation — only permitted runtime dependency with any external logic |

### 4.2 Build

| Tool | Role |
|---|---|
| `tsdown` | ESM + CJS dual build |
| `tsconfig.json` | Strict TypeScript config |

### 4.3 Testing

| Tool | Role |
|---|---|
| `vitest` | Test runner |
| `@vitest/coverage-v8` | Code coverage (80% minimum) |

### 4.4 Code Quality

| Tool | Role |
|---|---|
| ESLint | Linting |
| Prettier | Formatting |

### 4.5 Module Format

- **Primary:** ESM (`"type": "module"` in `package.json`)
- **Secondary:** CJS (dual build via tsdown)
- All internal imports use `.js` extension (ESM-compatible Node resolution)

### 4.6 Dependency Policy

- **Zero** runtime dependencies with native bindings — ever
- `@paralleldrive/cuid2` is the **only** justified runtime dependency
- All other dependencies must be `devDependencies`
- Never add a runtime dependency without explicit justification against this policy

---

## 5. TypeScript Standards

### 5.1 Compiler Settings (Non-Negotiable)

```json
{
  "compilerOptions": {
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,
    "noImplicitOverride": true,
    "forceConsistentCasingInFileNames": true,
    "moduleResolution": "bundler",
    "target": "ES2022",
    "module": "ESNext",
    "lib": ["ES2022"],
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true
  }
}
```

### 5.2 Type Safety Rules

- **Never use `any`** — use `unknown` and narrow with type guards
- **Never use non-null assertion (`!`)** — prove non-null through control flow
- **Never use type casting (`as T`)** unless inside a type guard function that validates the cast
- **No `@ts-ignore` or `@ts-expect-error`** without a comment explaining why it is unavoidable
- All function parameters and return types must be **explicitly typed** for public functions and class methods
- Internal helper functions may rely on inference if the inferred type is obvious and non-ambiguous
- Use `satisfies` operator to validate shapes without widening types

### 5.3 Type Design Patterns

**Prefer type aliases over interfaces for unions and mapped types:**
```ts
// Good
type FieldValue = string | number | boolean | null
type PrimaryKey = string | number

// Good — use interface for object shapes that are implemented or extended
interface IndexManager<TRecord extends Record<string, unknown>> {
  add(record: TRecord, offset: number): void
}
```

**Use discriminated unions for variant types:**
```ts
type FieldDefinition =
  | { type: 'string'; unique?: boolean; required?: boolean; default?: string | (() => string) }
  | { type: 'number'; unique?: boolean; required?: boolean; default?: number | (() => number) }
  | { type: 'enum'; values: string[]; default?: string }
  | { type: 'relation'; model: string; foreignKey: string; relation: RelationType }
```

**Use `satisfies` for validated config objects:**
```ts
const defaultConfig = {
  schemaMismatchStrategy: 'block',
  strict: false,
  autoCompact: true,
  autoCompactThreshold: 30,
} satisfies Partial<OrionDBConfig>
```

**Use generics with constraints — never raw generics:**
```ts
// Bad
function process<T>(record: T): T

// Good
function process<TRecord extends Record<string, unknown>>(record: TRecord): TRecord
```

### 5.4 Type Inference for Public API

The public API must provide full type inference:
- `db.user.create({ data: { ... } })` → return type reflects model schema
- `db.user.findMany({ select: { name: true } })` → return type includes only `name`
- `db.user.findMany({ include: { posts: true } })` → return type includes `posts` array
- `where` clause types are field-aware — invalid field names are compile-time errors
- No code generation step — all types derived via TypeScript mapped types and conditional types

---

## 6. Code Style & Formatting

### 6.1 Prettier Config

```json
{
  "semi": false,
  "singleQuote": true,
  "trailingComma": "all",
  "printWidth": 100,
  "tabWidth": 2,
  "arrowParens": "always"
}
```

### 6.2 ESLint Rules (Key)

```ts
// eslint.config.ts — key rules
{
  '@typescript-eslint/no-explicit-any': 'error',
  '@typescript-eslint/no-non-null-assertion': 'error',
  '@typescript-eslint/explicit-function-return-type': 'warn',
  '@typescript-eslint/consistent-type-imports': 'error',
  'no-console': ['warn', { allow: ['warn', 'error'] }],
  'prefer-const': 'error',
  'no-var': 'error',
}
```

### 6.3 General Code Rules

- **`const` by default.** Use `let` only when reassignment is necessary. Never `var`.
- **No magic numbers.** Extract all numeric literals into named constants.
- **No magic strings.** Error codes, field names, and file names are constants.
- **Early returns** over deeply nested conditionals.
- **Guard clauses first** — validate inputs at the top of functions, not buried inside.
- **No commented-out code** in committed code — delete it or put it in a TODO with a ticket reference.
- **One export per concept** — don't bundle unrelated logic in one file.
- **Avoid side effects at module level** — no code that runs on import except type definitions and constants.
- **Functions do one thing.** If a function has more than one clear responsibility, split it.
- **Function length:** No hard limit, but functions longer than ~60 lines are a signal to decompose.
- **Avoid abbreviations** in names unless they are universal (e.g., `id`, `db`, `fs`, `ctx`).

### 6.4 Async / Await Rules

- Always use `async/await` over raw `.then()/.catch()` chains
- Always `await` Promises — never fire-and-forget unless explicitly documented
- All async functions must have explicit `Promise<T>` return types on public interfaces
- Never mix `async/await` and `.then()` in the same function

### 6.5 Comments

- **JSDoc** for all public-facing types, interfaces, and methods
- **Inline comments** only when the code is genuinely non-obvious — not to narrate what the code does
- **`// TODO:`** comments must include context on what needs doing and why
- Never write comments that just repeat the code:

```ts
// Bad: increments the counter
counter++

// Good: only needed if non-obvious
// +1 for the newline character that appendFile writes after the serialized record
offset += Buffer.byteLength(serializedLine, 'utf8') + 1
```

---

## 7. Naming Conventions

### 7.1 General

| Construct | Convention | Example |
|---|---|---|
| Variables | `camelCase` | `byteOffset`, `recordCount` |
| Functions | `camelCase` | `getOffset`, `rebuildIndex` |
| Classes | `PascalCase` | `IndexManager`, `OrionDBError` |
| Interfaces | `PascalCase` | `IndexManagerOptions`, `OrionDBConfig` |
| Type aliases | `PascalCase` | `PrimaryKey`, `FieldValue`, `FieldDefinition` |
| Enums | `PascalCase` (name) + `SCREAMING_SNAKE` (values) | `ErrorCode.VALIDATION_ERROR` |
| Constants | `SCREAMING_SNAKE_CASE` | `DEFAULT_COMPACT_THRESHOLD`, `SYSTEM_FIELDS` |
| Files | `kebab-case` | `index-manager.ts`, `persistence.ts` |
| Test files | mirror source with `.test.ts` suffix | `index-manager.test.ts` |
| Generic type params | `T` prefix or descriptive: `TRecord`, `TSchema` | `TRecord extends Record<string, unknown>` |

### 7.2 Domain-Specific Names

Always use these exact terms — do not invent synonyms:

| Concept | Canonical Name |
|---|---|
| The primary in-memory lookup | `logicalIndex` |
| The byte-offset lookup | `physicalIndex` |
| Soft-deleted record | `tombstone` |
| Record stored in NDJSON | `record` (not `row`, `entry`, `document`) |
| Database root folder | `dbLocation` |
| One model's folder | `modelDir` |
| The append file | `data.ndjson` |
| Schema on disk | `_schema.json` |
| Record unique identifier | `id` or `primaryKey` |
| Field-to-value-to-ids map | `logicalIndex` |
| Id-to-byteOffset map | `physicalIndex` |

---

## 8. Module & Import Rules

### 8.1 Import Order

Enforce this order with ESLint `import/order`:

1. Node.js built-ins (`node:fs`, `node:path`, `node:readline`)
2. External packages (`@paralleldrive/cuid2`)
3. Internal absolute imports (`oriondb/errors`)
4. Internal relative imports (`../errors/index.js`)

Always use `node:` prefix for Node.js built-ins:

```ts
// Good
import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'

// Bad
import { createReadStream } from 'fs'
```

### 8.2 ESM Import Extensions

All relative imports must use `.js` extension (required for Node ESM):

```ts
// Good
import { OrionDBError } from '../errors/index.js'

// Bad
import { OrionDBError } from '../errors'
import { OrionDBError } from '../errors/index'
```

### 8.3 Type Imports

Always use `import type` for type-only imports:

```ts
import type { IndexManagerOptions, PrimaryKey } from './types.js'
import { IndexManagerImpl } from './impl.js'
```

### 8.4 Barrel Export Rules

- `src/index.ts` is the **only** public barrel
- Internal modules never import from `src/index.ts` — always import directly from the source module
- The barrel exports exactly what the requirements define as public API — nothing more

---

## 9. Error Handling Standards

### 9.1 Error Hierarchy (Implemented — Do Not Change)

```
OrionDBError (base class)
├── ValidationError           (VALIDATION_ERROR)
├── UniqueConstraintError     (UNIQUE_CONSTRAINT_VIOLATION)
├── RecordNotFoundError       (RECORD_NOT_FOUND)
├── QueryError                (INVALID_QUERY)
├── SchemaError (abstract)
│   ├── SchemaMismatchError   (SCHEMA_MISMATCH)
│   └── SchemaValidationError (SCHEMA_VALIDATION_ERROR)
├── RelationError             (RELATION_ERROR)
└── CompactionError           (COMPACTION_ERROR)
```

### 9.2 OrionDBError Shape

```ts
interface OrionDBErrorOptions {
  model?: string
  field?: string
  meta?: unknown
}

class OrionDBError extends Error {
  code: string
  model?: string
  field?: string
  meta?: unknown
}
```

### 9.3 Error Throwing Rules

- **Always throw a typed error class instance** — never `throw new Error('...')`
- **Always include the `model` field** when the error is model-specific
- **Always include the `field` field** when the error is field-specific
- **Always include `meta`** for structured diagnostic data the caller can inspect programmatically
- Never throw inside a `finally` block
- Never swallow errors silently — if you catch, rethrow or log + rethrow

```ts
// Bad
throw new Error('Record not found')

// Good
throw new RecordNotFoundError(`No record found with id: ${id}`, {
  model: this.modelName,
  meta: { id },
})
```

### 9.4 Error Handling in Async Functions

```ts
// Bad — unhandled rejection
async function doSomething() {
  const result = await riskyOperation() // may reject — not caught
  return result
}

// Good
async function doSomething(): Promise<Result> {
  try {
    const result = await riskyOperation()
    return result
  } catch (error) {
    if (error instanceof OrionDBError) throw error // let typed errors propagate
    throw new CompactionError('Unexpected failure during compaction', {
      meta: { cause: error },
    })
  }
}
```

### 9.5 `console` Usage

- `console.warn` — allowed for non-fatal warnings (e.g., malformed final NDJSON line on startup)
- `console.error` — allowed for fatal/unexpected errors before rethrowing
- `console.log` — **never** in library code (use warn/error only)
- `console.debug` — only behind a debug flag, never unconditional

---

## 10. Testing Standards

### 10.1 Framework

- **Vitest** — all tests
- **No external mocking libraries** — use Vitest's built-in `vi.fn()`, `vi.spyOn()`, `vi.mock()`
- **No snapshot tests** for core logic — explicit assertions only
- Coverage via `@vitest/coverage-v8`

### 10.2 Coverage Requirements

| Metric | Minimum |
|---|---|
| Statements | 80% |
| Branches | 80% |
| Functions | 80% |
| Lines | 80% |

Critical modules (errors, index-manager, persistence) should target **95%+**.

### 10.3 Test File Structure

```ts
// tests/unit/index-manager/index-manager.test.ts

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { IndexManagerImpl } from '../../../src/index-manager/index.js'

describe('IndexManager', () => {
  describe('add()', () => {
    it('adds a record to the logical index for indexed fields', () => { ... })
    it('adds a record to the physical index', () => { ... })
    it('uses upsert behavior — overwrites existing entry for same id', () => { ... })
    it('does not add non-indexed fields to the logical index', () => { ... })
  })

  describe('update()', () => {
    it('removes old logical index entries for changed fields', () => { ... })
    it('adds new logical index entries after update', () => { ... })
    it('updates physical index to new offset', () => { ... })
  })

  describe('delete()', () => {
    it('removes entry from logical index', () => { ... })
    it('removes entry from physical index', () => { ... })
    it('is a no-op if id does not exist', () => { ... })
  })

  describe('getOffset()', () => {
    it('returns byte offset for known id', () => { ... })
    it('returns undefined for unknown id', () => { ... })
  })

  describe('getByField()', () => {
    it('returns set of primary keys for indexed field', () => { ... })
    it('returns undefined for non-indexed field', () => { ... })
    it('returns undefined for unknown value', () => { ... })
  })

  describe('rebuild()', () => {
    it('clears all existing index data before rebuilding', () => { ... })
    it('correctly indexes non-deleted records', () => { ... })
    it('removes tombstoned records from both indexes', () => { ... })
    it('applies last-occurrence-wins for duplicate ids', () => { ... })
    it('tracks byte offsets correctly', () => { ... })
    it('discards malformed final line with console.warn', () => { ... })
  })
})
```

### 10.4 Test Design Principles

- **One assertion per test where possible** — multiple related assertions are acceptable if they test one behavior
- **Descriptive test names** — the test name should read as a specification: _"returns undefined for unknown id"_
- **Arrange-Act-Assert pattern** — explicit setup, one action, one or more assertions
- **No test interdependence** — each test must be independently runnable
- **Use `beforeEach`** to reset shared state, never rely on test execution order
- **Test the contract, not the implementation** — test observable behavior, not internal data structures directly (unless the internal state is the point of the test, like with IndexManager)
- **Test edge cases explicitly:**
  - Empty inputs
  - Single-item inputs
  - Boundary values (0, 1, max)
  - Deleted records
  - Malformed input
  - Duplicate IDs

### 10.5 File System Tests

For tests that require actual file I/O (persistence, rebuild):
- Use `os.tmpdir()` + a unique subdirectory per test run
- Clean up in `afterEach` using `fs.rm(dir, { recursive: true, force: true })`
- Never use hardcoded paths
- Never write test files to the source directory

```ts
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

let testDir: string

beforeEach(() => {
  testDir = mkdtempSync(join(tmpdir(), 'oriondb-test-'))
})

afterEach(() => {
  rmSync(testDir, { recursive: true, force: true })
})
```

### 10.6 Running Tests

```bash
# Run all tests
pnpm test

# Run with coverage
pnpm test --coverage

# Run specific file
pnpm test tests/unit/index-manager/index-manager.test.ts

# Watch mode
pnpm test --watch
```

---

## 11. Feature Implementation Sequence

Features must be implemented **in this exact order**. Do not begin a feature until the previous feature's tests are fully passing.

| # | Feature | Status | Branch |
|---|---|---|---|
| 1 | Error System | ✅ Done | merged |
| 2 | Index Manager | 🔄 In Progress | `feature/index-manager` |
| 3 | Schema Definition & Validation | ⬜ Pending | `feature/schema` |
| 4 | Persistence Write (create, createMany) | ⬜ Pending | `feature/persistence-write` |
| 5 | Persistence Read (findUnique, findFirst, findMany) | ⬜ Pending | `feature/persistence-read` |
| 6 | Persistence Update (update, updateMany) | ⬜ Pending | `feature/persistence-update` |
| 7 | Persistence Delete (delete, deleteMany) | ⬜ Pending | `feature/persistence-delete` |
| 8 | Query Filtering (where, operators) | ⬜ Pending | `feature/query-filter` |
| 9 | Sorting & Pagination (orderBy, skip, take) | ⬜ Pending | `feature/sort-pagination` |
| 10 | Aggregations (count, aggregate, groupBy) | ⬜ Pending | `feature/aggregations` |
| 11 | Relationships (include, nested writes) | ⬜ Pending | `feature/relations` |
| 12 | Compaction | ⬜ Pending | `feature/compaction` |
| 13 | Client & createClient | ⬜ Pending | `feature/client` |
| 14 | Public API & Barrel Exports | ⬜ Pending | `feature/public-api` |

### 11.1 Definition of Done (Per Feature)

A feature is **done** when:
1. Implementation is complete and matches the spec in this document
2. All tests for the feature pass with zero failures
3. Coverage for the feature's module is ≥ 80% (target 95% for core modules)
4. No TypeScript errors (`pnpm typecheck`)
5. No ESLint errors (`pnpm lint`)
6. Code is formatted (`pnpm format`)
7. PR is reviewed and merged to `main`

---

## 12. Index Manager — Deep Spec

### 12.1 Purpose

The Index Manager is the core lookup acceleration layer. It maintains two in-memory structures per model:

- **Logical Index:** "Which record IDs match this field value?" → `Map<fieldName, Map<fieldValue, Set<primaryKey>>>`
- **Physical Index:** "Where in the file is this record?" → `Map<primaryKey, byteOffset>`

### 12.2 Types

```ts
// src/index-manager/index.ts

export type PrimaryKey = string | number
export type FieldValue = string | number | boolean | null

export interface IndexManagerOptions {
  primaryKeyField: string           // which field is the PK (e.g. 'id')
  indexedFields: Set<string>        // only these fields enter the logical index
}

export interface IndexManager<TRecord extends Record<string, unknown>> {
  add(record: TRecord, offset: number): void
  update(oldRecord: TRecord, newRecord: TRecord, newOffset: number): void
  delete(id: PrimaryKey): void
  getOffset(id: PrimaryKey): number | undefined
  getByField(field: string, value: FieldValue): Set<PrimaryKey> | undefined
  clear(): void
  rebuild(filePath: string): Promise<void>
  has(id: PrimaryKey): boolean
  size(): number
}
```

### 12.3 Concrete Class

```ts
export class IndexManagerImpl<TRecord extends Record<string, unknown>>
  implements IndexManager<TRecord>
```

### 12.4 Internal State

```ts
private logicalIndex: Map<string, Map<FieldValue, Set<PrimaryKey>>>
private physicalIndex: Map<PrimaryKey, number>
private readonly options: IndexManagerOptions
```

### 12.5 Method Contracts

#### `add(record, offset)`

- Extracts `id` from `record[options.primaryKeyField]`
- For each field in `options.indexedFields`: update `logicalIndex[field][value]` to include this id
- Update `physicalIndex[id] = offset`
- **Upsert behavior:** if id already exists, overwrite both indexes (required for rebuild)
- PK field is implicitly indexed in physical index always
- PK field is NOT added to logical index (physical index covers PK lookups)

#### `update(oldRecord, newRecord, newOffset)`

- Extract `id` from `newRecord[options.primaryKeyField]`
- For each field in `options.indexedFields`:
  - Remove `id` from `logicalIndex[field][oldValue]`
  - If the old value's Set is now empty, delete the Set from the Map
  - Add `id` to `logicalIndex[field][newValue]`
- Update `physicalIndex[id] = newOffset`
- Order: logical first, physical last

#### `delete(id)`

- For each field in `options.indexedFields`:
  - Find the value of that field for this id — this requires either the record or scanning
  - **Implementation note:** store record snapshot or iterate logical index
  - Remove `id` from the relevant Set
  - If Set is now empty, delete it from the Map
- Delete `physicalIndex[id]`
- No-op if `id` does not exist

> **Implementation note for delete:** The logical index structure (field→value→ids) makes reverse lookup (id→values) non-trivial. Two recommended approaches:
> 1. Keep a reverse map: `Map<PrimaryKey, Map<fieldName, FieldValue>>` — most efficient
> 2. Scan logical index during delete — acceptable at Phase 1 dataset sizes

#### `getOffset(id)`

- Return `physicalIndex.get(id)` or `undefined`

#### `getByField(field, value)`

- If field not in `options.indexedFields`, return `undefined`
- Return `logicalIndex.get(field)?.get(value)` or `undefined`

#### `clear()`

- `logicalIndex.clear()`
- `physicalIndex.clear()`

#### `rebuild(filePath)`

1. `this.clear()`
2. Open file with `node:readline` createInterface
3. Track `currentOffset = 0`
4. For each line:
   a. Try `JSON.parse(line)`
   b. On parse failure: check if this is the **last line** — if yes, `console.warn` and discard; if not the last line, throw `ValidationError`
   c. Extract id and `_deleted` flag
   d. If `_deleted === true`: call `this.delete(id)` (removes from both indexes if present)
   e. If `_deleted === false`: call `this.add(record, currentOffset)` (upsert — last occurrence wins)
   f. Increment: `currentOffset += Buffer.byteLength(line, 'utf8') + 1`
5. Resolve promise

> **Critical:** The byte offset recorded for a record is the offset of the **start** of its line, not after. Step (f) increments **after** recording the offset in step (e).

#### `has(id)`

- Return `physicalIndex.has(id)`

#### `size()`

- Return `physicalIndex.size`

### 12.6 Index Update Ordering Rule

This order is **mandatory** in all write operations:

1. Validate inputs first — no index mutation until validation passes
2. Update **logical index** first
3. Update **physical index** last

Rationale: physical index inconsistency (wrong offset) causes corrupted reads, which is more dangerous than temporarily stale logical index entries.

### 12.7 Test Coverage Requirements for Index Manager

Every method must have tests for:
- Normal operation
- Edge case: empty state
- Edge case: non-existent id / field
- Edge case: tombstone handling (rebuild)
- Edge case: last-occurrence-wins (rebuild with duplicates)
- Edge case: malformed final line (rebuild)
- Edge case: non-indexed field ignored in logical index

---

## 13. Schema System — Deep Spec

### 13.1 Schema Definition API

```ts
const db = await createClient({ location: './mydb' })

const User = db.model('User', {
  id:     { type: 'string', primary: true, default: () => createId() },
  name:   { type: 'string' },
  email:  { type: 'string', unique: true },
  status: { type: 'enum', values: ['active', 'inactive'], default: 'active' },
  posts:  { type: 'relation', model: 'Post', foreignKey: 'authorId', relation: 'one-to-many' }
})
```

### 13.2 Field Types

| Type | TS Type | Storage | Notes |
|---|---|---|---|
| `string` | `string` | JSON string | |
| `number` | `number` | JSON number | Integers and floats unified |
| `boolean` | `boolean` | JSON boolean | |
| `date` | `Date` | ISO 8601 string | Input: Date or ISO string. Output: Date object. |
| `json` | `Record<string, unknown>` | JSON object | No deep validation |
| `enum` | `string` (union) | JSON string | Validated against declared values |

### 13.3 Primary Key Rules

- Every model **must** have exactly one `primary: true` field
- PK field is engine-managed (not processed by general default logic)
- Duplicate PK on insert → `UniqueConstraintError`
- Missing PK with no default → `ValidationError`

### 13.4 Schema Mismatch Detection

Comparison on startup between `_schema.json` (disk) and code-defined schema.

**Compared:** field names, field types, enum values, unique constraints, primary key designation, relationship definitions  
**NOT compared:** default values, field ordering

| Strategy | Behavior |
|---|---|
| `block` (default) | Throw `SchemaMismatchError` with full diff |
| `warn-and-continue` | Log warning, proceed with code schema |
| `auto-migrate` | Apply additive changes; destructive changes throw |

**Safe additive changes:** new optional field with default, new model, new enum value  
**Always destructive:** removing a field, changing a field type, removing enum value, changing PK field

### 13.5 Relationship Validation (Always Hard Error)

Validated before `schemaMismatchStrategy` is applied:
- Model A declares relation to Model B → B must exist
- `foreignKey: 'authorId'` → `authorId` must be a field on the referenced model
- Bidirectional relation must be declared consistently on both sides

---

## 14. Persistence Layer — Deep Spec

### 14.1 Write Path (create)

```
1.  Validate record against schema
2.  Apply defaults for missing optional fields
3.  Check unique constraints via logical index
4.  Serialize: JSON.stringify(record) + '\n'
5.  Get currentOffset from in-memory file size counter (NEVER fs.stat)
6.  fs.appendFile(filePath, serializedLine)
7.  Update logical index (indexed fields)
8.  Update physical index (id → currentOffset)
9.  Increment counter: counter += Buffer.byteLength(serializedLine, 'utf8')
10. Update meta.json counters
11. Return record to caller
```

**Critical:** Step 5 uses the in-memory counter, not a fresh `fs.stat`. Counter is initialized on startup from actual file size and kept in sync through all write operations.

### 14.2 Read Path — Indexed Lookup (findUnique by PK or unique field)

```
1. Logical index lookup → primary key       O(1)
2. Physical index lookup → byte offset      O(1)
3. fs.open(filePath)
4. fs.read() at exact byte offset           O(1)
5. JSON.parse single line
6. Strip system fields
7. Return record
```

### 14.3 Read Path — Full Scan (findMany with non-indexed filter)

```
1. readline stream over data.ndjson
2. Skip lines where _deleted === true
3. Apply where filter to each record
4. Collect matches — early exit when take is satisfied
5. Apply orderBy
6. Apply skip + take
7. Resolve includes via batched resolver
8. Return results
```

### 14.4 Delete Path

```
1. Validate record exists via physical index
2. Read current record via direct file read (physical index offset)
3. Create tombstone: { ...record, _deleted: true, _updatedAt: now }
4. fs.appendFile(filePath, JSON.stringify(tombstone) + '\n')
5. Remove from logical index
6. Remove from physical index
7. Increment tombstone counter in meta.json
```

### 14.5 File Size Counter

- Initialized on startup from `(await fs.stat(filePath)).size`
- Incremented after every append by `Buffer.byteLength(line, 'utf8')`
- Reset after compaction from new file's actual size
- **Never call `fs.stat` on the hot write path**

### 14.6 System Field Handling

- System fields (`_deleted`, `_createdAt`, `_updatedAt`) are always written to disk
- System fields are **always stripped** before returning records to the caller
- System fields are validated on read in `strict` mode only

---

## 15. Query Engine — Deep Spec

### 15.1 Supported Operations

| Operation | Description |
|---|---|
| `create` | Validate → defaults → unique check → write → return record |
| `createMany` | Validate all → write all atomically → return `{ count }` |
| `findUnique` | Index lookup → direct read → return record or null |
| `findUniqueOrThrow` | Same but throws `RecordNotFoundError` |
| `findFirst` | Scan → first match → return record or null |
| `findMany` | Scan → filter → sort → paginate → return array |
| `update` | Find → validate → append new record → update indexes → return record |
| `updateMany` | Find all matches → validate all → append all → return `{ count }` |
| `delete` | Find → append tombstone → remove from indexes → return record |
| `deleteMany` | Find all matches → append all tombstones → return `{ count }` |
| `count` | Filter scan → count matches → return number |
| `aggregate` | Filter scan → compute `_count`, `_avg`, `_sum`, `_min`, `_max` |
| `groupBy` | Filter scan → group by field → aggregate per group |

### 15.2 Where Operators

| Operator | Types | Example |
|---|---|---|
| `equals` | all | `{ name: 'Alice' }` or `{ name: { equals: 'Alice' } }` |
| `not` | all | `{ name: { not: 'Alice' } }` |
| `in` | string, number, enum | `{ status: { in: ['active'] } }` |
| `notIn` | string, number, enum | `{ status: { notIn: ['banned'] } }` |
| `contains` | string | `{ name: { contains: 'ali' } }` |
| `startsWith` | string | `{ name: { startsWith: 'Al' } }` |
| `endsWith` | string | `{ name: { endsWith: 'ce' } }` |
| `gt` | number, date | `{ age: { gt: 18 } }` |
| `gte` | number, date | `{ age: { gte: 18 } }` |
| `lt` | number, date | `{ age: { lt: 65 } }` |
| `lte` | number, date | `{ age: { lte: 65 } }` |
| `AND` | logical | `{ AND: [{...}, {...}] }` |
| `OR` | logical | `{ OR: [{...}, {...}] }` |
| `NOT` | logical | `{ NOT: { status: 'banned' } }` |

### 15.3 select + include Rules

- `select` and `include` **cannot be used at the same level** → throws `QueryError`
- To include a relation while selecting scalar fields, nest inside `select`:

```ts
// Correct
select: { name: true, posts: { select: { title: true } } }

// Throws QueryError
select: { name: true }, include: { posts: true }
```

### 15.4 Performance Characteristics

| Operation | Complexity |
|---|---|
| `findUnique` by PK | O(1) |
| `findUnique` by unique field | O(1) |
| `findMany` with indexed filter | O(k) — k matching records |
| `findMany` with non-indexed filter | O(n) — full scan |
| `findMany` with `take` | Early exit |
| `include` resolution | O(m) — batched, single scan per related model |

---

## 16. Relationship Resolver — Deep Spec

### 16.1 Supported Relationship Types (Phase 1)

| Type | Example |
|---|---|
| One-to-One | User → Profile |
| One-to-Many | User → Posts |

Many-to-Many is Phase 2.

### 16.2 N+1 Prevention — Batched Resolution (Hard Requirement)

`include` clauses must use batched resolution. Per-record lookups are **not acceptable**.

```
1. Execute primary query → collect result records
2. Extract all unique FK values from result records in one pass
3. Execute ONE scan per related model, filtered to relevant FK values
4. Group related records by FK in memory
5. Attach grouped results to each primary record
```

Example: `db.user.findMany({ include: { posts: true } })` returning 1000 users must trigger **exactly one** post scan — never 1000 separate lookups.

### 16.3 Nested Writes

Supported on `create` and `update`. Implemented **last** in the sequence after flat CRUD is stable.

```ts
// Nested create
db.user.create({
  data: {
    name: 'Alice',
    posts: { create: [{ title: 'Post 1' }] }
  }
})
```

**Atomicity:** parent + all children validated before any writes. Any failure aborts entirely.  
**Order:** parent created first, then children.

---

## 17. Compaction — Deep Spec

### 17.1 What It Does

Rewrites `data.ndjson` to contain only the latest non-deleted version of each record. Removes tombstones and superseded versions.

### 17.2 Trigger Conditions

| Trigger | Condition |
|---|---|
| Automatic | `tombstoneCount / totalLines >= autoCompactThreshold` (default 30%) |
| Manual | `db.compact(modelName)` or `db.compactAll()` |

Auto-check runs after every `delete` and `deleteMany`.

### 17.3 Non-Blocking Algorithm

```
1.  Check compaction lock — skip if already in progress
2.  Set compaction lock
3.  Scan data.ndjson → build { id → latestRecord } for non-deleted records
4.  Write to data.ndjson.tmp
5.  Queue all incoming writes (operation-level queue)
6.  fs.rename(data.ndjson.tmp, data.ndjson)  ← atomic
7.  Rebuild BOTH logical and physical indexes from new file
8.  Reset file size counter from actual new file size
9.  Flush queued writes in order
10. Update meta.json
11. Release compaction lock
```

**All physical index entries are invalidated by compaction.** Every record is at a new offset in the compacted file. Full index rebuild is mandatory.

**Reads are not blocked during compaction.** Writes are queued at operation granularity.

---

## 18. Public API & Barrel Exports

### 18.1 Public Exports from `src/index.ts`

```ts
export { createClient } from './client/index.js'
export {
  OrionDBError,
  ValidationError,
  UniqueConstraintError,
  RecordNotFoundError,
  QueryError,
  SchemaError,
  SchemaMismatchError,
  SchemaValidationError,
  RelationError,
  CompactionError,
} from './errors/index.js'
```

### 18.2 `createClient` Config

```ts
interface OrionDBConfig {
  location: string
  schemaMismatchStrategy?: 'block' | 'warn-and-continue' | 'auto-migrate'  // default: 'block'
  strict?: boolean                // default: false
  autoCompact?: boolean           // default: true
  autoCompactThreshold?: number   // default: 30 (percentage 0–100)
}
```

### 18.3 Model Client Methods

| Method | Return |
|---|---|
| `create(args)` | `Promise<Record>` |
| `createMany(args)` | `Promise<{ count: number }>` |
| `findUnique(args)` | `Promise<Record \| null>` |
| `findUniqueOrThrow(args)` | `Promise<Record>` |
| `findFirst(args)` | `Promise<Record \| null>` |
| `findMany(args)` | `Promise<Record[]>` |
| `update(args)` | `Promise<Record>` |
| `updateMany(args)` | `Promise<{ count: number }>` |
| `delete(args)` | `Promise<Record>` |
| `deleteMany(args)` | `Promise<{ count: number }>` |
| `count(args)` | `Promise<number>` |
| `aggregate(args)` | `Promise<AggregateResult>` |
| `groupBy(args)` | `Promise<GroupByResult[]>` |

### 18.4 Database-Level Methods

| Method | Description |
|---|---|
| `db.compact(modelName)` | Manually compact a single model |
| `db.compactAll()` | Manually compact all models |
| `db.$disconnect()` | Flush pending operations and close |

---

## 19. Performance Constraints

These are **hard requirements**, not aspirational targets:

| Constraint | Rule |
|---|---|
| `findUnique` by PK | Must use physical index — O(1) file read |
| `findUnique` by unique field | Must use logical + physical index — O(1) file read |
| `include` resolution | Must be batched — one scan per related model, never per-record |
| Write path | Must never call `fs.stat` — use in-memory file size counter |
| Index rebuild | Must use `readline` streaming — never load full file into memory |
| `take` optimization | Must exit scan early when `take` limit is satisfied |
| Compaction | Must be non-blocking — reads unaffected, writes queued |

---

## 20. Crash Safety & Canonical State Rules

### 20.1 Canonical State Rules

1. **The append log is the source of truth.** Last valid occurrence of a record ID in `data.ndjson` = current state.
2. **Last occurrence wins during rebuild.** Each new occurrence of an ID overwrites the previous index entry.
3. **Tombstones are not permanent.** A write after a tombstone resurrects the record — this is correct append-only log behavior.
4. **Only indexed fields are tracked in the logical index.**

### 20.2 Crash Consistency (Phase 1 Documented Limits)

| Scenario | Behavior |
|---|---|
| Crash during single record write | Malformed final line detected and discarded on startup. No data loss for previous records. |
| Crash during batch write | Fully-written records in batch recovered. Partially-written batch line discarded. Operation not retried. |
| Uniqueness after crash | Constraints enforced in-memory. Last-occurrence-wins produces consistent final state. Temporary disk violations possible — resolved on restart. |

Full crash atomicity (WAL) is Phase 2.

### 20.3 Startup Sequence

```
1.  Validate config
2.  Create dbLocation if not exists
3.  Read _schema.json from disk
4.  Compare disk schema with code schema
    a. Validate relationship consistency (always hard error)
    b. Apply schemaMismatchStrategy on mismatch
5.  Create model directories if not exists
6.  For each model:
    a. fs.stat(data.ndjson) → initialize in-memory file size counter
    b. readline stream → rebuild logical + physical indexes
    c. Last occurrence wins; discard malformed final line with console.warn
7.  Write updated _schema.json
8.  Write _meta.json if not exists
9.  Return client to caller
```

---

## 21. Phase 1 Non-Goals

These must **never** influence Phase 1 architecture. Do not design around them. Do not add hooks for them. Implement them in Phase 2 when the spec changes.

| Out of Scope | Reason |
|---|---|
| Multi-process access | Requires file locking and WAL |
| Network layer / server mode | Contradicts local-first positioning |
| Sync API | Phase 2 |
| User-defined indexes (beyond unique) | Phase 2 |
| `orderBy` on related fields | Phase 2 |
| Array field types | Phase 2 |
| Full transaction API | Phase 2 |
| Many-to-Many relationships | Phase 2 |
| WAL-inspired storage format | Phase 2 |
| Chunked / sharded table files | Phase 2 |
| Transactional index rollback | Phase 2 |
| Snapshot-based index persistence | Phase 2 |
| Query result caching | Phase 2 |
| GUI / visual explorer | Never |
| Authentication / authorization | Never |
| Distributed / replicated storage | Never |

---

## 22. Git & Branching Strategy

### 22.1 Branch Naming

```
main                          ← always production-ready
feature/<feature-name>        ← one branch per feature in the sequence
fix/<short-description>       ← bug fixes
chore/<short-description>     ← non-functional changes (deps, config, docs)
```

### 22.2 Commit Message Format (Conventional Commits)

```
<type>(<scope>): <short summary>

Types: feat | fix | test | refactor | chore | docs | build | ci
Scope: index-manager | schema | persistence | query | relations | errors | compaction | client | api

Examples:
feat(index-manager): implement IndexManagerImpl with logical and physical indexes
test(index-manager): add full test coverage for rebuild and tombstone handling
fix(persistence): correct byte offset tracking after compaction reset
chore(deps): upgrade vitest to 1.6.0
```

### 22.3 PR Rules

- One PR per feature (matches the feature sequence)
- PR title = commit message format
- All tests must pass before merge
- No `any`, no `@ts-ignore`, no `console.log` in library code

---

## 23. Definition of Done

A feature branch is **complete and mergeable** when ALL of the following are true:

- [ ] Implementation matches the spec in this document and `OrionDB-Requirements.md`
- [ ] All tests for the feature pass: `pnpm test`
- [ ] Zero TypeScript errors: `pnpm typecheck`
- [ ] Zero ESLint errors: `pnpm lint`
- [ ] Code is formatted: `pnpm format --check`
- [ ] Coverage ≥ 80% overall, ≥ 95% for the feature module: `pnpm test --coverage`
- [ ] No `any`, no `!` non-null assertions, no `@ts-ignore` without justification
- [ ] No `console.log` in library code
- [ ] All imports use `node:` prefix for Node built-ins
- [ ] All relative imports use `.js` extension
- [ ] No runtime dependencies added without explicit justification
- [ ] PR description summarizes what was implemented and links to relevant spec sections

---

*End of OrionDB Developer Instructions*  
*This document must be updated when architectural decisions change.*  
