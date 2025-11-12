# Repository Guidelines

## Project Structure & Module Organization

This workspace implements an educational SQL database following classic database architecture with clear separation of concerns. All crates live under `crates/` and the workspace root (`Cargo.toml`) tracks shared dependencies. Generated artifacts land in `target/`; do not commit its contents.

### Database Architecture Overview

```
SQL Text → Parser → Logical Plan → Planner (optimize) → Physical Plan
    ↓
Physical Plan → Executor → Storage (via Buffer Pool) + WAL
    ↓
Results → Pretty Printer → User
```

### Workspace Crates

#### Foundation Layer

**`crates/types/`** — Core Data Types
- Foundational data type definitions for the entire database
- `SqlType` enum: Int, Text, Bool (schema-level types)
- `Value` enum: Int(i64), Text(String), Bool(bool), Null (runtime values)
- Type comparison methods and property-based testing with proptest
- Every crate depends on this for basic data representation

**`crates/common/`** — Shared Utilities & Error Types
- Cross-cutting concerns used throughout the database
- `Row`: Tuple struct with `values: Vec<Value>` and optional `RecordId`
- Identifiers: `ColumnId` (u16), `TableId(u64)`, `PageId(u64)`, `RecordId { page_id, slot }`
- `DbError` enum: Parser, Planner, Executor, Catalog, Storage, Wal, Constraint variants
- `Config` struct: Builder pattern for data_dir, page_size, buffer_pool_pages, wal_enabled
- `RecordBatch`: Results container with columns and rows
- `pretty` module: Table rendering utilities with TableStyleKind

#### SQL Processing Layer

**`crates/expr/`** — Expression Evaluation
- SQL expression AST and evaluation logic
- `Expr` enum: Literal, Column(String), Unary, Binary
- `BinaryOp`: Eq, Ne, Lt, Le, Gt, Ge, And, Or; `UnaryOp`: Not
- `EvalContext`: Evaluates expressions against rows with schema context
- Expression evaluation with NULL handling and type checking
- Used by: Parser creates Expr trees, planner resolves to ResolvedExpr, executor evaluates

**`crates/parser/`** — SQL Parser
- SQL text → AST conversion using sqlparser-rs
- `Statement` enum: CreateTable, DropTable, CreateIndex, DropIndex, Insert, Select, Update, Delete
- `ColumnDef`: name + type string; `SelectItem`: Wildcard or Column(String)
- Primary key constraint extraction support
- `parse_sql()`: Main entry point with identifier normalization (lowercase) and type string uppercase conversion
- Bridges sqlparser AST to internal AST with validation

**`crates/catalog/`** — Metadata Management
- Persistent schema catalog with pretty JSON serialization
- `Catalog`: Main metadata store with tables, indexes, and lookup maps
- `TableMeta`: id, name, schema, indexes, storage descriptor, primary_key field
- `TableSchema`: Column definitions with validation; `Column`: name, type, column_id
- `IndexMeta`: id, name, kind, columns, storage descriptor; `IndexKind`: BTree (with type support checking)
- `StorageDescriptor`: UUID-based file identification
- Operations: `create_table()`, `drop_table()`, `table()`, `table_by_id()`, `create_index()` (builder pattern), `drop_index()`
- `save()`/`load()`: Persistent catalog with rebuild indexes on load, primary key support, reserved name validation

#### Storage & Persistence Layer

**`crates/storage/`** — Physical Storage Layer
- Heap file implementation with slotted page format
- `Page`: 4KB pages with slotted layout (header, slot array, tuple area)
- `PageHeader`: num_slots, free_offset; `Slot`: offset, length
- `HeapTable` trait: insert(), get(), update(), delete()
- `HeapFile`: File-backed heap table implementation
- Append-only with slot-based addressing, uses RecordId for addressing
- Constants: `PAGE_SIZE = 4096`, bincode legacy config for serialization

**`crates/buffer/`** — Buffer Pool Manager
- LRU page cache between storage and executor
- `Pager` trait: fetch_page(), allocate_page(), flush()
- `FilePager`: LRU cache implementation with dirty page tracking
- File-per-table storage model (table_{id}.tbl)
- LRU eviction policy, automatic dirty page flushing, lazy loading from disk, sequential page ID allocation
- Executor accesses pages through Pager, not directly

**`crates/wal/`** — Write-Ahead Log
- Crash recovery and durability through logical logging
- `WalRecord` enum: Insert, Update, Delete, CreateTable, DropTable (logical operations)
- `Wal`: Append-only log manager with fsync support
- Operations: `append()` (write record), `sync()` (fsync for durability), `replay()` (recovery)
- Length-prefixed framing (4-byte LE), redo-only, single WAL file, logical records

#### Query Processing Layer

**`crates/planner/`** — Query Planner
- SQL AST → optimized physical execution plans
- `LogicalPlan`: TableScan, Filter, Project, Insert, Update, Delete (with string names)
- `PhysicalPlan`: SeqScan, IndexScan, Filter, Project, Insert, Update, Delete (with IDs)
- `ResolvedExpr`: Column(ColumnId) instead of Column(String)
- `IndexPredicate`: Eq, Range for index scans
- `Planner`: Main planning logic; `PlanningContext`: Holds catalog reference
- Optimizations: Name binding (Column names → ColumnId ordinals), predicate pushdown, projection pruning, index selection
- Explain functions for debugging plans

**`crates/executor/`** — Query Executor
- Volcano-style iterator execution model with pull-based execution
- Modules: `lib.rs` (Executor trait, ExecutionContext), `scan.rs` (SeqScanExec, IndexScanExec), `filter.rs` (FilterExec), `project.rs` (ProjectExec), `dml.rs` (InsertExec, UpdateExec, DeleteExec), `builder.rs` (build_executor factory), `pk_index.rs` (PrimaryKeyIndex)
- `Executor` trait: open(), next(), close(), schema()
- `ExecutionContext`: catalog, pager, wal, data_dir access
- Expression evaluator: `eval_resolved_expr()` with NULL handling
- WAL-first writes (log before modifying storage), primary key uniqueness enforcement
- Status: SeqScan and Insert fully functional, Update/Delete partial (no RID tracking)
- Integration point for all other crates

#### Tools & User Interfaces

**`crates/wal-viewer/`** — WAL Inspection Tool
- CLI for viewing WAL contents: replay WAL and display records, filter by table ID
- Pagination: --limit, --offset; Output formats: Table (pretty-printed) or JSON; Table styles: Modern, Ascii, Plain
- Usage: `wal-viewer <path> --table <id> --format json --limit 10`

**`crates/repl/`** — Interactive SQL Console
- Main database binary with REPL interface using rustyline
- Command history persistence, execute-and-exit mode (-e flag)
- DDL: CREATE/DROP TABLE/INDEX with primary key support
- DML: INSERT, SELECT, UPDATE, DELETE
- Pretty-printed query results, catalog persistence, WAL integration
- Meta-commands: .schema, .tables
- `DatabaseState`: Manages catalog, pager, WAL lifecycle
- Parser → Planner → Executor pipeline with automatic catalog/WAL setup

### Key Integration Points
- Parser normalizes identifiers, Catalog enforces them
- Planner resolves names via Catalog
- Executor coordinates Storage + Buffer + WAL + Catalog
- All errors flow through `common::DbError` enum
- All values flow through `types::Value`
- Primary key enforcement: Catalog metadata → Executor PK index checks

## Dependency Management
- Pin every third-party or shared crate version under `[workspace.dependencies]` in the root `Cargo.toml`; child crates must never specify their own versions.
- Reference those shared dependencies from member crates using `{ workspace = true }`, even for path-only crates such as `common`, `expr`, or `types`.
- If a crate needs extra features, add them to the workspace definition so all consumers stay aligned.
- When introducing a new dependency, update the workspace table first, then wire it into the specific crate via `workspace = true`.

## Build, Test, and Development Commands
- `cargo check` — fast validation of code and dependency wiring without producing binaries.
- `cargo test` — executes all unit and property tests across workspace members.
- `cargo fmt -- --check` — verify formatting before submitting changes; omit `--check` to auto-format.
- `cargo clippy --all-targets --all-features` — run lints to enforce idiomatic Rust.
- `scripts/coverage.sh` — wraps `cargo llvm-cov` to run workspace-wide tests with coverage instrumentation, emitting both HTML (`target/llvm-cov/html/index.html`) and LCOV (`target/llvm-cov/lcov.info`) outputs; install the tool once via `cargo install cargo-llvm-cov`. Pass extra cargo filters (e.g., `-- --package parser`) to narrow the run.

## Coding Style & Naming Conventions
- Follow `rustfmt` defaults (4-space indentation, trailing commas for multi-line literals).
- Modules use snake_case (`mod storage_backend`); types and traits use UpperCamelCase (`SqlValue`).
- Prefer expressive enum/struct names tied to database concepts; avoid abbreviations unless ubiquitous (e.g., `sql`).
- Keep shared dependency versions pinned via `[workspace.dependencies]`; reference them with `workspace = true`.

## Builder Pattern with Bon
- Use the `bon` crate (version 3) for ergonomic compile-time-checked builders on constructors with 4+ parameters.
- Apply `#[bon::bon]` to impl blocks and `#[builder]` to methods that should have builder APIs.
- Prefer builders for: multi-parameter constructors, public configuration structs, complex operator initialization.
- Pattern: `#[bon::bon] impl MyStruct { #[builder] pub fn new(...) -> Self { ... } }`
- Usage: `MyStruct::builder().field1(val1).field2(val2).build()`
- Benefits: compile-time validation, named parameters, any-order initialization, zero runtime cost.
- Examples: `IndexScanExec::builder()` (crates/executor/src/scan.rs), `Config` struct (future: crates/common/src/lib.rs).

## Testing Guidelines
- Unit tests live alongside implementation files using the `mod tests` pattern.
- Property-based tests in `crates/types` leverage `proptest`; name them `prop_*` for clarity.
- Add targeted integration tests in `tests/` directories when behavior spans crates.
- Run `cargo test` locally before opening a PR; include failing-seed reproduction steps if a proptest fails.
- When you need executable documentation for test coverage, run `scripts/coverage.sh`; it runs the entire workspace with coverage instrumentation and leaves reports under `target/llvm-cov/`.

## Clippy Lint Standards
- All code must pass `cargo clippy --all-targets --all-features` with zero warnings before merging.
- Address clippy suggestions by fixing the underlying issue, not by suppressing warnings unless absolutely necessary.
- Common fixes:
  - Use `io::Error::other()` instead of `io::Error::new(ErrorKind::Other, _)`
  - Use `.first()` instead of `.get(0)`
  - Use array literals `[x; n]` instead of `vec![x; n]` for compile-time constant arrays
  - Use iterator methods (`.iter().enumerate()`) instead of indexing loops when appropriate
  - Add `.truncate(true/false)` when using `.create(true)` in OpenOptions to make intent explicit
  - Remove unused imports and variables, or prefix with `_` if intentionally unused
- For test helpers that will be used in future tests, use `#[allow(dead_code)]` with a comment explaining why.
- Run clippy after every significant change; don't batch up lint fixes—address them immediately.
- If clippy suggests a change that would reduce code clarity, discuss in PR review rather than suppressing.

## Commit & Pull Request Guidelines
- Write commits in the imperative mood, e.g., `Pin workspace dependencies` or `Add row serialization tests`.
- Squash trivial fixups; keep logical units separate for clarity.
- PRs should describe motivation, summarize code changes, list validation commands, and link relevant issues.
- Include screenshots or logs when UI/CLI behavior changes, and mention any follow-up work needed.
