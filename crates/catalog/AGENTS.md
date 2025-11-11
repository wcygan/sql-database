# Catalog Integration Guide

## Role Within The Workspace
- `crates/catalog` is the metadata authority for the SQL database: it persists table schemas, column layouts, and index definitions and hands those contracts to planners, executors, and the storage layer.
- Every DDL action parsed in `crates/parser` (CREATE/DROP TABLE or INDEX) ultimately calls into `Catalog`, which validates identifiers via `common::DbError::Catalog` and emits shared IDs (`TableId`, `ColumnId`, `IndexId`) that downstream crates rely on.
- The crate serializes itself as pretty JSON via `Catalog::save`/`load`; the CLI and storage subsystems place that file under the configured `common::Config::data_dir` so restarts see the same logical schema.
- Lookup helpers (`table`, `table_by_id`, `table_summaries`) are the only supported way other crates discover schemas; bypassing them risks diverging from the in-memory indexes that keep name/ID resolution deterministic.

## Integration Contracts
- **Parser (`crates/parser`)** – Parser-normalized identifiers must be lowercase before they reach `Catalog::create_table`/`create_index`; catalog enforces reserved names and will surface friendly parser errors when constraints fail. Keep parser column/type strings aligned with `types::SqlType` so `TableSchema::try_new` succeeds.
- **Types (`crates/types`)** – Columns embed `SqlType`, and `IndexKind::supports_type` encodes which SQL types each index can target. Adding a new SQL type means updating this crate (schema validation + index guards) alongside the `types` definition.
- **Expression & Planning (`crates/expr`)** – Planners read `TableMeta::schema`, `columns()`, and `TableSchema::column_index` to resolve projection ordinals. The planner assumes ordinals stay stable once returned, so any schema migration must bump table ids or rewrite the table to preserve ordering.
- **Storage (`crates/storage`)** – `StorageDescriptor::file_id` bridges logical tables/indexes to heap/index files. Storage components map `Uuid`s to actual segments and should use the descriptor instead of inventing filenames to keep WAL/backup layers consistent.
- **Common (`crates/common`)** – All public APIs return `DbResult<T>` with `DbError::Catalog` so CLI callers can bubble metadata problems up uniformly. Never introduce ad-hoc errors; extend the shared enum if a new failure mode spans crates.

## Module Layout & Extension Points
- `src/lib.rs` contains everything: `Catalog`, `TableMeta`, `TableSchema`, `Column`, `IndexMeta`, `IndexKind`, `StorageDescriptor`, plus unit tests under `mod tests`. Keep new metadata types in this file until the crate grows large enough to justify submodules; doing so avoids circular imports for serde serialization.
- Lookup tables (`table_name_index`, `index_name_lookup`, etc.) must be rebuilt through the provided helpers whenever you mutate `tables` or `indexes`. When adding new collections, follow the same `rebuild_*` pattern so `serde` skip fields stay in sync after `load`.
- To introduce a new index implementation, extend `IndexKind`, teach `supports_type` about the valid `SqlType`s, and ensure `TableMeta::add_index` populates any extra storage metadata you require. Coordinate with `storage` so the new kind has a matching physical representation.
- Adding per-column attributes (e.g., nullability, defaults) should happen inside `Column`; migrate existing JSON by giving new fields sensible defaults and updating `TableSchema::try_new` to validate the new semantics.

## Persistence & Runtime Expectations
- `Catalog::load` tolerates missing files by returning `Catalog::new()`, letting tests spin up ephemeral schemas without fixture setup. Deployment code is expected to create the catalog file next to data files inside `Config::data_dir`.
- Reserved identifiers (`_catalog`, `_primary`, `sqlite_master`) protect internal bookkeeping; if a new subsystem needs its own reserved namespace, extend `RESERVED_*` and document the reason so parser authors know what to guard against.
- `StorageDescriptor::new` assigns fresh UUIDs whenever tables or indexes are created; storage implementers should treat these as stable primary keys even if a table is dropped and recreated with the same name.

## Development & Testing Workflow
- `cargo check -p catalog` — fastest feedback loop while editing schema/index logic.
- `cargo test -p catalog` — exercises the in-crate unit tests that cover persistence, validation, and helper accessors; run `cargo test` at the workspace root when changes influence parser/planner/storage collaboration.
- `cargo fmt -- --check` and `cargo clippy -p catalog --all-targets` — keep formatting and lints consistent before sending patches to teammates that own downstream crates.
- Snapshot new catalog JSON structures (when necessary) via focused tests that round-trip through `save`/`load`; avoid checking in generated files under `target/`.

## Collaboration & Pull Requests
- Mention every affected crate in PR descriptions (parser for new DDL syntax, storage for file-layout changes, expr/planner for new metadata) so reviewers can coordinate rollouts.
- Include the validation commands above plus any crate-specific checks you ran (e.g., parser tests if you touched identifier handling).
- When catalog changes alter observable CLI behavior (new errors, new summaries), attach example shell transcripts or JSON snippets so documentation and UI layers can be updated in lockstep.
