# Tasks

- [x] Fix UPDATE and DELETE paths so they actually mutate storage, emit WAL records, and add regression tests proving rows change.
- [ ] Enforce primary-key uniqueness (schema metadata, insert-time checks, and tests that reject duplicate keys):
  - [x] Add `primary_key: Option<Vec<ColumnId>>` field to TableMeta with validation (catalog/src/lib.rs:304)
  - [x] Add `set_primary_key()` method with bounds/duplicate checking (catalog/src/lib.rs:346-376)
  - [x] Update `create_table()` signature to accept optional primary key (catalog/src/lib.rs:98-120)
  - [x] Write 9 unit tests for PK metadata and persistence (catalog/src/lib.rs:1042-1146)
  - [x] Extend parser to support `PRIMARY KEY (col1, col2, ...)` table constraint syntax (parser/src/lib.rs:344-373)
  - [x] Thread PK column list from parser → REPL → catalog during CREATE TABLE (repl/src/main.rs:120-160, executor/src/lib.rs:416-541)
  - [x] Implement `PrimaryKeyIndex` struct with HashMap-based uniqueness checking (executor/src/pk_index.rs:1-227, 9 unit tests)
  - [ ] Build PK index on ExecutionContext table open by scanning existing rows
  - [ ] Add PK uniqueness validation to INSERT (check index before heap insert)
  - [ ] Write regression tests proving duplicate PK inserts are rejected
  - [ ] Forbid UPDATE operations that modify PK columns
  - [ ] Write regression tests proving PK updates are rejected
  - [ ] Remove PK entries from index on DELETE
  - [ ] Write regression tests proving deleted PKs can be reinserted
