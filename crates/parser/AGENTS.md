# Parser Guidelines

## Role Within The Workspace
- `crates/parser` is the SQL entry point for the entire workspace: it converts raw text into the canonical `Statement`/`Expr`/`Value` tree that the catalog, planner, and execution layers consume.
- Third-party parsing is delegated to `sqlparser` (Generic dialect). We immediately normalize identifiers, strip unsupported features, and lower everything into our own AST defined in `src/ast.rs`.
- The crate must always return `common::DbResult`/`DbError` variants so downstream crates can surface uniform diagnostics, whether the error originated from syntax, unsupported constructs, or semantic guards.

## Integration Contracts
- **Catalog (`crates/catalog`)** – `Statement::CreateTable`, `CreateIndex`, `DropTable`, and `DropIndex` feed directly into catalog mutations. Keep column/type strings and identifier casing consistent (lowercase identifiers + uppercase SQL types) so catalog lookups stay deterministic.
- **Expression Layer (`crates/expr`)** – Scalar expressions in SELECT/WHERE/INSERT map to `expr::Expr`, `BinaryOp`, and `UnaryOp`. Any new SQL expression support must preserve this mapping so evaluation logic does not need parser-specific branches.
- **Value Modeling (`crates/types`)** – Literal mapping (`map_value`) produces `types::Value`. Adding new literal forms (e.g., decimals, timestamps) requires coordinating with `types` so serialization, planning, and storage all understand the value.
- **Common Utilities (`crates/common`)** – Reuse shared identifiers (`TableId`, `ColumnId` when available), error helpers, and result types to keep parser results compatible with scheduler/executor code.
- When expanding the SQL surface area, confirm the consumer crates actually handle the new `Statement` variant; otherwise guard it behind explicit `DbError::Parser` messages so the CLI cannot submit unsupported operations.

## Module Layout & Extension Points
- `src/lib.rs` houses the `parse_sql` entry point plus the mapping helpers that bridge `sqlparser` AST nodes to our internal enums.
- `src/ast.rs` defines the statements and projection items that downstream crates pattern-match on; additions here are breaking changes and must be reflected in `catalog`, planners, and any binary that matches the enum.
- `src/tests.rs` demonstrates the supported SQL subset end-to-end. Mirror any new feature with at least one integration-style test plus error-path coverage (e.g., rejecting multi-row INSERT).

### Adding A New Statement Or Expression
1. Extend the enums/structs in `src/ast.rs` (and the `expr` or `types` crates if new expression/literal support is needed).
2. Update the appropriate mapper in `src/lib.rs` (`map_statement`, `map_select_item`, `map_expr`, etc.) and ensure unsupported branches still produce `DbError::Parser`.
3. Add targeted tests in `src/tests.rs` that exercise both success and failure cases.
4. Coordinate with downstream crates so they understand the new AST node; prefer compiler errors over runtime panics by matching exhaustively on enums.

## Build, Test, and Verification Commands
- `cargo check -p parser` — fastest feedback loop while editing mapping logic.
- `cargo test -p parser` — runs the parser-focused suite; add full-workspace `cargo test` when changes interact with `expr`, `types`, or `catalog`.
- `cargo fmt` / `cargo fmt -- --check` — keep diffs minimal so tutorial docs can borrow snippets verbatim.
- `cargo clippy --all-targets --all-features` — catches overlooked clones or fallible conversions before parser changes land in the educational material.

## Error Handling & Reporting
- Always wrap third-party errors (from `sqlparser` or string parsing) in `DbError::Parser` with clear, user-facing context so the CLI can display actionable guidance.
- Normalize identifiers via `normalize_ident*` helpers to avoid case-sensitivity surprises across catalog/storage crates.
- Reject unsupported SQL constructs explicitly; vague errors make it harder for other crates to decide whether the parser or executor should implement the feature next.

## Collaboration & Pull Requests
- Document the motivation in PRs when expanding the SQL subset so catalog/planner owners know what to implement next.
- Include the commands above in PR descriptions, plus any workspace-wide checks that were affected.
- When parser changes influence CLI UX (e.g., new syntax accepted), attach sample input/output transcripts so docs and tests in other crates can be updated in lockstep.
