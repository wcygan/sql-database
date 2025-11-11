# Planner Guidelines

## Role Within The Workspace

- `crates/planner` bridges the parser and executor: it converts SQL AST into optimized physical execution plans.
- Responsibilities: name binding (column names → ordinals), logical optimization (predicate pushdown, projection pruning), and access method selection (SeqScan vs IndexScan).
- The planner is read-only on the catalog; it never modifies schemas, only queries them to validate and optimize plans.
- Version 1 focuses on correctness over performance: simple rule-based optimization with single-column index selection.

## Integration Contracts

- **Parser (`crates/parser`)** – Consumes `Statement` variants (Select, Insert, Update, Delete) and resolves string-based column references to `ColumnId` using catalog schemas.
- **Catalog (`crates/catalog`)** – Reads table schemas (`TableMeta`), column metadata, and index definitions (`IndexMeta`, `IndexKind`) to perform binding and optimization.
- **Expression (`crates/expr`)** – Reuses `Expr` for predicate representation; planner creates `ResolvedExpr` with `Column(ColumnId)` instead of `Column(String)` for executor efficiency.
- **Executor (future)** – Produces `PhysicalPlan` nodes that map directly to Volcano operator implementations with resolved table IDs, column ordinals, and access methods.
- **Types (`crates/types`)** – Uses `Value` for literals in expressions and `SqlType` for type checking (minimal in v1).
- **Common (`crates/common`)** – Returns `DbResult<T>` with `DbError::Planner` for all errors; uses shared types like `TableId`, `ColumnId`.

## Module Layout & Extension Points

- `src/lib.rs` contains all implementation: `LogicalPlan`, `PhysicalPlan`, `ResolvedExpr`, `Planner`, binding logic, optimization rules, and explain functions.
- `src/tests.rs` exercises binding, optimization, index selection, and error handling with unit and integration tests.
- To add new optimization rules, extend `Planner::optimize()` with new transformation functions following the `pushdown` and `prune_project` patterns.
- To support new index types, update `find_index_for_col()` to handle additional `IndexKind` variants beyond `BTree`.
- To add new physical operators, extend `PhysicalPlan` enum and update `bind()` method to generate them.

## Key Algorithms

### Name Binding

- Resolves column names to `ColumnId` using `TableSchema::column_index()`
- Case-insensitive matching via `eq_ignore_ascii_case()`
- Validates all column references exist before generating physical plan
- Transforms `expr::Expr` into `ResolvedExpr` with ordinal references

### Predicate Pushdown

- Moves `Filter` nodes closer to `TableScan` to reduce data volume early
- Currently pushes through wildcard projections only (v1 simplification)
- Preserves correctness by only pushing when column references remain valid

### Index Selection

- Analyzes `WHERE` predicates to identify simple patterns: `col = val`, `col > val`, etc.
- Checks catalog for single-column BTree indexes matching predicate column
- Converts `SeqScan` to `IndexScan` when beneficial
- Extracts `IndexPredicate::Eq` for equality or `IndexPredicate::Range` for comparisons
- Falls back to `SeqScan` for complex predicates or missing indexes

## Build & Verification Commands

- `cargo check -p planner` — fast validation during development
- `cargo test -p planner` — run planner-focused tests
- `cargo fmt -- --check` / `cargo clippy -p planner --all-targets` — maintain code quality
- `cargo test` — workspace-wide validation
- `cargo llvm-cov --package planner --html` — coverage report (target 90%+)

## Testing Strategy

### Unit Tests

- **Binding**: Column name resolution, case-insensitive matching, error handling
- **Optimization**: Predicate pushdown correctness, projection pruning effectiveness
- **Index Selection**: Equality predicates, range predicates, fallback to SeqScan
- **Error Paths**: Unknown tables, unknown columns, unsupported statements

### Integration Tests

- End-to-end planning with real catalog fixtures
- Verify plan structure matches expected operators
- Test `explain_logical()` and `explain_physical()` output
- Validate plan correctness for all DML statement types

### Coverage Goals

- Target 90%+ line coverage following project standards
- All optimization rules exercised with before/after plan verification
- Index selection logic fully covered (with/without indexes, different predicates)
- Error paths tested (unknown entities, type mismatches, DDL rejection)

## Error Handling

- Return `DbError::Planner` for all planning failures with descriptive context
- Unknown tables: `"unknown table 'tablename'"`
- Unknown columns: `"unknown column 'colname'"`
- Unsupported statements: `"DDL handled elsewhere in v1"`
- Validate predicates against schema during binding phase
- Reject plans that reference non-existent indexes or incompatible index kinds

## Coding Patterns

### Plan Transformations

```rust
// Optimization pattern: transform input recursively, then wrap
fn optimize_rule(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::SomeNode { input, .. } => {
            // Recurse first
            let optimized_input = optimize_rule(*input);
            // Then apply transformation
            transform(optimized_input)
        }
        // Base cases pass through unchanged
        other => other,
    }
}
```

### Binding Pattern

```rust
// Always use schema from physical plan for column resolution
let schema = Self::output_schema(&input_physical);
let resolved = Self::bind_expr_with_schema(&schema, expr)?;
```

### Index Selection Pattern

```rust
// Try index optimization after binding filter
if let PhysicalPlan::SeqScan { table_id, schema } = &input_physical {
    if let Some((col_id, pred)) = Self::try_extract_index_predicate(schema, &resolved) {
        if let Some(index_name) = Self::find_index_for_col(ctx, table_id, col_id) {
            // Replace SeqScan with IndexScan
            return Ok(PhysicalPlan::IndexScan { ... });
        }
    }
}
```

## Future Extensions (Beyond v1)

- **Join Planning**: Support multi-table queries with join reordering
- **Cost-Based Optimization**: Replace rule-based optimizer with cost model
- **Multi-Column Indexes**: Extend index selection to composite indexes
- **Subqueries**: Support correlated and uncorrelated subqueries
- **Aggregate Pushdown**: Push GROUP BY closer to sources
- **Predicate Simplification**: Apply algebraic rules to simplify expressions
- **Statistics**: Use table/column statistics for better access method choices
- **Hash Join**: Support hash join for equality predicates
- **Sort-Merge Join**: Support sorted inputs for merge joins

## Debugging Tips

- Use `explain_logical()` to inspect plan before optimization
- Use `explain_physical()` to verify final plan structure
- Add temporary debug prints in optimization rules to trace transformations
- Test optimizations in isolation with hand-crafted logical plans
- Check catalog state with `catalog.table()` when binding fails
- Verify column ordinals match schema with `TableSchema::columns()`

## Collaboration & Pull Requests

- Mention affected crates in PR descriptions: executor for new operators, catalog for index changes
- Include validation commands: `cargo test -p planner` and any affected integration tests
- Document optimization behavior changes with example plans
- When adding new physical operators, coordinate with future executor implementation
- Include explain output for test cases to show plan structure clearly
