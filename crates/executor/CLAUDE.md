# Executor Guidelines

## Role Within The Workspace

- `crates/executor` implements the Volcano-style iterator execution model: it transforms physical plans from the planner into actual row-producing pipelines.
- Responsibilities: opening/closing operators, pulling rows through the iterator interface, evaluating resolved expressions, coordinating with storage/WAL for DML operations.
- The executor is the integration point where planner plans meet storage reality; it never modifies schemas or plans, only executes them.
- Version 1 focuses on correctness with simple operators: SeqScan works fully, Insert works with WAL, Update/Delete are partially implemented (missing RID tracking).

## Integration Contracts

- **Planner (`crates/planner`)** – Consumes `PhysicalPlan` trees with resolved table IDs, column ordinals (`ColumnId`), and `ResolvedExpr` predicates.
- **Storage (`crates/storage`)** – Uses `HeapTable` trait to insert/get/update/delete rows; accesses pages via `RecordId { page_id, slot }`.
- **Buffer (`crates/buffer`)** – Accesses pages through `Pager` trait (not used directly; storage layer handles this).
- **WAL (`crates/wal`)** – Logs DML operations via `WalRecord` variants before applying changes to ensure durability.
- **Catalog (`crates/catalog`)** – Queries table metadata to resolve table IDs to file paths and schemas.
- **Expression (`crates/expr`)** – Evaluates `ResolvedExpr` trees (literals, column references, unary/binary ops) against `Row` instances.
- **Types (`crates/types`)** – Uses `Value` enum for scalars; evaluates comparison/logical operators on `Value`.
- **Common (`crates/common`)** – Returns `DbResult<T>` with `DbError::Executor` for all errors; uses `Row(Vec<Value>)` tuple struct.

## Module Layout & Extension Points

- `src/lib.rs` contains public API (`execute_query`, `execute_dml`), `Executor` trait, and `ExecutionContext`.
- `src/scan.rs` implements `SeqScanExec` (fully functional) and `IndexScanExec` (stub that delegates to SeqScan).
- `src/filter.rs` implements `FilterExec` and expression evaluation (`eval_resolved_expr`, `eval_unary_op`, `eval_binary_op`).
- `src/project.rs` implements `ProjectExec` for column selection and reordering.
- `src/dml.rs` implements `InsertExec` (full WAL integration), `UpdateExec` and `DeleteExec` (partial: count matches but don't modify storage due to missing RID tracking).
- `src/builder.rs` factory function `build_executor()` constructs operator trees from `PhysicalPlan`.
- `src/tests.rs` unit tests (currently placeholder; needs comprehensive coverage).

To add new operators:
1. Create struct implementing `Executor` trait (open/next/close/schema methods)
2. Add variant handling in `builder::build_executor()`
3. Write unit tests exercising operator in isolation
4. Add integration test combining with other operators

## Key Algorithms

### Volcano Iterator Model

- Each operator implements `open() → next() → close()` lifecycle
- `open()`: Initialize resources (open files, allocate buffers, reset state)
- `next()`: Pull next row from children, apply operator logic, return `Option<Row>`
- `close()`: Clean up resources (close files, flush buffers)
- Pull-based execution: operators request rows from children on-demand

### Expression Evaluation

- `eval_resolved_expr()` recursively evaluates `ResolvedExpr` trees
- Column references resolved to `ColumnId` (u16 ordinal) → lookup in `row.0[idx]`
- Literals return cloned `Value`
- Unary/binary ops dispatch to type-specific evaluation functions
- NULL propagation: unary/binary ops on NULL produce NULL (except NOT which handles NULL explicitly)
- Type checking: operators reject invalid type combinations (e.g., NOT on Int)

### DML Execution

**Insert Flow:**
1. Evaluate value expressions (typically literals) against empty row
2. Insert into storage via `HeapTable::insert()` to get `RecordId`
3. Log to WAL with `WalRecord::Insert { table, row, rid }`
4. Call `wal.sync()` for durability
5. Return single row with affected count (1)

**Update/Delete Flow (Partial):**
1. Build scan + filter pipeline to find matching rows
2. For each row: apply assignments (update) or just count (delete)
3. Currently does NOT modify storage (missing RID propagation through iterator)
4. Return single row with affected count

**WAL-First Write**: Always log to WAL before modifying storage to ensure crash recovery works correctly.

### Scan Implementation

**SeqScan:**
- Track current `(page_id, slot)` position
- Fetch rows via `HeapTable::get(rid)` in nested loop over pages/slots
- Handle empty/deleted slots by advancing position
- Heuristic: assume max ~100 slots per page, move to next page after trying 100 slots
- Compute total pages on first `next()` call (cached for duration of scan)

**IndexScan:**
- Stub implementation: creates SeqScan and delegates all operations
- Future: query B+Tree index for matching RIDs, fetch rows from heap

## Build & Verification Commands

- `cargo check -p executor` — fast validation during development
- `cargo test -p executor` — run executor-focused tests (currently minimal)
- `cargo fmt -- --check` / `cargo clippy -p executor --all-targets` — maintain code quality
- `cargo test --workspace` — ensure executor integrates with other crates
- `cargo llvm-cov --package executor --html` — coverage report (target 90%+, currently low)

## Testing Strategy

### Unit Tests (TODO - High Priority)

- **SeqScan**: Create in-memory heap table, insert known rows, verify scan returns all rows in order
- **Filter**: Mock input executor, test predicates (equality, comparison, logical AND/OR), NULL handling
- **Project**: Test column selection, reordering, out-of-bounds column access errors
- **Insert**: Test value evaluation, WAL logging, storage insertion, error paths
- **Expression Evaluation**: Test all operators (Eq/Ne/Lt/Le/Gt/Ge/And/Or/Not) with all value types

### Integration Tests (TODO)

- End-to-end query execution: catalog setup → build plan → execute → verify results
- DML persistence: insert → close → reopen → verify data persists
- WAL integration: insert → simulate crash → replay WAL → verify data recovered
- Complex queries: scan → filter → project chains

### Coverage Goals

- Target 90%+ line coverage following project standards
- All expression evaluation paths covered (each operator x value type combination)
- Error paths tested (unknown tables, column out of bounds, type mismatches)
- Operator lifecycle tested (open/next/close sequences)

## Error Handling

- Return `DbError::Executor` for all execution failures with descriptive context
- Unknown tables: propagated from catalog lookup
- Column out of bounds: `"column index {idx} out of bounds (row has {len} columns)"`
- Type errors: `"NOT requires boolean, got {:?}"`, `"invalid binary operation: {:?} {:?} {:?}"`
- Expression evaluation failures propagated up through operator stack
- Storage/WAL errors propagated with original error type

## Coding Patterns

### Executor Implementation Pattern

```rust
impl Executor for MyOperator {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        // Initialize child operators first
        self.input.open(ctx)?;
        // Then initialize own state
        self.state = State::Ready;
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        // Pull from children, apply operator logic
        let row = match self.input.next(ctx)? {
            Some(r) => r,
            None => return Ok(None),
        };
        // Transform row
        Ok(Some(transformed_row))
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        // Clean up own resources first
        self.cleanup()?;
        // Then close children
        self.input.close(ctx)
    }

    fn schema(&self) -> &[String] {
        // Return output schema
        &self.output_schema
    }
}
```

### Borrow Checker Pattern (HeapTable)

```rust
// WRONG: Cannot borrow ctx twice
let mut heap_table = ctx.heap_table(table_id)?;
let rid = heap_table.insert(&row)?;
ctx.log_dml(record)?; // ERROR: ctx already borrowed by heap_table

// RIGHT: Drop heap_table before second borrow
let rid = {
    let mut heap_table = ctx.heap_table(table_id)?;
    heap_table.insert(&row)?
};
ctx.log_dml(record)?; // OK: heap_table dropped
```

### Row Access Pattern

```rust
// Row is a tuple struct: Row(Vec<Value>)
let row = Row(vec![Value::Int(42), Value::Text("foo".into())]);

// Access values via .0
let first_value = &row.0[0];
let values = &row.0;

// No Row::new() constructor exists!
```

### ColumnId Pattern

```rust
// ColumnId is a type alias: type ColumnId = u16
let col_id: ColumnId = 3;
let idx = col_id as usize; // Direct cast, no .0 accessor

// Used in ResolvedExpr::Column(ColumnId)
match expr {
    ResolvedExpr::Column(col_id) => {
        let idx = *col_id as usize;
        row.0.get(idx)?.clone()
    }
}
```

## Future Extensions (Beyond v1)

- **RID Tracking**: Modify `Row` to carry `RecordId` or return `(Row, RID)` tuples to enable Update/Delete
- **IndexScan Implementation**: Query B+Tree index (requires `index` crate), fetch rows by RID
- **Join Operators**: HashJoin, MergeJoin (requires multi-input operator support)
- **Sort Operator**: External merge sort for ORDER BY
- **Aggregate Operator**: Hash aggregation for GROUP BY
- **Limit/Offset**: Row count limits
- **Vectorization**: Process rows in batches instead of one-at-a-time
- **Parallel Execution**: Split scans across threads
- **Expression JIT**: Compile hot predicates for faster evaluation

## Known Limitations (v1)

1. **Update/Delete Don't Modify Storage**: Operators count matching rows but don't call `heap_table.update/delete()` because `Row` doesn't carry `RecordId`. Need to propagate RID through iterator pipeline.

2. **SeqScan Page Count Heuristic**: Uses trial-and-error probing to find total pages instead of querying `HeapFile` metadata. Works but inefficient. Should add `num_pages()` method to `HeapTable` trait.

3. **Slot Iteration Logic**: Assumes max 100 slots per page and moves to next page after 100 empty slots. Should use proper page header to determine actual slot count.

4. **No Transaction Support**: All operations auto-commit. No begin/commit/rollback or multi-statement transactions.

5. **No Concurrency Control**: No locking or MVCC. Single-threaded execution only.

6. **Arithmetic Operators Missing**: Expression evaluator only supports comparison (Eq/Ne/Lt/Le/Gt/Ge) and logical (And/Or/Not) operators. `expr` crate doesn't define Plus/Minus/Multiply/Divide variants yet.

7. **Test Coverage Low**: Only placeholder tests exist. Need comprehensive unit and integration tests to reach 90%+ coverage goal.

## Debugging Tips

- Add temporary debug prints in `next()` to trace row flow through operators
- Use `explain_physical()` from planner to inspect plan structure before execution
- Check `ExecutionContext` catalog/pager/wal state when operators fail
- Test operators in isolation with mock inputs before integration testing
- Verify RIDs are valid before calling `HeapTable::get()` (page exists, slot allocated)
- Check expression evaluation separately with known row values

## Collaboration & Pull Requests

- Mention affected crates: planner for plan changes, storage for HeapTable interface changes
- Include validation commands: `cargo test -p executor` and integration tests
- When adding operators, document expected input/output schema transformations
- When modifying expression evaluation, test with all value type combinations
- Include example queries that exercise new operators

## Testing Priority (Next Steps)

1. **High Priority - Basic Functionality**:
   - SeqScan: reads all inserted rows
   - Filter: applies predicates correctly
   - Insert: persists data and logs to WAL
   - Expression evaluation: all operators work

2. **Medium Priority - Integration**:
   - End-to-end SELECT query
   - WAL replay after simulated crash
   - Complex filter predicates (AND/OR combinations)

3. **Future - Advanced Features**:
   - Update/Delete with RID tracking
   - IndexScan with B+Tree
   - Join operators
   - Aggregate operators

By following these guidelines, you can extend the executor with new operators, improve test coverage, and integrate advanced features while maintaining consistency with the rest of the workspace.
