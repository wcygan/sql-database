# Test Macros Guide

This document describes the declarative macros provided by the `testsupport` crate to reduce test boilerplate across the SQL database workspace.

## Overview

The `testsupport` crate provides four main macros that dramatically simplify test setup:

1. **`test_db!`** - Database context with catalog, tables, and schemas
2. **`test_pager!`** - Buffer pool pager setup
3. **`test_wal!`** - Write-ahead log setup
4. **`row!`** - Typed row construction

These macros are available through `testsupport::prelude::*`.

---

## `test_db!` - Database Context Setup

Replaces 15-20 lines of catalog/pager/WAL setup with a single macro call.

### Syntax

```rust
// Single table without primary key
test_db!(context_var, table: "name", cols: ["col1" => Type1, "col2" => Type2, ...])

// Single table with primary key
test_db!(context_var, table: "name",
         cols: ["col1" => Type1, "col2" => Type2, ...],
         pk: [col_index1, col_index2, ...])

// Multiple tables
test_db!(context_var, tables: [
    ("table1", ["col1" => Type1, "col2" => Type2]),
    ("table2", ["col1" => Type1, "col2" => Type2])
])
```

### Examples

#### Basic Single Table

**Before (17 lines):**
```rust
let temp_dir = tempfile::tempdir().unwrap();
let mut catalog = Catalog::new();
catalog.create_table(
    "users",
    vec![
        Column::new("id", SqlType::Int),
        Column::new("name", SqlType::Text),
        Column::new("active", SqlType::Bool),
    ],
    None
).unwrap();

let catalog = Box::leak(Box::new(catalog));
let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
let wal = Box::leak(Box::new(wal::Wal::open(temp_dir.path().join("test.wal")).unwrap()));
let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());
```

**After (3 lines):**
```rust
test_db!(test_ctx, table: "users",
         cols: ["id" => SqlType::Int, "name" => SqlType::Text, "active" => SqlType::Bool]);
let mut ctx = test_ctx.execution_context();
```

**Savings: 14 lines (82% reduction)**

#### With Primary Key

```rust
test_db!(test_ctx, table: "users",
         cols: ["id" => SqlType::Int, "name" => SqlType::Text],
         pk: [0]); // PRIMARY KEY (id)

let mut ctx = test_ctx.execution_context();
let table_id = TableId(1);
```

#### Composite Primary Key

```rust
test_db!(test_ctx, table: "users",
         cols: ["id" => SqlType::Int, "name" => SqlType::Text, "email" => SqlType::Text],
         pk: [0, 2]); // PRIMARY KEY (id, email)
```

#### Multiple Tables

```rust
test_db!(test_ctx, tables: [
    ("users", ["id" => SqlType::Int, "name" => SqlType::Text]),
    ("posts", ["id" => SqlType::Int, "title" => SqlType::Text, "user_id" => SqlType::Int]),
    ("comments", ["id" => SqlType::Int, "post_id" => SqlType::Int, "text" => SqlType::Text])
]);

let ctx = test_ctx.execution_context();
assert!(ctx.catalog.table("users").is_ok());
assert!(ctx.catalog.table("posts").is_ok());
assert!(ctx.catalog.table("comments").is_ok());
```

### What You Get

- `TestContext` instance with isolated temporary storage
- Initialized catalog with specified tables
- FilePager with 10-page buffer pool
- WAL file for durability
- Call `.execution_context()` to get `ExecutionContext` for query execution

---

## `test_pager!` - Buffer Pool Setup

Simplifies buffer pool tests by creating temporary directory, FilePager, and TableId.

### Syntax

```rust
// Default capacity (10 pages)
test_pager!(pager_var, table_var)

// Custom capacity
test_pager!(pager_var, table_var, capacity: N)
```

### Examples

#### Basic Pager Setup

**Before (4 lines):**
```rust
let dir = tempfile::tempdir().unwrap();
let mut pager = FilePager::new(dir.path(), 2);
let table = TableId(1);
```

**After (1 line):**
```rust
test_pager!(pager, table);
```

**Savings: 3 lines (75% reduction)**

#### Custom Capacity for LRU Testing

```rust
test_pager!(pager, table, capacity: 1); // Small buffer for eviction tests

let pid1 = pager.allocate_page(table).unwrap();
pager.fetch_page(table, pid1).unwrap().data[0] = 99;

// Allocate another to trigger eviction
let _pid2 = pager.allocate_page(table).unwrap();
```

#### Page Allocation Test

```rust
test_pager!(pager, table, capacity: 10);

let pid0 = pager.allocate_page(table).unwrap();
let pid1 = pager.allocate_page(table).unwrap();
let pid2 = pager.allocate_page(table).unwrap();

assert_eq!(pid0.0, 0);
assert_eq!(pid1.0, 1);
assert_eq!(pid2.0, 2);
```

### What You Get

- Temporary directory (auto-cleaned on drop)
- `FilePager` instance with specified capacity
- `TableId(1)` for page operations

---

## `test_wal!` - Write-Ahead Log Setup

Creates temporary directory and WAL file for durability tests.

### Syntax

```rust
test_wal!(wal_var)
```

### Example

**Before (3 lines):**
```rust
let dir = tempfile::tempdir().unwrap();
let wal_path = dir.path().join("test.wal");
let mut wal = wal::Wal::open(&wal_path).unwrap();
```

**After (1 line):**
```rust
test_wal!(wal);
```

**Savings: 2 lines (67% reduction)**

#### WAL Append and Replay

```rust
test_wal!(wal);

let record = WalRecord::Insert {
    table: TableId(1),
    row: vec![Value::Int(1), Value::Text("Alice".into())],
    rid: RecordId { page_id: PageId(0), slot: 0 },
};

wal.append(&record).unwrap();
wal.sync().unwrap();
```

### What You Get

- Temporary directory (auto-cleaned on drop)
- `Wal` instance ready for append/sync/replay operations

---

## `row!` - Typed Row Construction

Simplifies row creation for tests with type-specific variants.

### Syntax

```rust
// Mixed values
row![Value1, Value2, ...]

// All integers
row![int: 1, 2, 3]

// All text
row![text: "alice", "bob"]

// All booleans
row![bool: true, false, true]
```

### Examples

#### Mixed Types

**Before:**
```rust
let row = Row::new(vec![
    Value::Int(1),
    Value::Text("Alice".to_string()),
    Value::Bool(true),
]);
```

**After:**
```rust
let row = row![
    Value::Int(1),
    Value::Text("Alice".to_string()),
    Value::Bool(true)
];
```

#### Type-Specific Variants

```rust
// Integer rows
let ids = row![int: 1, 2, 3, 4, 5];

// Text rows
let names = row![text: "alice", "bob", "charlie"];

// Boolean rows
let flags = row![bool: true, false, true];
```

**Savings: Automatic type wrapping, more readable test data**

---

## Impact Summary

### Lines of Code Saved

| Pattern | Before | After | Savings |
|---------|--------|-------|---------|
| Database setup | 17 lines | 3 lines | 82% |
| Pager setup | 4 lines | 1 line | 75% |
| WAL setup | 3 lines | 1 line | 67% |
| Row creation | 5 lines | 1 line | 80% |

### Codebase-Wide Impact

- **70+ test setups** simplified across executor, buffer, storage, wal crates
- **~200 lines removed** from test code
- **Improved readability**: Setup intent is clear and declarative
- **Consistent patterns**: All tests use same infrastructure

---

## Usage Patterns

### Integration Test Pattern

```rust
use testsupport::prelude::*;
use types::SqlType;

#[test]
fn test_query_with_filter() {
    // Setup (3 lines)
    test_db!(test_ctx, table: "users",
             cols: ["id" => SqlType::Int, "name" => SqlType::Text, "age" => SqlType::Int]);
    let mut ctx = test_ctx.execution_context();

    // Insert test data
    let table_id = TableId(1);
    let rows = vec![
        row![int: 1, text: "Alice", int: 30],
        row![int: 2, text: "Bob", int: 25],
    ];
    insert_test_rows(&mut ctx, table_id, rows).unwrap();

    // Execute query and verify results
    // ...
}
```

### Buffer Pool Pattern

```rust
use testsupport::prelude::*;

#[test]
fn test_lru_eviction() {
    test_pager!(pager, table, capacity: 2);

    // Test eviction logic
    let pid1 = pager.allocate_page(table).unwrap();
    let pid2 = pager.allocate_page(table).unwrap();
    let pid3 = pager.allocate_page(table).unwrap(); // Triggers eviction

    // Verify behavior...
}
```

### Multi-Table Pattern

```rust
#[test]
fn test_join_query() {
    test_db!(test_ctx, tables: [
        ("users", ["id" => SqlType::Int, "name" => SqlType::Text]),
        ("orders", ["id" => SqlType::Int, "user_id" => SqlType::Int, "total" => SqlType::Int])
    ]);

    let mut ctx = test_ctx.execution_context();
    // Setup join test...
}
```

---

## Migration Guide

### Converting Existing Tests

1. Add `testsupport` to `dev-dependencies` if needed:
   ```toml
   [dev-dependencies]
   testsupport = { workspace = true }
   ```

2. Import macros:
   ```rust
   use testsupport::prelude::*;
   ```

3. Replace setup code with macro:
   ```rust
   // Old:
   // let temp_dir = tempfile::tempdir().unwrap();
   // let mut catalog = Catalog::new();
   // ...17 lines...

   // New:
   test_db!(test_ctx, table: "users", cols: ["id" => SqlType::Int, "name" => SqlType::Text]);
   let mut ctx = test_ctx.execution_context();
   ```

4. Update variable names if needed (old `ctx` → new `test_ctx.execution_context()`)

### Gradual Adoption

- New tests should use macros by default
- Existing tests can be migrated opportunistically
- No breaking changes to existing test infrastructure

---

## Examples in the Codebase

See these files for complete examples:

- **`crates/executor/src/tests/macro_demo.rs`** - Database setup examples
- **`crates/buffer/src/tests_macro_demo.rs`** - Pager setup examples
- **`crates/testsupport/src/macros.rs`** - Macro implementations and unit tests

---

## Future Enhancements

Potential additions based on usage patterns:

1. **Index creation macro**: Simplify index setup in catalog
2. **Assertion macros**: `assert_next!`, `assert_error_contains!`
3. **Expression builder macros**: `lit!`, `col!`, `binary!`
4. **Data generation macros**: Bulk insert helpers

---

## Best Practices

### Do

✅ Use `test_db!` for all executor/planner tests needing catalog
✅ Use `test_pager!` for buffer pool-specific tests
✅ Use `test_wal!` for durability/recovery tests
✅ Use `row!` for readable test data construction
✅ Keep table schemas simple in tests (3-5 columns max)

### Don't

❌ Don't use macros when you need custom setup (e.g., specific temp dir handling)
❌ Don't nest macro calls excessively
❌ Don't use `test_db!` for tests that don't need catalog (use `TestContext::new()` directly)
❌ Don't create overly complex schemas in macro calls (extract to helper if needed)

---

## Troubleshooting

### "use of unresolved module or unlinked crate `testsupport`"

Add `testsupport` to `[dev-dependencies]`:
```toml
[dev-dependencies]
testsupport = { workspace = true }
```

### "method not found in `FilePager`"

Import the `Pager` trait:
```rust
use buffer::Pager;
```

### "cannot borrow as mutable"

Remember to destructure `TestContext`:
```rust
test_db!(test_ctx, ...);
let mut ctx = test_ctx.execution_context(); // Creates ExecutionContext
```

### Macro expansion debugging

Use `cargo expand` to see expanded macro code:
```bash
cargo expand -p testsupport --test macros
```

---

## Implementation Details

All macros are declarative (`macro_rules!`) and:

- Expand at compile time (zero runtime cost)
- Type-safe (compiler validates all inputs)
- Hygienic (don't pollute namespace)
- Well-documented with examples

See `crates/testsupport/src/macros.rs` for full implementations.
