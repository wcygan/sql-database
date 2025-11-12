# Expression Builder Macros Implementation

## Overview

Implemented declarative macros in `testsupport` to replace repetitive expression helper functions in executor tests. These macros provide cleaner syntax and eliminate boilerplate for creating `ResolvedExpr` instances.

## Macros Implemented

### 1. `lit!` - Literal Expressions

Creates `ResolvedExpr::Literal` with type-specific variants.

**Syntax:**
```rust
lit!(int: 42)              // Integer literal
lit!(text: "alice")        // Text literal
lit!(bool: true)           // Boolean literal
lit!(Value::Null)          // NULL literal
lit!(existing_value)       // Wrap existing Value
```

**Before:**
```rust
pub fn lit_int(value: i64) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Int(value))
}
pub fn lit_text(value: &str) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Text(value.to_string()))
}
pub fn lit_bool(value: bool) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Bool(value))
}
pub fn lit_null() -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Null)
}
```

**After:**
```rust
// All replaced by single macro with variants
let int_expr = lit!(int: 42);
let text_expr = lit!(text: "alice");
let bool_expr = lit!(bool: true);
let null_expr = lit!(Value::Null);
```

**Savings:** 4 functions → 1 macro with variants

---

### 2. `col!` - Column References

Creates `ResolvedExpr::Column` with automatic type casting.

**Syntax:**
```rust
col!(0)     // Column 0
col!(5)     // Column 5
```

**Before:**
```rust
pub fn col(id: ColumnId) -> ResolvedExpr {
    ResolvedExpr::Column(id)
}
```

**After:**
```rust
let id_col = col!(0);
let name_col = col!(1);
```

**Savings:** 1 function → 1 macro (cleaner syntax)

---

### 3. `binary!` - Binary Expressions

Creates `ResolvedExpr::Binary` with automatic boxing.

**Syntax:**
```rust
binary!(left, op, right)
```

**Before:**
```rust
pub fn binary(left: ResolvedExpr, op: BinaryOp, right: ResolvedExpr) -> ResolvedExpr {
    ResolvedExpr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}
```

**After:**
```rust
// id = 42
let expr = binary!(col!(0), BinaryOp::Eq, lit!(int: 42));

// Complex: (id = 1) AND (age > 30)
let expr = binary!(
    binary!(col!(0), BinaryOp::Eq, lit!(int: 1)),
    BinaryOp::And,
    binary!(col!(2), BinaryOp::Gt, lit!(int: 30))
);
```

**Savings:** Automatic boxing, cleaner composition

---

### 4. `unary!` - Unary Expressions

Creates `ResolvedExpr::Unary` with automatic boxing.

**Syntax:**
```rust
unary!(op, expr)
```

**Before:**
```rust
pub fn unary(op: UnaryOp, expr: ResolvedExpr) -> ResolvedExpr {
    ResolvedExpr::Unary {
        op,
        expr: Box::new(expr),
    }
}
```

**After:**
```rust
// NOT active
let expr = unary!(UnaryOp::Not, col!(2));
```

**Savings:** Automatic boxing

---

## Migration Path

### Deprecated Functions

The original helper functions in `crates/executor/src/tests/helpers.rs` are now deprecated with helpful migration messages:

```rust
#[deprecated(note = "Use `lit!(int: value)` macro from testsupport instead")]
pub fn lit_int(value: i64) -> ResolvedExpr { ... }

#[deprecated(note = "Use `col!(id)` macro from testsupport instead")]
pub fn col(id: ColumnId) -> ResolvedExpr { ... }

// ... etc
```

Existing code continues to work but shows deprecation warnings guiding users to the new macros.

### Usage

```rust
use testsupport::prelude::*;  // Imports all macros

// Old style (still works, but deprecated)
let expr = lit_int(42);

// New style
let expr = lit!(int: 42);
```

---

## Examples

### Simple Expression

**Old:**
```rust
let expr = binary(
    col(0),
    BinaryOp::Eq,
    lit_int(42)
);
```

**New:**
```rust
let expr = binary!(col!(0), BinaryOp::Eq, lit!(int: 42));
```

### Complex Expression

**Old:**
```rust
let expr = binary(
    binary(col(0), BinaryOp::Eq, lit_int(1)),
    BinaryOp::And,
    binary(col(2), BinaryOp::Gt, lit_int(30))
);
```

**New:**
```rust
let expr = binary!(
    binary!(col!(0), BinaryOp::Eq, lit!(int: 1)),
    BinaryOp::And,
    binary!(col!(2), BinaryOp::Gt, lit!(int: 30))
);
```

### Predicate Builder

**Old:**
```rust
fn make_filter(id: i64, name: &str) -> ResolvedExpr {
    binary(
        col(0),
        BinaryOp::Eq,
        lit_int(id)
    )
}
```

**New:**
```rust
fn make_filter(id: i64, name: &str) -> ResolvedExpr {
    binary!(col!(0), BinaryOp::Eq, lit!(int: id))
}
```

---

## Implementation Details

### Location

- **Macros:** `crates/testsupport/src/macros.rs` (lines 203-401)
- **Tests:** Unit tests in same file (lines 576-659)
- **Doc tests:** 9 doc test examples
- **Deprecated functions:** `crates/executor/src/tests/helpers.rs` (lines 162-240)

### Test Coverage

```rust
#[test]
fn test_lit_macro_int() {
    let expr = lit!(int: 42);
    assert!(matches!(expr, ResolvedExpr::Literal(Value::Int(42))));
}

#[test]
fn test_binary_macro() {
    let expr = binary!(col!(0), BinaryOp::Eq, lit!(int: 42));
    match expr {
        ResolvedExpr::Binary { left, op, right } => {
            assert!(matches!(*left, ResolvedExpr::Column(0)));
            assert!(matches!(op, BinaryOp::Eq));
            assert!(matches!(*right, ResolvedExpr::Literal(Value::Int(42))));
        }
        _ => panic!("expected binary expression"),
    }
}

#[test]
fn test_complex_expression() {
    let expr = binary!(
        binary!(col!(0), BinaryOp::Eq, lit!(int: 1)),
        BinaryOp::And,
        binary!(col!(2), BinaryOp::Gt, lit!(int: 30))
    );
    assert!(matches!(expr, ResolvedExpr::Binary { .. }));
}
```

**Total Tests:**
- 9 unit tests for expression macros
- 9 doc tests
- All passing ✓

---

## Benefits

### Code Reduction

- **Before:** 6 helper functions (40+ lines)
- **After:** 4 macros with variants (same functionality, more features)

### Improved Ergonomics

1. **Type-specific variants:** `lit!(int: 42)` is clearer than `lit_int(42)`
2. **Consistent syntax:** All macros follow similar patterns
3. **Composability:** Macros nest cleanly for complex expressions
4. **Automatic type handling:** No need to manually wrap values

### Better Test Readability

**Before:**
```rust
let predicate = binary(
    col(0),
    BinaryOp::Eq,
    lit_int(1)
);
```

**After:**
```rust
let predicate = binary!(col!(0), BinaryOp::Eq, lit!(int: 1));
```

More concise, equally readable, and the `lit!(int: ...)` syntax makes the type explicit.

---

## Verification

All tests pass:

```bash
# Test expression macros
cargo test -p testsupport --lib macros::tests
# Output: 19 passed (including 9 new expression macro tests)

# Test doc tests
cargo test --doc -p testsupport
# Output: 48 passed; 0 failed; 0 ignored

# Test executor (uses deprecated functions, still works)
cargo test -p executor
# Output: 161 passed; 0 failed; 0 ignored

# Full workspace
cargo test --workspace --lib
# Output: All tests pass ✓
```

---

## Migration Timeline

1. **Current:** Both macros and functions available
2. **Near-term:** Deprecation warnings guide users to macros
3. **Future:** Consider removing deprecated functions after full migration

---

## Documentation

### Updated Files

- `crates/testsupport/src/lib.rs` - Added expression macros to overview
- `crates/testsupport/src/macros.rs` - Full macro documentation with examples
- `crates/executor/src/tests/helpers.rs` - Deprecation notices with migration guide

### User Guide

See `crates/testsupport/MACROS.md` for complete macro documentation including:
- Syntax reference
- Before/after examples
- Migration guide
- Best practices

---

## Summary

Successfully implemented 4 expression builder macros that:

✅ Replace 6 repetitive helper functions
✅ Provide cleaner, more expressive syntax
✅ Maintain backward compatibility via deprecation
✅ Include comprehensive tests (18 tests total)
✅ Are fully documented with examples
✅ Integrate seamlessly with existing test infrastructure

The macros are production-ready and available via `testsupport::prelude::*`.
