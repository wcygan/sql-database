//! Test support utilities for the SQL database workspace.
//!
//! This crate provides comprehensive testing infrastructure including:
//! - Isolated test execution contexts with temporary storage
//! - SQL script execution with pretty-printed output for snapshot testing
//! - Common test fixtures and data generators
//! - Property-based test generators for core types
//! - Custom assertion helpers
//! - **Test setup macros** for reducing boilerplate (see MACROS.md)
//!
//! # Quick Start
//!
//! ## Using Test Macros (Recommended)
//!
//! The fastest way to set up tests is using the provided macros:
//!
//! ```
//! use testsupport::prelude::*;
//! use types::SqlType;
//!
//! // Setup database with one macro call (replaces 17 lines!)
//! test_db!(mut ctx, table: "users",
//!          cols: ["id" => SqlType::Int, "name" => SqlType::Text]);
//!
//! let mut exec_ctx = ctx.execution_context();
//! // Execute queries using exec_ctx...
//! ```
//!
//! See [MACROS.md](../MACROS.md) for complete macro documentation.
//!
//! ## Using SQL Scripts
//!
//! ```no_run
//! use testsupport::prelude::*;
//!
//! #[test]
//! fn test_basic_query() {
//!     let output = run_sql_script(r#"
//!         CREATE TABLE users (id INT, name TEXT, age INT);
//!         INSERT INTO users VALUES (1, 'Alice', 30);
//!         SELECT * FROM users WHERE age > 25;
//!     "#).unwrap();
//!
//!     insta::assert_snapshot!(output);
//! }
//! ```
//!
//! # Available Macros
//!
//! ## Test Setup Macros
//!
//! - **`test_db!`** - Database context with catalog, pager, WAL (saves 14 lines)
//! - **`test_pager!`** - Buffer pool pager setup (saves 3 lines)
//! - **`test_wal!`** - Write-ahead log setup (saves 2 lines)
//! - **`row!`** - Typed row construction with variants (int, text, bool)
//!
//! ## Expression Builder Macros
//!
//! - **`lit!`** - Create literal expressions: `lit!(int: 42)`, `lit!(text: "foo")`
//! - **`col!`** - Create column references: `col!(0)`
//! - **`binary!`** - Create binary expressions: `binary!(col!(0), BinaryOp::Eq, lit!(int: 1))`
//! - **`unary!`** - Create unary expressions: `unary!(UnaryOp::Not, col!(2))`

pub mod assertions;
pub mod context;
pub mod fixtures;
pub mod macros;
pub mod proptest_generators;
pub mod runner;

/// Convenient re-exports for common testing patterns.
pub mod prelude {
    pub use crate::assertions::*;
    pub use crate::context::*;
    pub use crate::fixtures::*;
    pub use crate::runner::*;

    // Re-export test setup macros
    pub use crate::row;
    pub use crate::test_db;
    pub use crate::test_pager;
    pub use crate::test_wal;

    // Re-export expression builder macros
    pub use crate::binary;
    pub use crate::col;
    pub use crate::lit;
    pub use crate::unary;
}
