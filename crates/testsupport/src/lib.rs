//! Test support utilities for the SQL database workspace.
//!
//! This crate provides comprehensive testing infrastructure including:
//! - Isolated test execution contexts with temporary storage
//! - SQL script execution with pretty-printed output for snapshot testing
//! - Common test fixtures and data generators
//! - Property-based test generators for core types
//! - Custom assertion helpers
//!
//! # Example Usage
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

pub mod assertions;
pub mod context;
pub mod fixtures;
pub mod proptest_generators;
pub mod runner;

/// Convenient re-exports for common testing patterns.
pub mod prelude {
    pub use crate::assertions::*;
    pub use crate::context::*;
    pub use crate::fixtures::*;
    pub use crate::runner::*;
}
