//! Test helpers and utilities for executor tests.

use crate::{ExecutionContext, Executor};
use catalog::{Catalog, Column};
use common::{ColumnId, DbError, DbResult, Row};
use expr::{BinaryOp, UnaryOp};
use planner::ResolvedExpr;
use std::collections::VecDeque;
use types::{SqlType, Value};

/// Mock executor for testing operators in isolation.
///
/// Allows controlled row iteration and error injection for testing.
pub struct MockExecutor {
    rows: VecDeque<Row>,
    schema: Vec<String>,
    next_error: Option<DbError>,
    open_called: bool,
    close_called: bool,
    open_error: Option<DbError>,
    close_error: Option<DbError>,
}

impl MockExecutor {
    /// Create a mock executor that returns the given rows.
    pub fn new(rows: Vec<Row>, schema: Vec<String>) -> Self {
        Self {
            rows: rows.into(),
            schema,
            next_error: None,
            open_called: false,
            close_called: false,
            open_error: None,
            close_error: None,
        }
    }

    /// Create a mock executor that returns an error on next().
    pub fn with_next_error(error: DbError) -> Self {
        Self {
            rows: VecDeque::new(),
            schema: vec![],
            next_error: Some(error),
            open_called: false,
            close_called: false,
            open_error: None,
            close_error: None,
        }
    }

    /// Create a mock executor that returns an error on open().
    #[allow(dead_code)]
    pub fn with_open_error(error: DbError) -> Self {
        Self {
            rows: VecDeque::new(),
            schema: vec![],
            next_error: None,
            open_called: false,
            close_called: false,
            open_error: Some(error),
            close_error: None,
        }
    }

    /// Create a mock executor that returns an error on close().
    #[allow(dead_code)]
    pub fn with_close_error(error: DbError) -> Self {
        Self {
            rows: VecDeque::new(),
            schema: vec![],
            next_error: None,
            open_called: false,
            close_called: false,
            open_error: None,
            close_error: Some(error),
        }
    }

    /// Check if open() was called.
    #[allow(dead_code)]
    pub fn was_opened(&self) -> bool {
        self.open_called
    }

    /// Check if close() was called.
    #[allow(dead_code)]
    pub fn was_closed(&self) -> bool {
        self.close_called
    }
}

impl Executor for MockExecutor {
    fn open(&mut self, _ctx: &mut ExecutionContext) -> DbResult<()> {
        self.open_called = true;
        if let Some(error) = self.open_error.take() {
            return Err(error);
        }
        Ok(())
    }

    fn next(&mut self, _ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        if let Some(error) = self.next_error.take() {
            return Err(error);
        }
        Ok(self.rows.pop_front())
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> DbResult<()> {
        self.close_called = true;
        if let Some(error) = self.close_error.take() {
            return Err(error);
        }
        Ok(())
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }
}

/// Create a simple test catalog with a users table.
pub fn create_test_catalog() -> Catalog {
    let mut catalog = Catalog::new();

    let columns = vec![
        Column::new("id", SqlType::Int),
        Column::new("name", SqlType::Text),
        Column::new("active", SqlType::Bool),
    ];

    catalog
        .create_table("users", columns)
        .expect("failed to create table");

    catalog
}

// Row builders

/// Create a row with integer values.
pub fn int_row(values: &[i64]) -> Row {
    Row::new(values.iter().map(|&v| Value::Int(v)).collect())
}

/// Create a row with text values.
pub fn text_row(values: &[&str]) -> Row {
    Row::new(values.iter().map(|&v| Value::Text(v.to_string())).collect())
}

/// Create a row with boolean values.
#[allow(dead_code)]
pub fn bool_row(values: &[bool]) -> Row {
    Row::new(values.iter().map(|&v| Value::Bool(v)).collect())
}

/// Create a row with mixed values.
#[allow(dead_code)]
pub fn make_row(values: Vec<Value>) -> Row {
    Row::new(values)
}

// Expression builders

/// Create a literal expression.
#[allow(dead_code)]
pub fn lit(value: Value) -> ResolvedExpr {
    ResolvedExpr::Literal(value)
}

/// Create a literal integer expression.
pub fn lit_int(value: i64) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Int(value))
}

/// Create a literal text expression.
pub fn lit_text(value: &str) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Text(value.to_string()))
}

/// Create a literal boolean expression.
pub fn lit_bool(value: bool) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Bool(value))
}

/// Create a literal NULL expression.
pub fn lit_null() -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Null)
}

/// Create a column reference expression.
pub fn col(id: ColumnId) -> ResolvedExpr {
    ResolvedExpr::Column(id)
}

/// Create a binary expression.
pub fn binary(left: ResolvedExpr, op: BinaryOp, right: ResolvedExpr) -> ResolvedExpr {
    ResolvedExpr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

/// Create a unary expression.
pub fn unary(op: UnaryOp, expr: ResolvedExpr) -> ResolvedExpr {
    ResolvedExpr::Unary {
        op,
        expr: Box::new(expr),
    }
}

// Assertion helpers

/// Assert that next() returns the expected row.
pub fn assert_next_row(exec: &mut dyn Executor, ctx: &mut ExecutionContext, expected: Row) {
    match exec.next(ctx) {
        Ok(Some(row)) => assert_eq!(row.values, expected.values, "row mismatch"),
        Ok(None) => panic!("expected row, got None"),
        Err(e) => panic!("expected row, got error: {}", e),
    }
}

/// Assert that next() returns None (exhausted).
pub fn assert_exhausted(exec: &mut dyn Executor, ctx: &mut ExecutionContext) {
    match exec.next(ctx) {
        Ok(None) => {}
        Ok(Some(row)) => panic!("expected exhausted, got row: {:?}", row),
        Err(e) => panic!("expected exhausted, got error: {}", e),
    }
}

/// Assert that a result is an error containing the expected message.
pub fn assert_error_contains<T: std::fmt::Debug>(result: DbResult<T>, expected_msg: &str) {
    match result {
        Ok(val) => panic!(
            "expected error containing '{}', got Ok({:?})",
            expected_msg, val
        ),
        Err(e) => {
            let error_str = format!("{}", e);
            assert!(
                error_str.contains(expected_msg),
                "expected error containing '{}', got '{}'",
                expected_msg,
                error_str
            );
        }
    }
}

/// Assert that a result is a specific error variant.
#[allow(dead_code)]
pub fn assert_executor_error<T: std::fmt::Debug>(result: DbResult<T>) {
    match result {
        Ok(val) => panic!("expected Executor error, got Ok({:?})", val),
        Err(DbError::Executor(_)) => {}
        Err(e) => panic!("expected Executor error, got {}", e),
    }
}
