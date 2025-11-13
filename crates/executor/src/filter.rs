//! Filter operator: applies WHERE predicates.

use crate::{ExecutionContext, Executor};
use common::{DbResult, ExecutionStats, Row};
use planner::ResolvedExpr;
use std::time::Instant;
use types::Value;

/// Filter operator - applies a predicate to rows from a child operator.
///
/// Only passes through rows where the predicate evaluates to true.
/// NULL predicate results are treated as false (SQL semantics).
pub struct FilterExec {
    input: Box<dyn Executor>,
    predicate: ResolvedExpr,
    stats: ExecutionStats,
}

impl FilterExec {
    /// Create a new filter operator.
    pub fn new(input: Box<dyn Executor>, predicate: ResolvedExpr) -> Self {
        Self {
            input,
            predicate,
            stats: ExecutionStats::default(),
        }
    }

    /// Evaluate the predicate against a row.
    fn eval_predicate(&self, row: &Row) -> DbResult<bool> {
        let result = eval_resolved_expr(&self.predicate, row)?;

        // NULL is treated as false in WHERE clauses
        match result {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            other => Err(common::DbError::Executor(format!(
                "predicate must evaluate to boolean, got {:?}",
                other
            ))),
        }
    }
}

impl Executor for FilterExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        self.stats = ExecutionStats::default();
        let result = self.input.open(ctx)?;
        self.stats.open_time = start.elapsed();
        Ok(result)
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let start = Instant::now();

        loop {
            let row = match self.input.next(ctx)? {
                Some(r) => r,
                None => {
                    self.stats.total_next_time += start.elapsed();
                    return Ok(None);
                }
            };

            if self.eval_predicate(&row)? {
                self.stats.rows_produced += 1;
                self.stats.total_next_time += start.elapsed();
                return Ok(Some(row));
            } else {
                self.stats.rows_filtered += 1;
            }
        }
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        let result = self.input.close(ctx)?;
        self.stats.close_time = start.elapsed();
        Ok(result)
    }

    fn schema(&self) -> &[String] {
        self.input.schema()
    }

    fn stats(&self) -> Option<&ExecutionStats> {
        Some(&self.stats)
    }
}

/// Evaluate a resolved expression against a row.
///
/// This is the core expression evaluator for the executor. It handles
/// column references, literals, and unary/binary operations.
pub fn eval_resolved_expr(expr: &ResolvedExpr, row: &Row) -> DbResult<Value> {
    match expr {
        ResolvedExpr::Literal(v) => Ok(v.clone()),
        ResolvedExpr::Column(col_id) => {
            let idx = *col_id as usize;
            row.values
                .get(idx)
                .ok_or_else(|| {
                    common::DbError::Executor(format!(
                        "column index {} out of bounds (row has {} columns)",
                        idx,
                        row.values.len()
                    ))
                })
                .cloned()
        }
        ResolvedExpr::Unary { op, expr } => {
            let val = eval_resolved_expr(expr, row)?;
            eval_unary_op(*op, val)
        }
        ResolvedExpr::Binary { left, op, right } => {
            let left_val = eval_resolved_expr(left, row)?;
            let right_val = eval_resolved_expr(right, row)?;
            eval_binary_op(left_val, *op, right_val)
        }
    }
}

/// Evaluate a unary operation.
fn eval_unary_op(op: expr::UnaryOp, val: Value) -> DbResult<Value> {
    use expr::UnaryOp;

    match (op, val) {
        (UnaryOp::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
        (UnaryOp::Not, Value::Null) => Ok(Value::Null),
        (UnaryOp::Not, val) => Err(common::DbError::Executor(format!(
            "NOT requires boolean, got {:?}",
            val
        ))),
    }
}

/// Evaluate a binary operation.
fn eval_binary_op(left: Value, op: expr::BinaryOp, right: Value) -> DbResult<Value> {
    use expr::BinaryOp;

    // Handle NULL propagation
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    match (left, op, right) {
        // Comparison operators
        (Value::Int(a), BinaryOp::Eq, Value::Int(b)) => Ok(Value::Bool(a == b)),
        (Value::Int(a), BinaryOp::Ne, Value::Int(b)) => Ok(Value::Bool(a != b)),
        (Value::Int(a), BinaryOp::Lt, Value::Int(b)) => Ok(Value::Bool(a < b)),
        (Value::Int(a), BinaryOp::Le, Value::Int(b)) => Ok(Value::Bool(a <= b)),
        (Value::Int(a), BinaryOp::Gt, Value::Int(b)) => Ok(Value::Bool(a > b)),
        (Value::Int(a), BinaryOp::Ge, Value::Int(b)) => Ok(Value::Bool(a >= b)),

        (Value::Text(a), BinaryOp::Eq, Value::Text(b)) => Ok(Value::Bool(a == b)),
        (Value::Text(a), BinaryOp::Ne, Value::Text(b)) => Ok(Value::Bool(a != b)),

        (Value::Bool(a), BinaryOp::Eq, Value::Bool(b)) => Ok(Value::Bool(a == b)),
        (Value::Bool(a), BinaryOp::Ne, Value::Bool(b)) => Ok(Value::Bool(a != b)),

        // Logical operators
        (Value::Bool(a), BinaryOp::And, Value::Bool(b)) => Ok(Value::Bool(a && b)),
        (Value::Bool(a), BinaryOp::Or, Value::Bool(b)) => Ok(Value::Bool(a || b)),

        (left, op, right) => Err(common::DbError::Executor(format!(
            "invalid binary operation: {:?} {:?} {:?}",
            left, op, right
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::{
        assert_exhausted, assert_next_row, create_test_catalog, MockExecutor,
    };
    use expr::{BinaryOp, UnaryOp};
    use testsupport::prelude::*;

    // ===== FilterExec Tests =====

    #[test]
    fn filter_passes_matching_rows() {
        let rows = vec![int_row(&[1, 10]), int_row(&[2, 20]), int_row(&[3, 30])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "value".into()]));

        // WHERE value > 15
        let predicate = binary(col(1), BinaryOp::Gt, lit!(int: 15));
        let mut filter = FilterExec::new(input, predicate);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        filter.open(&mut ctx).unwrap();

        // Should return rows where value > 15
        assert_next_row(&mut filter, &mut ctx, int_row(&[2, 20]));
        assert_next_row(&mut filter, &mut ctx, int_row(&[3, 30]));
        assert_exhausted(&mut filter, &mut ctx);

        filter.close(&mut ctx).unwrap();
    }

    #[test]
    fn filter_blocks_non_matching_rows() {
        let rows = vec![int_row(&[1, 10]), int_row(&[2, 20])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "value".into()]));

        // WHERE value > 100 (no matches)
        let predicate = binary(col(1), BinaryOp::Gt, lit!(int: 100));
        let mut filter = FilterExec::new(input, predicate);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        filter.open(&mut ctx).unwrap();
        assert_exhausted(&mut filter, &mut ctx);
        filter.close(&mut ctx).unwrap();
    }

    #[test]
    fn filter_empty_input_returns_none() {
        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let predicate = lit!(bool: true);
        let mut filter = FilterExec::new(input, predicate);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        filter.open(&mut ctx).unwrap();
        assert_exhausted(&mut filter, &mut ctx);
        filter.close(&mut ctx).unwrap();
    }

    #[test]
    fn filter_null_predicate_treated_as_false() {
        let rows = vec![int_row(&[1]), int_row(&[2])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));
        let predicate = lit!(Value::Null); // Always NULL
        let mut filter = FilterExec::new(input, predicate);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        filter.open(&mut ctx).unwrap();
        assert_exhausted(&mut filter, &mut ctx); // NULL treated as false
        filter.close(&mut ctx).unwrap();
    }

    #[test]
    fn filter_non_boolean_predicate_returns_error() {
        let rows = vec![int_row(&[1])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));
        let predicate = lit!(int: 42); // Not a boolean
        let mut filter = FilterExec::new(input, predicate);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        filter.open(&mut ctx).unwrap();
        assert_error_contains(filter.next(&mut ctx), "boolean");
    }

    // ===== Expression Evaluation - Literals =====

    #[test]
    fn eval_literal_int() {
        let row = Row::new(vec![]);
        let expr = lit!(int: 42);
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Int(42));
    }

    #[test]
    fn eval_literal_text() {
        let row = Row::new(vec![]);
        let expr = lit!(text: "hello");
        assert_eq!(
            eval_resolved_expr(&expr, &row).unwrap(),
            Value::Text("hello".into())
        );
    }

    #[test]
    fn eval_literal_bool() {
        let row = Row::new(vec![]);
        let expr = lit!(bool: true);
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));
    }

    #[test]
    fn eval_literal_null() {
        let row = Row::new(vec![]);
        let expr = lit!(Value::Null);
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Null);
    }

    // ===== Expression Evaluation - Column References =====

    #[test]
    fn eval_column_by_id() {
        let row = int_row(&[10, 20, 30]);
        assert_eq!(eval_resolved_expr(&col(0), &row).unwrap(), Value::Int(10));
        assert_eq!(eval_resolved_expr(&col(1), &row).unwrap(), Value::Int(20));
        assert_eq!(eval_resolved_expr(&col(2), &row).unwrap(), Value::Int(30));
    }

    #[test]
    fn eval_column_out_of_bounds_returns_error() {
        let row = int_row(&[10, 20]);
        assert_error_contains(eval_resolved_expr(&col(5), &row), "out of bounds");
    }

    #[test]
    fn eval_column_first_position() {
        let row = text_row(&["first", "second"]);
        assert_eq!(
            eval_resolved_expr(&col(0), &row).unwrap(),
            Value::Text("first".into())
        );
    }

    #[test]
    fn eval_column_last_position() {
        let row = text_row(&["first", "second", "third"]);
        assert_eq!(
            eval_resolved_expr(&col(2), &row).unwrap(),
            Value::Text("third".into())
        );
    }

    // ===== Unary Operations =====

    #[test]
    fn eval_not_true_returns_false() {
        let row = Row::new(vec![]);
        let expr = unary(UnaryOp::Not, lit!(bool: true));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_not_false_returns_true() {
        let row = Row::new(vec![]);
        let expr = unary(UnaryOp::Not, lit!(bool: false));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));
    }

    #[test]
    fn eval_not_null_returns_null() {
        let row = Row::new(vec![]);
        let expr = unary(UnaryOp::Not, lit!(Value::Null));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Null);
    }

    #[test]
    fn eval_not_int_returns_error() {
        let row = Row::new(vec![]);
        let expr = unary(UnaryOp::Not, lit!(int: 42));
        assert_error_contains(eval_resolved_expr(&expr, &row), "NOT requires boolean");
    }

    #[test]
    fn eval_not_text_returns_error() {
        let row = Row::new(vec![]);
        let expr = unary(UnaryOp::Not, lit!(text: "foo"));
        assert_error_contains(eval_resolved_expr(&expr, &row), "NOT requires boolean");
    }

    // ===== Binary Operations - Int Comparison =====

    #[test]
    fn eval_int_eq() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 42), BinaryOp::Eq, lit!(int: 42));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(int: 42), BinaryOp::Eq, lit!(int: 43));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_int_ne() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 42), BinaryOp::Ne, lit!(int: 43));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(int: 42), BinaryOp::Ne, lit!(int: 42));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_int_lt() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 10), BinaryOp::Lt, lit!(int: 20));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(int: 20), BinaryOp::Lt, lit!(int: 10));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_int_le() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 10), BinaryOp::Le, lit!(int: 10));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(int: 20), BinaryOp::Le, lit!(int: 10));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_int_gt() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 20), BinaryOp::Gt, lit!(int: 10));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(int: 10), BinaryOp::Gt, lit!(int: 20));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_int_ge() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 10), BinaryOp::Ge, lit!(int: 10));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(int: 10), BinaryOp::Ge, lit!(int: 20));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    // ===== Binary Operations - Text Comparison =====

    #[test]
    fn eval_text_eq() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(text: "hello"), BinaryOp::Eq, lit!(text: "hello"));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(text: "hello"), BinaryOp::Eq, lit!(text: "world"));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_text_ne() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(text: "hello"), BinaryOp::Ne, lit!(text: "world"));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(text: "hello"), BinaryOp::Ne, lit!(text: "hello"));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    // ===== Binary Operations - Bool Comparison =====

    #[test]
    fn eval_bool_eq() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(bool: true), BinaryOp::Eq, lit!(bool: true));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(bool: true), BinaryOp::Eq, lit!(bool: false));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_bool_ne() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(bool: true), BinaryOp::Ne, lit!(bool: false));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));

        let expr = binary(lit!(bool: true), BinaryOp::Ne, lit!(bool: true));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    // ===== Binary Operations - Logical =====

    #[test]
    fn eval_and_true_true() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(bool: true), BinaryOp::And, lit!(bool: true));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));
    }

    #[test]
    fn eval_and_true_false() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(bool: true), BinaryOp::And, lit!(bool: false));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_and_false_false() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(bool: false), BinaryOp::And, lit!(bool: false));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    #[test]
    fn eval_or_true_true() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(bool: true), BinaryOp::Or, lit!(bool: true));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));
    }

    #[test]
    fn eval_or_true_false() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(bool: true), BinaryOp::Or, lit!(bool: false));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));
    }

    #[test]
    fn eval_or_false_false() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(bool: false), BinaryOp::Or, lit!(bool: false));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(false));
    }

    // ===== Binary Operations - NULL Propagation =====

    #[test]
    fn eval_null_eq_int_returns_null() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(Value::Null), BinaryOp::Eq, lit!(int: 42));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Null);
    }

    #[test]
    fn eval_int_eq_null_returns_null() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 42), BinaryOp::Eq, lit!(Value::Null));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Null);
    }

    #[test]
    fn eval_null_eq_null_returns_null() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(Value::Null), BinaryOp::Eq, lit!(Value::Null));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Null);
    }

    #[test]
    fn eval_null_and_bool_returns_null() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(Value::Null), BinaryOp::And, lit!(bool: true));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Null);
    }

    // ===== Binary Operations - Type Errors =====

    #[test]
    fn eval_int_eq_text_returns_error() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 42), BinaryOp::Eq, lit!(text: "42"));
        assert_error_contains(eval_resolved_expr(&expr, &row), "invalid binary operation");
    }

    #[test]
    fn eval_int_lt_bool_returns_error() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 42), BinaryOp::Lt, lit!(bool: true));
        assert_error_contains(eval_resolved_expr(&expr, &row), "invalid binary operation");
    }

    #[test]
    fn eval_and_with_int_returns_error() {
        let row = Row::new(vec![]);
        let expr = binary(lit!(int: 1), BinaryOp::And, lit!(bool: true));
        assert_error_contains(eval_resolved_expr(&expr, &row), "invalid binary operation");
    }

    // ===== Complex Expressions =====

    #[test]
    fn eval_nested_and_or() {
        let row = Row::new(vec![]);
        // (true OR false) AND true
        let left = binary(lit!(bool: true), BinaryOp::Or, lit!(bool: false));
        let expr = binary(left, BinaryOp::And, lit!(bool: true));
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));
    }

    #[test]
    fn eval_nested_not() {
        let row = Row::new(vec![]);
        // NOT (NOT true)
        let inner = unary(UnaryOp::Not, lit!(bool: true));
        let expr = unary(UnaryOp::Not, inner);
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));
    }

    #[test]
    fn eval_comparison_in_logical() {
        let row = int_row(&[10, 20]);
        // (col0 < 15) AND (col1 > 15)
        let left = binary(col(0), BinaryOp::Lt, lit!(int: 15));
        let right = binary(col(1), BinaryOp::Gt, lit!(int: 15));
        let expr = binary(left, BinaryOp::And, right);
        assert_eq!(eval_resolved_expr(&expr, &row).unwrap(), Value::Bool(true));
    }
}
