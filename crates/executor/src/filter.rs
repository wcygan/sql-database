//! Filter operator: applies WHERE predicates.

use crate::{ExecutionContext, Executor};
use common::{DbResult, Row};
use planner::ResolvedExpr;
use types::Value;

/// Filter operator - applies a predicate to rows from a child operator.
///
/// Only passes through rows where the predicate evaluates to true.
/// NULL predicate results are treated as false (SQL semantics).
pub struct FilterExec {
    input: Box<dyn Executor>,
    predicate: ResolvedExpr,
}

impl FilterExec {
    /// Create a new filter operator.
    pub fn new(input: Box<dyn Executor>, predicate: ResolvedExpr) -> Self {
        Self { input, predicate }
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
        self.input.open(ctx)
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        loop {
            let row = match self.input.next(ctx)? {
                Some(r) => r,
                None => return Ok(None),
            };

            if self.eval_predicate(&row)? {
                return Ok(Some(row));
            }
        }
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        self.input.close(ctx)
    }

    fn schema(&self) -> &[String] {
        self.input.schema()
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
            row.0
                .get(idx)
                .ok_or_else(|| {
                    common::DbError::Executor(format!(
                        "column index {} out of bounds (row has {} columns)",
                        idx,
                        row.0.len()
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
