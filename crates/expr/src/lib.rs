#[cfg(test)]
mod tests;

use common::{DbError, DbResult, Row};
use std::cmp::Ordering;
#[allow(unused_imports)]
use types::{SqlType, Value};

/// Binary comparison and logical operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

/// Unary operators (currently just logical NOT).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UnaryOp {
    Not,
}

/// Expression abstract syntax tree.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Literal(Value),
    Column(String),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
}

/// Evaluation context consisting of the row schema (column names in order).
pub struct EvalContext<'a> {
    pub schema: &'a [String],
}

impl<'a> EvalContext<'a> {
    /// Evaluate an expression over a given row.
    pub fn eval(&self, expr: &Expr, row: &Row) -> DbResult<Value> {
        match expr {
            Expr::Literal(v) => Ok(v.clone()),
            Expr::Column(name) => {
                let idx = self
                    .schema
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(name))
                    .ok_or_else(|| DbError::Executor(format!("unknown column '{name}'")))?;
                Ok(row.values[idx].clone())
            }
            Expr::Unary { op, expr } => {
                let v = self.eval(expr, row)?;
                match op {
                    UnaryOp::Not => {
                        let b = v.as_bool().ok_or_else(|| {
                            DbError::Executor(format!("NOT expects bool, got {:?}", v))
                        })?;
                        Ok(Value::Bool(!b))
                    }
                }
            }
            Expr::Binary { left, op, right } => {
                let lv = self.eval(left, row)?;
                let rv = self.eval(right, row)?;
                self.eval_binary(&lv, *op, &rv)
            }
        }
    }

    fn eval_binary(&self, l: &Value, op: BinaryOp, r: &Value) -> DbResult<Value> {
        use BinaryOp::*;

        match op {
            And | Or => {
                let lb = l.as_bool().ok_or_else(|| {
                    DbError::Executor(format!("AND/OR expects bools, got {:?}", l))
                })?;
                let rb = r.as_bool().ok_or_else(|| {
                    DbError::Executor(format!("AND/OR expects bools, got {:?}", r))
                })?;
                return Ok(Value::Bool(match op {
                    And => lb && rb,
                    Or => lb || rb,
                    _ => unreachable!(),
                }));
            }
            _ => {}
        }

        let ord = l.cmp_same_type(r).ok_or_else(|| {
            DbError::Executor(format!("incompatible types for {:?}: {:?}, {:?}", op, l, r))
        })?;

        let result = match op {
            Eq => ord == Ordering::Equal,
            Ne => ord != Ordering::Equal,
            Lt => ord == Ordering::Less,
            Le => ord != Ordering::Greater,
            Gt => ord == Ordering::Greater,
            Ge => ord != Ordering::Less,
            _ => unreachable!(),
        };

        Ok(Value::Bool(result))
    }
}
