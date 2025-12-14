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
    /// Column reference with optional table/alias qualifier.
    ///
    /// Examples:
    /// - `Column { table: None, name: "id" }` - unqualified column
    /// - `Column { table: Some("users"), name: "id" }` - qualified column
    /// - `Column { table: Some("u"), name: "id" }` - alias-qualified column
    Column {
        /// Optional table name or alias qualifier.
        table: Option<String>,
        /// Column name.
        name: String,
    },
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
            Expr::Column { table, name } => {
                let idx = self.find_column(table.as_deref(), name)?;
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

    /// Find column index in schema, supporting qualified and unqualified references.
    ///
    /// Schema entries may be:
    /// - Simple names: `"id"`, `"name"`
    /// - Qualified names: `"users.id"`, `"orders.user_id"`
    ///
    /// Matching rules:
    /// - Qualified ref (`table.col`): Match `"table.col"` exactly
    /// - Unqualified ref (`col`): Match simple `"col"` or suffix `".col"`
    fn find_column(&self, table: Option<&str>, name: &str) -> DbResult<usize> {
        if let Some(qualifier) = table {
            // Qualified column reference: look for exact "table.column" match
            let full_name = format!("{}.{}", qualifier, name);
            self.schema
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&full_name))
                .ok_or_else(|| DbError::Executor(format!("unknown column '{}.{}'", qualifier, name)))
        } else {
            // Unqualified: try exact match first, then suffix match
            self.schema
                .iter()
                .position(|c| {
                    c.eq_ignore_ascii_case(name)
                        || c.to_lowercase().ends_with(&format!(".{}", name.to_lowercase()))
                })
                .ok_or_else(|| DbError::Executor(format!("unknown column '{}'", name)))
        }
    }
}
