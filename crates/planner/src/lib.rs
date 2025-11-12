//! Query planner: converts SQL AST to optimized physical execution plans.
//!
//! The planner bridges between the parser's abstract syntax tree and the executor's
//! runtime operators. It performs three main tasks:
//!
//! 1. **Name Binding** - Resolves column names to ordinals using catalog schemas
//! 2. **Optimization** - Applies simple rules like predicate pushdown and projection pruning
//! 3. **Access Method Selection** - Chooses between sequential and index scans
//!
//! # Architecture
//!
//! ```text
//! Parser AST
//!     ↓
//! Logical Plan (table names, column names)
//!     ↓
//! Optimize (pushdown, pruning)
//!     ↓
//! Bind (names → IDs)
//!     ↓
//! Physical Plan (table IDs, column ordinals, access methods)
//!     ↓
//! Executor
//! ```
//!
//! # Example
//!
//! ```no_run
//! use planner::{Planner, PlanningContext};
//! use catalog::Catalog;
//! use parser::parse_sql;
//!
//! let catalog = Catalog::new();
//! let mut ctx = PlanningContext::new(&catalog);
//! let stmt = parse_sql("SELECT name FROM users WHERE id = 1").unwrap().remove(0);
//! let plan = Planner::plan(stmt, &mut ctx).unwrap();
//! ```

#[cfg(test)]
mod tests;

use catalog::{Catalog, IndexKind, TableMeta};
use common::{ColumnId, DbError, DbResult, TableId};
use expr::{BinaryOp, Expr, UnaryOp};
use parser::{SelectItem, Statement};
use types::Value;

/// Logical plan node - optimizer-friendly representation with string names.
///
/// Logical plans use table/column names and are independent of physical
/// storage details. They're the intermediate form used for optimization.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlan {
    TableScan {
        table: String,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    Insert {
        table: String,
        values: Vec<Expr>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        predicate: Option<Expr>,
    },
    Delete {
        table: String,
        predicate: Option<Expr>,
    },
}

/// Physical plan node - executor-ready with resolved IDs and access methods.
///
/// Physical plans use table IDs, column ordinals, and concrete access methods.
/// They're ready for the executor to process.
#[derive(Clone, Debug, PartialEq)]
pub enum PhysicalPlan {
    SeqScan {
        table_id: TableId,
        schema: Vec<String>,
    },
    IndexScan {
        table_id: TableId,
        index_name: String,
        predicate: IndexPredicate,
        schema: Vec<String>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: ResolvedExpr,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<(String, ColumnId)>,
    },
    Insert {
        table_id: TableId,
        values: Vec<ResolvedExpr>,
    },
    Update {
        table_id: TableId,
        assignments: Vec<(ColumnId, ResolvedExpr)>,
        predicate: Option<ResolvedExpr>,
    },
    Delete {
        table_id: TableId,
        predicate: Option<ResolvedExpr>,
    },
}

/// Index predicate for simple index scans.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexPredicate {
    Eq {
        col: ColumnId,
        value: ResolvedExpr,
    },
    Range {
        col: ColumnId,
        low: ResolvedExpr,
        high: ResolvedExpr,
    },
}

/// Resolved expression with column references bound to ordinals.
///
/// Unlike `expr::Expr` which uses string column names, `ResolvedExpr`
/// uses numeric column IDs so the executor can avoid name lookups.
#[derive(Clone, Debug, PartialEq)]
pub enum ResolvedExpr {
    Literal(Value),
    Column(ColumnId),
    Unary {
        op: UnaryOp,
        expr: Box<ResolvedExpr>,
    },
    Binary {
        left: Box<ResolvedExpr>,
        op: BinaryOp,
        right: Box<ResolvedExpr>,
    },
}

/// Planning context - holds catalog for schema lookups.
pub struct PlanningContext<'a> {
    pub catalog: &'a Catalog,
}

impl<'a> PlanningContext<'a> {
    /// Create a new planning context.
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// Look up a table by name.
    pub fn table(&self, name: &str) -> DbResult<&TableMeta> {
        self.catalog
            .table(name)
            .map_err(|e| DbError::Planner(format!("{e}")))
    }
}

/// Main planner entry point.
pub struct Planner;

impl Planner {
    /// Convert a parser statement into an optimized physical plan.
    ///
    /// # Steps
    ///
    /// 1. Lower AST to logical plan
    /// 2. Apply optimization rules
    /// 3. Bind names to IDs and select access methods
    ///
    /// # Errors
    ///
    /// Returns `DbError::Planner` if:
    /// - Table or column names don't exist
    /// - Statement type is unsupported (DDL in v1)
    pub fn plan(stmt: Statement, ctx: &mut PlanningContext) -> DbResult<PhysicalPlan> {
        let logical = Self::lower_to_logical(stmt)?;
        let optimized = Self::optimize(logical, ctx)?;
        Self::bind(optimized, ctx)
    }

    /// Lower parser AST to logical plan.
    fn lower_to_logical(stmt: Statement) -> DbResult<LogicalPlan> {
        match stmt {
            Statement::CreateTable { .. }
            | Statement::DropTable { .. }
            | Statement::CreateIndex { .. }
            | Statement::DropIndex { .. } => {
                Err(DbError::Planner("DDL handled elsewhere in v1".into()))
            }
            Statement::Explain { query, .. } => {
                // For EXPLAIN, just plan the inner query
                // The analyze flag will be handled by the REPL/executor
                Self::lower_to_logical(*query)
            }
            Statement::Insert { table, values } => Ok(LogicalPlan::Insert { table, values }),
            Statement::Update {
                table,
                assignments,
                selection,
            } => Ok(LogicalPlan::Update {
                table,
                assignments,
                predicate: selection,
            }),
            Statement::Delete { table, selection } => Ok(LogicalPlan::Delete {
                table,
                predicate: selection,
            }),
            Statement::Select {
                columns,
                table,
                selection,
            } => {
                let scan = LogicalPlan::TableScan { table };
                let with_filter = if let Some(pred) = selection {
                    LogicalPlan::Filter {
                        input: Box::new(scan),
                        predicate: pred,
                    }
                } else {
                    scan
                };
                let cols = if columns.iter().any(|c| matches!(c, SelectItem::Wildcard)) {
                    LogicalPlan::Project {
                        input: Box::new(with_filter),
                        columns: vec!["*".into()],
                    }
                } else {
                    let names = columns
                        .into_iter()
                        .map(|c| match c {
                            SelectItem::Column(name) => name,
                            SelectItem::Wildcard => unreachable!(),
                        })
                        .collect();
                    LogicalPlan::Project {
                        input: Box::new(with_filter),
                        columns: names,
                    }
                };
                Ok(cols)
            }
        }
    }

    /// Apply optimization rules.
    fn optimize(plan: LogicalPlan, _ctx: &mut PlanningContext) -> DbResult<LogicalPlan> {
        let p1 = Self::pushdown(plan);
        let p2 = Self::prune_project(p1);
        Ok(p2)
    }

    /// Push filters closer to table scans.
    fn pushdown(plan: LogicalPlan) -> LogicalPlan {
        use LogicalPlan::*;
        match plan {
            Filter { input, predicate } => match *input {
                Project {
                    input: inner,
                    columns,
                } => {
                    // Only push down if projection is wildcard
                    if columns.len() == 1 && columns[0] == "*" {
                        Filter {
                            input: inner,
                            predicate,
                        }
                    } else {
                        Filter {
                            input: Box::new(Project {
                                input: inner,
                                columns,
                            }),
                            predicate,
                        }
                    }
                }
                other => Filter {
                    input: Box::new(Self::pushdown(other)),
                    predicate,
                },
            },
            Project { input, columns } => Project {
                input: Box::new(Self::pushdown(*input)),
                columns,
            },
            Insert { .. } | Update { .. } | Delete { .. } | TableScan { .. } => plan,
        }
    }

    /// Remove redundant projections.
    fn prune_project(plan: LogicalPlan) -> LogicalPlan {
        use LogicalPlan::*;
        match plan {
            Project { input, columns } => match *input {
                Project {
                    input: inner,
                    columns: inner_cols,
                } => {
                    // Remove double project when top is wildcard
                    if columns.len() == 1 && columns[0] == "*" {
                        Project {
                            input: inner,
                            columns: inner_cols,
                        }
                    } else {
                        Project {
                            input: Box::new(Self::prune_project(Project {
                                input: inner,
                                columns: inner_cols,
                            })),
                            columns,
                        }
                    }
                }
                other => Project {
                    input: Box::new(Self::prune_project(other)),
                    columns,
                },
            },
            Filter { input, predicate } => Filter {
                input: Box::new(Self::prune_project(*input)),
                predicate,
            },
            other => other,
        }
    }

    /// Bind names to IDs and generate physical plan.
    fn bind(plan: LogicalPlan, ctx: &mut PlanningContext) -> DbResult<PhysicalPlan> {
        match plan {
            LogicalPlan::TableScan { table } => {
                let t = ctx.table(&table)?;
                Ok(PhysicalPlan::SeqScan {
                    table_id: t.id,
                    schema: t.schema.columns().iter().map(|c| c.name.clone()).collect(),
                })
            }
            LogicalPlan::Filter { input, predicate } => {
                let input_physical = Self::bind(*input, ctx)?;
                let resolved = Self::bind_expr(&input_physical, predicate, ctx)?;

                // Try index scan optimization
                if let PhysicalPlan::SeqScan { table_id, schema } = &input_physical
                    && let Some((col_id, pred)) =
                        Self::try_extract_index_predicate(schema, &resolved)
                    && let Some(index_name) = Self::find_index_for_col(ctx, table_id, col_id)
                {
                    let idx_scan = PhysicalPlan::IndexScan {
                        table_id: *table_id,
                        index_name,
                        predicate: pred,
                        schema: schema.clone(),
                    };
                    return Ok(PhysicalPlan::Filter {
                        input: Box::new(idx_scan),
                        predicate: resolved,
                    });
                }

                Ok(PhysicalPlan::Filter {
                    input: Box::new(input_physical),
                    predicate: resolved,
                })
            }
            LogicalPlan::Project { input, columns } => {
                let input_physical = Self::bind(*input, ctx)?;
                let schema = Self::output_schema(&input_physical);

                if columns.len() == 1 && columns[0] == "*" {
                    let cols: Vec<(String, ColumnId)> = schema
                        .iter()
                        .enumerate()
                        .map(|(i, name)| (name.clone(), i as ColumnId))
                        .collect();
                    return Ok(PhysicalPlan::Project {
                        input: Box::new(input_physical),
                        columns: cols,
                    });
                }

                let cols = columns
                    .into_iter()
                    .map(|name| {
                        let idx = schema
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&name))
                            .ok_or_else(|| DbError::Planner(format!("unknown column '{name}'")))?
                            as ColumnId;
                        Ok((name, idx))
                    })
                    .collect::<DbResult<Vec<_>>>()?;

                Ok(PhysicalPlan::Project {
                    input: Box::new(input_physical),
                    columns: cols,
                })
            }
            LogicalPlan::Insert { table, values } => {
                let t = ctx.table(&table)?;
                let vals = values
                    .into_iter()
                    .map(Self::bind_expr_seq)
                    .collect::<DbResult<Vec<_>>>()?;
                Ok(PhysicalPlan::Insert {
                    table_id: t.id,
                    values: vals,
                })
            }
            LogicalPlan::Update {
                table,
                assignments,
                predicate,
            } => {
                let t = ctx.table(&table)?;
                let schema = &t.schema;
                let schema_names: Vec<String> =
                    schema.columns().iter().map(|c| c.name.clone()).collect();
                let assigns = assignments
                    .into_iter()
                    .map(|(name, e)| {
                        let idx = schema
                            .column_index(&name)
                            .ok_or_else(|| DbError::Planner(format!("unknown column '{name}'")))?;
                        let re = Self::bind_expr_with_schema(&schema_names, e)?;
                        Ok((idx, re))
                    })
                    .collect::<DbResult<Vec<_>>>()?;
                let pred = predicate
                    .map(|p| Self::bind_expr_with_schema(&schema_names, p))
                    .transpose()?;
                Ok(PhysicalPlan::Update {
                    table_id: t.id,
                    assignments: assigns,
                    predicate: pred,
                })
            }
            LogicalPlan::Delete { table, predicate } => {
                let t = ctx.table(&table)?;
                let schema_names: Vec<String> =
                    t.schema.columns().iter().map(|c| c.name.clone()).collect();
                let pred = predicate
                    .map(|p| Self::bind_expr_with_schema(&schema_names, p))
                    .transpose()?;
                Ok(PhysicalPlan::Delete {
                    table_id: t.id,
                    predicate: pred,
                })
            }
        }
    }

    /// Get the output schema (column names) from a physical plan.
    fn output_schema(plan: &PhysicalPlan) -> Vec<String> {
        match plan {
            PhysicalPlan::SeqScan { schema, .. } | PhysicalPlan::IndexScan { schema, .. } => {
                schema.clone()
            }
            PhysicalPlan::Filter { input, .. } | PhysicalPlan::Project { input, .. } => {
                Self::output_schema(input)
            }
            PhysicalPlan::Insert { .. }
            | PhysicalPlan::Update { .. }
            | PhysicalPlan::Delete { .. } => vec![],
        }
    }

    /// Bind expression with input schema context.
    fn bind_expr(
        input: &PhysicalPlan,
        e: Expr,
        _ctx: &mut PlanningContext,
    ) -> DbResult<ResolvedExpr> {
        let schema = Self::output_schema(input);
        Self::bind_expr_with_schema(&schema, e)
    }

    /// Bind standalone expression (no column context).
    fn bind_expr_seq(e: Expr) -> DbResult<ResolvedExpr> {
        Self::bind_expr_with_schema(&[], e)
    }

    /// Bind expression with explicit schema.
    fn bind_expr_with_schema(schema: &[String], e: Expr) -> DbResult<ResolvedExpr> {
        match e {
            Expr::Literal(v) => Ok(ResolvedExpr::Literal(v)),
            Expr::Column(name) => {
                let idx = schema
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(&name))
                    .ok_or_else(|| DbError::Planner(format!("unknown column '{name}'")))?;
                Ok(ResolvedExpr::Column(idx as ColumnId))
            }
            Expr::Unary { op, expr } => Ok(ResolvedExpr::Unary {
                op,
                expr: Box::new(Self::bind_expr_with_schema(schema, *expr)?),
            }),
            Expr::Binary { left, op, right } => Ok(ResolvedExpr::Binary {
                left: Box::new(Self::bind_expr_with_schema(schema, *left)?),
                op,
                right: Box::new(Self::bind_expr_with_schema(schema, *right)?),
            }),
        }
    }

    /// Try to extract a simple index predicate from an expression.
    fn try_extract_index_predicate(
        _schema: &[String],
        pred: &ResolvedExpr,
    ) -> Option<(ColumnId, IndexPredicate)> {
        if let ResolvedExpr::Binary { left, op, right } = pred
            && let (ResolvedExpr::Column(col), ResolvedExpr::Literal(val)) = (&**left, &**right)
        {
            return Some(match op {
                BinaryOp::Eq => (
                    *col,
                    IndexPredicate::Eq {
                        col: *col,
                        value: ResolvedExpr::Literal(val.clone()),
                    },
                ),
                BinaryOp::Lt | BinaryOp::Le => (
                    *col,
                    IndexPredicate::Range {
                        col: *col,
                        low: ResolvedExpr::Literal(Value::Int(i64::MIN)),
                        high: ResolvedExpr::Literal(val.clone()),
                    },
                ),
                BinaryOp::Gt | BinaryOp::Ge => (
                    *col,
                    IndexPredicate::Range {
                        col: *col,
                        low: ResolvedExpr::Literal(val.clone()),
                        high: ResolvedExpr::Literal(Value::Int(i64::MAX)),
                    },
                ),
                _ => return None,
            });
        }
        None
    }

    /// Find an index covering a specific column.
    fn find_index_for_col(
        ctx: &PlanningContext,
        table_id: &TableId,
        col: ColumnId,
    ) -> Option<String> {
        let table_meta = ctx.catalog.table_by_id(*table_id).ok()?;
        for idx in table_meta.indexes() {
            if idx.columns.len() == 1 && idx.columns[0] == col {
                // Only use BTree indexes for now (support both equality and range)
                if matches!(idx.kind, IndexKind::BTree) {
                    return Some(idx.name.clone());
                }
            }
        }
        None
    }
}

/// Pretty-print a logical plan for debugging.
pub fn explain_logical(p: &LogicalPlan) -> String {
    match p {
        LogicalPlan::TableScan { table } => format!("TableScan table={}", table),
        LogicalPlan::Filter { input, predicate } => format!(
            "Filter [{predicate:?}]\n  {}",
            indent(&explain_logical(input))
        ),
        LogicalPlan::Project { input, columns } => format!(
            "Project cols={:?}\n  {}",
            columns,
            indent(&explain_logical(input))
        ),
        LogicalPlan::Insert { table, values } => {
            format!("Insert table={} values={:?}", table, values)
        }
        LogicalPlan::Update {
            table,
            assignments,
            predicate,
        } => format!(
            "Update table={} assigns={:?} pred={:?}",
            table, assignments, predicate
        ),
        LogicalPlan::Delete { table, predicate } => {
            format!("Delete table={} pred={:?}", table, predicate)
        }
    }
}

/// Pretty-print a physical plan for debugging.
pub fn explain_physical(p: &PhysicalPlan) -> String {
    match p {
        PhysicalPlan::SeqScan { table_id, .. } => format!("SeqScan table_id={}", table_id.0),
        PhysicalPlan::IndexScan {
            table_id,
            index_name,
            predicate,
            ..
        } => format!(
            "IndexScan table_id={} index={} pred={predicate:?}",
            table_id.0, index_name
        ),
        PhysicalPlan::Filter { input, predicate } => format!(
            "Filter [{predicate:?}]\n  {}",
            indent(&explain_physical(input))
        ),
        PhysicalPlan::Project { input, columns } => format!(
            "Project {:?}\n  {}",
            columns,
            indent(&explain_physical(input))
        ),
        PhysicalPlan::Insert { table_id, values } => {
            format!("Insert table_id={} values={:?}", table_id.0, values)
        }
        PhysicalPlan::Update {
            table_id,
            assignments,
            predicate,
        } => format!(
            "Update table_id={} assigns={:?} pred={:?}",
            table_id.0, assignments, predicate
        ),
        PhysicalPlan::Delete {
            table_id,
            predicate,
        } => format!("Delete table_id={} pred={:?}", table_id.0, predicate),
    }
}

fn indent(s: &str) -> String {
    s.lines()
        .map(|l| format!("  {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}
