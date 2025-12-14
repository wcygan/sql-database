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
use parser::{JoinType, SelectItem, Statement};
use types::Value;

// Re-export for use by executor and internal use
pub use parser::{JoinType as PlanJoinType, SortDirection};

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
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByExpr>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<u64>,
        offset: Option<u64>,
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
    /// Join two plans together.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        /// Join condition (ON clause).
        condition: Expr,
        /// Effective name (alias or table name) for the left side.
        left_name: String,
        /// Effective name (alias or table name) for the right side.
        right_name: String,
    },
}

/// Logical ORDER BY expression with column name.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderByExpr {
    pub column: String,
    pub direction: SortDirection,
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
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<ResolvedOrderByExpr>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<u64>,
        offset: Option<u64>,
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
    /// Nested loop join - for each row from left, scan all right rows.
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        /// Join condition with resolved column ordinals.
        condition: ResolvedExpr,
        /// Combined schema: left columns first, then right columns.
        /// Column names are prefixed with table/alias name (e.g., "users.id").
        schema: Vec<String>,
    },
}

/// Physical ORDER BY expression with resolved column ID.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedOrderByExpr {
    pub column_id: ColumnId,
    pub direction: SortDirection,
}

/// Index predicate for index scans.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexPredicate {
    /// Single-column equality: col = value
    Eq { col: ColumnId, value: ResolvedExpr },
    /// Composite key equality: (col1, col2, ...) = (val1, val2, ...)
    CompositeEq {
        columns: Vec<ColumnId>,
        values: Vec<ResolvedExpr>,
    },
    /// Range predicate (B+Tree only)
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
                from,
                joins,
                selection,
                order_by,
                limit,
                offset,
            } => {
                // Build initial scan from primary FROM table
                let from_name = from.effective_name().to_string();
                let mut plan = LogicalPlan::TableScan {
                    table: from.name.clone(),
                };

                // Add JOINs left-to-right
                let mut current_left_name = from_name;
                for join_clause in joins {
                    let right_name = join_clause.table.effective_name().to_string();
                    let right_scan = LogicalPlan::TableScan {
                        table: join_clause.table.name.clone(),
                    };
                    plan = LogicalPlan::Join {
                        left: Box::new(plan),
                        right: Box::new(right_scan),
                        join_type: join_clause.join_type,
                        condition: join_clause.condition,
                        left_name: current_left_name.clone(),
                        right_name: right_name.clone(),
                    };
                    // For chained joins, the effective name becomes complex
                    // but we don't support chained joins in v1, so this is fine
                    current_left_name = format!("{}_{}", current_left_name, right_name);
                }

                let with_filter = if let Some(pred) = selection {
                    LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate: pred,
                    }
                } else {
                    plan
                };
                let with_project = if columns.iter().any(|c| matches!(c, SelectItem::Wildcard)) {
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

                // Add Sort node if ORDER BY is present
                let with_sort = if !order_by.is_empty() {
                    let order_exprs = order_by
                        .into_iter()
                        .map(|o| OrderByExpr {
                            column: o.column,
                            direction: o.direction,
                        })
                        .collect();
                    LogicalPlan::Sort {
                        input: Box::new(with_project),
                        order_by: order_exprs,
                    }
                } else {
                    with_project
                };

                // Add Limit node if LIMIT or OFFSET is present
                let with_limit = if limit.is_some() || offset.is_some() {
                    LogicalPlan::Limit {
                        input: Box::new(with_sort),
                        limit,
                        offset,
                    }
                } else {
                    with_sort
                };

                Ok(with_limit)
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
            Sort { input, order_by } => Sort {
                input: Box::new(Self::pushdown(*input)),
                order_by,
            },
            Limit {
                input,
                limit,
                offset,
            } => Limit {
                input: Box::new(Self::pushdown(*input)),
                limit,
                offset,
            },
            Insert { .. } | Update { .. } | Delete { .. } | TableScan { .. } => plan,
            // For joins, recurse into both sides but don't try to push filters through yet
            Join {
                left,
                right,
                join_type,
                condition,
                left_name,
                right_name,
            } => Join {
                left: Box::new(Self::pushdown(*left)),
                right: Box::new(Self::pushdown(*right)),
                join_type,
                condition,
                left_name,
                right_name,
            },
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
            Sort { input, order_by } => Sort {
                input: Box::new(Self::prune_project(*input)),
                order_by,
            },
            Limit {
                input,
                limit,
                offset,
            } => Limit {
                input: Box::new(Self::prune_project(*input)),
                limit,
                offset,
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

                // Try index scan optimization using composite key selection
                if let PhysicalPlan::SeqScan { table_id, schema } = &input_physical
                    && let Some((index_name, idx_pred)) =
                        Self::find_best_index(ctx, table_id, &resolved)
                {
                    let idx_scan = PhysicalPlan::IndexScan {
                        table_id: *table_id,
                        index_name,
                        predicate: idx_pred,
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
            LogicalPlan::Sort { input, order_by } => {
                let input_physical = Self::bind(*input, ctx)?;
                let schema = Self::output_schema(&input_physical);

                // Resolve column names to column IDs
                let resolved_order_by = order_by
                    .into_iter()
                    .map(|order_expr| {
                        let col_id = schema
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&order_expr.column))
                            .ok_or_else(|| {
                                DbError::Planner(format!(
                                    "unknown column '{}' in ORDER BY",
                                    order_expr.column
                                ))
                            })? as ColumnId;
                        Ok(ResolvedOrderByExpr {
                            column_id: col_id,
                            direction: order_expr.direction,
                        })
                    })
                    .collect::<DbResult<Vec<_>>>()?;

                Ok(PhysicalPlan::Sort {
                    input: Box::new(input_physical),
                    order_by: resolved_order_by,
                })
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_physical = Self::bind(*input, ctx)?;
                Ok(PhysicalPlan::Limit {
                    input: Box::new(input_physical),
                    limit,
                    offset,
                })
            }
            LogicalPlan::Join {
                left,
                right,
                join_type: _,
                condition,
                left_name,
                right_name,
            } => {
                // Bind left and right sides
                let left_physical = Self::bind(*left, ctx)?;
                let right_physical = Self::bind(*right, ctx)?;

                // Get schemas from both sides
                let left_schema = Self::output_schema(&left_physical);
                let right_schema = Self::output_schema(&right_physical);

                // Build combined schema with table/alias prefixes
                let combined_schema: Vec<String> = left_schema
                    .iter()
                    .map(|col| {
                        // If already qualified, keep it; otherwise prefix with table name
                        if col.contains('.') {
                            col.clone()
                        } else {
                            format!("{}.{}", left_name, col)
                        }
                    })
                    .chain(right_schema.iter().map(|col| {
                        if col.contains('.') {
                            col.clone()
                        } else {
                            format!("{}.{}", right_name, col)
                        }
                    }))
                    .collect();

                // Bind condition expression with combined schema
                let resolved_condition =
                    Self::bind_expr_with_schema(&combined_schema, condition)?;

                Ok(PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left_physical),
                    right: Box::new(right_physical),
                    condition: resolved_condition,
                    schema: combined_schema,
                })
            }
        }
    }

    /// Get the output schema (column names) from a physical plan.
    fn output_schema(plan: &PhysicalPlan) -> Vec<String> {
        match plan {
            PhysicalPlan::SeqScan { schema, .. }
            | PhysicalPlan::IndexScan { schema, .. }
            | PhysicalPlan::NestedLoopJoin { schema, .. } => schema.clone(),
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Limit { input, .. } => Self::output_schema(input),
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
            Expr::Column { table, name } => {
                let idx = Self::find_column_in_schema(schema, table.as_deref(), &name)?;
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

    /// Find column in schema, supporting both qualified and unqualified references.
    ///
    /// Schema entries may be simple ("id") or qualified ("users.id").
    /// - Qualified ref: Look for exact match "table.column"
    /// - Unqualified ref: Match simple "column" or suffix ".column", error if ambiguous
    fn find_column_in_schema(
        schema: &[String],
        table: Option<&str>,
        name: &str,
    ) -> DbResult<usize> {
        if let Some(qualifier) = table {
            // Qualified: look for exact "table.column" match
            let full_name = format!("{}.{}", qualifier, name);
            schema
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&full_name))
                .ok_or_else(|| {
                    DbError::Planner(format!("unknown column '{}.{}'", qualifier, name))
                })
        } else {
            // Unqualified: search for simple match or suffix match
            // First try exact match
            if let Some(idx) = schema.iter().position(|c| c.eq_ignore_ascii_case(name)) {
                return Ok(idx);
            }
            // Then try suffix match (for qualified schema columns)
            let suffix = format!(".{}", name.to_lowercase());
            let matches: Vec<usize> = schema
                .iter()
                .enumerate()
                .filter(|(_, c)| c.to_lowercase().ends_with(&suffix))
                .map(|(i, _)| i)
                .collect();
            match matches.len() {
                0 => Err(DbError::Planner(format!("unknown column '{}'", name))),
                1 => Ok(matches[0]),
                _ => Err(DbError::Planner(format!(
                    "ambiguous column '{}' (exists in multiple tables)",
                    name
                ))),
            }
        }
    }

    /// Try to extract a simple index predicate from an expression (single-column).
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

    /// Extract all equality predicates from a conjunction (AND tree).
    fn extract_equality_predicates(pred: &ResolvedExpr) -> Vec<(ColumnId, ResolvedExpr)> {
        let mut result = Vec::new();
        Self::collect_equality_predicates(pred, &mut result);
        result
    }

    /// Recursively collect equality predicates from AND expressions.
    fn collect_equality_predicates(pred: &ResolvedExpr, out: &mut Vec<(ColumnId, ResolvedExpr)>) {
        match pred {
            ResolvedExpr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => {
                Self::collect_equality_predicates(left, out);
                Self::collect_equality_predicates(right, out);
            }
            ResolvedExpr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                // col = val
                if let (ResolvedExpr::Column(col), ResolvedExpr::Literal(_)) = (&**left, &**right) {
                    out.push((*col, (**right).clone()));
                }
                // val = col
                else if let (ResolvedExpr::Literal(_), ResolvedExpr::Column(col)) =
                    (&**left, &**right)
                {
                    out.push((*col, (**left).clone()));
                }
            }
            _ => {}
        }
    }

    /// Check if a predicate contains only equality comparisons (no ranges).
    fn is_pure_equality_predicate(pred: &ResolvedExpr) -> bool {
        match pred {
            ResolvedExpr::Binary {
                op: BinaryOp::Eq, ..
            } => true,
            ResolvedExpr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => Self::is_pure_equality_predicate(left) && Self::is_pure_equality_predicate(right),
            _ => false,
        }
    }

    /// Find the best index for a predicate, supporting composite keys.
    ///
    /// Ranking:
    /// 1. Full composite match > prefix match > single column
    /// 2. For equality: prefer Hash > BTree
    /// 3. For range: require BTree
    fn find_best_index(
        ctx: &PlanningContext,
        table_id: &TableId,
        pred: &ResolvedExpr,
    ) -> Option<(String, IndexPredicate)> {
        let table_meta = ctx.catalog.table_by_id(*table_id).ok()?;
        let indexes: Vec<_> = table_meta.indexes().to_vec();

        if indexes.is_empty() {
            return None;
        }

        let is_equality_only = Self::is_pure_equality_predicate(pred);
        let eq_preds = Self::extract_equality_predicates(pred);

        if eq_preds.is_empty() {
            // No equality predicates - try range predicates with single-column extraction
            if let Some((col, range_pred)) = Self::try_extract_index_predicate(&[], pred) {
                for idx in &indexes {
                    if idx.columns.len() == 1
                        && idx.columns[0] == col
                        && matches!(idx.kind, IndexKind::BTree)
                    {
                        return Some((idx.name.clone(), range_pred));
                    }
                }
            }
            return None;
        }

        // Build map of column -> value for quick lookup
        let pred_map: std::collections::HashMap<ColumnId, ResolvedExpr> =
            eq_preds.into_iter().collect();

        // Score each index by prefix column coverage
        let mut best_match: Option<(&catalog::IndexMeta, usize)> = None;

        for idx in &indexes {
            // Filter by index kind based on predicate type
            if !is_equality_only && !matches!(idx.kind, IndexKind::BTree) {
                continue; // Range requires BTree
            }
            if !matches!(idx.kind, IndexKind::BTree | IndexKind::Hash) {
                continue; // Only BTree and Hash supported
            }

            // Check prefix match: index columns must match predicate columns in order
            let mut matched_count = 0;
            for &col in &idx.columns {
                if pred_map.contains_key(&col) {
                    matched_count += 1;
                } else {
                    break; // Prefix match broken
                }
            }

            if matched_count > 0 {
                let is_better = match &best_match {
                    None => true,
                    Some((_, best_count)) => {
                        // Prefer more columns matched
                        matched_count > *best_count
                    }
                };
                if is_better {
                    best_match = Some((idx, matched_count));
                }
            }
        }

        let (best_idx, matched_count) = best_match?;

        // Build the predicate
        let columns: Vec<ColumnId> = best_idx.columns[..matched_count].to_vec();
        let values: Vec<ResolvedExpr> = columns
            .iter()
            .map(|col| pred_map.get(col).cloned().unwrap())
            .collect();

        let predicate = if matched_count == 1 {
            IndexPredicate::Eq {
                col: columns[0],
                value: values.into_iter().next().unwrap(),
            }
        } else {
            IndexPredicate::CompositeEq { columns, values }
        };

        Some((best_idx.name.clone(), predicate))
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
        LogicalPlan::Sort { input, order_by } => {
            format!("Sort {:?}\n  {}", order_by, indent(&explain_logical(input)))
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => format!(
            "Limit limit={:?} offset={:?}\n  {}",
            limit,
            offset,
            indent(&explain_logical(input))
        ),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            left_name,
            right_name,
        } => format!(
            "Join type={:?} on={:?} ({} x {})\n  left: {}\n  right: {}",
            join_type,
            condition,
            left_name,
            right_name,
            indent(&explain_logical(left)),
            indent(&explain_logical(right))
        ),
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
        PhysicalPlan::Sort { input, order_by } => format!(
            "Sort {:?}\n  {}",
            order_by,
            indent(&explain_physical(input))
        ),
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => format!(
            "Limit limit={:?} offset={:?}\n  {}",
            limit,
            offset,
            indent(&explain_physical(input))
        ),
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            schema,
        } => format!(
            "NestedLoopJoin on={:?} schema={:?}\n  left: {}\n  right: {}",
            condition,
            schema,
            indent(&explain_physical(left)),
            indent(&explain_physical(right))
        ),
    }
}

fn indent(s: &str) -> String {
    s.lines()
        .map(|l| format!("  {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}
