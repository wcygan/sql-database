mod ast;
#[cfg(test)]
mod tests;

pub use ast::*;

use common::{DbError, DbResult};
use expr::{BinaryOp, Expr, UnaryOp};
use sqlparser::ast as sqlast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use types::Value;

/// Parse SQL text into the internal AST statements.
pub fn parse_sql(sql: &str) -> DbResult<Vec<Statement>> {
    let dialect = GenericDialect {};
    let stmts = SqlParser::parse_sql(&dialect, sql)
        .map_err(|e| DbError::Parser(format!("SQL parse error: {e}")))?;

    stmts.into_iter().map(map_statement).collect()
}

fn map_statement(stmt: sqlast::Statement) -> DbResult<Statement> {
    use sqlast::Statement as SqlStatement;

    match stmt {
        SqlStatement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            let table = normalize_object_name(&name)?;
            let primary_key = resolve_primary_key(&columns, &constraints)?;

            let mapped_columns = columns
                .into_iter()
                .map(|col| ColumnDef {
                    name: normalize_ident_owned(col.name),
                    ty: col.data_type.to_string().to_uppercase(),
                })
                .collect();

            Ok(Statement::CreateTable {
                name: table,
                columns: mapped_columns,
                primary_key,
            })
        }
        SqlStatement::Drop {
            object_type, names, ..
        } => match object_type {
            sqlast::ObjectType::Table => Ok(Statement::DropTable {
                name: first_name(names)?,
            }),
            sqlast::ObjectType::Index => Ok(Statement::DropIndex {
                name: first_name(names)?,
            }),
            _ => Err(DbError::Parser(format!(
                "unsupported DROP type: {object_type:?}"
            ))),
        },
        SqlStatement::CreateIndex {
            name,
            table_name,
            columns,
            ..
        } => {
            let index_name = name
                .ok_or_else(|| DbError::Parser("index name required".into()))
                .map(|n| normalize_object_name(&n))??;
            let table = normalize_object_name(&table_name)?;
            let column = map_index_column(columns.first())?;
            Ok(Statement::CreateIndex {
                name: index_name,
                table,
                column,
            })
        }
        SqlStatement::Insert {
            table_name, source, ..
        } => {
            let table = normalize_object_name(&table_name)?;
            let source = source.ok_or_else(|| DbError::Parser("INSERT source missing".into()))?;
            let values = extract_values(*source)?;
            Ok(Statement::Insert { table, values })
        }
        SqlStatement::Query(query) => map_select(*query),
        SqlStatement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            let table = table_name_from_with_joins(&table)?;
            let assignments = assignments
                .into_iter()
                .map(|assign| {
                    let ident = assign
                        .id
                        .last()
                        .ok_or_else(|| DbError::Parser("invalid assignment target".into()))?;
                    Ok((normalize_ident(ident), map_expr(assign.value)?))
                })
                .collect::<DbResult<Vec<_>>>()?;
            let selection = selection.map(map_expr).transpose()?;
            Ok(Statement::Update {
                table,
                assignments,
                selection,
            })
        }
        SqlStatement::Delete {
            from, selection, ..
        } => {
            if from.is_empty() {
                return Err(DbError::Parser("DELETE requires FROM source".into()));
            }
            let table = table_name_from_with_joins(&from[0])?;
            if from.len() > 1 {
                return Err(DbError::Parser("multi-table DELETE not supported".into()));
            }
            let selection = selection.map(map_expr).transpose()?;
            Ok(Statement::Delete { table, selection })
        }
        SqlStatement::Explain {
            statement, analyze, ..
        } => {
            let query = Box::new(map_statement(*statement)?);
            Ok(Statement::Explain { query, analyze })
        }
        _ => Err(DbError::Parser("unsupported statement".into())),
    }
}

fn map_select(query: sqlast::Query) -> DbResult<Statement> {
    use sqlast::SetExpr;

    let select = match *query.body {
        SetExpr::Select(select) => select,
        SetExpr::Values(_) => {
            return Err(DbError::Parser("standalone VALUES not supported".into()))
        }
        _ => return Err(DbError::Parser("SET operations not supported".into())),
    };

    let sqlast::Select {
        projection,
        from,
        selection,
        ..
    } = *select;

    if from.is_empty() {
        return Err(DbError::Parser("SELECT requires FROM clause".into()));
    }
    if from.len() > 1 {
        return Err(DbError::Parser("joins not supported".into()));
    }
    let table = table_name_from_with_joins(&from[0])?;
    let columns = projection
        .into_iter()
        .map(map_select_item)
        .collect::<DbResult<Vec<_>>>()?;
    let selection = selection.map(map_expr).transpose()?;

    // Extract ORDER BY clauses
    let order_by = query
        .order_by
        .into_iter()
        .map(map_order_by_expr)
        .collect::<DbResult<Vec<_>>>()?;

    // Extract LIMIT
    let limit = query
        .limit
        .map(|expr| match expr {
            sqlast::Expr::Value(sqlast::Value::Number(n, _)) => n
                .parse::<u64>()
                .map_err(|_| DbError::Parser(format!("invalid LIMIT value: {}", n))),
            _ => Err(DbError::Parser(
                "LIMIT must be a non-negative integer".into(),
            )),
        })
        .transpose()?;

    // Extract OFFSET
    let offset = query
        .offset
        .map(|offset_expr| match offset_expr.value {
            sqlast::Expr::Value(sqlast::Value::Number(n, _)) => n
                .parse::<u64>()
                .map_err(|_| DbError::Parser(format!("invalid OFFSET value: {}", n))),
            _ => Err(DbError::Parser(
                "OFFSET must be a non-negative integer".into(),
            )),
        })
        .transpose()?;

    Ok(Statement::Select {
        columns,
        table,
        selection,
        order_by,
        limit,
        offset,
    })
}

fn map_order_by_expr(expr: sqlast::OrderByExpr) -> DbResult<ast::OrderByExpr> {
    // Extract column name from expression
    let column = match expr.expr {
        sqlast::Expr::Identifier(ident) => normalize_ident(&ident),
        sqlast::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 1 {
                normalize_ident(&parts[0])
            } else {
                return Err(DbError::Parser(
                    "qualified column names not supported in ORDER BY".into(),
                ));
            }
        }
        _ => {
            return Err(DbError::Parser(
                "ORDER BY supports column names only".into(),
            ))
        }
    };

    // Extract sort direction (default is ASC)
    let direction = if let Some(asc) = expr.asc {
        if asc {
            ast::SortDirection::Asc
        } else {
            ast::SortDirection::Desc
        }
    } else {
        // Default to ASC when not specified
        ast::SortDirection::Asc
    };

    Ok(ast::OrderByExpr { column, direction })
}

fn extract_values(query: sqlast::Query) -> DbResult<Vec<Expr>> {
    match *query.body {
        sqlast::SetExpr::Values(values) => {
            let mut rows = values.rows.into_iter();
            let row = rows
                .next()
                .ok_or_else(|| DbError::Parser("INSERT requires at least one row".into()))?;
            if rows.next().is_some() {
                return Err(DbError::Parser("multi-row INSERT not supported".into()));
            }
            row.into_iter().map(map_expr).collect()
        }
        _ => Err(DbError::Parser("INSERT expects VALUES list".into())),
    }
}

fn map_select_item(item: sqlast::SelectItem) -> DbResult<SelectItem> {
    match item {
        sqlast::SelectItem::Wildcard(options) => {
            ensure_plain_wildcard(&options)?;
            Ok(SelectItem::Wildcard)
        }
        sqlast::SelectItem::QualifiedWildcard(_, _) => {
            Err(DbError::Parser("qualified wildcard not supported".into()))
        }
        sqlast::SelectItem::UnnamedExpr(expr) => match expr {
            sqlast::Expr::Identifier(ident) => Ok(SelectItem::Column(normalize_ident_owned(ident))),
            sqlast::Expr::CompoundIdentifier(parts) => {
                let ident = parts
                    .last()
                    .ok_or_else(|| DbError::Parser("invalid identifier".into()))?;
                Ok(SelectItem::Column(normalize_ident(ident)))
            }
            other => Err(DbError::Parser(format!(
                "unsupported select item: {other:?}"
            ))),
        },
        sqlast::SelectItem::ExprWithAlias { .. } => {
            Err(DbError::Parser("select aliases not supported".into()))
        }
    }
}

fn map_expr(expr: sqlast::Expr) -> DbResult<Expr> {
    use sqlast::Expr as SqlExpr;

    match expr {
        SqlExpr::Identifier(ident) => Ok(Expr::Column(normalize_ident_owned(ident))),
        SqlExpr::CompoundIdentifier(idents) => {
            let ident = idents
                .last()
                .ok_or_else(|| DbError::Parser("invalid identifier".into()))?;
            Ok(Expr::Column(normalize_ident(ident)))
        }
        SqlExpr::Value(value) => Ok(Expr::Literal(map_value(value)?)),
        SqlExpr::BinaryOp { left, op, right } => Ok(Expr::Binary {
            left: Box::new(map_expr(*left)?),
            op: map_binary_op(op)?,
            right: Box::new(map_expr(*right)?),
        }),
        SqlExpr::UnaryOp { op, expr } => Ok(Expr::Unary {
            op: map_unary_op(op)?,
            expr: Box::new(map_expr(*expr)?),
        }),
        SqlExpr::Nested(expr) => map_expr(*expr),
        _ => Err(DbError::Parser("unsupported expr".into())),
    }
}

fn map_value(value: sqlast::Value) -> DbResult<Value> {
    use sqlast::Value as SqlValue;

    match value {
        SqlValue::Number(num, _) => {
            let parsed = num
                .parse::<i64>()
                .map_err(|_| DbError::Parser(format!("invalid int literal: {num}")))?;
            Ok(Value::Int(parsed))
        }
        SqlValue::SingleQuotedString(s) => Ok(Value::Text(s)),
        SqlValue::Boolean(b) => Ok(Value::Bool(b)),
        SqlValue::Null => Ok(Value::Null),
        other => Err(DbError::Parser(format!("unsupported literal: {other:?}"))),
    }
}

fn map_binary_op(op: sqlast::BinaryOperator) -> DbResult<BinaryOp> {
    use sqlast::BinaryOperator as SqlBinary;

    Ok(match op {
        SqlBinary::Eq => BinaryOp::Eq,
        SqlBinary::NotEq => BinaryOp::Ne,
        SqlBinary::Lt => BinaryOp::Lt,
        SqlBinary::LtEq => BinaryOp::Le,
        SqlBinary::Gt => BinaryOp::Gt,
        SqlBinary::GtEq => BinaryOp::Ge,
        SqlBinary::And => BinaryOp::And,
        SqlBinary::Or => BinaryOp::Or,
        other => return Err(DbError::Parser(format!("unsupported operator: {other:?}"))),
    })
}

fn map_unary_op(op: sqlast::UnaryOperator) -> DbResult<UnaryOp> {
    use sqlast::UnaryOperator as SqlUnary;

    Ok(match op {
        SqlUnary::Not => UnaryOp::Not,
        other => {
            return Err(DbError::Parser(format!(
                "unsupported unary operator: {other:?}"
            )))
        }
    })
}

fn normalize_ident(ident: &sqlast::Ident) -> String {
    ident.value.to_lowercase()
}

fn normalize_ident_owned(ident: sqlast::Ident) -> String {
    ident.value.to_lowercase()
}

fn normalize_object_name(name: &sqlast::ObjectName) -> DbResult<String> {
    name.0
        .first()
        .map(|ident| ident.value.to_lowercase())
        .ok_or_else(|| DbError::Parser("invalid object name".into()))
}

fn first_name(mut names: Vec<sqlast::ObjectName>) -> DbResult<String> {
    if names.is_empty() {
        return Err(DbError::Parser("DROP requires a target".into()));
    }
    normalize_object_name(&names.remove(0))
}

fn table_name_from_with_joins(table: &sqlast::TableWithJoins) -> DbResult<String> {
    if !table.joins.is_empty() {
        return Err(DbError::Parser("joins not supported".into()));
    }
    match &table.relation {
        sqlast::TableFactor::Table { name, .. } => normalize_object_name(name),
        _ => Err(DbError::Parser("unsupported table factor".into())),
    }
}

fn map_index_column(column: Option<&sqlast::OrderByExpr>) -> DbResult<String> {
    let column = column.ok_or_else(|| DbError::Parser("index column required".into()))?;
    match &column.expr {
        sqlast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        sqlast::Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(normalize_ident)
            .ok_or_else(|| DbError::Parser("invalid identifier".into())),
        other => Err(DbError::Parser(format!(
            "unsupported index column: {other:?}"
        ))),
    }
}

fn ensure_plain_wildcard(options: &sqlast::WildcardAdditionalOptions) -> DbResult<()> {
    let has_options = options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_rename.is_some()
        || options.opt_replace.is_some();
    if has_options {
        Err(DbError::Parser("wildcard options not supported".into()))
    } else {
        Ok(())
    }
}

/// Resolve primary key from inline column constraints and table-level constraints.
/// Returns error if PK defined in both places.
fn resolve_primary_key(
    columns: &[sqlast::ColumnDef],
    constraints: &[sqlast::TableConstraint],
) -> DbResult<Option<Vec<String>>> {
    let inline_pk = extract_inline_primary_key(columns)?;
    let table_pk = extract_primary_key(constraints)?;

    match (table_pk, inline_pk) {
        (Some(_), Some(_)) => Err(DbError::Parser(
            "PRIMARY KEY defined both inline and at table level".into(),
        )),
        (Some(pk), None) | (None, Some(pk)) => Ok(Some(pk)),
        (None, None) => Ok(None),
    }
}

/// Extract PRIMARY KEY constraint from table constraints.
/// Returns Some(Vec<String>) if PRIMARY KEY is found, None otherwise.
fn extract_primary_key(constraints: &[sqlast::TableConstraint]) -> DbResult<Option<Vec<String>>> {
    use sqlast::TableConstraint;

    for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                columns,
                is_primary,
                ..
            } if *is_primary => {
                let pk_columns: Vec<String> = columns.iter().map(normalize_ident).collect();

                if pk_columns.is_empty() {
                    return Err(DbError::Parser(
                        "PRIMARY KEY must include at least one column".into(),
                    ));
                }

                return Ok(Some(pk_columns));
            }
            _ => continue,
        }
    }
    Ok(None)
}

/// Extract PRIMARY KEY defined inline on column definitions.
fn extract_inline_primary_key(columns: &[sqlast::ColumnDef]) -> DbResult<Option<Vec<String>>> {
    use sqlast::ColumnOption;

    let mut pk_columns = Vec::new();
    for column in columns {
        let has_primary_key = column.options.iter().any(|opt| {
            matches!(
                opt.option,
                ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            )
        });
        if has_primary_key {
            pk_columns.push(normalize_ident(&column.name));
        }
    }

    match pk_columns.len() {
        0 => Ok(None),
        1 => Ok(Some(pk_columns)),
        _ => Err(DbError::Parser(
            "multiple PRIMARY KEY column constraints; use PRIMARY KEY (col1, col2)".into(),
        )),
    }
}
