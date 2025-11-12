//! SQL script execution for snapshot testing.
//!
//! Provides utilities to execute multi-statement SQL scripts and capture
//! pretty-printed output suitable for use with the `insta` snapshot testing
//! framework.

use crate::context::TestContext;
use catalog::{Column, IndexKind};
use common::{
    pretty::{self, TableStyleKind},
    DbResult, RecordBatch,
};
use executor::{build_executor, execute_dml, execute_query};
use parser::{parse_sql, Statement};
use planner::{PhysicalPlan, Planner, PlanningContext};
use types::SqlType;
use wal::WalRecord;

/// Execute a SQL script and return pretty-printed output.
///
/// This function:
/// 1. Creates a fresh isolated test environment
/// 2. Parses and executes each statement in the script
/// 3. Captures all output (query results, affected row counts, error messages)
/// 4. Returns formatted output suitable for snapshot testing
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
///
/// let output = run_sql_script(r#"
///     CREATE TABLE users (id INT, name TEXT, age INT);
///     INSERT INTO users VALUES (1, 'Alice', 30);
///     INSERT INTO users VALUES (2, 'Bob', 25);
///     SELECT * FROM users WHERE age > 25;
/// "#).unwrap();
///
/// insta::assert_snapshot!(output);
/// ```
pub fn run_sql_script(sql: &str) -> DbResult<String> {
    let mut ctx = TestContext::new()?;
    run_sql_script_with_context(sql, &mut ctx)
}

/// Execute a SQL script using a specific test context.
///
/// This is useful when you need a pre-configured catalog or want to
/// run multiple scripts against the same database state.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
///
/// let mut ctx = TestContext::new().unwrap();
///
/// // First script creates tables
/// let output1 = run_sql_script_with_context(r#"
///     CREATE TABLE users (id INT, name TEXT);
/// "#, &mut ctx).unwrap();
///
/// // Second script uses the existing table
/// let output2 = run_sql_script_with_context(r#"
///     INSERT INTO users VALUES (1, 'Alice');
///     SELECT * FROM users;
/// "#, &mut ctx).unwrap();
/// ```
pub fn run_sql_script_with_context(sql: &str, ctx: &mut TestContext) -> DbResult<String> {
    let mut output = String::new();

    let statements = parse_sql(sql)?;

    for stmt in statements {
        let result = execute_statement(stmt, ctx);
        match result {
            Ok(stmt_output) => {
                if !stmt_output.is_empty() {
                    if !output.is_empty() {
                        output.push('\n');
                    }
                    output.push_str(&stmt_output);
                }
            }
            Err(e) => {
                if !output.is_empty() {
                    output.push('\n');
                }
                output.push_str(&format!("Error: {}", e));
            }
        }
    }

    Ok(output)
}

fn execute_statement(stmt: Statement, ctx: &mut TestContext) -> DbResult<String> {
    match stmt {
        Statement::CreateTable {
            name,
            columns,
            primary_key,
        } => {
            let catalog_columns: Vec<Column> = columns
                .iter()
                .map(|col| {
                    let ty = map_sql_type(&col.ty)?;
                    Ok(Column::new(col.name.clone(), ty))
                })
                .collect::<DbResult<Vec<_>>>()?;

            let primary_key_ordinals = if let Some(pk_names) = primary_key {
                let mut ordinals = Vec::new();
                for pk_name in &pk_names {
                    let ordinal = columns
                        .iter()
                        .position(|col| col.name.eq_ignore_ascii_case(pk_name))
                        .ok_or_else(|| {
                            common::DbError::Parser(format!(
                                "PRIMARY KEY column '{}' not found in table columns",
                                pk_name
                            ))
                        })? as u16;
                    ordinals.push(ordinal);
                }
                Some(ordinals)
            } else {
                None
            };

            let table_id = ctx
                .catalog_mut()
                .create_table(&name, catalog_columns, primary_key_ordinals)?;

            // Log to WAL
            let mut exec_ctx = ctx.execution_context();
            exec_ctx.log_dml(WalRecord::CreateTable {
                name: name.clone(),
                table: table_id,
            })?;

            Ok(format!("Created table '{}' (id = {}).", name, table_id.0))
        }
        Statement::DropTable { name } => {
            let table_id = ctx.catalog().table(&name)?.id;
            ctx.catalog_mut().drop_table(&name)?;

            // Log to WAL
            let mut exec_ctx = ctx.execution_context();
            exec_ctx.log_dml(WalRecord::DropTable { table: table_id })?;

            Ok(format!("Dropped table '{}'.", name))
        }
        Statement::CreateIndex {
            name,
            table,
            column,
        } => {
            ctx.catalog_mut()
                .create_index()
                .table_name(&table)
                .index_name(&name)
                .columns(&[column.as_str()])
                .kind(IndexKind::BTree)
                .call()?;

            Ok(format!("Created index '{}' on '{}'.", name, table))
        }
        Statement::DropIndex { name } => {
            let table_name = ctx
                .catalog()
                .tables()
                .find(|table| table.index(&name).is_ok())
                .map(|table| table.name.clone())
                .ok_or_else(|| {
                    common::DbError::Catalog(format!("index '{}' not found", name))
                })?;

            ctx.catalog_mut().drop_index(&table_name, &name)?;

            Ok(format!("Dropped index '{}' on '{}'.", name, table_name))
        }
        Statement::Explain { query, analyze } => {
            let mut planning_ctx = PlanningContext::new(ctx.catalog());
            let plan = Planner::plan(*query, &mut planning_ctx)?;

            let mut output = String::new();

            if analyze {
                // EXPLAIN ANALYZE: Execute the query and show statistics
                output.push_str("EXPLAIN ANALYZE:\n");
                output.push_str(&planner::explain_physical(&plan));
                output.push('\n');

                let mut exec_ctx = ctx.execution_context();
                let mut executor = build_executor(plan)?;
                executor.open(&mut exec_ctx)?;

                // Consume all rows to collect statistics
                let mut row_count = 0;
                while executor.next(&mut exec_ctx)?.is_some() {
                    row_count += 1;
                }
                executor.close(&mut exec_ctx)?;

                // Display statistics
                output.push_str("\nExecution Statistics:\n");
                output.push_str(&executor::format_explain_analyze(executor.as_ref(), "Query"));
                output.push_str(&format!("\nTotal rows: {}", row_count));
            } else {
                // EXPLAIN: Just show the plan without executing
                output.push_str("EXPLAIN:\n");
                output.push_str(&planner::explain_physical(&plan));
            }

            Ok(output)
        }
        other => {
            let mut planning_ctx = PlanningContext::new(ctx.catalog());
            let plan = Planner::plan(other, &mut planning_ctx)?;

            match plan {
                PhysicalPlan::Insert { .. }
                | PhysicalPlan::Update { .. }
                | PhysicalPlan::Delete { .. } => {
                    let mut exec_ctx = ctx.execution_context();
                    let count = execute_dml(plan, &mut exec_ctx)?;
                    Ok(format!("{} row(s) affected.", count))
                }
                ref query_plan => {
                    let schema = infer_schema(query_plan);
                    let mut exec_ctx = ctx.execution_context();
                    let rows = execute_query(plan, &mut exec_ctx)?;
                    let batch = RecordBatch { columns: schema, rows };
                    let rendered = pretty::render_record_batch(&batch, TableStyleKind::Modern);
                    Ok(rendered)
                }
            }
        }
    }
}

fn map_sql_type(raw: &str) -> DbResult<SqlType> {
    match raw.trim().to_uppercase().as_str() {
        "INT" | "INTEGER" => Ok(SqlType::Int),
        "TEXT" | "STRING" | "VARCHAR" => Ok(SqlType::Text),
        "BOOL" | "BOOLEAN" => Ok(SqlType::Bool),
        other => Err(common::DbError::Parser(format!(
            "unsupported SQL type '{}'",
            other
        ))),
    }
}

fn infer_schema(plan: &PhysicalPlan) -> Vec<String> {
    match plan {
        PhysicalPlan::SeqScan { schema, .. } => schema.clone(),
        PhysicalPlan::IndexScan { schema, .. } => schema.clone(),
        PhysicalPlan::Filter { input, .. } => infer_schema(input),
        PhysicalPlan::Project { columns, .. } => {
            columns.iter().map(|(name, _)| name.clone()).collect()
        }
        PhysicalPlan::Insert { .. } | PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. } => {
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_simple_query() {
        let output = run_sql_script(
            r#"
            CREATE TABLE users (id INT, name TEXT, age INT);
            INSERT INTO users VALUES (1, 'Alice', 30);
            INSERT INTO users VALUES (2, 'Bob', 25);
            SELECT * FROM users;
        "#,
        );

        assert!(output.is_ok());
        let output = output.unwrap();
        eprintln!("Output: {}", output);
        assert!(output.contains("Created table 'users'"));
        // Each INSERT produces "1 row(s) affected"
        assert!(output.contains("1 row(s) affected"));
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[test]
    fn test_run_query_with_filter() {
        let output = run_sql_script(
            r#"
            CREATE TABLE users (id INT, name TEXT, age INT);
            INSERT INTO users VALUES (1, 'Alice', 30);
            INSERT INTO users VALUES (2, 'Bob', 25);
            SELECT * FROM users WHERE age > 25;
        "#,
        );

        assert!(output.is_ok());
        let output = output.unwrap();
        assert!(output.contains("Alice"));
        assert!(!output.contains("Bob")); // Bob's age is 25, not > 25
    }

    #[test]
    fn test_run_script_with_error() {
        let output = run_sql_script(
            r#"
            CREATE TABLE users (id INT, name TEXT);
            SELECT * FROM nonexistent_table;
        "#,
        );

        assert!(output.is_ok());
        let output = output.unwrap();
        assert!(output.contains("Error"));
    }

    #[test]
    fn test_run_multiple_scripts_same_context() {
        let mut ctx = TestContext::new().unwrap();

        // First script creates table
        let output1 = run_sql_script_with_context(
            r#"
            CREATE TABLE users (id INT, name TEXT);
        "#,
            &mut ctx,
        );
        assert!(output1.is_ok());

        // Second script uses existing table
        let output2 = run_sql_script_with_context(
            r#"
            INSERT INTO users VALUES (1, 'Alice');
            SELECT * FROM users;
        "#,
            &mut ctx,
        );
        assert!(output2.is_ok());
        let output2 = output2.unwrap();
        assert!(output2.contains("Alice"));
    }
}
