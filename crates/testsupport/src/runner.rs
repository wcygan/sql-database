//! SQL script execution for snapshot testing.
//!
//! Provides utilities to execute multi-statement SQL scripts and capture
//! pretty-printed output suitable for use with the `insta` snapshot testing
//! framework.

use crate::context::TestContext;
use common::{
    pretty::{self, TableStyleKind},
    DbResult, RecordBatch,
};
use database::{Database, QueryResult};

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
/// #[tokio::test]
/// async fn test_example() {
///     let output = run_sql_script(r#"
///         CREATE TABLE users (id INT, name TEXT, age INT);
///         INSERT INTO users VALUES (1, 'Alice', 30);
///         INSERT INTO users VALUES (2, 'Bob', 25);
///         SELECT * FROM users WHERE age > 25;
///     "#).await.unwrap();
///
///     insta::assert_snapshot!(output);
/// }
/// ```
pub async fn run_sql_script(sql: &str) -> DbResult<String> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10)
        .await
        .map_err(|e| common::DbError::Io(std::io::Error::other(e.to_string())))?;

    run_sql_script_with_db(sql, &db).await
}

/// Execute a SQL script using a specific database instance.
///
/// This is useful when you need a pre-configured catalog or want to
/// run multiple scripts against the same database state.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
/// use database::Database;
///
/// #[tokio::test]
/// async fn test_example() {
///     let temp_dir = tempfile::tempdir().unwrap();
///     let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await.unwrap();
///
///     // First script creates tables
///     let output1 = run_sql_script_with_db(r#"
///         CREATE TABLE users (id INT, name TEXT);
///     "#, &db).await.unwrap();
///
///     // Second script uses the existing table
///     let output2 = run_sql_script_with_db(r#"
///         INSERT INTO users VALUES (1, 'Alice');
///         SELECT * FROM users;
///     "#, &db).await.unwrap();
/// }
/// ```
pub async fn run_sql_script_with_db(sql: &str, db: &Database) -> DbResult<String> {
    let mut output = String::new();

    // Split into individual statements by semicolon
    let mut current_statement = String::new();
    for line in sql.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") {
            continue;
        }

        current_statement.push_str(line);
        current_statement.push(' ');

        // If line ends with semicolon, execute the statement
        if line.ends_with(';') {
            let stmt = current_statement.trim().trim_end_matches(';');
            if !stmt.is_empty() {
                match db.execute(stmt).await {
                    Ok(result) => {
                        let stmt_output = format_result(result, stmt);
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
            current_statement.clear();
        }
    }

    // Handle any remaining statement without semicolon
    let stmt = current_statement.trim();
    if !stmt.is_empty() {
        match db.execute(stmt).await {
            Ok(result) => {
                let stmt_output = format_result(result, stmt);
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

/// Format the query result for display.
fn format_result(result: QueryResult, stmt: &str) -> String {
    match result {
        QueryResult::Rows { schema, rows } => {
            let batch = RecordBatch { columns: schema, rows };
            pretty::render_record_batch(&batch, TableStyleKind::Modern)
        }
        QueryResult::Count { affected } => {
            format!("{} row(s) affected.", affected)
        }
        QueryResult::Empty => {
            // For DDL, try to infer what happened from the SQL
            let stmt_upper = stmt.trim().to_uppercase();
            if stmt_upper.starts_with("CREATE TABLE") {
                // Extract table name
                if let Some(name_start) = stmt_upper.find("TABLE") {
                    let rest = &stmt[name_start + 5..].trim();
                    if let Some(name_end) = rest.find(|c: char| c.is_whitespace() || c == '(') {
                        let name = &rest[..name_end].trim();
                        return format!("Created table '{}'.", name);
                    }
                }
                "Table created.".to_string()
            } else if stmt_upper.starts_with("DROP TABLE") {
                "Table dropped.".to_string()
            } else if stmt_upper.starts_with("CREATE INDEX") {
                "Index created.".to_string()
            } else if stmt_upper.starts_with("DROP INDEX") {
                "Index dropped.".to_string()
            } else {
                String::new()
            }
        }
    }
}

/// Execute a SQL script using a specific test context (deprecated).
///
/// This is kept for backwards compatibility. New code should use
/// `run_sql_script` or `run_sql_script_with_db` instead.
#[deprecated(note = "Use run_sql_script or run_sql_script_with_db instead")]
pub async fn run_sql_script_with_context(sql: &str, _ctx: &mut TestContext) -> DbResult<String> {
    // For now, create a temporary database
    run_sql_script(sql).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_run_simple_query() {
        let output = run_sql_script(
            r#"
            CREATE TABLE users (id INT, name TEXT, age INT);
            INSERT INTO users VALUES (1, 'Alice', 30);
            INSERT INTO users VALUES (2, 'Bob', 25);
            SELECT * FROM users;
        "#,
        )
        .await;

        assert!(output.is_ok());
        let output = output.unwrap();
        eprintln!("Output: {}", output);
        assert!(output.contains("Created table 'users'"));
        // Each INSERT produces "1 row(s) affected"
        assert!(output.contains("1 row(s) affected"));
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[tokio::test]
    async fn test_run_query_with_filter() {
        let output = run_sql_script(
            r#"
            CREATE TABLE users (id INT, name TEXT, age INT);
            INSERT INTO users VALUES (1, 'Alice', 30);
            INSERT INTO users VALUES (2, 'Bob', 25);
            SELECT * FROM users WHERE age > 25;
        "#,
        )
        .await;

        assert!(output.is_ok());
        let output = output.unwrap();
        assert!(output.contains("Alice"));
        assert!(!output.contains("Bob")); // Bob's age is 25, not > 25
    }

    #[tokio::test]
    async fn test_run_script_with_error() {
        let output = run_sql_script(
            r#"
            CREATE TABLE users (id INT, name TEXT);
            SELECT * FROM nonexistent_table;
        "#,
        )
        .await;

        assert!(output.is_ok());
        let output = output.unwrap();
        assert!(output.contains("Error"));
    }

    #[tokio::test]
    async fn test_run_multiple_scripts_same_context() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10)
            .await
            .unwrap();

        // First script creates table
        let output1 = run_sql_script_with_db(
            r#"
            CREATE TABLE users (id INT, name TEXT);
        "#,
            &db,
        )
        .await;
        assert!(output1.is_ok());

        // Second script uses existing table
        let output2 = run_sql_script_with_db(
            r#"
            INSERT INTO users VALUES (1, 'Alice');
            SELECT * FROM users;
        "#,
            &db,
        )
        .await;
        assert!(output2.is_ok());
        let output2 = output2.unwrap();
        assert!(output2.contains("Alice"));
    }
}
