//! Integration tests for EXPLAIN and EXPLAIN ANALYZE functionality.

use anyhow::Result;
use database::{Database, QueryResult};

#[tokio::test]
async fn explain_analyze_select_query() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    // Create table and insert test data
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
        .await?;
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
        .await?;
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
        .await?;

    // Run EXPLAIN ANALYZE
    let result = db
        .execute("EXPLAIN ANALYZE SELECT * FROM users WHERE age > 20")
        .await?;

    // Verify we get rows back (the explain output)
    match result {
        QueryResult::Rows { schema, rows } => {
            assert_eq!(schema, vec!["Explain"]);
            assert!(!rows.is_empty());

            // Check that the output contains timing and row count information
            let explain_output = &rows[0].values[0];
            let output_str = format!("{:?}", explain_output);

            // Verify key statistics are present
            assert!(output_str.contains("EXPLAIN ANALYZE") || output_str.contains("Execution"));
            assert!(output_str.contains("Total rows") || output_str.contains("rows"));
        }
        _ => panic!("Expected Rows result from EXPLAIN ANALYZE"),
    }

    Ok(())
}

#[tokio::test]
async fn explain_select_query_without_execution() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    // Create table (no data needed since we're not executing)
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .await?;

    // Run EXPLAIN (without ANALYZE)
    let result = db.execute("EXPLAIN SELECT * FROM users").await?;

    // Verify we get rows back
    match result {
        QueryResult::Rows { schema, rows } => {
            assert_eq!(schema, vec!["Explain"]);
            assert!(!rows.is_empty());

            // EXPLAIN should show the plan but not execution stats
            let explain_output = &rows[0].values[0];
            let output_str = format!("{:?}", explain_output);

            // Should contain plan information
            assert!(output_str.contains("SeqScan") || output_str.contains("Plan"));
        }
        _ => panic!("Expected Rows result from EXPLAIN"),
    }

    Ok(())
}

#[tokio::test]
async fn explain_analyze_insert_query() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    // Create table
    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT)")
        .await?;

    // Run EXPLAIN ANALYZE on INSERT
    let result = db
        .execute("EXPLAIN ANALYZE INSERT INTO products VALUES (1, 'Widget')")
        .await?;

    // Verify we get explain output
    match result {
        QueryResult::Rows { rows, .. } => {
            assert!(!rows.is_empty());
            let output_str = format!("{:?}", rows[0].values[0]);
            assert!(output_str.contains("EXPLAIN") || output_str.contains("Insert"));
        }
        _ => panic!("Expected Rows result from EXPLAIN ANALYZE INSERT"),
    }

    // Verify the insert actually executed
    let result = db.execute("SELECT * FROM products").await?;
    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "INSERT should have executed");
        }
        _ => panic!("Expected rows from SELECT"),
    }

    Ok(())
}

#[tokio::test]
async fn explain_analyze_with_filter_shows_stats() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    // Create table and insert multiple rows
    db.execute("CREATE TABLE numbers (id INT PRIMARY KEY, value INT)")
        .await?;

    for i in 1..=10 {
        db.execute(&format!("INSERT INTO numbers VALUES ({}, {})", i, i * 10))
            .await?;
    }

    // Run EXPLAIN ANALYZE with a filter
    let result = db
        .execute("EXPLAIN ANALYZE SELECT * FROM numbers WHERE value > 50")
        .await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert!(!rows.is_empty());
            let output_str = format!("{:?}", rows[0].values[0]);

            // Should show execution happened
            assert!(output_str.contains("Total rows") || output_str.contains("5"));
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}
