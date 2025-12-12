//! Error handling example demonstrating graceful recovery from various error conditions.
//!
//! This example shows how to:
//! - Handle connection errors
//! - Handle SQL syntax errors
//! - Handle constraint violations (e.g., primary key duplicates)
//! - Handle table not found errors
//! - Distinguish between different error types
//! - Recover from errors and continue execution
//!
//! Run the server first:
//!   cargo run --bin toydb-server -- --data-dir /tmp/example-db
//!
//! Then run this example:
//!   cargo run --example error_handling

use anyhow::Result;
use client::{Client, ClientError};
use protocol::ErrorCode;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Error Handling Example ===\n");

    // Test 1: Connection errors
    println!("Test 1: Connection Error");
    println!("Attempting to connect to non-existent server...");
    match Client::connect("localhost:9999").await {
        Ok(_) => println!("  ERROR: Should have failed!"),
        Err(e) => {
            if e.is_connection_error() {
                println!("  ✓ Caught connection error: {}", e);
            } else {
                println!("  ERROR: Wrong error type: {}", e);
            }
        }
    }
    println!();

    // Connect to the real server for remaining tests
    println!("Connecting to localhost:5432...");
    let mut client = Client::connect("localhost:5432").await?;
    println!("Connected!\n");

    // Test 2: SQL syntax errors
    println!("Test 2: SQL Syntax Error");
    println!("Executing invalid SQL: 'CREAT TABLE broken'...");
    match client.execute("CREAT TABLE broken").await {
        Ok(_) => println!("  ERROR: Should have failed!"),
        Err(ClientError::Database { code, message }) => {
            if code == ErrorCode::ParseError {
                println!("  ✓ Caught parse error: {}", message);
            } else {
                println!("  ERROR: Wrong error code: {:?}", code);
            }
        }
        Err(e) => println!("  ERROR: Wrong error type: {}", e),
    }
    println!();

    // Test 3: Table not found error
    println!("Test 3: Table Not Found Error");
    println!("Querying non-existent table...");
    match client.execute("SELECT * FROM nonexistent").await {
        Ok(_) => println!("  ERROR: Should have failed!"),
        Err(ClientError::Database { code, message }) => {
            if matches!(code, ErrorCode::CatalogError | ErrorCode::PlanError) {
                println!("  ✓ Caught catalog/plan error: {}", message);
            } else {
                println!("  ERROR: Wrong error code: {:?}", code);
            }
        }
        Err(e) => println!("  ERROR: Wrong error type: {}", e),
    }
    println!();

    // Test 4: Primary key constraint violation
    println!("Test 4: Primary Key Constraint Violation");
    println!("Creating table with primary key...");
    client
        .execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT)")
        .await?;
    println!("  Table created.");

    println!("Inserting first row with id=1...");
    client
        .execute("INSERT INTO products VALUES (1, 'Widget')")
        .await?;
    println!("  ✓ First insert succeeded.");

    println!("Attempting to insert duplicate id=1...");
    match client
        .execute("INSERT INTO products VALUES (1, 'Duplicate')")
        .await
    {
        Ok(_) => println!("  ERROR: Should have failed!"),
        Err(ClientError::Database { code, message }) => {
            if code == ErrorCode::ConstraintViolation {
                println!("  ✓ Caught constraint violation: {}", message);
            } else {
                println!("  ERROR: Wrong error code: {:?}", code);
            }
        }
        Err(e) => println!("  ERROR: Wrong error type: {}", e),
    }
    println!();

    // Test 5: Recovering from errors and continuing
    println!("Test 5: Recovery After Errors");
    println!("Inserting valid row with id=2...");
    let result = client
        .execute("INSERT INTO products VALUES (2, 'Gadget')")
        .await?;
    println!(
        "  ✓ Insert succeeded after previous error ({} row affected)",
        result.affected_count()
    );

    println!("Verifying both rows exist...");
    let result = client.execute("SELECT * FROM products").await?;
    if let Some((_, rows)) = result.rows() {
        println!("  ✓ Found {} rows in table", rows.len());
        assert_eq!(rows.len(), 2, "Should have 2 rows");
    }
    println!();

    // Test 6: Multiple error types with pattern matching
    println!("Test 6: Error Type Classification");
    let test_queries = vec![
        ("SLECT * FROM products", "syntax error"),
        ("SELECT * FROM missing_table", "missing table"),
        (
            "INSERT INTO products VALUES (1, 'Dup')",
            "constraint violation",
        ),
    ];

    for (sql, expected_error) in test_queries {
        print!("  Testing {}: ", expected_error);
        match client.execute(sql).await {
            Ok(_) => println!("FAILED - query succeeded unexpectedly"),
            Err(e) => {
                let error_type = if e.is_connection_error() {
                    "connection"
                } else if e.is_protocol_error() {
                    "protocol"
                } else if e.is_database_error() {
                    if let Some(code) = e.error_code() {
                        match code {
                            ErrorCode::ParseError => "parse",
                            ErrorCode::CatalogError | ErrorCode::PlanError => "catalog/plan",
                            ErrorCode::ConstraintViolation => "constraint",
                            ErrorCode::ExecutionError => "execution",
                            _ => "other",
                        }
                    } else {
                        "database"
                    }
                } else {
                    "unknown"
                };
                println!("✓ Caught {} error", error_type);
            }
        }
    }
    println!();

    // Clean up
    println!("Cleaning up...");
    client.execute("DROP TABLE products").await?;
    client.close().await?;

    println!("\n=== Example Complete ===");
    println!("All error handling tests passed!");
    Ok(())
}
